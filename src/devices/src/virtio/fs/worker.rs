#[cfg(any(target_os = "macos", target_os = "windows"))]
use crossbeam_channel::Sender;
#[cfg(any(target_os = "macos", target_os = "windows"))]
use utils::worker_message::WorkerMessage;

#[cfg(unix)]
use std::os::fd::{AsRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawHandle, RawHandle};
use std::sync::atomic::AtomicI32;
use std::sync::Arc;
use std::thread;

use utils::epoll::{ControlOperation, Epoll, EpollEvent, EventSet};
use utils::eventfd::EventFd;
use vm_memory::GuestMemoryMmap;

use super::super::{FsError, Queue};
use super::defs::{HPQ_INDEX, REQ_INDEX};
use super::descriptor_utils::{Reader, Writer};
use super::filesystem::FileSystem;
use super::server::Server;
use crate::virtio::{InterruptTransport, VirtioShmRegion};

#[cfg(unix)]
type Pollable = RawFd;
#[cfg(windows)]
type Pollable = RawHandle;

const HPQ_EVENT: u64 = 0;
const REQ_EVENT: u64 = 1;
const STOP_EVENT: u64 = 2;

pub struct FsWorker<F: FileSystem + Sync + 'static> {
    queues: Vec<Queue>,
    queue_evts: Vec<Arc<EventFd>>,
    interrupt: InterruptTransport,
    mem: GuestMemoryMmap,
    shm_region: Option<VirtioShmRegion>,
    server: Server<F>,
    stop_fd: EventFd,
    exit_code: Arc<AtomicI32>,
    #[cfg(any(target_os = "macos", target_os = "windows"))]
    map_sender: Option<Sender<WorkerMessage>>,
}

/// Queue ownership returned after the worker reaches a reversible stop boundary.
pub struct FsWorkerState {
    pub queues: Vec<Queue>,
    pub queue_evts: Vec<Arc<EventFd>>,
    /// Negotiated FUSE session options retained across worker reconstruction.
    pub session_options: u64,
}

impl<F: FileSystem + Sync + Send + 'static> FsWorker<F> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        fs: Arc<F>,
        queues: Vec<Queue>,
        queue_evts: Vec<Arc<EventFd>>,
        interrupt: InterruptTransport,
        mem: GuestMemoryMmap,
        shm_region: Option<VirtioShmRegion>,
        stop_fd: EventFd,
        exit_code: Arc<AtomicI32>,
        #[cfg(any(target_os = "macos", target_os = "windows"))] map_sender: Option<
            Sender<WorkerMessage>,
        >,
        session_options: u64,
    ) -> Self {
        Self {
            queues,
            queue_evts,
            interrupt,
            mem,
            shm_region,
            server: Server::new(fs, session_options),
            stop_fd,
            exit_code,
            #[cfg(any(target_os = "macos", target_os = "windows"))]
            map_sender,
        }
    }

    pub fn run(self) -> thread::JoinHandle<FsWorkerState> {
        thread::Builder::new()
            .name("fs worker".into())
            .spawn(|| self.work())
            .unwrap()
    }

    fn work(mut self) -> FsWorkerState {
        let virtq_hpq_ev = eventfd_pollable(&self.queue_evts[HPQ_INDEX]);
        let virtq_req_ev = eventfd_pollable(&self.queue_evts[REQ_INDEX]);
        let stop_ev = eventfd_pollable(&self.stop_fd);

        let epoll = Epoll::new().unwrap();

        let _ = epoll.ctl(
            ControlOperation::Add,
            virtq_hpq_ev,
            &EpollEvent::new(EventSet::IN, HPQ_EVENT),
        );
        let _ = epoll.ctl(
            ControlOperation::Add,
            virtq_req_ev,
            &EpollEvent::new(EventSet::IN, REQ_EVENT),
        );
        let _ = epoll.ctl(
            ControlOperation::Add,
            stop_ev,
            &EpollEvent::new(EventSet::IN, STOP_EVENT),
        );

        'work: loop {
            let mut epoll_events = vec![EpollEvent::new(EventSet::empty(), 0); 32];
            match epoll.wait(epoll_events.len(), -1, epoll_events.as_mut_slice()) {
                Ok(ev_cnt) => {
                    // Stop wins over queue readiness from the same wait. This bounds quiescence
                    // by a request already owned by the worker rather than the guest's queued
                    // backlog.
                    if epoll_events[..ev_cnt].iter().any(|event| {
                        event.data() == STOP_EVENT && event.event_set().contains(EventSet::IN)
                    }) {
                        debug!("stopping worker thread");
                        let _ = self.stop_fd.read();
                        break;
                    }
                    for event in &epoll_events[0..ev_cnt] {
                        let source = event.data();
                        let event_set = event.event_set();
                        match source {
                            HPQ_EVENT if event_set.contains(EventSet::IN) => {
                                if self.handle_event(HPQ_INDEX) {
                                    break 'work;
                                }
                            }
                            REQ_EVENT if event_set.contains(EventSet::IN) => {
                                if self.handle_event(REQ_INDEX) {
                                    break 'work;
                                }
                            }
                            STOP_EVENT if event_set.contains(EventSet::IN) => {
                                unreachable!("stop events are handled before queue events");
                            }
                            _ => {
                                log::warn!(
                                    "Received unknown event: {event_set:?} from fd: {source:?}"
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    debug!("failed to consume muxer epoll event: {e}");
                }
            }
        }

        FsWorkerState {
            queues: self.queues,
            queue_evts: self.queue_evts,
            session_options: self.server.session_options(),
        }
    }

    fn handle_event(&mut self, queue_index: usize) -> bool {
        debug!("Fs: queue event: {queue_index}");
        if let Err(e) = self.queue_evts[queue_index].read() {
            error!("Failed to get queue event: {e:?}");
        }

        loop {
            self.queues[queue_index]
                .disable_notification(&self.mem)
                .unwrap();

            if self.process_queue(queue_index) {
                // Re-enable guest notification before returning ownership. The queue cursors are
                // captured by the transport after the worker joins.
                let _ = self.queues[queue_index].enable_notification(&self.mem);
                // The stop event may already be consumed. Exit the worker instead of
                // waiting for another event that reset/quiesce will never send.
                return true;
            }

            if !self.queues[queue_index]
                .enable_notification(&self.mem)
                .unwrap()
            {
                break;
            }
        }
        false
    }

    fn process_queue(&mut self, queue_index: usize) -> bool {
        let queue = &mut self.queues[queue_index];
        loop {
            // A FUSE operation is synchronous once consumed. Between descriptors, observe stop
            // before taking ownership of more guest work.
            match self.stop_fd.read() {
                Ok(_) => return true,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => {
                    error!("failed to read virtio-fs stop event: {error}");
                    return true;
                }
            }
            let Some(head) = queue.pop(&self.mem) else {
                return false;
            };
            let reader = Reader::new(&self.mem, head.clone())
                .map_err(FsError::QueueReader)
                .unwrap();
            let writer = Writer::new(&self.mem, head.clone())
                .map_err(FsError::QueueWriter)
                .unwrap();

            if let Err(e) = self.server.handle_message(
                reader,
                writer,
                &self.shm_region,
                &self.exit_code,
                #[cfg(any(target_os = "macos", target_os = "windows"))]
                &self.map_sender,
            ) {
                error!("error handling message: {e:?}");
            }

            if let Err(e) = queue.add_used(&self.mem, head.index, 0) {
                error!("failed to add used elements to the queue: {e:?}");
            }

            if queue.needs_notification(&self.mem).unwrap() {
                self.interrupt.signal_used_queue();
            }
        }
    }
}

#[cfg(unix)]
fn eventfd_pollable(event: &EventFd) -> Pollable {
    event.as_raw_fd()
}

#[cfg(windows)]
fn eventfd_pollable(event: &EventFd) -> Pollable {
    event.as_raw_handle()
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::sync::Mutex;
    use std::time::Duration;

    use utils::eventfd::EFD_NONBLOCK;
    use virtio_bindings::virtio_ring::{VRING_DESC_F_NEXT, VRING_DESC_F_WRITE};
    use vm_memory::{Bytes, GuestAddress};

    use super::*;
    use crate::legacy::DummyIrqChip;
    use crate::virtio::fs::fuse::{InHeader, Opcode, OutHeader};
    use crate::virtio::queue::tests::VirtQueue;

    const WAIT: Duration = Duration::from_secs(5);
    const SESSION_OPTIONS: u64 = 0x1234;

    // Hold a real FUSE request inside the server, after the worker's epoll dispatch.
    struct BlockingFs {
        started: Sender<u64>,
        proceed: Mutex<Receiver<()>>,
    }

    impl FileSystem for BlockingFs {
        type Inode = u64;
        type Handle = u64;

        fn request_error(&self, inode: u64) -> Option<i32> {
            self.started.send(inode).unwrap();
            self.proceed.lock().unwrap().recv_timeout(WAIT).unwrap();
            Some(5) // Linux EIO: the request still receives a complete FUSE reply.
        }
    }

    struct Fixture<'a> {
        rings: Vec<VirtQueue<'a>>,
        worker: FsWorker<BlockingFs>,
        started: Receiver<u64>,
        proceed: Sender<()>,
    }

    fn fixture(mem: &GuestMemoryMmap, queue_index: usize, event_idx: bool) -> Fixture<'_> {
        let (started_tx, started) = channel();
        let (proceed, proceed_rx) = channel();
        let rings = vec![
            VirtQueue::new(GuestAddress(0), mem, 8),
            VirtQueue::new(GuestAddress(0x1000), mem, 8),
        ];
        let mut queues = rings
            .iter()
            .map(VirtQueue::create_queue)
            .collect::<Vec<_>>();
        queues[queue_index].set_event_idx(event_idx);
        for request in 0..2u16 {
            let address = 0x2000 + u64::from(request) * 0x1000;
            let header = InHeader {
                len: size_of::<InHeader>() as u32,
                opcode: Opcode::Getattr as u32,
                unique: u64::from(request) + 1,
                nodeid: u64::from(request) + 1,
                ..Default::default()
            };
            mem.write_obj(header, GuestAddress(address)).unwrap();
            let ring = &rings[queue_index];
            ring.dtable[usize::from(request) * 2].set(
                address,
                header.len,
                VRING_DESC_F_NEXT as u16,
                request * 2 + 1,
            );
            ring.dtable[usize::from(request) * 2 + 1].set(
                address + 0x100,
                128,
                VRING_DESC_F_WRITE as u16,
                0,
            );
            ring.avail.ring[usize::from(request)].set(request * 2);
        }
        rings[queue_index].avail.idx.set(2);
        Fixture {
            rings,
            worker: FsWorker::new(
                Arc::new(BlockingFs {
                    started: started_tx,
                    proceed: Mutex::new(proceed_rx),
                }),
                queues,
                (0..2)
                    .map(|_| Arc::new(EventFd::new(EFD_NONBLOCK).unwrap()))
                    .collect(),
                InterruptTransport::new(DummyIrqChip::new().into(), "fs-worker-test".into())
                    .unwrap(),
                mem.clone(),
                None,
                EventFd::new(EFD_NONBLOCK).unwrap(),
                Arc::new(AtomicI32::new(0)),
                #[cfg(any(target_os = "macos", target_os = "windows"))]
                None,
                SESSION_OPTIONS,
            ),
            started,
            proceed,
        }
    }

    #[test]
    fn stop_consumed_during_request_exits_worker_and_preserves_queued_request() {
        for queue_index in [HPQ_INDEX, REQ_INDEX] {
            for event_idx in [false, true] {
                let mem = GuestMemoryMmap::from_ranges(&[(GuestAddress(0), 0x10000)]).unwrap();
                let fixture = fixture(&mem, queue_index, event_idx);
                let stop = fixture.worker.stop_fd.try_clone().unwrap();
                fixture.worker.queue_evts[queue_index].write(1).unwrap();
                let (done_tx, done_rx) = channel();
                let worker = thread::spawn(move || done_tx.send(fixture.worker.work()).unwrap());

                // No stop is pending at epoll dispatch. Only process_queue can consume it
                // after this in-flight request completes; the second request stays queued.
                assert_eq!(fixture.started.recv_timeout(WAIT).unwrap(), 1);
                stop.write(1).unwrap();
                fixture.proceed.send(()).unwrap();
                let state = done_rx
                    .recv_timeout(WAIT)
                    .expect("worker lost the consumed stop");
                worker.join().unwrap();
                assert_eq!(state.session_options, SESSION_OPTIONS);
                assert_eq!(state.queue_evts.len(), 2);
                let queue = &state.queues[queue_index];
                let captured = queue.capture_state().unwrap();
                assert_eq!(captured.next_avail, 1);
                assert_eq!(captured.next_used, 1);
                assert_eq!(queue.len(&mem), 1);
                assert_eq!(captured.event_idx_enabled, event_idx);
                if event_idx {
                    assert_eq!(fixture.rings[queue_index].used.event.get(), 1);
                } else {
                    assert_eq!(fixture.rings[queue_index].used.flags.get(), 0);
                }
                let reply: OutHeader = mem.read_obj(GuestAddress(0x2100)).unwrap();
                assert_eq!(reply.unique, 1);
                assert_eq!(reply.error, -5);
                assert!(matches!(
                    fixture.started.try_recv(),
                    Err(std::sync::mpsc::TryRecvError::Disconnected)
                ));
            }
        }
    }

    #[test]
    fn ordinary_queue_event_completes_requests_and_reenables_notifications() {
        for queue_index in [HPQ_INDEX, REQ_INDEX] {
            for event_idx in [false, true] {
                let mem = GuestMemoryMmap::from_ranges(&[(GuestAddress(0), 0x10000)]).unwrap();
                let mut fixture = fixture(&mem, queue_index, event_idx);
                fixture.worker.queue_evts[queue_index].write(1).unwrap();
                let worker = thread::spawn(move || {
                    assert!(!fixture.worker.handle_event(queue_index));
                    fixture.worker
                });
                for inode in [1, 2] {
                    assert_eq!(fixture.started.recv_timeout(WAIT).unwrap(), inode);
                    fixture.proceed.send(()).unwrap();
                }
                let worker = worker.join().unwrap();
                let queue = &worker.queues[queue_index];
                let captured = queue.capture_state().unwrap();
                assert_eq!(captured.next_avail, 2);
                assert_eq!(captured.next_used, 2);
                assert_eq!(queue.len(&mem), 0);
                assert_eq!(worker.server.session_options(), SESSION_OPTIONS);
                if event_idx {
                    assert_eq!(fixture.rings[queue_index].used.event.get(), 2);
                } else {
                    assert_eq!(fixture.rings[queue_index].used.flags.get(), 0);
                }
                for request in 0..2u64 {
                    let reply: OutHeader = mem
                        .read_obj(GuestAddress(0x2100 + request * 0x1000))
                        .unwrap();
                    assert_eq!(reply.unique, request + 1);
                    assert_eq!(reply.error, -5);
                }
            }
        }
    }

    #[test]
    fn pending_stop_wins_over_queued_requests() {
        let mem = GuestMemoryMmap::from_ranges(&[(GuestAddress(0), 0x10000)]).unwrap();
        let fixture = fixture(&mem, REQ_INDEX, false);
        fixture.worker.queue_evts[REQ_INDEX].write(1).unwrap();
        fixture.worker.stop_fd.write(1).unwrap();
        let state = fixture.worker.work();
        assert_eq!(state.session_options, SESSION_OPTIONS);
        let captured = state.queues[REQ_INDEX].capture_state().unwrap();
        assert_eq!(captured.next_avail, 0);
        assert_eq!(captured.next_used, 0);
        assert_eq!(state.queues[REQ_INDEX].len(&mem), 2);
        assert!(matches!(
            fixture.started.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Disconnected)
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn stop_read_error_propagates_without_consuming_requests() {
        use std::fs::OpenOptions;
        use std::os::fd::{FromRawFd, IntoRawFd};

        let mem = GuestMemoryMmap::from_ranges(&[(GuestAddress(0), 0x10000)]).unwrap();
        let mut fixture = fixture(&mem, REQ_INDEX, false);
        // A real write-only descriptor produces EBADF rather than WouldBlock.
        // Transfer ownership so the invalid event handle is closed exactly once.
        fixture.worker.stop_fd = unsafe {
            EventFd::from_raw_fd(
                OpenOptions::new()
                    .write(true)
                    .open("/dev/null")
                    .unwrap()
                    .into_raw_fd(),
            )
        };
        assert_ne!(
            fixture.worker.stop_fd.read().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        fixture.worker.queue_evts[REQ_INDEX].write(1).unwrap();
        assert!(fixture.worker.handle_event(REQ_INDEX));
        let captured = fixture.worker.queues[REQ_INDEX].capture_state().unwrap();
        assert_eq!(captured.next_avail, 0);
        assert_eq!(captured.next_used, 0);
        assert_eq!(fixture.rings[REQ_INDEX].used.flags.get(), 0);
        assert!(matches!(
            fixture.started.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
    }
}
