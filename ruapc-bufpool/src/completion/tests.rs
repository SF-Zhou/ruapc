use super::*;
use std::cell::Cell;
use std::sync::Weak;
use std::sync::atomic::AtomicUsize;
use std::task::Wake;
use std::thread;

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn poll<T>(receiver: &mut Receiver<T>, waker: &Waker) -> Poll<Result<T, Canceled>> {
    Pin::new(receiver).poll(&mut Context::from_waker(waker))
}

#[test]
fn send_before_poll_and_try_recv() {
    let (sender, mut receiver) = channel();
    assert_eq!(receiver.try_recv(), Ok(None));
    assert!(!sender.is_canceled());
    assert_eq!(sender.send(42), Ok(()));
    assert_eq!(poll(&mut receiver, Waker::noop()), Poll::Ready(Ok(42)));
    assert!(receiver.inner.is_none());
    assert_eq!(receiver.try_recv(), Err(Canceled));

    let (sender, mut receiver) = channel();
    assert_eq!(sender.send(73), Ok(()));
    assert_eq!(receiver.try_recv(), Ok(Some(73)));
    assert!(receiver.inner.is_none());
}

#[test]
fn pending_receiver_is_woken_by_cross_thread_send_or_producer_drop() {
    for send_value in [false, true] {
        let (sender, mut receiver) = channel();
        let wakes = Arc::new(WakeCount::default());
        let waker = Waker::from(Arc::clone(&wakes));
        assert_eq!(poll(&mut receiver, &waker), Poll::Pending);
        thread::spawn(move || {
            if send_value {
                assert_eq!(sender.send(42), Ok(()));
            } else {
                drop(sender);
            }
        })
        .join()
        .unwrap();
        assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
        assert_eq!(
            poll(&mut receiver, &waker),
            Poll::Ready(if send_value { Ok(42) } else { Err(Canceled) })
        );
    }
}

#[test]
fn concurrent_registration_cannot_miss_completion() {
    for send_value in [false, true] {
        for _ in 0..256 {
            let (sender, mut receiver) = channel();
            let wakes = Arc::new(WakeCount::default());
            let waker = Waker::from(Arc::clone(&wakes));
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let producer_barrier = Arc::clone(&barrier);
            let producer = thread::spawn(move || {
                producer_barrier.wait();
                if send_value {
                    assert_eq!(sender.send(42), Ok(()));
                } else {
                    drop(sender);
                }
            });
            barrier.wait();
            let first_poll = poll(&mut receiver, &waker);
            producer.join().unwrap();
            let expected = if send_value { Ok(42) } else { Err(Canceled) };
            if first_poll.is_pending() {
                assert!(wakes.0.load(Ordering::Relaxed) > 0);
                assert_eq!(poll(&mut receiver, &waker), Poll::Ready(expected));
            } else {
                assert_eq!(first_poll, Poll::Ready(expected));
            }
        }
    }
}

#[test]
fn completion_wakes_only_the_most_recent_waker() {
    let (sender, mut receiver) = channel();
    let old = Arc::new(WakeCount::default());
    let new = Arc::new(WakeCount::default());
    assert_eq!(
        poll(&mut receiver, &Waker::from(Arc::clone(&old))),
        Poll::Pending
    );
    assert_eq!(
        poll(&mut receiver, &Waker::from(Arc::clone(&new))),
        Poll::Pending
    );
    assert_eq!(sender.send(42), Ok(()));
    assert_eq!(old.0.load(Ordering::Relaxed), 0);
    assert_eq!(new.0.load(Ordering::Relaxed), 1);
    assert_eq!(receiver.try_recv(), Ok(Some(42)));
}

#[test]
fn close_wakes_pending_receiver_and_rejects_the_original_value() {
    let (sender, mut receiver) = channel::<Box<usize>>();
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(Arc::clone(&wakes));
    assert!(poll(&mut receiver, &waker).is_pending());
    receiver.close();
    assert!(sender.is_canceled());
    assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
    assert_eq!(receiver.try_recv(), Err(Canceled));
    let value = Box::new(42);
    let original = std::ptr::from_ref(value.as_ref());
    let returned = sender.send(value).unwrap_err();
    assert_eq!(std::ptr::from_ref(returned.as_ref()), original);
    assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
}

#[test]
fn close_retains_a_value_that_was_already_delivered() {
    let (sender, mut receiver) = channel();
    assert_eq!(sender.send(42), Ok(()));
    receiver.close();
    assert_eq!(receiver.try_recv(), Ok(Some(42)));
}

struct ReentrantWaker<T> {
    inner: Weak<Inner<T>>,
    wakes: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl<T> ReentrantWaker<T> {
    fn assert_unlocked(&self) {
        let inner = self.inner.upgrade().unwrap();
        assert!(inner.state.try_lock().is_ok(), "callback under state lock");
    }
}

impl<T: Send + 'static> Wake for ReentrantWaker<T> {
    fn wake(self: Arc<Self>) {
        self.assert_unlocked();
        self.wakes.fetch_add(1, Ordering::Relaxed);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.assert_unlocked();
        self.wakes.fetch_add(1, Ordering::Relaxed);
    }
}

impl<T> Drop for ReentrantWaker<T> {
    fn drop(&mut self) {
        self.assert_unlocked();
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn waker_callbacks_run_outside_the_state_lock() {
    for action in 0..5 {
        let (sender, mut receiver) = channel::<usize>();
        let inner = Arc::clone(&sender.inner);
        let wakes = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(ReentrantWaker {
            inner: Arc::downgrade(&inner),
            wakes: Arc::clone(&wakes),
            drops: Arc::clone(&drops),
        }));
        assert!(poll(&mut receiver, &waker).is_pending());
        drop(waker);
        match action {
            0 => {
                assert_eq!(sender.send(42), Ok(()));
                assert_eq!(receiver.try_recv(), Ok(Some(42)));
            }
            1 => drop(sender),
            2 => receiver.close(),
            3 => drop(receiver),
            4 => assert!(poll(&mut receiver, Waker::noop()).is_pending()),
            _ => unreachable!(),
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(wakes.load(Ordering::Relaxed), usize::from(action < 3));
    }
}

struct DropProbe {
    inner: Weak<Inner<DropProbe>>,
    drops: Arc<AtomicUsize>,
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        let inner = self.inner.upgrade().unwrap();
        assert!(
            inner.state.try_lock().is_ok(),
            "value dropped under state lock"
        );
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn dropping_an_unreceived_value_runs_outside_the_state_lock() {
    let (sender, receiver) = channel();
    let inner = Arc::clone(&sender.inner);
    let drops = Arc::new(AtomicUsize::new(0));
    assert!(
        sender
            .send(DropProbe {
                inner: Arc::downgrade(&inner),
                drops: Arc::clone(&drops),
            })
            .is_ok()
    );
    drop(receiver);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn concurrent_send_and_receiver_drop_destroy_the_value_once() {
    struct CountDrop(Arc<AtomicUsize>);

    impl Drop for CountDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    for _ in 0..512 {
        let (sender, mut receiver) = channel();
        let drops = Arc::new(AtomicUsize::new(0));
        let value = CountDrop(Arc::clone(&drops));
        assert!(poll(&mut receiver, Waker::noop()).is_pending());
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let producer_barrier = Arc::clone(&barrier);
        let producer = thread::spawn(move || {
            producer_barrier.wait();
            drop(sender.send(value));
        });
        barrier.wait();
        drop(receiver);
        producer.join().unwrap();
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn endpoints_preserve_send_sync_and_unpin_for_send_values() {
    fn assert_traits<T: Send + Sync + Unpin>() {}
    // Cell is Send but not Sync. The mutex must preserve endpoint Sync without
    // imposing an unnecessary Sync bound on the transported value.
    assert_traits::<Sender<Cell<usize>>>();
    assert_traits::<Receiver<Cell<usize>>>();
    assert_traits::<Receiver<std::marker::PhantomPinned>>();
}

#[test]
fn optional_sender_retains_pointer_sized_storage() {
    let pointer_size = std::mem::size_of::<usize>();
    assert_eq!(std::mem::size_of::<Sender<Cell<usize>>>(), pointer_size);
    assert_eq!(
        std::mem::size_of::<Option<Sender<Cell<usize>>>>(),
        pointer_size
    );
}

#[test]
fn synchronous_wake_can_consume_while_the_producer_still_owns_the_state() {
    struct ConsumeOnWake {
        receiver: Mutex<Option<Receiver<usize>>>,
        result: Mutex<Option<Result<Option<usize>, Canceled>>>,
    }

    impl Wake for ConsumeOnWake {
        fn wake(self: Arc<Self>) {
            let mut receiver = self.receiver.lock().unwrap().take().unwrap();
            *self.result.lock().unwrap() = Some(receiver.try_recv());
        }
    }

    for send_value in [false, true] {
        let (sender, receiver) = channel();
        let task = Arc::new(ConsumeOnWake {
            receiver: Mutex::new(Some(receiver)),
            result: Mutex::new(None),
        });
        let waker = Waker::from(Arc::clone(&task));
        assert!(poll(task.receiver.lock().unwrap().as_mut().unwrap(), &waker).is_pending());
        if send_value {
            assert_eq!(sender.send(42), Ok(()));
        } else {
            drop(sender);
        }
        assert_eq!(
            task.result.lock().unwrap().take(),
            Some(if send_value {
                Ok(Some(42))
            } else {
                Err(Canceled)
            })
        );
    }
}

#[test]
fn panicking_wake_preserves_the_published_outcome() {
    struct PanicOnWake;

    impl Wake for PanicOnWake {
        fn wake(self: Arc<Self>) {
            panic!("intentional wake panic");
        }
    }

    let (sender, mut receiver) = channel();
    let waker = Waker::from(Arc::new(PanicOnWake));
    assert!(poll(&mut receiver, &waker).is_pending());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sender.send(42))).is_err());
    assert_eq!(receiver.try_recv(), Ok(Some(42)));

    let (sender, mut receiver) = channel::<usize>();
    assert!(poll(&mut receiver, &waker).is_pending());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| receiver.close())).is_err());
    assert!(sender.is_canceled());
    assert_eq!(sender.send(42), Err(42));
    assert_eq!(receiver.try_recv(), Err(Canceled));
}
