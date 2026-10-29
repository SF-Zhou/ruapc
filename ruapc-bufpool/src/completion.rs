//! A single-value completion shared by buffer allocation and READ ownership.
//! This hidden module is workspace implementation support, not an application
//! channel API. It does not grant memory recovery or RDMA completion authority.
//!
//! The mutex decides delivery versus cancellation. Atomic flags only provide
//! cheap observations before taking that mutex. All user callbacks, including
//! waker cloning and destruction, run after releasing the state lock.

#![forbid(unsafe_code)]

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// Creates one producer and one receiver for a single value.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        state: Mutex::new(State {
            outcome: Outcome::Pending,
            waker: None,
        }),
        complete: AtomicBool::new(false),
        receiver_closed: AtomicBool::new(false),
    });
    (
        Sender {
            inner: Arc::clone(&inner),
        },
        Receiver { inner: Some(inner) },
    )
}

struct Inner<T> {
    state: Mutex<State<T>>,
    complete: AtomicBool,
    receiver_closed: AtomicBool,
}

struct State<T> {
    outcome: Outcome<T>,
    waker: Option<Waker>,
}

enum Outcome<T> {
    Pending,
    Value(T),
    Closed,
}

/// The unique producer of a completion value.
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Sender<T> {
    /// Delivers a value, or returns it if the receiver has already closed.
    ///
    /// A successful send means that delivery won the race with cancellation;
    /// the receiver can still be dropped before consuming the value.
    pub fn send(self, value: T) -> Result<(), T> {
        let inner = &self.inner;
        let waker = {
            let mut state = inner.state.lock().unwrap();
            if inner.receiver_closed.load(Ordering::Relaxed) {
                return Err(value);
            }
            // The sender is unique, and receiver closure is checked under the
            // same mutex. No other operation can have published an outcome.
            debug_assert!(matches!(state.outcome, Outcome::Pending));
            state.outcome = Outcome::Value(value);
            inner.complete.store(true, Ordering::Release);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    /// Returns whether the receiver has closed or been dropped.
    ///
    /// A `true` result guarantees that a subsequent send returns its value.
    pub fn is_canceled(&self) -> bool {
        self.inner.receiver_closed.load(Ordering::Acquire)
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("is_canceled", &self.is_canceled())
            .finish_non_exhaustive()
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // Sending or receiver closure already published the final state. In
        // either case there is no producer disappearance left to announce.
        // Keeping the Arc directly also preserves Option<Sender>'s null niche.
        let inner = &self.inner;
        if inner.complete.load(Ordering::Acquire) {
            return;
        }
        let waker = {
            let mut state = inner.state.lock().unwrap();
            if matches!(state.outcome, Outcome::Pending) {
                state.outcome = Outcome::Closed;
                inner.complete.store(true, Ordering::Release);
            }
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Receives the value without requiring an asynchronous runtime.
pub struct Receiver<T> {
    inner: Option<Arc<Inner<T>>>,
}

impl<T> Receiver<T> {
    /// Takes an available value, returns `None` while pending, or reports
    /// closure when no value can arrive.
    pub fn try_recv(&mut self) -> Result<Option<T>, Canceled> {
        let Some(inner) = self.inner.as_ref() else {
            return Err(Canceled);
        };
        if !inner.complete.load(Ordering::Acquire) {
            return Ok(None);
        }
        let inner = self.inner.take().expect("ready receiver has an inner");
        let (outcome, waker) = match Arc::try_unwrap(inner) {
            // Once the producer has returned, the receiver normally owns the
            // allocation exclusively. Consume its mutex without locking it;
            // the Arc operation also replaces the final reference decrement.
            Ok(inner) => {
                let State { outcome, waker } = inner.state.into_inner().unwrap();
                (outcome, waker)
            }
            // Completion is published before the producer wakes the task, so
            // the producer may still hold its reference. The same state mutex
            // handles that race without requiring it to finish first.
            Err(inner) => {
                let mut state = inner.state.lock().unwrap();
                (
                    std::mem::replace(&mut state.outcome, Outcome::Closed),
                    state.waker.take(),
                )
            }
        };
        self.finish_receive(outcome, waker).map(Some)
    }

    /// Prevents future sends while retaining a value already delivered.
    pub fn close(&mut self) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        let waker = {
            let mut state = inner.state.lock().unwrap();
            inner.receiver_closed.store(true, Ordering::Release);
            if matches!(state.outcome, Outcome::Pending) {
                state.outcome = Outcome::Closed;
                inner.complete.store(true, Ordering::Release);
            }
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn finish_receive(&mut self, outcome: Outcome<T>, waker: Option<Waker>) -> Result<T, Canceled> {
        // Both the saved waker and the last shared allocation can run user
        // destructors. The caller has already released the mutex.
        let inner = self.inner.take();
        drop(waker);
        drop(inner);
        match outcome {
            Outcome::Value(value) => Ok(value),
            Outcome::Closed => Err(Canceled),
            Outcome::Pending => unreachable!("completion flag without an outcome"),
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, Canceled>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.try_recv() {
            Ok(Some(value)) => return Poll::Ready(Ok(value)),
            Err(error) => return Poll::Ready(Err(error)),
            Ok(None) => {}
        }

        // Cloning a custom waker can call user code, including the producer.
        // Recheck the outcome under the mutex after cloning to avoid missing a
        // completion that raced with this registration.
        let mut new_waker = Some(cx.waker().clone());
        let inner = this.inner.as_ref().expect("pending receiver has an inner");
        let (outcome, old_waker) = {
            let mut state = inner.state.lock().unwrap();
            if matches!(state.outcome, Outcome::Pending) {
                (None, std::mem::replace(&mut state.waker, new_waker.take()))
            } else {
                (
                    Some(std::mem::replace(&mut state.outcome, Outcome::Closed)),
                    state.waker.take(),
                )
            }
        };
        drop(new_waker);
        match outcome {
            Some(outcome) => Poll::Ready(this.finish_receive(outcome, old_waker)),
            None => {
                drop(old_waker);
                Poll::Pending
            }
        }
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let complete = self
            .inner
            .as_ref()
            .is_none_or(|inner| inner.complete.load(Ordering::Acquire));
        f.debug_struct("Receiver")
            .field("complete", &complete)
            .finish_non_exhaustive()
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let (outcome, waker) = {
            let mut state = inner.state.lock().unwrap();
            inner.receiver_closed.store(true, Ordering::Release);
            inner.complete.store(true, Ordering::Release);
            (
                std::mem::replace(&mut state.outcome, Outcome::Closed),
                state.waker.take(),
            )
        };
        drop(waker);
        drop(outcome);
    }
}

/// The producer disappeared or the receiver closed without an available value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Canceled;

impl fmt::Display for Canceled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("completion channel closed")
    }
}

impl std::error::Error for Canceled {}

#[cfg(test)]
mod tests {
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
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sender.send(42))).is_err()
        );
        assert_eq!(receiver.try_recv(), Ok(Some(42)));

        let (sender, mut receiver) = channel::<usize>();
        assert!(poll(&mut receiver, &waker).is_pending());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| receiver.close())).is_err()
        );
        assert!(sender.is_canceled());
        assert_eq!(sender.send(42), Err(42));
        assert_eq!(receiver.try_recv(), Err(Canceled));
    }
}
