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

#[cfg(test)]
mod tests;

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
