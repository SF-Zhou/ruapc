use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio_util::sync::{CancellationToken, DropGuard, WaitForCancellationFuture};

/// Registration count and shutdown signals shared by owners, handles and guards.
#[derive(Debug, Default)]
struct TaskSupervisorState {
    /// Outstanding guards, including the internal stop observer.
    running: AtomicU64,
    /// Token cancelled when stop is requested.
    stop: CancellationToken,
    /// Token cancelled when the registration count reaches zero.
    stopped: CancellationToken,
}

/// Shutdown signal and guard-based accounting for registered tasks.
///
/// Hold a guard for each task whose exit must be tracked. Manually registered
/// tasks choose how to respond to [`stop`](Self::stop); tasks spawned through
/// [`TaskSupervisorHandle::try_spawn`] drop their future when shutdown wins
/// the select. Dropping the supervisor requests stop without waiting.
///
/// [`all_stopped`](Self::all_stopped) waits for these registrations to end.
/// Application RPC handlers are not registered with the server's supervisors.
///
/// # Examples
///
/// ```rust,no_run
/// # use ruapc::TaskSupervisor;
/// # #[tokio::main]
/// # async fn main() {
/// let supervisor = TaskSupervisor::create();
/// let guard = supervisor.try_start_async_task().unwrap();
/// tokio::spawn(async move {
///     guard.stopped().await;
///     // Finish task-specific cleanup before the guard drops.
/// });
/// supervisor.stop();
/// supervisor.all_stopped().await;
/// # }
/// ```
#[derive(Debug)]
pub struct TaskSupervisor(Arc<TaskSupervisorState>);

/// Cloneable, non-owning access to a [`TaskSupervisor`].
///
/// A handle can register child tasks and observe shutdown, but dropping it
/// never requests shutdown. The owning [`TaskSupervisor`] remains solely
/// responsible for the supervised lifecycle.
#[derive(Debug, Clone)]
pub struct TaskSupervisorHandle(Arc<TaskSupervisorState>);

/// Registration held for the lifetime of a tracked task.
///
/// Dropping it decrements the count and signals completion at zero. It does
/// not cancel the task; retain it until all work being tracked has finished.
#[derive(Debug)]
pub struct TaskSupervisorGuard(Arc<TaskSupervisorState>);

impl TaskSupervisorState {
    fn finish_async_task(&self) {
        let running = self.running.fetch_sub(1, Ordering::AcqRel) - 1;
        if running == 0 {
            self.stopped.cancel();
        }
    }
}

impl TaskSupervisor {
    /// Creates a supervisor inside a Tokio runtime.
    ///
    /// An internal task holds a guard until stop is requested, keeping
    /// [`all_stopped`](Self::all_stopped) pending before shutdown even when no
    /// other tasks are registered.
    #[must_use]
    pub fn create() -> Self {
        let supervisor = Self(Arc::default());

        let guard = supervisor.start_async_task();
        tokio::spawn(async move {
            guard.stopped().await;
        });

        supervisor
    }

    /// Signals shutdown and rejects subsequent task registrations.
    ///
    /// Returns immediately. Manually registered tasks must observe the signal
    /// or finish on their own; [`TaskSupervisorHandle::try_spawn`] races its
    /// future against this signal. Await [`all_stopped`](Self::all_stopped) to
    /// wait for outstanding guards to be released.
    pub fn stop(&self) {
        self.0.stop.cancel();
    }

    /// Returns a guard that requests stop on drop without waiting for tasks.
    #[must_use]
    pub fn drop_guard(&self) -> DropGuard {
        self.0.stop.clone().drop_guard()
    }

    /// Returns a cloneable, non-owning handle for `'static` task factories.
    #[must_use]
    pub fn handle(&self) -> TaskSupervisorHandle {
        TaskSupervisorHandle(self.0.clone())
    }

    /// Waits for the stop signal; this does not wait for task exit.
    pub fn stopped(&self) -> WaitForCancellationFuture<'_> {
        self.0.stop.cancelled()
    }

    /// Waits for all registration guards, including the internal stop observer,
    /// to be released after shutdown is requested.
    ///
    /// Only registered work is tracked. A forgotten guard keeps this pending.
    pub fn all_stopped(&self) -> WaitForCancellationFuture<'_> {
        self.0.stopped.cancelled()
    }

    /// Registers infrastructure during construction or under an existing task
    /// guard. These callers guarantee the task count cannot have reached zero;
    /// external callers must use the shutdown-aware `try_start_async_task`.
    #[must_use]
    pub(crate) fn start_async_task(&self) -> TaskSupervisorGuard {
        self.0.running.fetch_add(1, Ordering::AcqRel);
        TaskSupervisorGuard(self.0.clone())
    }

    /// Registers a task unless shutdown has begun. Hold the returned guard until
    /// the tracked work ends; registration alone does not cancel or spawn it.
    #[must_use]
    pub fn try_start_async_task(&self) -> Option<TaskSupervisorGuard> {
        try_start_async_task(&self.0)
    }
}

impl Drop for TaskSupervisor {
    fn drop(&mut self) {
        self.stop();
    }
}

impl TaskSupervisorHandle {
    /// Registers and spawns a future, racing it against the stop signal.
    ///
    /// When shutdown wins, the future is dropped without completing. Returns
    /// `None` and drops the unpolled future if registration observes shutdown.
    /// Requires a Tokio runtime when accepted.
    pub fn try_spawn<F>(&self, future: F) -> Option<tokio::task::JoinHandle<()>>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task = self.try_start_async_task()?;
        Some(tokio::spawn(async move {
            tokio::select! {
                () = task.stopped() => {}
                () = future => {}
            }
        }))
    }

    /// Registers a task with the same guard contract as
    /// [`TaskSupervisor::try_start_async_task`].
    #[must_use]
    pub fn try_start_async_task(&self) -> Option<TaskSupervisorGuard> {
        try_start_async_task(&self.0)
    }

    /// Waits for the stop signal, without waiting for registered tasks to exit.
    pub fn stopped(&self) -> WaitForCancellationFuture<'_> {
        self.0.stop.cancelled()
    }
}

fn try_start_async_task(state: &Arc<TaskSupervisorState>) -> Option<TaskSupervisorGuard> {
    // Register first so a concurrent shutdown cannot observe zero tasks
    // before this registration becomes visible.
    state.running.fetch_add(1, Ordering::AcqRel);
    let guard = TaskSupervisorGuard(state.clone());
    if state.stop.is_cancelled() {
        drop(guard);
        return None;
    }
    Some(guard)
}

impl TaskSupervisorGuard {
    /// Waits for the stop signal while retaining this task's registration.
    pub fn stopped(&self) -> WaitForCancellationFuture<'_> {
        self.0.stop.cancelled()
    }
}

impl Drop for TaskSupervisorGuard {
    fn drop(&mut self) {
        self.0.finish_async_task();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::*;

    #[tokio::test]
    async fn test_task_supervisor() {
        let task_supervisor = TaskSupervisor::create();
        assert_eq!(task_supervisor.0.running.load(Ordering::Acquire), 1);

        task_supervisor.stop();
        task_supervisor.stopped().await;
        task_supervisor.all_stopped().await;
        assert_eq!(task_supervisor.0.running.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn dropping_handle_does_not_stop_owner() {
        let supervisor = TaskSupervisor::create();
        drop(supervisor.handle());
        assert!(!supervisor.0.stop.is_cancelled());
        supervisor.stop();
        supervisor.all_stopped().await;
    }

    #[tokio::test]
    async fn handle_rejects_tasks_after_shutdown() {
        let supervisor = TaskSupervisor::create();
        let handle = supervisor.handle();
        supervisor.stop();
        supervisor.all_stopped().await;

        assert!(handle.try_start_async_task().is_none());
        assert_eq!(supervisor.0.running.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn try_spawn_is_cancelled_and_joined() {
        let supervisor = TaskSupervisor::create();
        let started = Arc::new(AtomicBool::new(false));
        let task_started = started.clone();
        let handle = supervisor
            .handle()
            .try_spawn(async move {
                task_started.store(true, Ordering::Release);
                std::future::pending::<()>().await;
            })
            .unwrap();
        tokio::task::yield_now().await;
        assert!(started.load(Ordering::Acquire));

        supervisor.stop();
        supervisor.all_stopped().await;
        handle.await.unwrap();
        assert_eq!(supervisor.0.running.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn try_spawn_rejects_future_after_shutdown() {
        let supervisor = TaskSupervisor::create();
        let polled = Arc::new(AtomicBool::new(false));
        supervisor.stop();
        supervisor.all_stopped().await;

        let task_polled = polled.clone();
        assert!(
            supervisor
                .handle()
                .try_spawn(async move {
                    task_polled.store(true, Ordering::Release);
                })
                .is_none()
        );
        assert!(!polled.load(Ordering::Acquire));
    }
}
