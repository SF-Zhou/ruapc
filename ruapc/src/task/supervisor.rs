use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio_util::sync::{CancellationToken, DropGuard, WaitForCancellationFuture};

/// Internal state for task supervision.
///
/// Tracks the number of running tasks and provides cancellation tokens
/// for coordinating shutdown.
#[derive(Debug, Default)]
struct TaskSupervisorState {
    /// Number of currently running tasks.
    running: AtomicU64,
    /// Token cancelled when stop is requested.
    stop: CancellationToken,
    /// Token cancelled when all tasks have stopped.
    stopped: CancellationToken,
}

/// Task lifecycle supervisor for graceful shutdown.
///
/// The `TaskSupervisor` tracks spawned async tasks and provides mechanisms
/// for graceful shutdown. It ensures all tasks complete before allowing
/// the server to fully stop.
///
/// # Examples
///
/// ```rust,no_run
/// # use ruapc::TaskSupervisor;
/// # #[tokio::main]
/// # async fn main() {
/// let supervisor = TaskSupervisor::create();
/// let guard = supervisor.start_async_task();
/// tokio::spawn(async move {
///     // Task work here
///     drop(guard); // Automatically decrements running count
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

/// RAII guard for tracking individual async tasks.
///
/// When dropped, automatically decrements the running task count
/// and signals completion if this was the last task.
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
    /// Creates a new task supervisor.
    ///
    /// Automatically starts with one guard task that monitors for completion.
    ///
    /// # Returns
    ///
    /// Returns a new `TaskSupervisor` instance.
    #[must_use]
    pub fn create() -> Self {
        let supervisor = Self(Arc::default());

        let guard = supervisor.start_async_task();
        tokio::spawn(async move {
            guard.stopped().await;
        });

        supervisor
    }

    /// Requests all tasks to stop.
    ///
    /// Cancels the stop token, signaling all tasks to gracefully shutdown.
    pub fn stop(&self) {
        self.0.stop.cancel();
    }

    /// Creates a drop guard for this supervisor.
    ///
    /// The returned guard will call `stop()` when dropped.
    ///
    /// # Returns
    ///
    /// Returns a `DropGuard` that calls `stop()` on drop.
    #[must_use]
    pub fn drop_guard(&self) -> DropGuard {
        self.0.stop.clone().drop_guard()
    }

    /// Returns a cloneable, non-owning handle for `'static` task factories.
    #[must_use]
    pub fn handle(&self) -> TaskSupervisorHandle {
        TaskSupervisorHandle(self.0.clone())
    }

    /// Returns a future that completes when stop is requested.
    ///
    /// # Returns
    ///
    /// Returns a future that resolves when `stop()` is called.
    pub fn stopped(&self) -> WaitForCancellationFuture<'_> {
        self.0.stop.cancelled()
    }

    /// Returns a future that completes when all tasks have stopped.
    ///
    /// # Returns
    ///
    /// Returns a future that resolves when the running task count reaches zero.
    pub fn all_stopped(&self) -> WaitForCancellationFuture<'_> {
        self.0.stopped.cancelled()
    }

    /// Starts tracking a new async task.
    ///
    /// Increments the running task counter and returns a guard that will
    /// automatically decrement it when dropped.
    ///
    /// # Returns
    ///
    /// Returns a `TaskSupervisorGuard` that must be kept alive for the
    /// duration of the task.
    #[must_use]
    pub fn start_async_task(&self) -> TaskSupervisorGuard {
        self.0.running.fetch_add(1, Ordering::AcqRel);
        TaskSupervisorGuard(self.0.clone())
    }

    /// Starts tracking a task unless shutdown has already begun.
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
    /// Spawns a task tracked by this supervisor, or rejects it after stop.
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

    /// Starts tracking a task unless shutdown has already begun.
    #[must_use]
    pub fn try_start_async_task(&self) -> Option<TaskSupervisorGuard> {
        try_start_async_task(&self.0)
    }

    /// Starts tracking a new async task.
    #[must_use]
    pub fn start_async_task(&self) -> TaskSupervisorGuard {
        self.0.running.fetch_add(1, Ordering::AcqRel);
        TaskSupervisorGuard(self.0.clone())
    }

    /// Returns a future that completes when the owner requests shutdown.
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
    /// Returns a future that completes when stop is requested.
    ///
    /// # Returns
    ///
    /// Returns a future that resolves when the supervisor's `stop()` is called.
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
