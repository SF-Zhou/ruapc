use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use tokio_util::sync::{CancellationToken, DropGuard, WaitForCancellationFuture};

#[derive(Debug, Default)]
struct TaskSupervisorState {
    running: AtomicU64,
    stop: CancellationToken,
    stopped: CancellationToken,
}

#[derive(Debug)]
pub struct TaskSupervisor(Arc<TaskSupervisorState>);

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
    #[must_use]
    pub fn create() -> Self {
        let supervisor = Self(Arc::default());

        let guard = supervisor.start_async_task();
        tokio::spawn(async move {
            guard.stopped().await;
        });

        supervisor
    }

    pub fn stop(&self) {
        self.0.stop.cancel();
    }

    #[must_use]
    pub fn drop_guard(&self) -> DropGuard {
        self.0.stop.clone().drop_guard()
    }

    pub fn stopped(&self) -> WaitForCancellationFuture<'_> {
        self.0.stop.cancelled()
    }

    pub fn all_stopped(&self) -> WaitForCancellationFuture<'_> {
        self.0.stopped.cancelled()
    }

    #[must_use]
    pub fn start_async_task(&self) -> TaskSupervisorGuard {
        self.0.running.fetch_add(1, Ordering::AcqRel);
        TaskSupervisorGuard(self.0.clone())
    }
}

impl Drop for TaskSupervisor {
    fn drop(&mut self) {
        self.stop();
    }
}

impl TaskSupervisorGuard {
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
}
