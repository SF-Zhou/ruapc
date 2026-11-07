use std::any::Any;

use crate::{Error, ErrorKind};

/// Runs a service handler future, converting a panic into an [`Error`].
///
/// Used by the common handler execution boundary: without this, a
/// panicking handler would be silently swallowed by `tokio::spawn` and the
/// client would hang until its request timed out. With it, the client
/// receives an [`ErrorKind::HandlerPanic`] error response immediately.
///
pub(crate) fn catch_handler_panic<F>(
    fut: F,
) -> impl Future<Output = std::result::Result<F::Output, Error>>
where
    F: Future,
{
    use futures_util::FutureExt as _;

    // `AssertUnwindSafe` is appropriate here: the future is dropped on
    // panic, and the surrounding context only holds `Arc`s and channel
    // handles whose invariants do not depend on the handler completing.
    // Return the adapter directly: an async wrapper would store the large
    // handler future both as an initial capture and across its await.
    std::panic::AssertUnwindSafe(fut)
        .catch_unwind()
        .map(|result| result.map_err(handler_panic))
}

pub(crate) fn handler_panic(panic: Box<dyn Any + Send>) -> Error {
    let msg = if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    };
    tracing::error!("service handler panicked: {msg}");
    Error::new(
        ErrorKind::HandlerPanic,
        format!("service handler panicked: {msg}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_catch_handler_panic_ok() {
        let result = catch_handler_panic(async { 42 }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_catch_handler_panic_str() {
        let result = catch_handler_panic(async { panic!("boom") }).await;
        let err = result.unwrap_err();
        assert_eq!(err.kind, ErrorKind::HandlerPanic);
        assert!(err.msg.contains("boom"));
    }

    #[tokio::test]
    async fn test_catch_handler_panic_string() {
        let value = 7;
        let result = catch_handler_panic(async move {
            // Panic containment must remain active after an earlier Pending.
            tokio::task::yield_now().await;
            assert_eq!(value, 8, "value mismatch");
        })
        .await;
        let err = result.unwrap_err();
        assert_eq!(err.kind, ErrorKind::HandlerPanic);
        assert!(err.msg.contains("value mismatch"));
    }
}
