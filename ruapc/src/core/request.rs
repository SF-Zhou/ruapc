use crate::Buffer;

/// RPC request wrapper that optionally carries a registered memory buffer.
///
/// `Request` is the standard way to pass request data to RPC methods.
/// It supports two modes:
///
/// - `Normal(&T)`: A standard request containing only serializable data.
/// - `WithBuffer(&T, &Buffer)`: A request that additionally carries a reference
///   to a registered memory buffer. The buffer's `RemoteBufferInfo` will be
///   automatically attached to the message metadata during serialization, so the
///   server can perform `remote_read` on the client's buffer.
///
/// # Lifetime
///
/// The lifetime parameter `'a` ties the request data and buffer to their
/// owning scope. This guarantees at compile time that the buffer remains
/// alive for the entire duration of the RPC call (across `.await` points),
/// preventing use-after-free in RDMA and TCP remote memory operations.
///
/// # Examples
///
/// ```rust,ignore
/// // Normal request (no buffer)
/// let rsp = client.echo(&ctx, Request::Normal(&my_req)).await?;
///
/// // Request with buffer
/// let buf = pool.allocate()?;
/// buf[..data.len()].copy_from_slice(data);
/// let rsp = client.upload(&ctx, Request::WithBuffer(&my_req, &buf)).await?;
/// // buf is guaranteed alive until here
/// ```
pub enum Request<'a, T> {
    /// A standard request with no attached buffer.
    Normal(&'a T),
    /// A request with an attached registered memory buffer.
    /// The buffer's remote info will be included in the message metadata.
    WithBuffer(&'a T, &'a Buffer),
}

impl<T> Request<'_, T> {
    /// Returns a reference to the inner request payload.
    pub fn inner(&self) -> &T {
        match self {
            Request::Normal(t) => t,
            Request::WithBuffer(t, _) => t,
        }
    }

    /// Returns the attached buffer, if any.
    pub fn buffer(&self) -> Option<&Buffer> {
        match self {
            Request::Normal(_) => None,
            Request::WithBuffer(_, buf) => Some(buf),
        }
    }
}

impl<T> std::ops::Deref for Request<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.inner()
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Request<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Request::Normal(t) => f.debug_tuple("Request::Normal").field(t).finish(),
            Request::WithBuffer(t, buf) => f
                .debug_struct("Request::WithBuffer")
                .field("inner", t)
                .field("buffer", buf)
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_normal_inner() {
        let val = 42u64;
        let req = Request::Normal(&val);
        assert_eq!(*req.inner(), 42);
        assert!(req.buffer().is_none());
        let debug = format!("{:?}", req);
        assert!(debug.contains("Normal"));
    }

    #[test]
    fn test_request_normal_deref() {
        let val = String::from("hello");
        let req = Request::Normal(&val);
        // Deref to &String
        assert_eq!(req.len(), 5);
    }

    #[test]
    fn test_request_with_buffer() {
        use crate::{BufferPool, Devices};
        use std::sync::Arc;
        let devices = Arc::new(Devices::default());
        let pool = BufferPool::new(devices, 4096, 4096, 0);
        let buf = pool.allocate().unwrap();
        let val = 42u64;
        let req = Request::WithBuffer(&val, &buf);
        assert_eq!(*req.inner(), 42);
        assert!(req.buffer().is_some());
        let debug = format!("{:?}", req);
        assert!(debug.contains("WithBuffer"));
    }
}
