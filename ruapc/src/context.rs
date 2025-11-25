use std::{net::SocketAddr, sync::Arc};

use serde::Serialize;
use tokio_util::sync::DropGuard;

use crate::{
    Error, Result, Router, Socket, SocketPoolConfig, State,
    msg::{MsgFlags, MsgMeta},
};

/// Socket endpoint information for RPC contexts.
///
/// Represents the connection endpoint for an RPC operation, which can be:
/// - Invalid: No endpoint specified
/// - Connected: An existing socket connection
/// - Address: A socket address to connect to
#[derive(Clone, Debug, Default)]
pub enum SocketEndpoint {
    /// No valid endpoint (default state).
    #[default]
    Invalid,
    /// An established socket connection.
    Connected(Socket),
    /// A socket address to establish a connection to.
    Address(SocketAddr),
}

/// RPC context carrying request metadata and connection information.
///
/// The `Context` is passed to all RPC service methods and contains:
/// - Shared state (router, socket pool, etc.)
/// - Connection endpoint information
/// - Lifecycle management through drop guards
///
/// # Examples
///
/// Creating a client context:
///
/// ```rust,no_run
/// # use ruapc::{Context, SocketPoolConfig};
/// # use std::{net::SocketAddr, str::FromStr};
/// let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
/// let addr = SocketAddr::from_str("127.0.0.1:8000").unwrap();
/// let ctx = ctx.with_addr(addr);
/// ```
#[derive(Clone)]
pub struct Context {
    pub(crate) drop_guard: Option<Arc<DropGuard>>,
    /// Shared state containing router and socket pool.
    pub state: Arc<State>,
    pub(crate) endpoint: SocketEndpoint,
}

impl Context {
    /// Creates a new context with the given socket pool configuration.
    ///
    /// This creates a context with a default router (containing only MetaService).
    ///
    /// # Arguments
    ///
    /// * `config` - Socket pool configuration
    ///
    /// # Errors
    ///
    /// Returns an error if state creation fails.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use ruapc::{Context, SocketPoolConfig};
    /// let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
    /// ```
    pub fn create(config: &SocketPoolConfig) -> Result<Self> {
        Self::create_with_router(Router::default(), config)
    }

    /// Creates a new context with a custom router and configuration.
    ///
    /// Use this when you need to create a client context with custom services registered.
    ///
    /// # Arguments
    ///
    /// * `router` - Custom router with registered services
    /// * `config` - Socket pool configuration
    ///
    /// # Errors
    ///
    /// Returns an error if state creation fails.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use ruapc::{Context, Router, SocketPoolConfig};
    /// let router = Router::default();
    /// let ctx = Context::create_with_router(router, &SocketPoolConfig::default()).unwrap();
    /// ```
    pub fn create_with_router(router: Router, config: &SocketPoolConfig) -> Result<Self> {
        let (state, drop_guard) = State::create(router, config)?;
        Ok(Self {
            state,
            endpoint: SocketEndpoint::Invalid,
            drop_guard: Some(Arc::new(drop_guard)),
        })
    }

    /// Creates a context with a specific state and address.
    ///
    /// Internal method used by RDMA implementation.
    #[cfg(feature = "rdma")]
    pub(crate) fn create_with_state_and_addr(state: &Arc<State>, addr: &SocketAddr) -> Self {
        Self {
            state: state.clone(),
            endpoint: SocketEndpoint::Address(*addr),
            drop_guard: None,
        }
    }

    /// Creates a new context with the specified target address.
    ///
    /// This method clones the current context and sets a new target address,
    /// useful for making requests to a specific server.
    ///
    /// # Arguments
    ///
    /// * `addr` - The target socket address
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use ruapc::{Context, SocketPoolConfig};
    /// # use std::{net::SocketAddr, str::FromStr};
    /// # let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
    /// let addr = SocketAddr::from_str("127.0.0.1:8000").unwrap();
    /// let client_ctx = ctx.with_addr(addr);
    /// ```
    #[must_use]
    pub fn with_addr(&self, addr: SocketAddr) -> Self {
        Self {
            state: self.state.clone(),
            endpoint: SocketEndpoint::Address(addr),
            drop_guard: self.drop_guard.clone(),
        }
    }

    /// Creates a server-side context with an established socket connection.
    ///
    /// Internal method used by the server to create contexts for incoming requests.
    #[must_use]
    pub(crate) fn server_ctx(state: &Arc<State>, socket: Socket) -> Self {
        Self {
            state: state.clone(),
            endpoint: SocketEndpoint::Connected(socket),
            drop_guard: None,
        }
    }

    /// Sends an RPC response back to the client.
    ///
    /// This method is called by service implementations to return results.
    ///
    /// # Type Parameters
    ///
    /// * `Rsp` - The response type
    /// * `E` - The error type
    ///
    /// # Arguments
    ///
    /// * `meta` - Message metadata from the original request
    /// * `rsp` - The result to send back (Ok or Err)
    pub async fn send_rsp<Rsp, E>(&mut self, mut meta: MsgMeta, rsp: std::result::Result<Rsp, E>)
    where
        Rsp: Serialize,
        E: std::error::Error + From<Error> + Serialize,
    {
        meta.flags.remove(MsgFlags::IsReq);
        meta.flags.insert(MsgFlags::IsRsp);
        match &mut self.endpoint {
            SocketEndpoint::Connected(socket) => {
                let _ = socket.send(&mut meta, &rsp, &self.state).await;
            }
            _ => {
                tracing::error!("invalid argument: send rsp without connected socket");
            }
        }
    }

    /// Sends an error response back to the client.
    ///
    /// Convenience method for sending error responses.
    ///
    /// # Arguments
    ///
    /// * `meta` - Message metadata from the original request
    /// * `err` - The error to send back
    pub async fn send_err_rsp(&mut self, meta: MsgMeta, err: Error) {
        self.send_rsp::<(), Error>(meta, Err(err)).await;
    }
}
