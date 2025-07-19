#![forbid(unsafe_code)]
#![feature(return_type_notation)]

pub use ruapc_macro::service;

mod error;
pub use error::{Error, ErrorKind, Result};

mod msg;
pub use msg::{MsgFlags, MsgMeta, RecvMsg};

mod router;
pub use router::{Method, Router};

mod waiter;
pub use waiter::Waiter;

mod receiver;
use receiver::Receiver;

mod socket;
pub use socket::Socket;

mod socket_pool;
pub use socket_pool::{SocketPool, SocketPoolConfig, SocketType};

mod tcp;
mod ws;

mod state;
pub use state::State;

mod context;
pub use context::{Context, SocketEndpoint};

mod client;
pub use client::{Client, ClientConfig};

mod server;
pub use server::Server;
