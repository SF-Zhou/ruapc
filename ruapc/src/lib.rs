#![forbid(unsafe_code)]
#![feature(return_type_notation)]

pub use ruapc_macro::service;

mod error;
pub use error::{Error, ErrorKind, Result};

mod msg;
pub use msg::{MsgFlags, MsgMeta, RecvMsg};

mod socket;
pub use socket::Socket;

mod socket_pool;
pub use socket_pool::SocketPool;

mod context;
pub use context::{Context, SocketEndpoint};

mod router;
pub use router::{Method, Router};

mod client;
pub use client::{Client, ClientConfig};

mod server;
pub use server::Server;
