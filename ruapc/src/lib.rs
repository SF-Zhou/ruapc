#![forbid(unsafe_code)]
#![feature(return_type_notation)]

pub use ruapc_macro::service;

mod error;
pub use error::{Error, ErrorKind, Result};

mod payload;
pub use payload::Payload;

mod msg;
pub use msg::{Message, MsgFlags, MsgMeta};

mod router;
pub use router::{Method, Router};

mod waiter;
pub use waiter::Waiter;

mod task_supervisor;
pub use task_supervisor::TaskSupervisor;

mod receiver;
use receiver::Receiver;

mod socket;
pub use socket::Socket;

mod socket_pool;
pub use socket_pool::{SocketPool, SocketPoolConfig, SocketType};

mod http;
mod tcp;
mod ws;

mod state;
pub use state::State;

pub mod services;

mod context;
pub use context::{Context, SocketEndpoint};

mod listener;
pub use listener::Listener;

mod client;
pub use client::{Client, ClientConfig};

mod server;
pub use server::Server;
