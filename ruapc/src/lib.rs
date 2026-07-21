#![deny(unsafe_code)]
#![feature(return_type_notation)]

pub use ruapc_macro::service;

mod error;
pub use error::{Error, ErrorKind, RemoteIoError, Result};

mod msg;
pub use msg::{Message, MsgFlags, MsgMeta, Payload};

mod core;
#[doc(hidden)]
pub use core::{CallPlain, CallWithBuffer, RawCall, RpcCall};
pub use core::{
    Client, ClientWithBuffer, Context, Listener, MethodInfo, ResultWithBuffer, Router, SentBuffer,
    Server, SocketEndpoint, State, WithBuffer,
};

mod task;
pub(crate) use task::Receiver;
pub use task::{TaskSupervisor, Waiter, WaiterCleaner};

mod devices;
pub use devices::{Buffer, BufferPool, Devices};

mod sockets;
pub use sockets::*;

pub mod services;

pub use ruapc_bufpool::Device as _;
pub use ruapc_bufpool::Devices as _;

#[cfg(feature = "rdma")]
mod rdma;
