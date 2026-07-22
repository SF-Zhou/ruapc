#![deny(unsafe_code)]
#![feature(return_type_notation)]

pub use ruapc_macro::service;

mod error;
pub use error::{Error, ErrorKind, RemoteIoError, Result};

mod msg;
pub use msg::{Message, MsgFlags, MsgMeta, Payload};

mod core;
pub use core::{
    AddrSet, Client, ClientWithBuffer, Context, Listener, MethodInfo, ResultWithBuffer, Router,
    SentBuffer, Server, SocketEndpoint, State, WithBuffer,
};
#[doc(hidden)]
pub use core::{CallPlain, CallWithBuffer, RawCall, RpcCall, catch_handler_panic, spawn_handler};

mod metrics;
pub(crate) use metrics::{MethodMetrics, Metrics};

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
#[cfg(feature = "rdma")]
pub use rdma::{
    NicSelector, RdmaConnDirection, RdmaDeviceLoad, RdmaNicInfo, RdmaPathEntry, RdmaPathInfo,
    RdmaPathReport, RdmaPathSelector,
};
