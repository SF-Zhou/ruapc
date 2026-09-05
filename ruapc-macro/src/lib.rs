//! # RuaPC Procedural Macros
//!
//! This crate provides procedural macros for the RuaPC RPC library.
//!
//! ## `#[service]` Macro
//!
//! The `#[service]` macro is used to define RPC service traits. It generates:
//! - Server-side dispatch code for handling requests
//! - Client-side implementation for making requests
//! - Method registration with the router
//!
//! ### Example
//!
//! ```rust,ignore
//! #[ruapc::service]
//! pub trait MyService {
//!     async fn my_method(&self, ctx: &Context, req: &Request) -> Result<Response>;
//! }
//! ```
//!
//! The service name defaults to the Rust trait name. Use `name = "..."` to
//! choose a stable wire name independently of the Rust API, and use
//! `internal` for control-plane services that must remain dispatchable but
//! hidden from public discovery and OpenAPI output:
//!
//! ```rust,ignore
//! #[ruapc::service(name = "Control", internal)]
//! pub trait ControlService {
//!     async fn ping(&self, ctx: &Context, req: &()) -> Result<()>;
//! }
//! ```
//!
//! A configured name must be non-empty, have no leading or trailing
//! whitespace, and cannot contain `/`, which is reserved as the separator in
//! wire method names such as `Control/ping`.
//!
//! ### Requirements
//!
//! Service methods must follow this signature:
//! - `async fn method_name(&self, ctx: &Context, req: &RequestType) -> Result<ResponseType>`
//! - Three parameters: `&self`, `&Context`, and a request reference
//! - Return type must be `Result<T>` where T is the response type
//!
//! ### `Result<WithBuffers<T>, E>` Return Type
//!
//! Declaring a method whose return type is `Result<WithBuffers<T>, E>`
//! makes the out-of-band buffer transfer part of the method's contract on
//! both sides. The contract is recognized by the *type system* (trait
//! dispatch inside `ruapc`), not by this macro, so any type alias (e.g.
//! `ruapc::ResultWithBuffers<T>` or a user-defined alias fixing a custom
//! error type) works:
//!
//! ```rust,ignore
//! #[ruapc::service]
//! pub trait BlobService {
//!     async fn download(&self, ctx: &Context, req: &DownloadReq) -> Result<WithBuffers<()>>;
//! }
//!
//! // Server handler: `WithBuffers` can only be produced by a completed
//! // `ctx.remote_write` (via the returned `SentBuffers` witness). The
//! // transfer happens inside the handler — observable, impossible to
//! // forget. For code paths with no payload, `ctx.sent_nothing()` costs
//! // nothing:
//! async fn download(&self, ctx: &Context, req: &DownloadReq) -> Result<WithBuffers<()>> {
//!     let bufs = /* fill pool buffers, set_len */;
//!     let sent = ctx.remote_write_all(bufs).await?;
//!     Ok(sent.reply(()))
//! }
//!
//! // Client provides the destination buffers and receives them all back
//! // as part of the same signature:
//! let (rsp, buffers) = client
//!     .with_write_buffers(bufs)
//!     .download(&ctx, &req)
//!     .await?
//!     .into_parts();
//! ```
//!
//! ### Generated Code
//!
//! The macro generates:
//! 1. A `ruapc_export` method for registering the service with a router
//! 2. Client trait implementations on `Client` and `ClientWithBuffers`, with
//!    one uniform body per method; plain vs. buffer-carrying calls are
//!    dispatched by return type through `ruapc`'s call glue traits
//! 3. Proper error handling and message serialization
//!
//! Each `async fn` in the trait is desugared to
//! `fn -> impl Future<Output = ...> + Send` (return-position `impl Trait`
//! in traits, stable since Rust 1.75), which makes the `Send` requirement
//! part of every method signature without nightly-only
//! `return_type_notation` bounds. Implementations keep writing plain
//! `async fn`; the compiler verifies at the impl site that the returned
//! future is `Send`.
//!
//! The generated `Client` / `ClientWithBuffers` impls also spell out the
//! `fn -> impl Future + Send` form instead of using `async fn`, so that
//! resolving a client call only consults signatures and never has to prove
//! the client bodies' futures `Send`. This keeps the `Send` proof acyclic
//! for transports whose connection setup recursively performs RPCs through
//! these client methods (e.g. the RDMA pool's `discover`/`prepare_connection`
//! calls), which would otherwise be rejected with a query cycle (E0391).

mod args;
mod expand;
mod model;

use proc_macro::TokenStream;
use syn::parse_macro_input;

/// Define an RPC service and its router registration and client implementations.
///
/// `name = "..."` overrides the wire name; `internal` hides the service from
/// public discovery and OpenAPI output. Methods must be declarations of the form
/// `async fn method(&self, ctx: &Context, req: &Request) -> ResponseResult`.
/// The return type may be an alias; RuaPC's call traits enforce its contract.
/// Unsupported items and signatures produce diagnostics at their source spans.
#[proc_macro_attribute]
pub fn service(attr: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as args::ServiceArgs);
    let declaration = parse_macro_input!(input as syn::ItemTrait);

    match model::Service::parse(args, declaration).and_then(expand::service) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}
