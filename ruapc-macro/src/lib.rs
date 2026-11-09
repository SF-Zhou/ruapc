//! RPC service declarations, router registration, and client implementations.
//!
//! `#[service]` generates `ruapc_export` and implements the declared service trait
//! for `ruapc::Client` and `ruapc::ClientWithBuffers`. Both use the same method
//! signatures and return types as the service implementation. Buffered clients
//! are available only as references inside a buffer builder's `run` callback.
//!
//! ```rust,ignore
//! #[ruapc::service]
//! pub trait MyService {
//!     async fn my_method(&self, ctx: &Context, req: &Request) -> Result<Response>;
//! }
//! ```
//!
//! ## Names
//!
//! The wire name defaults to the Rust trait name. `name = "..."` overrides it:
//!
//! ```rust,ignore
//! #[ruapc::service(name = "Control")]
//! pub trait ControlService {
//!     async fn ping(&self, ctx: &Context, req: &()) -> Result<()>;
//! }
//! ```
//!
//! Names must be nonempty, have no leading or trailing whitespace, and exclude
//! `/`, the separator in wire method names such as `Control/ping`.
//! All registered methods are included in the router's method directory and
//! OpenAPI output and can be called through unary HTTP or framed peer RPC.
//! Handlers enforce any required transport or request context.
//!
//! ## Method contract
//!
//! Traits contain only async method declarations with `&self`, `&Context`,
//! and `&Request` arguments and an explicit result type. Generic parameters,
//! where clauses, default bodies, and unsafe traits or methods are unsupported.
//! Return types may be `Result<T, E>` or aliases, including `ruapc::Result<T>`.
//! The generated call traits enforce serialization, schema, and error bounds.
//! Custom errors must implement `From<ruapc::Error>`. Clients use this conversion
//! for local failures and marked framework error responses, such as overload,
//! request decoding failures, or handler panics. Handler-returned errors are
//! deserialized directly as `E`. Receiving framework errors as custom errors
//! requires a server that emits the framework error flag.
//!
//! Methods become `fn -> impl Future<Output = ...> + Send`. Implementations
//! may still use `async fn`; the compiler checks that their futures are `Send`.
//!
//! ## Calls with buffers
//!
//! Buffer attachments preserve the service's declared `Result<T, E>` type.
//! Handlers await `ctx.remote_write_all(&data)`, then return their
//! ordinary application response. Owning client adapters return the unchanged
//! result and optional recovered original buffers on either branch:
//!
//! ```rust,ignore
//! let completed = client.with_buffers(&sources, destinations)
//!     .run(async |rpc| rpc.download(&ctx, &req).await).await;
//! let response = completed.result?;
//! let destinations = completed.buffers.expect("buffers recovered");
//! ```
//!
//! Advanced borrowing is provided separately by `ruapc-unsafe`; ordinary core
//! clients remain cancellation-safe without adding unsafe code to generated calls.

mod args;
mod expand;
mod model;

use proc_macro::TokenStream;
use syn::parse_macro_input;

/// Define an RPC service and its router registration and client implementations.
///
/// `name = "..."` overrides the wire name. Methods must be declarations of the form
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
