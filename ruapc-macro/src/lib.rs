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
//! ### Requirements
//!
//! Service methods must follow this signature:
//! - `async fn method_name(&self, ctx: &Context, req: &RequestType) -> Result<ResponseType>`
//! - Three parameters: `&self`, `&Context`, and a request reference
//! - Return type must be `Result<T>` where T is the response type
//!
//! ### `Result<WithBuffer<T>, E>` Return Type
//!
//! Declaring a method whose return type is `Result<WithBuffer<T>, E>` makes
//! the buffer transfer part of the method's contract on both sides. The
//! contract is recognized by the *type system* (trait dispatch inside
//! `ruapc`), not by this macro, so any type alias (e.g.
//! `ruapc::ResultWithBuffer<T>` or a user-defined alias fixing a custom
//! error type) works:
//!
//! ```rust,ignore
//! #[ruapc::service]
//! pub trait BlobService {
//!     async fn download(&self, ctx: &Context, req: &DownloadReq) -> Result<WithBuffer<()>>;
//! }
//!
//! // Server handler: `WithBuffer` can only be produced by a completed
//! // `ctx.remote_write` (via the returned `SentBuffer` witness). The push
//! // happens inside the handler — observable, impossible to forget. For
//! // code paths with no payload, `Buffer::empty` costs no memory:
//! async fn download(&self, ctx: &Context, req: &DownloadReq) -> Result<WithBuffer<()>> {
//!     let buf = /* fill a pool buffer, set_len, or Buffer::empty(...) */;
//!     let sent = ctx.remote_write(buf).await?;
//!     Ok(sent.reply(()))
//! }
//!
//! // Client receives the buffer as part of the same signature:
//! let (rsp, buffer) = client.download(&ctx, &req).await?.into_parts();
//! ```
//!
//! The response arrives without a pushed buffer → the client-side glue
//! materializes an empty buffer locally (the server must have used
//! `Buffer::empty` which causes `remote_write` to short-circuit without
//! touching the network).
//!
//! ### Generated Code
//!
//! The macro generates:
//! 1. A `ruapc_export` method for registering the service with a router
//! 2. Client trait implementations on `Client` and `ClientWithBuffer`, with
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
//! The generated `Client` / `ClientWithBuffer` impls also spell out the
//! `fn -> impl Future + Send` form instead of using `async fn`, so that
//! resolving a client call only consults signatures and never has to prove
//! the client bodies' futures `Send`. This keeps the `Send` proof acyclic
//! for transports whose connection setup recursively performs RPCs through
//! these client methods (e.g. the RDMA pool's bootstrap `info`/`connect`
//! calls), which would otherwise be rejected with a query cycle (E0391).

use proc_macro::TokenStream;
use quote::quote;
use syn::{FnArg, ItemTrait, ReturnType, TraitItem, parse_macro_input, parse_quote};

/// Procedural macro for defining RPC services.
///
/// This macro transforms a trait definition into a complete RPC service
/// with both client and server implementations.
///
/// # Panics
///
/// Panics at compile time if:
/// - Methods don't match the required signature
/// - Methods are named `ruapc_export` or `ruapc_request` (reserved names)
///
/// # Example
///
/// ```rust,ignore
/// #[service]
/// pub trait EchoService {
///     async fn echo(&self, ctx: &Context, req: &String) -> Result<String>;
/// }
/// ```
#[proc_macro_attribute]
pub fn service(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemTrait);

    let trait_ident = &input.ident;
    let visibility = input.vis;
    let trait_name = trait_ident.to_string();

    let mut trait_methods = vec![];
    let mut invoke_branchs = vec![];
    let mut client_methods = vec![];
    let mut client_with_buffer_methods = vec![];

    let krate = get_crate_name();

    let input_items = input.items;
    for item in &input_items {
        if let TraitItem::Fn(method) = item
            && method.sig.inputs.len() == 3
            && method.sig.asyncness.is_some()
            && let Some(receiver) = method.sig.receiver()
            && let FnArg::Typed(req_type) = &method.sig.inputs[2]
            && let ReturnType::Type(_, rsp_type) = &method.sig.output
        {
            let method_ident = &method.sig.ident;
            if *method_ident == "ruapc_export" || *method_ident == "ruapc_request" {
                panic!("the function cannot be named `ruapc_export` or `ruapc_request`!");
            }
            let method_name = format!("{trait_name}/{method_ident}");

            let req_type = req_type.ty.clone();

            // Desugar `async fn` into `fn -> impl Future + Send` (RPITIT) so
            // the trait works on stable Rust: the `Send` requirement becomes
            // part of the method signature instead of a nightly
            // `Self::method(..): Send` return-type-notation bound on
            // `ruapc_export`. Implementors still write plain `async fn` —
            // stable Rust accepts an `async fn` impl for an
            // `-> impl Future + Send` trait method and checks the returned
            // future for `Send` at the impl site. RPITIT captures all input
            // lifetimes automatically, so the reference parameters need no
            // explicit lifetime plumbing.
            let mut sig = method.sig.clone();
            sig.asyncness = None;
            sig.output = parse_quote! {
                -> impl ::core::future::Future<Output = #rsp_type> + Send
            };
            let attrs = &method.attrs;
            trait_methods.push(quote! { #(#attrs)* #sig; });

            // One uniform client body for every method. Whether the call
            // delivers a server-pushed buffer is decided by the *type
            // system* (see `ruapc::core::contract`), not by this macro:
            // `CallWithBuffer` applies iff the return type is
            // `Result<WithBuffer<T>, E>` (through any alias), `CallPlain`
            // otherwise. No name-based type detection is involved.
            //
            // Deliberately `fn -> impl Future + Send` (mirroring the trait
            // declaration) instead of `async fn`. With an `async fn` impl,
            // resolving a call to a client method runs the impl-vs-trait
            // signature comparison, which must prove the async body's
            // future is `Send` — and transports whose connection setup
            // recursively performs RPCs through these very methods (e.g.
            // the RDMA pool's info/connect bootstrap) would make that
            // proof cyclic (E0391). With the explicit signature, callers
            // discharge `Send` from the declared item bound alone and the
            // body is only type-checked once, acyclically.
            let client_body = quote! {
                fn #method_ident(#receiver, ctx: &#krate::Context, req: #req_type)
                    -> impl ::core::future::Future<Output = #rsp_type> + Send
                {
                    async move {
                        use #krate::{CallPlain as _, CallWithBuffer as _};
                        (&#krate::RpcCall::<#rsp_type>::new())
                            .ruapc_call(self, ctx, req, #method_name)
                            .await
                    }
                }
            };
            client_methods.push(client_body.clone());
            client_with_buffer_methods.push(client_body);

            // Server dispatch is uniform as well: `WithBuffer<T>` serializes
            // transparently as `T`, and the push already happened inside the
            // handler (the only way to construct a `WithBuffer` is through
            // a completed `Context::remote_write` + `SentBuffer::reply`).
            invoke_branchs.push(quote! {
                let this = self.clone();
                router.add_method::<#req_type, #rsp_type>(#method_name, Box::new(move |ctx, payload| {
                    let this = this.clone();
                    // `spawn_handler` applies the dispatch policies (load
                    // shedding, deadline enforcement, cancellation
                    // registration, metrics) around the handler future.
                    #krate::spawn_handler(ctx, #method_name, payload, move |mut ctx, payload| async move {
                        match payload.deserialize(&ctx.msg_meta) {
                            Ok(req) => {
                                // Convert handler panics into error responses;
                                // otherwise the client would hang until timeout.
                                match #krate::catch_handler_panic(this.#method_ident(&ctx, &req)).await {
                                    Ok(result) => ctx.send_rsp(result).await,
                                    Err(err) => ctx.send_err_rsp(err).await,
                                }
                            }
                            Err(err) => {
                                ctx.send_err_rsp(err).await;
                            }
                        }
                    });
                    Ok(())
                }));
            });
        } else {
            panic!(
                "the function should be in the form `async fn func(&self, ctx: &Context, req: &Req) -> Result<Rsp>`."
            );
        }
    }

    quote! {
        #visibility trait #trait_ident {
            const NAME: &'static str = #trait_name;

            #(#trait_methods)*

            fn ruapc_export(
                self: ::std::sync::Arc<Self>,
                router: &mut #krate::Router,
            )
            where
                Self: 'static + Send + Sync,
            {
                #(#invoke_branchs)*
            }
        }

        impl #trait_ident for #krate::Client {
            #(#client_methods)*
        }

        impl #trait_ident for #krate::ClientWithBuffer<'_> {
            #(#client_with_buffer_methods)*
        }
    }
    .into()
}

/// Gets the correct crate name for importing ruapc.
///
/// This function handles both cases:
/// - When ruapc-macro is used as a dependency (uses `::ruapc`)
/// - When ruapc-macro is in the same workspace (uses `crate`)
pub(crate) fn get_crate_name() -> proc_macro2::TokenStream {
    match proc_macro_crate::crate_name("ruapc") {
        Ok(proc_macro_crate::FoundCrate::Name(name)) => {
            let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote! { ::#ident }
        }
        _ => quote! { crate },
    }
}
