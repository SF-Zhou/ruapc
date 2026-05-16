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
//! ### Generated Code
//!
//! The macro generates:
//! 1. A `ruapc_export` method for registering the service with a router
//! 2. Client trait implementation on `Client` for making requests
//! 3. A `WithBuffer` extension trait with `_with_buffer` methods for each
//!    trait method, allowing callers to attach registered memory buffers
//! 4. Proper error handling and message serialization

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{FnArg, ItemTrait, ReturnType, TraitItem, parse_macro_input};

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

    let mut send_bounds = vec![];
    let mut invoke_branchs = vec![];
    let mut client_methods = vec![];
    let mut with_buffer_trait_methods = vec![];
    let mut with_buffer_impl_methods = vec![];

    let krate = get_crate_name();

    // Generate the WithBuffer trait name: e.g., EchoServiceWithBuffer
    let with_buffer_trait_ident = format_ident!("{}WithBuffer", trait_ident);

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
            let output = &method.sig.output;

            // Client trait impl: wraps &req in Request::Normal
            client_methods.push(quote! {
                async fn #method_ident(#receiver, ctx: &#krate::Context, req: #req_type) #output {
                    self.ruapc_request(ctx, #krate::Request::Normal(req), #method_name).await
                }
            });

            // Generate _with_buffer method name
            let with_buffer_ident = format_ident!("{}_with_buffer", method_ident);

            // WithBuffer trait method declaration
            with_buffer_trait_methods.push(quote! {
                async fn #with_buffer_ident(#receiver, ctx: &#krate::Context, req: #req_type, buffer: &#krate::Buffer) #output;
            });

            // WithBuffer trait implementation for Client
            with_buffer_impl_methods.push(quote! {
                async fn #with_buffer_ident(#receiver, ctx: &#krate::Context, req: #req_type, buffer: &#krate::Buffer) #output {
                    self.ruapc_request(ctx, #krate::Request::WithBuffer(req, buffer), #method_name).await
                }
            });

            send_bounds.push(quote! { Self::#method_ident(..): Send, });
            invoke_branchs.push(quote! {
                let this = self.clone();
                router.add_method::<#req_type, #rsp_type>(#method_name, Box::new(move |mut ctx, payload| {
                    let this = this.clone();
                    ::tokio::spawn(async move {
                        match payload.deserialize(&ctx.msg_meta) {
                            Ok(req) => {
                                let result = this.#method_ident(&ctx, &req).await;
                                ctx.send_rsp(result).await;
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

            #(#input_items)*

            fn ruapc_export(
                self: ::std::sync::Arc<Self>,
                router: &mut #krate::Router,
            )
            where
                Self: 'static + Send + Sync,
                #(#send_bounds)*
            {
                #(#invoke_branchs)*
            }
        }

        impl #trait_ident for #krate::Client {
            #(#client_methods)*
        }

        /// Extension trait providing `_with_buffer` variants for each method.
        ///
        /// These methods allow attaching a registered memory [`Buffer`] to the
        /// request. The buffer's `RemoteBufferInfo` will be included in the
        /// message metadata so the server can `remote_read` the buffer.
        #visibility trait #with_buffer_trait_ident {
            #(#with_buffer_trait_methods)*
        }

        impl #with_buffer_trait_ident for #krate::Client {
            #(#with_buffer_impl_methods)*
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
