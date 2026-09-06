//! Emit code from validated service declarations.
//!
//! Generated calls use the same statically dispatched contract traits for plain
//! and buffer-carrying responses. No runtime dispatch or allocations are added
//! by the macro beyond the router's existing per-method handler registration.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{FnArg, Ident, Signature, parse_quote};

use crate::model::{Method, Service};

pub(crate) fn service(service: Service) -> syn::Result<TokenStream> {
    let krate = crate_path()?;
    let Service {
        attrs,
        conditions,
        visibility,
        ident,
        supertraits,
        name,
        internal,
        methods,
    } = service;
    let bounds = (!supertraits.is_empty()).then(|| quote!(: #supertraits));
    let declarations = methods.iter().map(trait_method);
    let client_methods: Vec<_> = methods
        .iter()
        .map(|method| client_method(method, &krate))
        .collect();
    let register = if internal {
        quote!(add_internal_method)
    } else {
        quote!(add_method)
    };
    let registrations = methods
        .iter()
        .map(|method| registration(method, &krate, &register));

    Ok(quote! {
        #(#attrs)*
        #visibility trait #ident #bounds {
            /// Wire service name used to qualify each RPC method.
            const NAME: &'static str = #name;

            #(#declarations)*

            /// Register this service's handlers on the router.
            fn ruapc_export(
                self: ::std::sync::Arc<Self>,
                router: &mut #krate::Router,
            )
            where
                Self: 'static + ::core::marker::Send + ::core::marker::Sync,
            {
                #(#registrations)*
            }
        }

        #(#conditions)*
        impl #ident for #krate::Client {
            #(#client_methods)*
        }

        #(#conditions)*
        impl #ident for #krate::ClientWithBuffers<'_> {
            #(#client_methods)*
        }
    })
}

fn trait_method(method: &Method) -> TokenStream {
    let attrs = &method.attrs;
    let signature = send_signature(method);
    quote!(#(#attrs)* #signature;)
}

/// Declaring the `Send` future in both the trait and client impl lets call sites
/// discharge the bound without examining the generated async body. Using
/// `async fn` for the client impl would make recursive RDMA bootstrap RPCs cause
/// a compiler query cycle while proving that connection setup is `Send`.
fn send_signature(method: &Method) -> Signature {
    let mut signature = method.signature.clone();
    let response = &method.response;
    signature.asyncness = None;
    signature.output = parse_quote! {
        -> impl ::core::future::Future<Output = #response> + ::core::marker::Send
    };
    signature
}

fn client_method(method: &Method, krate: &TokenStream) -> TokenStream {
    let attrs = &method.attrs;
    let response = &method.response;
    let wire_name = &method.wire_name;
    let mut signature = send_signature(method);
    // Keep each argument's type (including aliases), while giving generated
    // bodies stable bindings regardless of the user's argument patterns.
    for (argument, name) in signature.inputs.iter_mut().skip(1).zip(["ctx", "req"]) {
        if let FnArg::Typed(argument) = argument {
            let ident = Ident::new(name, proc_macro2::Span::mixed_site());
            argument.pat = parse_quote!(#ident);
        }
    }
    let ctx = Ident::new("ctx", proc_macro2::Span::mixed_site());
    let req = Ident::new("req", proc_macro2::Span::mixed_site());
    quote! {
        #(#attrs)*
        #signature {
            async move {
                use #krate::{CallPlain as _, CallWithBuffer as _};
                (&#krate::RpcCall::<#response>::new())
                    .ruapc_call(self, #ctx, #req, #wire_name)
                    .await
            }
        }
    }
}

fn registration(method: &Method, krate: &TokenStream, register: &TokenStream) -> TokenStream {
    let conditions = &method.conditions;
    let ident = &method.signature.ident;
    let request = &method.request;
    let response = &method.response;
    let wire_name = &method.wire_name;
    quote! {
        #(#conditions)*
        {
            let this = self.clone();
            router.#register::<#request, #response>(#wire_name, ::std::boxed::Box::new(move |ctx, payload| {
                let this = this.clone();
                #krate::spawn_handler(ctx, #wire_name, payload, move |mut ctx, payload| async move {
                    match payload.deserialize(&ctx.msg_meta) {
                        Ok(req) => {
                            match #krate::catch_handler_panic(this.#ident(&ctx, &req)).await {
                                Ok(result) => ctx.send_rsp(result).await,
                                Err(err) => ctx.send_err_rsp(err).await,
                            }
                        }
                        Err(err) => ctx.send_err_rsp(err).await,
                    }
                });
                Ok(())
            }));
        }
    }
}

/// Resolve a renamed dependency as well as invocations inside RuaPC itself.
/// Missing dependencies are a macro diagnostic, not a misleading `crate::...`
/// lookup in the caller's unrelated crate.
fn crate_path() -> syn::Result<TokenStream> {
    match proc_macro_crate::crate_name("ruapc") {
        Ok(proc_macro_crate::FoundCrate::Name(name)) => {
            let ident = Ident::new(&name, proc_macro2::Span::call_site());
            Ok(quote!(::#ident))
        }
        Ok(proc_macro_crate::FoundCrate::Itself) => Ok(quote!(crate)),
        Err(error) => Err(syn::Error::new(proc_macro2::Span::call_site(), error)),
    }
}
