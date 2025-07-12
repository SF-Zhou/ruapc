use proc_macro::TokenStream;
use quote::quote;
use syn::{FnArg, ItemTrait, TraitItem, parse_macro_input};

#[proc_macro_attribute]
pub fn service(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemTrait);

    let trait_ident = &input.ident;
    let visibility = input.vis;
    let trait_name = trait_ident.to_string();

    let mut send_bounds = vec![];
    let mut invoke_branchs = vec![];
    let mut client_methods = vec![];

    let krate = get_crate_name();

    let input_items = input.items;
    for item in &input_items {
        if let TraitItem::Fn(method) = item
            && method.sig.inputs.len() == 3
            && method.sig.asyncness.is_some()
            && let Some(receiver) = method.sig.receiver()
            && let FnArg::Typed(req_type) = &method.sig.inputs[2]
        {
            let method_ident = &method.sig.ident;
            if *method_ident == "ruapc_export" || *method_ident == "ruapc_request" {
                panic!("the function cannot be named `ruapc_export` or `ruapc_request`!");
            }
            let method_name = format!("{trait_name}/{method_ident}");

            let req_type = req_type.ty.clone();
            let output = &method.sig.output;
            client_methods.push(quote! {
                async fn #method_ident(#receiver, ctx: &#krate::Context, req: #req_type) #output {
                    self.ruapc_request(ctx, req, #method_name).await
                }
            });

            send_bounds.push(quote! { Self::#method_ident(..): Send, });
            invoke_branchs.push(quote! {
                let this = self.clone();
                map.insert(
                    #method_name.into(),
                    Box::new(move |mut ctx, msg| {
                        let this = this.clone();
                        tokio::spawn(async move {
                            let meta = msg.meta.clone();
                            match msg.deserialize() {
                                Ok(req) => {
                                    let result = this.#method_ident(&ctx, &req).await;
                                    ctx.send_rsp(meta, result).await;
                                }
                                Err(err) => {
                                    ctx.send_err_rsp(meta, err).await;
                                }
                            }
                        });
                        Ok(())
                    }),
                );
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
            ) -> ::std::collections::HashMap<String, #krate::Method>
            where
                Self: 'static + Send + Sync,
                #(#send_bounds)*
            {
                let mut map = ::std::collections::HashMap::<String, #krate::Method>::default();
                #(#invoke_branchs)*
                map
            }
        }

        impl #trait_ident for #krate::Client {
            #(#client_methods)*
        }
    }
    .into()
}

pub(crate) fn get_crate_name() -> proc_macro2::TokenStream {
    match proc_macro_crate::crate_name("ruapc") {
        Ok(proc_macro_crate::FoundCrate::Name(name)) => {
            let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote! { ::#ident }
        }
        _ => quote! { ::crate },
    }
}
