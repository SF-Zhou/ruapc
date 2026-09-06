//! The boundary between Rust syntax and code generation.
//!
//! A `Service` contains only non-generic, safe traits and validated RPC method
//! declarations. Every `Method` has `&self`, two shared-reference arguments,
//! and an explicit response type. Type aliases and the meaning of the context,
//! request, and response types remain the Rust type checker's responsibility.

use syn::{
    Attribute, FnArg, Generics, Ident, ItemTrait, LitStr, Meta, ReturnType, Signature, Token,
    TraitItem, TraitItemFn, Type, TypeParamBound, Visibility, ext::IdentExt, parse_quote,
    punctuated::Punctuated, spanned::Spanned,
};

use crate::args::ServiceArgs;

pub(crate) struct Service {
    pub(crate) attrs: Vec<Attribute>,
    pub(crate) conditions: Vec<Attribute>,
    pub(crate) visibility: Visibility,
    pub(crate) ident: Ident,
    pub(crate) supertraits: Punctuated<TypeParamBound, Token![+]>,
    pub(crate) name: LitStr,
    pub(crate) internal: bool,
    pub(crate) methods: Vec<Method>,
}

pub(crate) struct Method {
    pub(crate) attrs: Vec<Attribute>,
    pub(crate) conditions: Vec<Attribute>,
    pub(crate) signature: Signature,
    pub(crate) request: Type,
    pub(crate) response: Box<Type>,
    pub(crate) wire_name: LitStr,
}

impl Service {
    pub(crate) fn parse(args: ServiceArgs, declaration: ItemTrait) -> syn::Result<Self> {
        reject_generics(&declaration.generics, "service traits")?;
        if declaration.unsafety.is_some() || declaration.auto_token.is_some() {
            return Err(syn::Error::new_spanned(
                &declaration.ident,
                "service traits cannot be unsafe or auto traits",
            ));
        }
        let name = args.name.unwrap_or_else(|| {
            LitStr::new(
                &declaration.ident.unraw().to_string(),
                declaration.ident.span(),
            )
        });
        let mut methods = Vec::with_capacity(declaration.items.len());
        let mut errors: Option<syn::Error> = None;
        for item in declaration.items {
            let result = match item {
                TraitItem::Fn(method) => Method::parse(method, &name),
                item => Err(syn::Error::new_spanned(
                    item,
                    "service traits can only contain RPC method declarations",
                )),
            };
            match result {
                Ok(method) => methods.push(method),
                Err(error) => match &mut errors {
                    Some(errors) => errors.combine(error),
                    None => errors = Some(error),
                },
            }
        }
        if let Some(errors) = errors {
            return Err(errors);
        }
        Ok(Self {
            conditions: conditional_attrs(&declaration.attrs)?,
            attrs: declaration.attrs,
            visibility: declaration.vis,
            ident: declaration.ident,
            supertraits: declaration.supertraits,
            name,
            internal: args.internal,
            methods,
        })
    }
}

impl Method {
    fn parse(method: TraitItemFn, service_name: &LitStr) -> syn::Result<Self> {
        let signature = method.sig;
        if signature.ident == "ruapc_export" || signature.ident == "ruapc_request" {
            return Err(syn::Error::new_spanned(
                &signature.ident,
                "`ruapc_export` and `ruapc_request` are reserved method names",
            ));
        }
        if signature.asyncness.is_none() {
            return Err(syn::Error::new_spanned(
                signature.fn_token,
                "RPC methods must be async",
            ));
        }
        reject_generics(&signature.generics, "RPC methods")?;
        if signature.constness.is_some()
            || signature.unsafety.is_some()
            || signature.abi.is_some()
            || signature.variadic.is_some()
        {
            return Err(syn::Error::new_spanned(
                &signature,
                "RPC methods cannot be const, unsafe, extern, or variadic",
            ));
        }
        if let Some(body) = method.default {
            return Err(syn::Error::new_spanned(
                body,
                "RPC methods must be declarations; put the handler body in an implementation",
            ));
        }
        if signature.inputs.len() != 3 {
            return Err(syn::Error::new_spanned(
                &signature.inputs,
                "RPC methods require exactly three arguments: `&self`, `&Context`, and `&Request`",
            ));
        }
        let valid_receiver = signature.receiver().is_some_and(|receiver| {
            receiver.reference.is_some()
                && receiver.mutability.is_none()
                && receiver.colon_token.is_none()
        });
        if !valid_receiver {
            return Err(syn::Error::new_spanned(
                &signature.inputs[0],
                "RPC methods require a shared `&self` receiver",
            ));
        }
        shared_argument(&signature.inputs[1], "context")?;
        let request = shared_argument(&signature.inputs[2], "request")?.clone();
        let ReturnType::Type(_, response) = &signature.output else {
            return Err(syn::Error::new_spanned(
                &signature.ident,
                "RPC methods require an explicit result return type",
            ));
        };
        Ok(Self {
            conditions: conditional_attrs(&method.attrs)?,
            attrs: method.attrs,
            request,
            response: response.clone(),
            wire_name: LitStr::new(
                &format!("{}/{}", service_name.value(), signature.ident.unraw()),
                signature.ident.span(),
            ),
            signature,
        })
    }
}

fn reject_generics(generics: &Generics, subject: &str) -> syn::Result<()> {
    if !generics.params.is_empty() || generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            generics,
            format!("{subject} cannot have generic parameters or where clauses"),
        ));
    }
    Ok(())
}

fn shared_argument<'a>(argument: &'a FnArg, role: &str) -> syn::Result<&'a Type> {
    if let FnArg::Typed(argument) = argument
        && let Type::Reference(reference) = argument.ty.as_ref()
        && reference.mutability.is_none()
    {
        return Ok(&argument.ty);
    }
    Err(syn::Error::new_spanned(
        argument,
        format!("the RPC {role} argument must be a shared reference"),
    ))
}

/// Copy only item-existence conditions onto generated impls and registration
/// blocks. A `cfg_attr` can mix `cfg` with attributes that are only valid on the
/// original declaration, so filter its nested attributes recursively.
fn conditional_attrs(attrs: &[Attribute]) -> syn::Result<Vec<Attribute>> {
    attrs
        .iter()
        .filter_map(|attr| conditional_meta(&attr.meta).transpose())
        .map(|meta| meta.map(|meta| parse_quote!(#[#meta])))
        .collect()
}

fn conditional_meta(meta: &Meta) -> syn::Result<Option<Meta>> {
    if meta.path().is_ident("cfg") {
        return Ok(Some(meta.clone()));
    }
    if let Meta::List(list) = meta
        && list.path.is_ident("cfg_attr")
    {
        let mut args = list
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?
            .into_iter();
        let Some(predicate) = args.next() else {
            return Err(syn::Error::new(
                list.span(),
                "cfg_attr requires a predicate",
            ));
        };
        let conditions: Vec<_> = args
            .filter_map(|meta| conditional_meta(&meta).transpose())
            .collect::<syn::Result<_>>()?;
        if !conditions.is_empty() {
            return Ok(Some(parse_quote!(cfg_attr(#predicate, #(#conditions),*))));
        }
    }
    Ok(None)
}
