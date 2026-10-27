//! Attribute syntax and wire-name validation, independent of the trait AST.

use syn::{
    Expr, ExprLit, Lit, LitStr, Meta, Token,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    spanned::Spanned,
};

#[derive(Default)]
pub(crate) struct ServiceArgs {
    pub(crate) name: Option<LitStr>,
}

impl Parse for ServiceArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let metas = Punctuated::<Meta, Token![,]>::parse_terminated(input)?;
        let mut args = Self::default();

        for meta in metas {
            match meta {
                Meta::NameValue(meta) if meta.path.is_ident("name") => {
                    if args.name.is_some() {
                        return Err(syn::Error::new(
                            meta.path.span(),
                            "duplicate `name` argument",
                        ));
                    }
                    let Expr::Lit(ExprLit {
                        lit: Lit::Str(name),
                        ..
                    }) = meta.value
                    else {
                        return Err(syn::Error::new(
                            meta.value.span(),
                            "`name` must be a string literal",
                        ));
                    };
                    validate_name(&name)?;
                    args.name = Some(name);
                }
                Meta::Path(path) if path.is_ident("name") => {
                    return Err(syn::Error::new(
                        path.span(),
                        "`name` requires a string value, for example `name = \"MyService\"`",
                    ));
                }
                Meta::List(meta) if meta.path.is_ident("name") => {
                    return Err(syn::Error::new(meta.span(), "expected `name = \"...\"`"));
                }
                other => {
                    return Err(syn::Error::new(
                        other.span(),
                        "unknown `service` argument; expected `name = \"...\"`",
                    ));
                }
            }
        }
        Ok(args)
    }
}

fn validate_name(name: &LitStr) -> syn::Result<()> {
    let value = name.value();
    let message = if value.is_empty() {
        "service name cannot be empty"
    } else if value.trim() != value {
        "service name cannot have leading or trailing whitespace"
    } else if value.contains('/') {
        "service name cannot contain `/`"
    } else {
        return Ok(());
    };
    Err(syn::Error::new(name.span(), message))
}
