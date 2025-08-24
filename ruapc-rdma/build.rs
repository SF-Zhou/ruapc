use std::collections::HashSet;
use std::env;
use std::path::PathBuf;

use bindgen::callbacks::{DeriveInfo, ParseCallbacks};

#[derive(Debug)]
struct CustomDerive;

impl ParseCallbacks for CustomDerive {
    fn add_derives(&self, info: &DeriveInfo<'_>) -> Vec<String> {
        match info.name {
            "ibv_device_attr" | "ibv_atomic_cap" | "ibv_port_state" | "ibv_mtu"
            | "ibv_port_cap_flags" | "ibv_port_attr" => {
                vec![
                    "Serialize".to_string(),
                    "Deserialize".to_string(),
                    "JsonSchema".to_string(),
                ]
            }
            _ => vec![],
        }
    }
}

fn replace_custom_types(input: &str) -> String {
    let mut ast = syn::parse_file(input).expect("Failed to parse generated bindings");

    for item in &mut ast.items {
        if let syn::Item::Struct(struct_item) = item {
            match struct_item.ident.to_string().as_str() {
                "ibv_device_attr" => {
                    if let syn::Fields::Named(ref mut fields) = struct_item.fields {
                        for field in fields.named.iter_mut() {
                            if let Some(ident) = &field.ident {
                                match ident.to_string().as_str() {
                                    "fw_ver" => {
                                        field.ty = syn::parse_str("FwVer")
                                            .expect("Failed to parse FwVer type");
                                    }
                                    "node_guid" | "sys_image_guid" => {
                                        field.ty = syn::parse_str("Guid")
                                            .expect("Failed to parse Guid type");
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                "ibv_wc" | "ibv_send_wr" | "ibv_recv_wr" => {
                    if let syn::Fields::Named(ref mut fields) = struct_item.fields {
                        for field in fields.named.iter_mut() {
                            if let Some(ident) = &field.ident
                                && ident == "wr_id"
                            {
                                field.ty =
                                    syn::parse_str("WRID").expect("Failed to parse WRID type");
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    prettyplease::unparse(&ast)
}

fn main() {
    let lib = pkg_config::Config::new()
        .statik(false)
        .probe("libibverbs")
        .unwrap_or_else(|_| panic!("please install libibverbs-dev and pkg-config"));

    let mut include_paths = lib.include_paths.into_iter().collect::<HashSet<_>>();
    include_paths.insert(PathBuf::from("/usr/include"));

    let builder = bindgen::Builder::default()
        .clang_args(include_paths.iter().map(|p| format!("-I{p:?}")))
        .header_contents("header.h", "#include <infiniband/verbs.h>")
        .derive_copy(true)
        .derive_debug(true)
        .derive_default(true)
        .generate_comments(false)
        .prepend_enum_name(false)
        .formatter(bindgen::Formatter::Rustfmt)
        .size_t_is_usize(true)
        .translate_enum_integer_types(true)
        .layout_tests(false)
        .default_enum_style(bindgen::EnumVariation::Rust {
            non_exhaustive: false,
        })
        .opaque_type("pthread_cond_t")
        .opaque_type("pthread_mutex_t")
        .allowlist_type("ibv_access_flags")
        .allowlist_type("ibv_comp_channel")
        .allowlist_type("ibv_context")
        .allowlist_type("ibv_cq")
        .allowlist_type("ibv_device")
        .allowlist_type("ibv_gid")
        .allowlist_type("ibv_mr")
        .allowlist_type("ibv_pd")
        .allowlist_type("ibv_port_attr")
        .allowlist_type("ibv_qp")
        .allowlist_type("ibv_qp_attr_mask")
        .allowlist_type("ibv_qp_init_attr")
        .allowlist_type("ibv_send_flags")
        .allowlist_type("ibv_wc")
        .allowlist_type("ibv_wc_flags")
        .allowlist_type("ibv_wc_status")
        .allowlist_type("ibv_atomic_cap")
        .allowlist_type("ibv_device_attr")
        .allowlist_type("ibv_device_cap_flags")
        .allowlist_function("ibv_ack_cq_events")
        .allowlist_function("ibv_alloc_pd")
        .allowlist_function("ibv_close_device")
        .allowlist_function("ibv_create_comp_channel")
        .allowlist_function("ibv_create_cq")
        .allowlist_function("ibv_create_qp")
        .allowlist_function("ibv_dealloc_pd")
        .allowlist_function("ibv_dereg_mr")
        .allowlist_function("ibv_destroy_comp_channel")
        .allowlist_function("ibv_destroy_cq")
        .allowlist_function("ibv_destroy_qp")
        .allowlist_function("ibv_free_device_list")
        .allowlist_function("ibv_get_cq_event")
        .allowlist_function("ibv_get_device_guid")
        .allowlist_function("ibv_get_device_list")
        .allowlist_function("ibv_modify_qp")
        .allowlist_function("ibv_req_notify_cq")
        .allowlist_function("ibv_poll_cq")
        .allowlist_function("ibv_post_recv")
        .allowlist_function("ibv_post_send")
        .allowlist_function("ibv_query_device")
        .allowlist_function("ibv_query_gid")
        .allowlist_function("ibv_query_port")
        .allowlist_function("ibv_open_device")
        .allowlist_function("ibv_reg_mr")
        .bitfield_enum("ibv_access_flags")
        .bitfield_enum("ibv_send_flags")
        .bitfield_enum("ibv_wc_flags")
        .bitfield_enum("ibv_qp_attr_mask")
        .bitfield_enum("ibv_device_cap_flags")
        .parse_callbacks(Box::new(CustomDerive))
        .no_copy("ibv_context")
        .no_copy("ibv_cq")
        .no_copy("ibv_qp")
        .no_copy("ibv_srq")
        .no_debug("ibv_device");

    let bindings = builder.generate().expect("Unable to generate bindings");

    let bindings_str = bindings.to_string();
    let modified_bindings = replace_custom_types(&bindings_str);

    std::fs::write(
        PathBuf::from(env::var("OUT_DIR").unwrap()).join("bindings.rs"),
        modified_bindings,
    )
    .expect("Couldn't write bindings!");
}
