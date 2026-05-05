//! Build script for ruapc-rdma-sys
//!
//! This script:
//! 1. Probes for libibverbs using pkg-config
//! 2. Generates FFI bindings using bindgen
//! 3. Applies custom type replacements (FwVer, Guid, WRID)
//! 4. Derives serialization traits for select types

use std::collections::HashSet;
use std::env;
use std::path::PathBuf;

use bindgen::callbacks::{DeriveInfo, ParseCallbacks};

/// Custom callback to add serde and schemars derives to specific ibverbs types
#[derive(Debug)]
struct CustomDerive;

impl ParseCallbacks for CustomDerive {
    /// Adds serde and schemars derives to specific ibverbs types
    ///
    /// These types need JSON serialization support for the ruapc project
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

/// Replaces C types with custom Rust wrapper types in generated bindings
///
/// This function post-processes the bindgen output to:
/// - Replace `fw_ver` field type with `FwVer` wrapper
/// - Replace `node_guid` and `sys_image_guid` field types with `Guid` wrapper
/// - Replace `wr_id` field type with `WRID` wrapper
/// - Replace `link_layer` field type with `LinkLayer` wrapper
///
/// These wrappers provide safer, more idiomatic Rust interfaces
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
                "ibv_port_attr" => {
                    if let syn::Fields::Named(ref mut fields) = struct_item.fields {
                        for field in fields.named.iter_mut() {
                            if let Some(ident) = &field.ident
                                && ident == "link_layer"
                            {
                                field.ty = syn::parse_str("LinkLayer")
                                    .expect("Failed to parse LinkLayer type");
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
    // Probe for libibverbs installation
    let lib = pkg_config::Config::new()
        .statik(false)
        .probe("libibverbs")
        .unwrap_or_else(|_| panic!("please install libibverbs-dev and pkg-config"));

    // Collect include paths from pkg-config and add /usr/include as fallback
    let mut include_paths = lib.include_paths.into_iter().collect::<HashSet<_>>();
    include_paths.insert(PathBuf::from("/usr/include"));

    // Configure bindgen to generate RDMA verb bindings
    let builder = bindgen::Builder::default()
        .clang_args(include_paths.iter().map(|p| format!("-I{p:?}")))
        .header_contents("header.h", "#include <infiniband/verbs.h>")
        // Enable common derives for generated types
        .derive_copy(true)
        .derive_debug(true)
        .derive_default(true)
        .generate_comments(false) // C comments often don't translate well
        .prepend_enum_name(false)
        .formatter(bindgen::Formatter::Rustfmt) // Format with rustfmt
        .size_t_is_usize(true)
        .translate_enum_integer_types(true)
        .layout_tests(false)
        .default_enum_style(bindgen::EnumVariation::Rust {
            non_exhaustive: false,
        })
        // pthread types are opaque - we provide safe wrappers
        .opaque_type("pthread_cond_t")
        .opaque_type("pthread_mutex_t")
        // Only bind types/functions we actually use
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
        // Types with function pointers shouldn't implement Copy
        .no_copy("ibv_context")
        .no_copy("ibv_cq")
        .no_copy("ibv_qp")
        .no_copy("ibv_srq")
        .no_debug("ibv_device");

    // Generate the FFI bindings
    let bindings = builder.generate().expect("Unable to generate bindings");

    // Post-process to apply custom type replacements
    let bindings_str = bindings.to_string();
    let modified_bindings = replace_custom_types(&bindings_str);

    std::fs::write(
        PathBuf::from(env::var("OUT_DIR").unwrap()).join("bindings.rs"),
        modified_bindings,
    )
    .expect("Couldn't write bindings!");
}
