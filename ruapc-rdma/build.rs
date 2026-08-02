//! Build script for ruapc-rdma
//!
//! This script:
//! 1. Probes for libibverbs using pkg-config
//! 2. Compiles the C shim (src/shim.c) wrapping all verbs entry points, so
//!    static inline functions and function-like macros in verbs.h (which
//!    bindgen cannot handle) are always honored
//! 3. Generates FFI bindings using bindgen
//! 4. Applies custom type replacements (FwVer, Guid, WRID)
//! 5. Derives serialization traits for select types
//! 6. Annotates capability flag enums for `enumflags2` and `strum`

use std::collections::HashSet;
use std::env;
use std::path::PathBuf;

use bindgen::callbacks::{AttributeInfo, DeriveInfo, ParseCallbacks};

const FLAG_ENUMS: &[&str] = &[
    "ibv_device_cap_flags",
    "ibv_port_cap_flags",
    "ibv_port_cap_flags2",
];

/// Custom callback to add serde and schemars derives to specific ibverbs types
#[derive(Debug)]
struct CustomDerive;

impl ParseCallbacks for CustomDerive {
    /// Adds serde and schemars derives to specific ibverbs types
    ///
    /// These types need JSON serialization support for the ruapc project
    fn add_derives(&self, info: &DeriveInfo<'_>) -> Vec<String> {
        let mut derives = match info.name {
            "ibv_device_attr"
            | "ibv_atomic_cap"
            | "ibv_port_state"
            | "ibv_mtu"
            | "ibv_port_cap_flags"
            | "ibv_port_cap_flags2"
            | "ibv_port_attr"
            | "ibv_transport_type"
            | "ibv_device_cap_flags" => {
                vec![
                    "Serialize".to_string(),
                    "Deserialize".to_string(),
                    "JsonSchema".to_string(),
                ]
            }
            _ => vec![],
        };
        if FLAG_ENUMS.contains(&info.name) {
            derives.push("strum::IntoStaticStr".to_string());
        }
        derives
    }

    fn add_attributes(&self, info: &AttributeInfo<'_>) -> Vec<String> {
        FLAG_ENUMS
            .contains(&info.name)
            .then(|| "#[enumflags2::bitflags]".to_string())
            .into_iter()
            .collect()
    }
}

/// Replaces C types with custom Rust wrapper types in generated bindings
///
/// This function post-processes the bindgen AST to:
/// - Replace `fw_ver` field type with `FwVer` wrapper
/// - Replace `node_guid` and `sys_image_guid` field types with `Guid` wrapper
/// - Replace `wr_id` field type with `WRID` wrapper
/// - Replace `link_layer` field type with `LinkLayer` wrapper
/// - Replace capability-mask integer fields with typed `BitFlags`
/// - Represent `ibv_port_cap_flags2` as `u16`, matching its struct field
///
/// These wrappers provide safer, more idiomatic Rust interfaces
fn replace_custom_types(ast: &mut syn::File) {
    for item in &mut ast.items {
        if let syn::Item::Enum(enum_item) = item
            && enum_item.ident == "ibv_port_cap_flags2"
        {
            let repr = enum_item
                .attrs
                .iter_mut()
                .find(|attr| attr.path().is_ident("repr"))
                .expect("ibv_port_cap_flags2 is missing repr");
            *repr = syn::parse_quote!(#[repr(u16)]);
        }

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
                                    "device_cap_flags" => {
                                        field.ty = syn::parse_str("BitFlags<ibv_device_cap_flags>")
                                            .expect("Failed to parse device capability flags type");
                                        field
                                            .attrs
                                            .push(syn::parse_quote!(#[schemars(with = "u32")]));
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
                            if let Some(ident) = &field.ident {
                                match ident.to_string().as_str() {
                                    "link_layer" => {
                                        field.ty = syn::parse_str("LinkLayer")
                                            .expect("Failed to parse LinkLayer type");
                                    }
                                    "port_cap_flags" => {
                                        field.ty = syn::parse_str("BitFlags<ibv_port_cap_flags>")
                                            .expect("Failed to parse port capability flags type");
                                        field
                                            .attrs
                                            .push(syn::parse_quote!(#[schemars(with = "u32")]));
                                    }
                                    "port_cap_flags2" => {
                                        field.ty = syn::parse_str("BitFlags<ibv_port_cap_flags2>")
                                            .expect("Failed to parse secondary port flags type");
                                        field
                                            .attrs
                                            .push(syn::parse_quote!(#[schemars(with = "u16")]));
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
}

fn main() {
    // Probe without emitting link metadata yet. libibverbs must appear after
    // the static shim on the linker command line when --as-needed is enabled.
    let mut pkg_config = pkg_config::Config::new();
    let lib = pkg_config
        .statik(false)
        .cargo_metadata(false)
        .probe("libibverbs")
        .unwrap_or_else(|_| panic!("please install libibverbs-dev and pkg-config"));

    // Collect include paths from pkg-config and add /usr/include as fallback
    let mut include_paths = lib.include_paths.into_iter().collect::<HashSet<_>>();
    include_paths.insert(PathBuf::from("/usr/include"));

    // Compile the C shim wrapping all verbs entry points (see src/shim.h for
    // details). Compiling it against the locally installed header guarantees
    // the wrappers match the exact semantics of this platform's rdma-core
    // version, including any macro / static inline compat layers.
    println!("cargo:rerun-if-changed=src/shim.c");
    println!("cargo:rerun-if-changed=src/shim.h");
    let mut shim = cc::Build::new();
    shim.file("src/shim.c");
    for path in &include_paths {
        shim.include(path);
    }
    shim.compile("ruapc_rdma_shim");

    // Emit libibverbs after cc has emitted the static shim link directive.
    pkg_config::Config::new()
        .statik(false)
        .probe("libibverbs")
        .unwrap_or_else(|_| panic!("please install libibverbs-dev and pkg-config"));

    // Configure bindgen to generate RDMA verb bindings
    let builder = bindgen::Builder::default()
        .clang_args(include_paths.iter().map(|p| format!("-I{p:?}")))
        .header("src/shim.h") // includes <infiniband/verbs.h>
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
        .allowlist_type("ibv_port_cap_flags")
        .allowlist_type("ibv_port_cap_flags2")
        // All verbs entry points go through the C shim (src/shim.h); Rust
        // never binds ibv_* symbols directly. This guarantees macro/inline
        // layers in verbs.h are honored, on any rdma-core version.
        .allowlist_function("ruapc_ibv_.*")
        .bitfield_enum("ibv_access_flags")
        .bitfield_enum("ibv_send_flags")
        .bitfield_enum("ibv_wc_flags")
        .bitfield_enum("ibv_qp_attr_mask")
        // Rebuild when the C headers change.
        .parse_callbacks(Box::new(
            bindgen::CargoCallbacks::new().rerun_on_header_files(true),
        ))
        .parse_callbacks(Box::new(CustomDerive))
        // Types with function pointers shouldn't implement Copy
        .no_copy("ibv_context")
        .no_copy("ibv_cq")
        .no_copy("ibv_qp")
        .no_copy("ibv_srq")
        .no_debug("ibv_device");

    // Generate the FFI bindings.
    let bindings = builder.generate().expect("Unable to generate bindings");

    // Post-process to apply custom type replacements
    let bindings_str = bindings.to_string();
    let mut ast = syn::parse_file(&bindings_str).expect("Failed to parse generated bindings");
    replace_custom_types(&mut ast);

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    std::fs::write(out_dir.join("bindings.rs"), prettyplease::unparse(&ast))
        .expect("Couldn't write bindings!");
}
