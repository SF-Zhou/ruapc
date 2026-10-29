use ruapc::{Context, CopyOp, Error, ErrorKind, RemoteSpace, Result, SocketPoolConfig};
use ruapc_bufpool::{MemoryKey, RemoteBufferInfo};
use ruapc_memory::{MemoryError, MemoryErrorKind};
use schemars::JsonSchema;

// Direct returns verify that Context keeps RPC errors while exposing the same
// space type as the standalone crate, without a wrapper or conversion.
fn read_space(ctx: &Context) -> Result<ruapc_memory::RemoteSpace<'_>> {
    ctx.remote_read_space()
}

fn write_space(ctx: &Context) -> Result<ruapc_memory::RemoteSpace<'_>> {
    ctx.remote_write_space()
}

#[tokio::test]
async fn public_memory_types_are_shared_and_context_keeps_rpc_errors() {
    let operation: CopyOp = ruapc_memory::CopyOp::new(1, 2, 3);
    let _: ruapc_memory::CopyOp = operation;
    assert_eq!(ruapc::MAX_COPY_OPS, ruapc_memory::MAX_COPY_OPS);
    assert_eq!(ruapc::MAX_REGIONS, ruapc_memory::MAX_REGIONS);

    let mut ctx = Context::create(&SocketPoolConfig::default()).unwrap();
    assert_eq!(
        read_space(&ctx).unwrap_err().kind,
        ErrorKind::MissingBufferInfo
    );
    assert_eq!(
        write_space(&ctx).unwrap_err().kind,
        ErrorKind::MissingBufferInfo
    );

    ctx.msg_meta.read_regions.push(RemoteBufferInfo {
        key: MemoryKey { lkey: 0, rkey: 0 },
        addr: 0x1000,
        len: 4,
    });
    let space: RemoteSpace<'_> = read_space(&ctx).unwrap();
    assert_eq!(space.total_len(), 4);
    assert_eq!(space.region_count(), 1);
    assert_eq!(space.regions().as_ptr(), ctx.msg_meta.read_regions.as_ptr());

    ctx.msg_meta.read_regions[0].addr = u64::MAX;
    assert_eq!(read_space(&ctx).unwrap_err().kind, ErrorKind::InvalidCopyOp);
}

#[test]
fn copy_operation_keeps_its_wire_fields_and_public_schema() {
    let operation = CopyOp::new(1, 2, 3);
    let json = serde_json::json!({"src_offset": 1, "dst_offset": 2, "len": 3});
    let packed: &[u8] = b"\x83\xaasrc_offset\x01\xaadst_offset\x02\xa3len\x03";
    assert_eq!(serde_json::to_value(operation).unwrap(), json);
    assert_eq!(serde_json::from_value::<CopyOp>(json).unwrap(), operation);
    assert_eq!(rmp_serde::to_vec_named(&operation).unwrap(), packed);
    assert_eq!(rmp_serde::from_slice::<CopyOp>(packed).unwrap(), operation);

    assert_eq!(CopyOp::schema_name(), "CopyOp");
    // The module-qualified schema_id moves with its defining crate. Both
    // import paths still have one identity and produce the same public schema.
    assert_eq!(CopyOp::schema_id(), ruapc_memory::CopyOp::schema_id());
    let schema = schemars::schema_for!(CopyOp);
    assert_eq!(schema, schemars::schema_for!(ruapc_memory::CopyOp));
    let schema = schema.as_value();
    assert_eq!(schema["title"], "CopyOp");
    assert_eq!(schema["type"], "object");
    assert_eq!(
        schema["required"],
        serde_json::json!(["src_offset", "dst_offset", "len"])
    );
    let properties = schema["properties"].as_object().unwrap();
    assert_eq!(properties.len(), 3);
    for name in ["src_offset", "dst_offset", "len"] {
        assert_eq!(properties[name]["type"], "integer");
        assert_eq!(properties[name]["format"], "uint64");
        assert_eq!(properties[name]["minimum"], 0);
    }
}

#[test]
fn memory_errors_keep_rpc_categories_and_move_the_diagnostic() {
    for (kind, expected) in [
        (MemoryErrorKind::InvalidCopyOp, ErrorKind::InvalidCopyOp),
        (MemoryErrorKind::InvalidArgument, ErrorKind::InvalidArgument),
    ] {
        let error = MemoryError::new(kind, "memory validation failed".into());
        let message_pointer = error.msg.as_ptr();
        let error: Error = error.into();
        assert_eq!(error.kind, expected);
        assert_eq!(error.msg, "memory validation failed");
        assert_eq!(error.msg.as_ptr(), message_pointer);
    }
}
