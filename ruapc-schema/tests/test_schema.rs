use ruapc_schema::{
    MethodContext, MethodProtocol, MethodSchema, MethodTransport, PayloadFormat, SchemaRegistry,
};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::{Value, json};

#[derive(JsonSchema, Serialize)]
struct Item {
    name: String,
}

#[derive(JsonSchema, Serialize)]
struct Request {
    item: Item,
}

#[derive(JsonSchema, Serialize)]
struct Response {
    items: Vec<Item>,
    next: Option<Box<Response>>,
}

#[derive(JsonSchema, Serialize)]
struct AdditionalRequest {
    enabled: bool,
}

fn assert_local_references_resolve(document: &Value) {
    fn visit(value: &Value, document: &Value) -> usize {
        match value {
            Value::Array(values) => values.iter().map(|value| visit(value, document)).sum(),
            Value::Object(fields) => {
                let mut count = 0;
                if let Some(reference) = fields.get("$ref").and_then(Value::as_str) {
                    let pointer = reference.strip_prefix('#').expect("local schema reference");
                    assert!(
                        document.pointer(pointer).is_some(),
                        "unresolved schema reference: {reference}"
                    );
                    count += 1;
                }
                count
                    + fields
                        .values()
                        .map(|value| visit(value, document))
                        .sum::<usize>()
            }
            _ => 0,
        }
    }

    assert!(
        visit(document, document) > 0,
        "fixture must contain references"
    );
}

#[test]
fn methods_with_matching_schema_names_keep_distinct_request_definitions() {
    mod messages {
        #[derive(schemars::JsonSchema, serde::Serialize)]
        pub struct Request {
            message: String,
        }
    }

    mod counts {
        #[derive(schemars::JsonSchema, serde::Serialize)]
        pub struct Request {
            count: u64,
        }
    }

    assert_eq!(
        messages::Request::schema_name(),
        counts::Request::schema_name()
    );
    assert_ne!(messages::Request::schema_id(), counts::Request::schema_id());

    let mut registry = SchemaRegistry::default();
    let message = registry.register::<messages::Request, ()>();
    let count = registry.register::<counts::Request, ()>();
    let document = registry
        .build([("Messages/send", &message), ("Counts/set", &count)].into_iter())
        .unwrap();
    let value = serde_json::to_value(document).unwrap();
    let paths = value["paths"].as_object().unwrap();
    assert_eq!(paths.len(), 2);
    assert!(paths.contains_key("/Messages/send"));
    assert!(paths.contains_key("/Counts/set"));

    let request_ref = |path: &str| {
        value["paths"][path]["post"]["requestBody"]["content"]["application/json"]["schema"]["$ref"]
            .as_str()
            .unwrap()
    };
    let message_ref = request_ref("/Messages/send");
    let count_ref = request_ref("/Counts/set");
    assert_ne!(message_ref, count_ref);

    for (reference, property, property_type) in [
        (message_ref, "message", "string"),
        (count_ref, "count", "integer"),
    ] {
        let schema = value.pointer(reference.strip_prefix('#').unwrap()).unwrap();
        let properties = schema["properties"].as_object().unwrap();
        assert_eq!(properties.len(), 1);
        assert_eq!(properties[property]["type"], property_type);
        assert_eq!(schema["required"], json!([property]));
    }
    assert_local_references_resolve(&value);
}

#[test]
fn openapi_keeps_response_envelopes_and_definitions_across_rebuilds() {
    let mut registry = SchemaRegistry::default();
    let first = registry.register::<Request, Result<Response, String>>();
    let initial = registry.build([("Svc/first", &first)].into_iter()).unwrap();
    let repeated = registry.build([("Svc/first", &first)].into_iter()).unwrap();
    assert_eq!(
        serde_json::to_value(&initial).unwrap(),
        serde_json::to_value(repeated).unwrap()
    );

    let second = registry.register::<AdditionalRequest, Response>();
    let document = registry
        .build([("Svc/second", &second), ("Svc/first", &first)].into_iter())
        .unwrap();
    assert_eq!(
        document
            .paths
            .paths
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["/Svc/first", "/Svc/second"]
    );
    let before = &initial.components.as_ref().unwrap().schemas;
    let after = &document.components.as_ref().unwrap().schemas;
    for (name, schema) in before {
        assert_eq!(
            serde_json::to_value(schema).unwrap(),
            serde_json::to_value(after.get(name).unwrap()).unwrap()
        );
    }
    assert!(after.contains_key("AdditionalRequest"));

    let value = serde_json::to_value(document).unwrap();
    assert_eq!(value["openapi"], "3.0.0");
    let operation = &value["paths"]["/Svc/first"]["post"];
    assert_eq!(operation["operationId"], "/Svc/first");
    assert_eq!(operation["requestBody"]["required"], true);
    let response_ref =
        operation["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
            .as_str()
            .unwrap();
    let response = value
        .pointer(response_ref.strip_prefix('#').unwrap())
        .unwrap();
    assert_eq!(response["oneOf"][0]["required"], json!(["Ok"]));
    assert_eq!(response["oneOf"][1]["required"], json!(["Err"]));
    assert_local_references_resolve(&value);
}

#[test]
fn unsupported_openapi_schema_is_returned_as_an_error() {
    struct Unsupported;

    impl JsonSchema for Unsupported {
        fn schema_name() -> std::borrow::Cow<'static, str> {
            "Unsupported".into()
        }

        fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
            serde_json::from_value(json!({"properties": 42})).unwrap()
        }
    }

    let mut registry = SchemaRegistry::default();
    let schema = registry.register::<Unsupported, ()>();
    assert!(
        registry
            .build([("Invalid/run", &schema)].into_iter())
            .is_err()
    );
}

#[test]
fn legacy_method_descriptions_default_to_serialized_standalone_calls() {
    let mut registry = SchemaRegistry::default();
    let schema = registry.register::<Request, Response>();
    let legacy = serde_json::to_value(&schema).unwrap();
    assert_eq!(legacy.as_object().unwrap().len(), 2);
    assert!(legacy.get("protocol").is_none());
    let restored: MethodSchema = serde_json::from_value(legacy).unwrap();
    assert_eq!(restored.protocol, MethodProtocol::default());

    let restored: MethodSchema = serde_json::from_value(json!({
        "request_schema": schema.request_schema,
        "response_schema": schema.response_schema,
        "protocol": { "response_format": "raw" }
    }))
    .unwrap();
    assert_eq!(restored.protocol.response_format, PayloadFormat::Raw);
    assert_eq!(restored.protocol.request_format, PayloadFormat::Serialized);
    assert_eq!(restored.protocol.transport, MethodTransport::Any);
    assert_eq!(restored.protocol.context, MethodContext::Standalone);
}

#[test]
fn raw_payloads_project_control_schemas_and_connection_requirements() {
    let mut registry = SchemaRegistry::default();
    registry.register_framework_error::<Result<(), String>>();
    let mut read = registry.register::<Request, Result<(), String>>();
    read.protocol = MethodProtocol {
        response_format: PayloadFormat::Raw,
        transport: MethodTransport::Stream,
        context: MethodContext::OriginalPeer,
        ..Default::default()
    };
    let mut write = registry.register::<Request, Result<(), String>>();
    write.protocol = MethodProtocol {
        request_format: PayloadFormat::Raw,
        ..read.protocol
    };
    write.protocol.response_format = PayloadFormat::Serialized;
    let mut rdma = registry.register::<Request, Result<(), String>>();
    rdma.protocol = MethodProtocol {
        transport: MethodTransport::Rdma,
        context: MethodContext::OriginalPeer,
        ..Default::default()
    };

    let encoded = serde_json::to_value(&read).unwrap();
    assert!(encoded["protocol"].get("request_format").is_none());
    assert_eq!(encoded["protocol"]["response_format"], "raw");
    let restored: MethodSchema = serde_json::from_value(encoded).unwrap();
    assert_eq!(restored.protocol, read.protocol);

    let build = || {
        serde_json::to_value(
            registry
                .build(
                    [
                        ("Memory/read", &read),
                        ("Memory/write", &write),
                        ("Memory/rdma", &rdma),
                    ]
                    .into_iter(),
                )
                .unwrap(),
        )
        .unwrap()
    };
    let document = build();
    assert_eq!(document, build());
    let read_op = &document["paths"]["/Memory/read"]["post"];
    let write_op = &document["paths"]["/Memory/write"]["post"];
    for operation in [read_op, write_op] {
        assert_eq!(operation["x-ruapc-transport"], "stream");
        assert_eq!(operation["x-ruapc-context"], "original_peer");
        assert!(
            operation["description"]
                .as_str()
                .unwrap()
                .contains("cannot be used as an independent unary JSON call")
        );
    }
    assert!(
        read_op["requestBody"]["content"]
            .get("application/json")
            .is_some()
    );
    assert!(
        write_op["responses"]["200"]["content"]
            .get("application/json")
            .is_some()
    );
    for (content, control) in [
        (
            &read_op["responses"]["200"]["content"],
            &read.response_schema,
        ),
        (&write_op["requestBody"]["content"], &write.request_schema),
    ] {
        assert!(content.get("application/json").is_none());
        let media = &content["application/octet-stream"];
        assert_eq!(media["schema"]["type"], "string");
        assert_eq!(media["schema"]["format"], "binary");
        assert_eq!(
            media["x-ruapc-control-schema"],
            serde_json::to_value(control).unwrap()
        );
    }
    assert_eq!(
        document["paths"]["/Memory/rdma"]["post"]["x-ruapc-transport"],
        "rdma"
    );
    let framework_error = &document["x-ruapc-framework-error"];
    assert_eq!(framework_error["flag"], 16);
    assert_eq!(framework_error["payload_format"], "serialized");
    assert_eq!(
        framework_error["unary_header"],
        json!({ "name": "x-ruapc-framework-error", "value": "true" })
    );
    assert_eq!(
        framework_error["schema"],
        serde_json::to_value(&read.response_schema).unwrap()
    );
    assert_local_references_resolve(&document);
}
