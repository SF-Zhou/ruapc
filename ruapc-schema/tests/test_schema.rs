use ruapc_schema::SchemaRegistry;
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
