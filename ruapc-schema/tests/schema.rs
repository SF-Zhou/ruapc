use ruapc_schema::{
    DescribeRequest, REFLECTION_PROTOCOL_VERSION, SchemaRegistry, ServerDescription,
};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::{Value, json};

#[derive(JsonSchema, Serialize)]
struct PublicItem {
    name: String,
}

#[derive(JsonSchema, Serialize)]
struct PublicRequest {
    item: PublicItem,
}

#[derive(JsonSchema, Serialize)]
struct PublicResponse {
    items: Vec<PublicItem>,
    next: Option<Box<PublicResponse>>,
}

#[derive(JsonSchema, Serialize)]
struct SecretDetails {
    secret: String,
}

#[derive(JsonSchema, Serialize)]
#[schemars(rename = "PublicRequest")]
struct InternalRequest {
    internal: SecretDetails,
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
fn internal_registration_does_not_change_public_names_or_definitions() {
    let mut registry = SchemaRegistry::default();
    registry.register::<InternalRequest, SecretDetails>(false);
    let public = registry.register::<PublicRequest, PublicResponse>(true);
    let document = registry
        .build([("Public/run", &public)].into_iter())
        .unwrap();

    let schemas = &document.components.as_ref().unwrap().schemas;
    assert!(schemas.contains_key("PublicRequest"));
    assert!(!schemas.contains_key("PublicRequest2"));
    assert!(!schemas.contains_key("SecretDetails"));
    assert_eq!(document.paths.paths.len(), 1);
    assert!(document.paths.paths.contains_key("/Public/run"));

    let mut public_only = SchemaRegistry::default();
    let expected_schema = public_only.register::<PublicRequest, PublicResponse>(true);
    let expected = public_only
        .build([("Public/run", &expected_schema)].into_iter())
        .unwrap();
    assert_eq!(
        serde_json::to_value(&document).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
    assert_local_references_resolve(&serde_json::to_value(document).unwrap());
}

#[test]
fn openapi_keeps_response_envelopes_and_definitions_across_rebuilds() {
    let mut registry = SchemaRegistry::default();
    let first = registry.register::<PublicRequest, Result<PublicResponse, String>>(true);
    let initial = registry.build([("Svc/first", &first)].into_iter()).unwrap();
    let repeated = registry.build([("Svc/first", &first)].into_iter()).unwrap();
    assert_eq!(
        serde_json::to_value(&initial).unwrap(),
        serde_json::to_value(repeated).unwrap()
    );

    let second = registry.register::<AdditionalRequest, PublicResponse>(true);
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
fn reflection_filters_exact_service_names_and_keeps_references() {
    let mut registry = SchemaRegistry::default();
    let schema = registry.register::<PublicRequest, PublicResponse>(true);
    let methods = [
        ("SearchV2/find", &schema),
        ("Search/zebra", &schema),
        ("Alpha/run", &schema),
        ("Search/apple", &schema),
    ];
    let openapi = registry.build(methods.into_iter()).unwrap();
    let describe = |service| {
        ServerDescription::from_methods(
            "serving-version",
            &DescribeRequest { service },
            methods.into_iter(),
            openapi.components.as_ref(),
        )
        .unwrap()
    };

    let all = describe(None);
    assert_eq!(all.protocol_version, REFLECTION_PROTOCOL_VERSION);
    assert_eq!(all.ruapc_version, "serving-version");
    assert_eq!(
        all.services
            .iter()
            .map(|service| service.name.as_str())
            .collect::<Vec<_>>(),
        ["Alpha", "Search", "SearchV2"]
    );
    let filtered = describe(Some("Search".into()));
    assert_eq!(filtered.services.len(), 1);
    assert_eq!(filtered.services[0].name, "Search");
    assert_eq!(
        filtered.services[0]
            .methods
            .iter()
            .map(|method| method.name.as_str())
            .collect::<Vec<_>>(),
        ["apple", "zebra"]
    );
    assert_eq!(filtered.components, all.components);
    let value = serde_json::to_value(filtered).unwrap();
    assert_local_references_resolve(&value);
    let restored: ServerDescription = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(restored).unwrap(), value);
    assert!(describe(Some("Sear".into())).services.is_empty());
}

#[test]
fn empty_reflection_has_the_existing_wire_shape() {
    let description = ServerDescription::from_methods(
        "implementation-version",
        &DescribeRequest::default(),
        std::iter::empty(),
        None,
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(description).unwrap(),
        json!({
            "protocol_version": 1,
            "ruapc_version": "implementation-version",
            "services": [],
            "components": {},
        })
    );
    assert_eq!(
        serde_json::to_value(DescribeRequest::default()).unwrap(),
        json!({"service": null})
    );
    assert_eq!(
        serde_json::to_value(schemars::schema_for!(DescribeRequest)).unwrap()["description"],
        "Selects the public services returned by [`ReflectionService::describe`]."
    );
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
    let schema = registry.register::<Unsupported, ()>(true);
    assert!(
        registry
            .build([("Invalid/run", &schema)].into_iter())
            .is_err()
    );
}
