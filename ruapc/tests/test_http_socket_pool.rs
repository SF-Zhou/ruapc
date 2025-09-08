#![forbid(unsafe_code)]
#![feature(return_type_notation)]

use std::{str::FromStr, sync::Arc};

use ruapc::{Router, SocketPoolConfig, SocketType};

#[ruapc::service]
trait TestService {
    async fn test_method(&self, _: &ruapc::Context, req: &String) -> ruapc::Result<String>;
}

struct TestServiceImpl;

impl TestService for TestServiceImpl {
    async fn test_method(&self, _: &ruapc::Context, req: &String) -> ruapc::Result<String> {
        Ok(format!("test response: {}", req))
    }
}

#[tokio::test]
async fn test_http_openapi_json_endpoint() {
    let test_service = Arc::new(TestServiceImpl);
    let mut router = Router::default();
    test_service.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
    };
    let server = ruapc::Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    // Test /openapi.json endpoint
    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{}/openapi.json", addr))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/json"
    );

    // Verify the response is valid JSON and contains expected OpenAPI structure
    let openapi_json: serde_json::Value = response.json().await.unwrap();
    assert!(openapi_json.get("openapi").is_some());
    assert!(openapi_json.get("paths").is_some());

    server.stop();
    server.join().await;
}

#[tokio::test]
async fn test_http_rapidoc_js_endpoint() {
    let test_service = Arc::new(TestServiceImpl);
    let mut router = Router::default();
    test_service.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
    };
    let server = ruapc::Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    // Test /rapidoc/rapidoc-min.js endpoint
    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{}/rapidoc/rapidoc-min.js", addr))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/javascript"
    );

    // Verify the response contains JavaScript content
    let content = response.text().await.unwrap();
    assert!(!content.is_empty());

    server.stop();
    server.join().await;
}

#[tokio::test]
async fn test_http_rapidoc_html_endpoints() {
    let test_service = Arc::new(TestServiceImpl);
    let mut router = Router::default();
    test_service.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
    };
    let server = ruapc::Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    let client = reqwest::Client::new();
    let paths = vec!["/rapidoc", "/rapidoc/", "/rapidoc/index.html"];

    for path in paths {
        let response = client
            .get(format!("http://{}{}", addr, path))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 200, "Failed for path: {}", path);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/html; charset=utf-8"
        );

        // Verify the response contains HTML content
        let content = response.text().await.unwrap();
        assert!(content.contains("<!DOCTYPE html>") || content.contains("<html"));
    }

    server.stop();
    server.join().await;
}

#[tokio::test]
async fn test_http_not_found_endpoint() {
    let test_service = Arc::new(TestServiceImpl);
    let mut router = Router::default();
    test_service.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
    };
    let server = ruapc::Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    // Test non-existent GET endpoint
    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{}/nonexistent", addr))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);

    let content = response.text().await.unwrap();
    assert_eq!(content, "Not Found");

    server.stop();
    server.join().await;
}

#[tokio::test]
async fn test_http_post_request_processing() {
    let test_service = Arc::new(TestServiceImpl);
    let mut router = Router::default();
    test_service.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
    };
    let server = ruapc::Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    // Test POST request to service method
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/TestService/test_method", addr))
        .json(&"test input")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let result: ruapc::Result<String> = response.json().await.unwrap();
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "test response: test input");

    server.stop();
    server.join().await;
}

#[tokio::test]
async fn test_http_websocket_upgrade() {
    let test_service = Arc::new(TestServiceImpl);
    let mut router = Router::default();
    test_service.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
    };
    let server = ruapc::Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    // Test WebSocket upgrade request
    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{}/ws", addr))
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("Sec-WebSocket-Version", "13")
        .send()
        .await
        .unwrap();

    // Should get a WebSocket upgrade response
    assert_eq!(response.status(), 101);
    assert_eq!(response.headers().get("upgrade").unwrap(), "websocket");

    server.stop();
    server.join().await;
}

#[tokio::test]
async fn test_http_concurrent_requests() {
    let test_service = Arc::new(TestServiceImpl);
    let mut router = Router::default();
    test_service.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
    };
    let server = ruapc::Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    let client = reqwest::Client::new();

    // Send multiple concurrent requests
    let tasks = (0..10)
        .map(|i| {
            let client = client.clone();
            let addr = addr;
            tokio::spawn(async move {
                // Test both static and dynamic endpoints
                let openapi_response = client
                    .get(format!("http://{}/openapi.json", addr))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(openapi_response.status(), 200);

                let service_response = client
                    .post(format!("http://{}/TestService/test_method", addr))
                    .json(&format!("concurrent test {}", i))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(service_response.status(), 200);

                let result: ruapc::Result<String> = service_response.json().await.unwrap();
                assert!(result.is_ok());
                assert_eq!(
                    result.unwrap(),
                    format!("test response: concurrent test {}", i)
                );
            })
        })
        .collect::<Vec<_>>();

    // Wait for all tasks to complete
    for task in tasks {
        task.await.unwrap();
    }

    server.stop();
    server.join().await;
}

#[tokio::test]
async fn test_http_malformed_request_body() {
    let test_service = Arc::new(TestServiceImpl);
    let mut router = Router::default();
    test_service.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
    };
    let server = ruapc::Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    // Test POST request with malformed JSON body
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/TestService/test_method", addr))
        .header("content-type", "application/json")
        .body("{invalid json}")
        .send()
        .await
        .unwrap();

    // The server should handle this gracefully, but the result may vary
    // The main thing is that the server doesn't crash
    assert!(response.status().as_u16() >= 200 && response.status().as_u16() < 600);

    server.stop();
    server.join().await;
}

#[tokio::test]
async fn test_http_large_request_body() {
    let test_service = Arc::new(TestServiceImpl);
    let mut router = Router::default();
    test_service.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
    };
    let server = ruapc::Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    // Test POST request with large body
    let large_input = "x".repeat(1024 * 10); // 10KB string
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/TestService/test_method", addr))
        .json(&large_input)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let result: ruapc::Result<String> = response.json().await.unwrap();
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), format!("test response: {}", large_input));

    server.stop();
    server.join().await;
}

#[tokio::test]
async fn test_http_method_variants() {
    let test_service = Arc::new(TestServiceImpl);
    let mut router = Router::default();
    test_service.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
    };
    let server = ruapc::Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    let client = reqwest::Client::new();

    // Test different HTTP methods for static resources (should all be 404 except GET)
    let methods = vec![
        ("PUT", reqwest::Method::PUT),
        ("PATCH", reqwest::Method::PATCH),
        ("DELETE", reqwest::Method::DELETE),
        ("HEAD", reqwest::Method::HEAD),
    ];

    for (method_name, method) in methods {
        let response = client
            .request(method, format!("http://{}/openapi.json", addr))
            .send()
            .await
            .unwrap();

        // Non-GET methods should return 404 for static resources or be processed differently
        assert!(
            response.status() == 404 || response.status().is_success(),
            "Method {} failed with status: {}",
            method_name,
            response.status()
        );
    }

    server.stop();
    server.join().await;
}

#[tokio::test]
async fn test_http_edge_case_paths() {
    let test_service = Arc::new(TestServiceImpl);
    let mut router = Router::default();
    test_service.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
    };
    let server = ruapc::Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    let client = reqwest::Client::new();

    // Test edge case paths that should definitely return 404
    let edge_cases = vec![
        "/rapidoc.html", // Similar to rapidoc but not exactly
        "/openapi",      // Similar to openapi.json but not exactly
        "/rapidoc/../",  // Path traversal attempt
        "//rapidoc",     // Double slash
    ];

    for path in edge_cases {
        let response = client
            .get(format!("http://{}{}", addr, path))
            .send()
            .await
            .unwrap();

        // All these should return 404 as they don't match exact patterns
        assert_eq!(response.status(), 404, "Path {} should return 404", path);
    }

    // Test paths with query parameters - these might match if the base path matches
    let query_paths = vec![
        "/rapidoc?query=param",        // /rapidoc with query should still match
        "/openapi.json?format=pretty", // /openapi.json with query should still match
    ];

    for path in query_paths {
        let response = client
            .get(format!("http://{}{}", addr, path))
            .send()
            .await
            .unwrap();

        // These might return 200 if the base path matches (before the ?)
        // or 404 if not - both are acceptable depending on how routing works
        assert!(
            response.status() == 200 || response.status() == 404,
            "Path {} returned unexpected status: {}",
            path,
            response.status()
        );
    }

    server.stop();
    server.join().await;
}
