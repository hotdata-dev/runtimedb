use anyhow::Result;
use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use rand::RngCore;
use runtimedb::http::app_server::{
    AppServer, PATH_QUERY, PATH_SAVED_QUERIES, PATH_SAVED_QUERY, PATH_SAVED_QUERY_EXECUTE,
    PATH_SAVED_QUERY_VERSIONS,
};
use runtimedb::RuntimeEngine;
use serde_json::json;
use tempfile::TempDir;
use tower::util::ServiceExt;

fn generate_test_secret_key() -> String {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    STANDARD.encode(key)
}

async fn setup_test() -> Result<(Router, TempDir)> {
    let temp_dir = tempfile::tempdir()?;
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path())
        .secret_key(generate_test_secret_key())
        .build()
        .await?;
    let app = AppServer::new(engine);
    Ok((app.router, temp_dir))
}

async fn send_request(router: &Router, request: Request<Body>) -> Result<axum::response::Response> {
    Ok(router.clone().oneshot(request).await?)
}

async fn response_json(response: axum::response::Response) -> Result<serde_json::Value> {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    Ok(serde_json::from_slice(&body)?)
}

async fn create_saved_query(router: &Router, name: &str, sql: &str) -> Result<serde_json::Value> {
    let response = send_request(
        router,
        Request::builder()
            .method("POST")
            .uri(PATH_SAVED_QUERIES)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "name": name, "sql": sql }),
            )?))?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    response_json(response).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_saved_query() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let json = create_saved_query(&router, "my query", "SELECT 1").await?;

    assert!(json["id"].as_str().unwrap().starts_with("svqr"));
    assert_eq!(json["name"], "my query");
    assert_eq!(json["latest_version"], 1);
    assert_eq!(json["sql"], "SELECT 1");
    assert!(json["sql_hash"].is_string());
    assert!(json["created_at"].is_string());
    assert!(json["updated_at"].is_string());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_saved_query_empty_name_rejected() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let response = send_request(
        &router,
        Request::builder()
            .method("POST")
            .uri(PATH_SAVED_QUERIES)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "name": "  ", "sql": "SELECT 1" }),
            )?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_saved_query_empty_sql_rejected() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let response = send_request(
        &router,
        Request::builder()
            .method("POST")
            .uri(PATH_SAVED_QUERIES)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "name": "q", "sql": "" }),
            )?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_saved_query() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "get test", "SELECT 42").await?;
    let id = created["id"].as_str().unwrap();

    let uri = PATH_SAVED_QUERY.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("GET")
            .uri(&uri)
            .body(Body::empty())?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;

    assert_eq!(json["id"], id);
    assert_eq!(json["name"], "get test");
    assert_eq!(json["sql"], "SELECT 42");
    assert_eq!(json["latest_version"], 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_saved_query_not_found() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let response = send_request(
        &router,
        Request::builder()
            .method("GET")
            .uri("/v1/queries/svqr_nonexistent")
            .body(Body::empty())?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_saved_queries() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    create_saved_query(&router, "q1", "SELECT 1").await?;
    create_saved_query(&router, "q2", "SELECT 2").await?;
    create_saved_query(&router, "q3", "SELECT 3").await?;

    let response = send_request(
        &router,
        Request::builder()
            .method("GET")
            .uri(format!("{}?limit=2&offset=0", PATH_SAVED_QUERIES))
            .body(Body::empty())?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;

    assert_eq!(json["count"], 2);
    assert_eq!(json["limit"], 2);
    assert!(json["has_more"].as_bool().unwrap());
    let queries = json["queries"].as_array().unwrap();
    assert_eq!(queries.len(), 2);
    // Results are ordered by name ASC
    assert_eq!(queries[0]["name"], "q1");
    assert_eq!(queries[1]["name"], "q2");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_saved_query_creates_new_version() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "versioned", "SELECT 1").await?;
    let id = created["id"].as_str().unwrap();

    let uri = PATH_SAVED_QUERY.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "sql": "SELECT 2" }),
            )?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;

    assert_eq!(json["latest_version"], 2);
    assert_eq!(json["sql"], "SELECT 2");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_saved_query_with_rename() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "old name", "SELECT 1").await?;
    let id = created["id"].as_str().unwrap();

    let uri = PATH_SAVED_QUERY.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "name": "new name", "sql": "SELECT 2" }),
            )?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;

    assert_eq!(json["name"], "new name");
    assert_eq!(json["sql"], "SELECT 2");
    assert_eq!(json["latest_version"], 2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_saved_query_not_found() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let response = send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri("/v1/queries/svqr_nonexistent")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "sql": "SELECT 1" }),
            )?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_saved_query() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "delete me", "SELECT 1").await?;
    let id = created["id"].as_str().unwrap();

    let uri = PATH_SAVED_QUERY.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("DELETE")
            .uri(&uri)
            .body(Body::empty())?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Verify it's gone
    let response = send_request(
        &router,
        Request::builder()
            .method("GET")
            .uri(&uri)
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_saved_query_not_found() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let response = send_request(
        &router,
        Request::builder()
            .method("DELETE")
            .uri("/v1/queries/svqr_nonexistent")
            .body(Body::empty())?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_saved_query_versions() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "versioned", "SELECT 1").await?;
    let id = created["id"].as_str().unwrap();

    // Create version 2
    let uri = PATH_SAVED_QUERY.replace("{id}", id);
    send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "sql": "SELECT 2" }),
            )?))?,
    )
    .await?;

    // Create version 3
    send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "sql": "SELECT 3" }),
            )?))?,
    )
    .await?;

    // List versions
    let versions_uri = PATH_SAVED_QUERY_VERSIONS.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("GET")
            .uri(&versions_uri)
            .body(Body::empty())?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;

    assert_eq!(json["saved_query_id"], id);
    assert!(json["has_more"].is_boolean());
    let versions = json["versions"].as_array().unwrap();
    assert_eq!(versions.len(), 3);
    // Ordered by version DESC (newest first)
    assert_eq!(versions[0]["version"], 3);
    assert_eq!(versions[0]["sql"], "SELECT 3");
    assert_eq!(versions[2]["version"], 1);
    assert_eq!(versions[2]["sql"], "SELECT 1");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_versions_not_found() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let response = send_request(
        &router,
        Request::builder()
            .method("GET")
            .uri("/v1/queries/svqr_nonexistent/versions")
            .body(Body::empty())?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_saved_query_latest() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "exec test", "SELECT 1 as num").await?;
    let id = created["id"].as_str().unwrap();

    let uri = PATH_SAVED_QUERY_EXECUTE.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("POST")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&json!({}))?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;

    assert_eq!(json["columns"], json!(["num"]));
    assert_eq!(json["row_count"], 1);
    assert_eq!(json["rows"][0][0], 1);
    assert!(json["query_run_id"].is_string());
    assert!(json["execution_time_ms"].is_number());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_saved_query_pinned_version() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "pin test", "SELECT 1 as v").await?;
    let id = created["id"].as_str().unwrap();

    // Update to version 2
    let update_uri = PATH_SAVED_QUERY.replace("{id}", id);
    send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri(&update_uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "sql": "SELECT 2 as v" }),
            )?))?,
    )
    .await?;

    // Execute pinned to version 1
    let exec_uri = PATH_SAVED_QUERY_EXECUTE.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("POST")
            .uri(&exec_uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&json!({ "version": 1 }))?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;

    // Should execute version 1's SQL (SELECT 1)
    assert_eq!(json["rows"][0][0], 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_saved_query_not_found() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let response = send_request(
        &router,
        Request::builder()
            .method("POST")
            .uri("/v1/queries/svqr_nonexistent/execute")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&json!({}))?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_run_linkage_on_saved_query_execution() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "linkage test", "SELECT 1 as x").await?;
    let id = created["id"].as_str().unwrap();

    // Execute the saved query
    let exec_uri = PATH_SAVED_QUERY_EXECUTE.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("POST")
            .uri(&exec_uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&json!({}))?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let exec_json = response_json(response).await?;
    let query_run_id = exec_json["query_run_id"].as_str().unwrap();

    // Check the query run via the query-runs list endpoint
    let response = send_request(
        &router,
        Request::builder()
            .method("GET")
            .uri("/query-runs?limit=10")
            .body(Body::empty())?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let list_json = response_json(response).await?;

    let runs = list_json["query_runs"].as_array().unwrap();
    let run = runs.iter().find(|r| r["id"] == query_run_id).unwrap();

    assert_eq!(run["saved_query_id"], id);
    assert_eq!(run["saved_query_version"], 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ad_hoc_query_run_has_null_linkage() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    // Execute an ad-hoc query
    let response = send_request(
        &router,
        Request::builder()
            .method("POST")
            .uri(PATH_QUERY)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "sql": "SELECT 1" }),
            )?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let exec_json = response_json(response).await?;
    let query_run_id = exec_json["query_run_id"].as_str().unwrap();

    // Check the query run
    let response = send_request(
        &router,
        Request::builder()
            .method("GET")
            .uri("/query-runs?limit=10")
            .body(Body::empty())?,
    )
    .await?;

    let list_json = response_json(response).await?;

    let runs = list_json["query_runs"].as_array().unwrap();
    let run = runs.iter().find(|r| r["id"] == query_run_id).unwrap();

    assert!(run["saved_query_id"].is_null());
    assert!(run["saved_query_version"].is_null());
    // Ad-hoc queries should still have sql_text and a snapshot
    assert!(run["sql_text"].is_string());
    assert!(run["snapshot_id"].is_string());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_noop_update_preserves_version() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "noop test", "SELECT 1").await?;
    let id = created["id"].as_str().unwrap();

    // Update with the exact same SQL and no name change
    let uri = PATH_SAVED_QUERY.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "sql": "SELECT 1" }),
            )?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;

    // Version should not increment on no-op
    assert_eq!(json["latest_version"], 1);
    assert_eq!(json["name"], "noop test");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_name_only_rename_preserves_version() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "old name", "SELECT 1").await?;
    let id = created["id"].as_str().unwrap();

    // Rename without providing SQL at all
    let uri = PATH_SAVED_QUERY.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "name": "new name" }),
            )?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;

    // Name changed but version should NOT increment
    assert_eq!(json["name"], "new name");
    assert_eq!(json["latest_version"], 1);

    // Verify only 1 version exists
    let versions_uri = PATH_SAVED_QUERY_VERSIONS.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("GET")
            .uri(&versions_uri)
            .body(Body::empty())?,
    )
    .await?;

    let json = response_json(response).await?;
    let versions = json["versions"].as_array().unwrap();
    assert_eq!(versions.len(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_saved_query_truly_empty_body() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "empty body test", "SELECT 1 as num").await?;
    let id = created["id"].as_str().unwrap();

    // Send POST with completely empty body (no Content-Type header either)
    let uri = PATH_SAVED_QUERY_EXECUTE.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("POST")
            .uri(&uri)
            .body(Body::empty())?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;
    assert_eq!(json["rows"][0][0], 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_saved_query_empty_body_with_content_type() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "ct empty test", "SELECT 1 as num").await?;
    let id = created["id"].as_str().unwrap();

    // Send POST with Content-Type: application/json but empty body
    let uri = PATH_SAVED_QUERY_EXECUTE.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("POST")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::empty())?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;
    assert_eq!(json["rows"][0][0], 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_saved_queries_stable_with_duplicate_names() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    // Create 5 queries all named "dup"
    let mut ids = Vec::new();
    for i in 0..5 {
        let json = create_saved_query(&router, "dup", &format!("SELECT {}", i)).await?;
        ids.push(json["id"].as_str().unwrap().to_string());
    }

    // Fetch page 1 (3 items)
    let response = send_request(
        &router,
        Request::builder()
            .method("GET")
            .uri(format!("{}?limit=3&offset=0", PATH_SAVED_QUERIES))
            .body(Body::empty())?,
    )
    .await?;
    let json = response_json(response).await?;
    assert_eq!(json["count"], 3);
    assert!(json["has_more"].as_bool().unwrap());
    let page1: Vec<String> = json["queries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|q| q["id"].as_str().unwrap().to_string())
        .collect();

    // Fetch page 2 (2 items)
    let response = send_request(
        &router,
        Request::builder()
            .method("GET")
            .uri(format!("{}?limit=3&offset=3", PATH_SAVED_QUERIES))
            .body(Body::empty())?,
    )
    .await?;
    let json = response_json(response).await?;
    assert_eq!(json["count"], 2);
    assert!(!json["has_more"].as_bool().unwrap());
    let page2: Vec<String> = json["queries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|q| q["id"].as_str().unwrap().to_string())
        .collect();

    // All pages should have distinct IDs (no duplicates, no skips)
    let mut all_ids: Vec<String> = page1.into_iter().chain(page2).collect();
    all_ids.sort();
    all_ids.dedup();
    assert_eq!(all_ids.len(), 5);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_empty_body_rejected() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "empty update", "SELECT 1").await?;
    let id = created["id"].as_str().unwrap();

    // Send update with neither name nor sql
    let uri = PATH_SAVED_QUERY.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&json!({}))?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_saved_query_nonexistent_version() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "version test", "SELECT 1 as num").await?;
    let id = created["id"].as_str().unwrap();

    // Try to execute version 99 which doesn't exist
    let uri = PATH_SAVED_QUERY_EXECUTE.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("POST")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "version": 99 }),
            )?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_saved_query_versions_pagination() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "paginated", "SELECT 1").await?;
    let id = created["id"].as_str().unwrap();

    // Create versions 2..5
    let update_uri = PATH_SAVED_QUERY.replace("{id}", id);
    for i in 2..=5 {
        send_request(
            &router,
            Request::builder()
                .method("PUT")
                .uri(&update_uri)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(
                    &json!({ "sql": format!("SELECT {}", i) }),
                )?))?,
        )
        .await?;
    }

    // Page 1: limit=3, offset=0 → versions 5, 4, 3
    let versions_uri = PATH_SAVED_QUERY_VERSIONS.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("GET")
            .uri(format!("{}?limit=3&offset=0", versions_uri))
            .body(Body::empty())?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;
    assert_eq!(json["count"], 3);
    assert!(json["has_more"].as_bool().unwrap());
    let versions = json["versions"].as_array().unwrap();
    assert_eq!(versions[0]["version"], 5);
    assert_eq!(versions[2]["version"], 3);

    // Page 2: limit=3, offset=3 → versions 2, 1
    let response = send_request(
        &router,
        Request::builder()
            .method("GET")
            .uri(format!("{}?limit=3&offset=3", versions_uri))
            .body(Body::empty())?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;
    assert_eq!(json["count"], 2);
    assert!(!json["has_more"].as_bool().unwrap());
    let versions = json["versions"].as_array().unwrap();
    assert_eq!(versions[0]["version"], 2);
    assert_eq!(versions[1]["version"], 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_with_tags_and_description() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let response = send_request(
        &router,
        Request::builder()
            .method("POST")
            .uri(PATH_SAVED_QUERIES)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&json!({
                "name": "tagged query",
                "sql": "SELECT 1",
                "tags": ["analytics", "daily"],
                "description": "A daily analytics query"
            }))?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::CREATED);
    let json = response_json(response).await?;

    assert_eq!(json["tags"], json!(["analytics", "daily"]));
    assert_eq!(json["description"], "A daily analytics query");

    // Verify via GET
    let id = json["id"].as_str().unwrap();
    let uri = PATH_SAVED_QUERY.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("GET")
            .uri(&uri)
            .body(Body::empty())?,
    )
    .await?;
    let json = response_json(response).await?;
    assert_eq!(json["tags"], json!(["analytics", "daily"]));
    assert_eq!(json["description"], "A daily analytics query");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_default_tags_and_description_when_omitted() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let json = create_saved_query(&router, "no meta", "SELECT 1").await?;

    assert_eq!(json["tags"], json!([]));
    assert_eq!(json["description"], "");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_tags_without_new_version() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "tag update", "SELECT 1").await?;
    let id = created["id"].as_str().unwrap();

    // Update tags only (no name or sql change)
    let uri = PATH_SAVED_QUERY.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "tags": ["new-tag"] }),
            )?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;

    assert_eq!(json["tags"], json!(["new-tag"]));
    assert_eq!(json["latest_version"], 1); // No new version

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_classification_present_for_simple_query() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let json = create_saved_query(&router, "classified", "SELECT 1").await?;

    // SELECT 1 should classify as full_scan (no table scan, projection, etc.)
    // The exact category depends on the plan, but it should be present
    assert!(
        json.get("category").is_some(),
        "category field should be present"
    );
    // Boolean classification fields should also be present
    assert!(
        json.get("num_tables").is_some(),
        "num_tables field should be present"
    );
    // has_predicate, has_join etc. are booleans — false values should be present
    assert!(
        json.get("has_predicate").is_some(),
        "has_predicate field should be present"
    );
    assert!(
        json.get("has_join").is_some(),
        "has_join field should be present"
    );
    assert!(
        json.get("has_aggregation").is_some(),
        "has_aggregation field should be present"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_version_listing_includes_category() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "cat versions", "SELECT 1").await?;
    let id = created["id"].as_str().unwrap();

    let versions_uri = PATH_SAVED_QUERY_VERSIONS.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("GET")
            .uri(&versions_uri)
            .body(Body::empty())?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;

    let versions = json["versions"].as_array().unwrap();
    assert_eq!(versions.len(), 1);
    // category should be present on version info
    assert!(
        versions[0].get("category").is_some(),
        "version should include category"
    );
    // boolean fields should be present on version info
    assert!(
        versions[0].get("num_tables").is_some(),
        "version should include num_tables"
    );
    assert!(
        versions[0].get("has_predicate").is_some(),
        "version should include has_predicate"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tags_in_list_response() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    send_request(
        &router,
        Request::builder()
            .method("POST")
            .uri(PATH_SAVED_QUERIES)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&json!({
                "name": "listed",
                "sql": "SELECT 1",
                "tags": ["report"],
                "description": "desc"
            }))?))?,
    )
    .await?;

    let response = send_request(
        &router,
        Request::builder()
            .method("GET")
            .uri(PATH_SAVED_QUERIES)
            .body(Body::empty())?,
    )
    .await?;

    let json = response_json(response).await?;
    let queries = json["queries"].as_array().unwrap();
    assert!(!queries.is_empty());
    assert_eq!(queries[0]["tags"], json!(["report"]));
    assert_eq!(queries[0]["description"], "desc");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_too_many_tags_rejected() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let tags: Vec<String> = (0..51).map(|i| format!("tag-{i}")).collect();
    let response = send_request(
        &router,
        Request::builder()
            .method("POST")
            .uri(PATH_SAVED_QUERIES)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&json!({
                "name": "too many tags",
                "sql": "SELECT 1",
                "tags": tags,
            }))?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = response_json(response).await?;
    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("Too many tags"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_tag_too_long_rejected() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let long_tag = "x".repeat(101);
    let response = send_request(
        &router,
        Request::builder()
            .method("POST")
            .uri(PATH_SAVED_QUERIES)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&json!({
                "name": "long tag",
                "sql": "SELECT 1",
                "tags": [long_tag],
            }))?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = response_json(response).await?;
    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("Tag exceeds maximum length"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_description_too_long_rejected() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let long_desc = "x".repeat(10_001);
    let response = send_request(
        &router,
        Request::builder()
            .method("POST")
            .uri(PATH_SAVED_QUERIES)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&json!({
                "name": "long desc",
                "sql": "SELECT 1",
                "description": long_desc,
            }))?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = response_json(response).await?;
    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("Description exceeds maximum length"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_whitespace_only_tag_rejected() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let response = send_request(
        &router,
        Request::builder()
            .method("POST")
            .uri(PATH_SAVED_QUERIES)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&json!({
                "name": "whitespace tag",
                "sql": "SELECT 1",
                "tags": ["  "],
            }))?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_empty_tags_array_accepted() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let response = send_request(
        &router,
        Request::builder()
            .method("POST")
            .uri(PATH_SAVED_QUERIES)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&json!({
                "name": "empty tags",
                "sql": "SELECT 1",
                "tags": [],
            }))?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::CREATED);
    let json = response_json(response).await?;
    assert_eq!(json["tags"], json!([]));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tags_are_trimmed() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let response = send_request(
        &router,
        Request::builder()
            .method("POST")
            .uri(PATH_SAVED_QUERIES)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&json!({
                "name": "trimmed tags",
                "sql": "SELECT 1",
                "tags": ["  analytics  ", "daily "],
            }))?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::CREATED);
    let json = response_json(response).await?;
    assert_eq!(json["tags"], json!(["analytics", "daily"]));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_set_category_override() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "override cat", "SELECT 1").await?;
    let id = created["id"].as_str().unwrap();
    let original_category = created["category"].as_str().unwrap().to_string();

    // Set category_override to "join"
    let uri = PATH_SAVED_QUERY.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "category_override": "join" }),
            )?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;

    // Category should now be the override value
    assert_eq!(json["category"], "join");
    assert_ne!(
        "join", original_category,
        "override should differ from auto"
    );
    assert_eq!(json["latest_version"], 1); // No new version

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_clear_category_override() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "clear cat", "SELECT 1").await?;
    let id = created["id"].as_str().unwrap();
    let original_category = created["category"].as_str().unwrap().to_string();

    // Set override
    let uri = PATH_SAVED_QUERY.replace("{id}", id);
    send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "category_override": "join" }),
            )?))?,
    )
    .await?;

    // Clear override with explicit null
    let response = send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "category_override": null }),
            )?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;

    // Category should revert to auto-detected
    assert_eq!(json["category"], original_category);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_set_table_size_override() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "table size", "SELECT 1").await?;
    let id = created["id"].as_str().unwrap();

    // Set table_size_override
    let uri = PATH_SAVED_QUERY.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "table_size_override": "large" }),
            )?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;

    assert_eq!(json["table_size"], "large");
    assert_eq!(json["latest_version"], 1); // No new version

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_invalid_category_override_rejected() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "bad cat", "SELECT 1").await?;
    let id = created["id"].as_str().unwrap();

    let uri = PATH_SAVED_QUERY.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "category_override": "not_a_real_category" }),
            )?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = response_json(response).await?;
    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("Unknown category"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_override_carried_forward_on_sql_change() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "carry fwd", "SELECT 1").await?;
    let id = created["id"].as_str().unwrap();

    // Set overrides on version 1
    let uri = PATH_SAVED_QUERY.replace("{id}", id);
    send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "category_override": "join", "table_size_override": "small" }),
            )?))?,
    )
    .await?;

    // Change SQL → creates version 2; overrides should carry forward
    let response = send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "sql": "SELECT 2" }),
            )?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;

    assert_eq!(json["latest_version"], 2);
    assert_eq!(json["category"], "join"); // carried forward
    assert_eq!(json["table_size"], "small"); // carried forward

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_override_without_new_version() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "override only", "SELECT 1").await?;
    let id = created["id"].as_str().unwrap();

    // Set override only (no sql/name/tags/description change)
    let uri = PATH_SAVED_QUERY.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "table_size_override": "medium" }),
            )?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;

    assert_eq!(json["latest_version"], 1); // No version bump
    assert_eq!(json["table_size"], "medium");

    // Verify via versions endpoint
    let versions_uri = PATH_SAVED_QUERY_VERSIONS.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("GET")
            .uri(&versions_uri)
            .body(Body::empty())?,
    )
    .await?;

    let json = response_json(response).await?;
    let versions = json["versions"].as_array().unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0]["table_size"], "medium");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_description_only_update() -> Result<()> {
    let (router, _dir) = setup_test().await?;

    let created = create_saved_query(&router, "desc only", "SELECT 1").await?;
    let id = created["id"].as_str().unwrap();

    let uri = PATH_SAVED_QUERY.replace("{id}", id);
    let response = send_request(
        &router,
        Request::builder()
            .method("PUT")
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(
                &json!({ "description": "updated description" }),
            )?))?,
    )
    .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await?;

    assert_eq!(json["description"], "updated description");
    assert_eq!(json["latest_version"], 1); // No version bump

    Ok(())
}
