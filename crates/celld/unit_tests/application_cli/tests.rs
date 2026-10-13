use super::*;
use axum::{
    extract::{Json, Query},
    routing::{get, post},
    Router,
};
use std::collections::HashMap;

#[tokio::test]
async fn create_posts_without_listing_and_list_keeps_one_encoded_page() {
    let router = Router::new()
        .route(
            "/v1/applications/solo",
            post(|Json(body): Json<Value>| async move {
                assert_eq!(body, json!({"revision":{"token":null}}));
                Json(json!({"created":"solo"}))
            }),
        )
        .route(
            "/v1/applications",
            get(|Query(query): Query<HashMap<String, String>>| async move {
                assert_eq!(query.get("limit").map(String::as_str), Some("1"));
                assert_eq!(
                    query.get("cursor").map(String::as_str),
                    Some("opaque&/?+= token")
                );
                Json(json!({"applications":[],"next_cursor":"later"}))
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let token = "a".repeat(40);
    let args = |command: &str, extra: &[&str]| {
        [command, "--api", &api]
            .into_iter()
            .chain(extra.iter().copied())
            .map(str::to_owned)
            .collect()
    };
    assert_eq!(
        execute(args("create", &["--app", "solo"]), &token)
            .await
            .unwrap()["created"],
        "solo"
    );
    assert_eq!(
        execute(
            args("list", &["--limit", "1", "--cursor", "opaque&/?+= token"]),
            &token
        )
        .await
        .unwrap()["next_cursor"],
        "later"
    );
    assert!(execute(
        args("create", &["--tenant", "one", "--app", "solo"]),
        &token
    )
    .await
    .is_err());
    assert!(
        execute(args("get", &["--app", "solo", "--limit", "1"]), &token)
            .await
            .is_err()
    );
    server.abort();
}
