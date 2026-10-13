use super::*;

#[test]
fn handlers_name_the_pyodide_release_they_need() {
    let handler = |config: serde_json::Value| -> Handler {
        serde_json::from_value(serde_json::json!({
            "id": "main", "language": "javascript", "entrypoint": "index.js",
            "artifact_sha256": "a".repeat(64), "runtime_image_sha256": "b".repeat(64),
            "worker_config": config,
        }))
        .unwrap()
    };
    assert!(!needs_runtime(&handler(serde_json::json!({}))).unwrap());
    assert!(needs_runtime(&handler(handler_config())).unwrap());
    assert!(needs_runtime(&handler(serde_json::json!({"pyodide": "0.1.0"}))).is_err());
}

#[test]
fn pinned_scripts_get_the_names_esbuild_gives_a_bundled_copy() {
    let wrapped =
        String::from_utf8(in_bundle_scope(b"var seen = typeof process;").unwrap()).unwrap();
    assert!(wrapped.starts_with("import { RUNTIME_URL, WebAssembly, fetch, self,"));
    assert!(
        wrapped.contains("const process = undefined;")
            && wrapped.ends_with("var seen = typeof process;")
    );
    assert!(in_bundle_scope(&[0xff]).is_err());
}
