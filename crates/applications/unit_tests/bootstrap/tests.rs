use super::*;

#[test]
fn private_bootstrap_bounds_identity_and_resolution() {
    let mut value: Bootstrap = serde_json::from_value(serde_json::json!({
        "version":1, "application":{"app":"one"},
        "deployment":"v1", "manifest_sha256":"a".repeat(64),
        "handler":{"id":"main","language":"javascript","entrypoint":"main.mjs",
            "artifact_sha256":"b".repeat(64), "runtime_image_sha256":"c".repeat(64),
            "environment":{"PRIVATE_TOKEN":"resolved-value"}}
    }))
    .unwrap();
    let bytes = value.encode_disk().unwrap();
    let observed = Bootstrap::decode_disk(&bytes).unwrap();
    assert_eq!(observed.application, value.application);
    assert_eq!(observed.handler.environment, value.handler.environment);
    assert!(Bootstrap::decode_disk(&bytes[..DISK_BYTES - 1]).is_err());
    let mut trailing = bytes.clone();
    trailing[DISK_BYTES - 1] = b'x';
    assert!(Bootstrap::decode_disk(&trailing).is_err());
    value.handler.secrets.insert(
        "SECRET".to_owned().try_into().unwrap(),
        "missing".to_owned().try_into().unwrap(),
    );
    assert!(value.encode_disk().is_err());
    value.handler.secrets.clear();
    value.handler.entrypoint = "../outside.mjs".into();
    assert!(value.encode_disk().is_err());
    value.handler.entrypoint = "main.mjs".into();
    value.manifest_sha256 = "z".repeat(64);
    assert!(value.encode_disk().is_err());
    value.manifest_sha256 = "a".repeat(64);
    value.version = 2;
    assert!(value.encode_disk().is_err());
    value.version = 1;
    value.handler.environment.insert(
        "LARGE".to_owned().try_into().unwrap(),
        "x".repeat(DISK_BYTES),
    );
    assert!(value.encode_disk().is_err());
}
