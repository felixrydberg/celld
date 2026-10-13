use super::*;

const ACCESS_KEY: &str = "celldtest";
const SECRET_KEY: &str = "celldtest-secret-key";
const MINIO_IMAGE: &str =
    "chainguard/minio@sha256:e7ca559d9f7c0b5f24f5f669bb92f40f3ca88d56273b808bf3a7c116c17d2ffa";
const FLEET: &str = "00000000-0000-4000-8000-000000000000";
const ONE: &str = "11111111-1111-4111-8111-111111111111";
const TWO: &str = "22222222-2222-4222-8222-222222222222";

async fn docker(args: &[&str]) -> String {
    let output = timeout(
        Duration::from_secs(60),
        Command::new("docker")
            .args(args)
            .env(
                "DOCKER_HOST",
                std::env::var("DOCKER_HOST")
                    .unwrap_or_else(|_| "unix:///run/celld-docker.sock".into()),
            )
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("Docker command timed out")
    .unwrap();
    assert!(
        output.status.success(),
        "docker {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

struct Minio {
    name: String,
    endpoint: String,
    directory: PathBuf,
}

impl Drop for Minio {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.name])
            .env(
                "DOCKER_HOST",
                std::env::var("DOCKER_HOST")
                    .unwrap_or_else(|_| "unix:///run/celld-docker.sock".into()),
            )
            .stdin(Stdio::null())
            .output();
    }
}

impl Minio {
    async fn new() -> Self {
        let run = std::env::var("CELLD_APPLICATION_RUN_ID")
            .expect("run through the bounded E2E reporter");
        let artifacts = PathBuf::from(
            std::env::var_os("CELLD_APPLICATION_NODE_ARTIFACTS")
                .expect("E2E artifact directory required"),
        );
        let directory = tempfile::Builder::new()
            .prefix("t32-minio-")
            .tempdir_in(artifacts)
            .unwrap()
            .keep();
        std::fs::create_dir(directory.join("data")).unwrap();
        let port = free_port();
        // Register ownership before Docker starts; panic/drop and the reporter
        // both remove this exact run label, including a partially started fixture.
        let fixture = Self {
            name: format!("{run}-t32-minio"),
            endpoint: format!("http://127.0.0.1:{port}"),
            directory,
        };
        if let Ok(configured) = std::env::var("CELLD_CONTAINER_MINIO_IMAGE") {
            assert_eq!(
                configured, MINIO_IMAGE,
                "T32 requires the pinned MinIO digest"
            );
        }
        docker(&[
            "run",
            "-d",
            "--name",
            &fixture.name,
            "--user",
            "root",
            "--label",
            &format!("celld-t29-run={run}"),
            "--publish",
            &format!("127.0.0.1:{port}:9000"),
            "--env",
            &format!("MINIO_ROOT_USER={ACCESS_KEY}"),
            "--env",
            &format!("MINIO_ROOT_PASSWORD={SECRET_KEY}"),
            "--volume",
            &format!("{}:/data", fixture.directory.join("data").display()),
            MINIO_IMAGE,
            "server",
            "/data",
            "--address",
            ":9000",
        ])
        .await;
        timeout(Duration::from_secs(60), async {
            loop {
                if matches!(reqwest::get(format!("{}/minio/health/live", fixture.endpoint)).await, Ok(reply) if reply.status() == 200) { break; }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }).await.expect("MinIO did not become ready");
        fixture
    }

    /// An independent S3 oracle. curl signs and sends the literal URI, without
    /// the Rust client's Path conversion or URL dot-segment normalization.
    async fn oracle(&self, method: &str, bucket: &str, key: &str, body: &[u8]) -> (u16, Vec<u8>) {
        self.oracle_query(method, bucket, key, body, "").await
    }

    async fn oracle_query(
        &self,
        method: &str,
        bucket: &str,
        key: &str,
        body: &[u8],
        query: &str,
    ) -> (u16, Vec<u8>) {
        let input = self.directory.join("oracle-input");
        let output = self.directory.join("oracle-output");
        std::fs::write(&input, body).unwrap();
        let uri = format!("{}/{bucket}/{}{query}", self.endpoint, encode_key(key));
        let mut command = Command::new("curl");
        command
            .args([
                "--silent",
                "--show-error",
                "--path-as-is",
                "--max-time",
                "30",
                "--aws-sigv4",
                "aws:amz:us-east-1:s3",
                "--user",
                &format!("{ACCESS_KEY}:{SECRET_KEY}"),
                "--request",
                method,
                "--write-out",
                "%{http_code}",
            ])
            .arg("--output")
            .arg(&output)
            .arg(&uri)
            .kill_on_drop(true);
        if method == "PUT" {
            command
                .args([
                    "--header",
                    "Content-Type: text/plain",
                    "--header",
                    "x-amz-meta-oracle: external",
                ])
                .arg("--data-binary")
                .arg(format!("@{}", input.display()));
        }
        let reply = command.output().await.unwrap();
        assert!(
            reply.status.success(),
            "curl oracle {method} {key:?}: {}",
            String::from_utf8_lossy(&reply.stderr)
        );
        let status = String::from_utf8(reply.stdout).unwrap().parse().unwrap();
        let bytes = std::fs::read(output).unwrap();
        let mut records = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join("oracle.jsonl"))
            .unwrap();
        writeln!(records, "{}", json!({"method":method,"bucket":bucket,"key":key,"status":status,"response_bytes":bytes.len()})).unwrap();
        (status, bytes)
    }

    async fn listed_keys(&self, bucket: &str) -> Vec<String> {
        let (status, body) = self
            .oracle_query("GET", bucket, "", b"", "?list-type=2&encoding-type=url")
            .await;
        assert_eq!(status, 200, "independent bucket listing failed");
        let xml = String::from_utf8(body).unwrap();
        xml.split("<Key>")
            .skip(1)
            .map(|tail| tail.split_once("</Key>").unwrap().0.to_string())
            .collect()
    }
}

async fn fleet_cli(minio: &Minio, args: &[&str]) -> std::process::Output {
    timeout(
        Duration::from_secs(120),
        Command::new(env!("CARGO_BIN_EXE_celld"))
            .args(args)
            .args([
                "--bucket",
                &format!("{FLEET}/fleet-prefix"),
                "--endpoint",
                &minio.endpoint,
                "--region",
                "us-east-1",
            ])
            .env_remove("CELLD_INTERNAL_DEV_STORE")
            .env_remove("CELLD_TEST_BUCKET")
            .env_remove("AWS_SESSION_TOKEN")
            .env("AWS_ACCESS_KEY_ID", ACCESS_KEY)
            .env("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
            .env("AWS_REGION", "us-east-1")
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("fixture CLI timed out")
    .unwrap()
}

fn refused_fleet(reply: &std::process::Output) {
    assert!(
        !reply.status.success(),
        "fleet bucket unexpectedly permitted"
    );
    assert!(
        String::from_utf8_lossy(&reply.stderr).contains("fleet bucket"),
        "wrong fleet refusal: {}",
        String::from_utf8_lossy(&reply.stderr)
    );
}

fn cli_ok(reply: std::process::Output) -> Vec<u8> {
    assert!(
        reply.status.success(),
        "fixture CLI: {}",
        String::from_utf8_lossy(&reply.stderr)
    );
    reply.stdout
}

fn encode_key(key: &str) -> String {
    let mut encoded = String::new();
    for byte in key.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~/".contains(&byte) {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

async fn raw(node: &Node, host: &str, operation: Value) -> (u16, Value) {
    let reply = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/raw", node.public))
        .header("host", host)
        .json(&operation)
        .timeout(Duration::from_secs(65))
        .send()
        .await
        .unwrap();
    let status = reply.status().as_u16();
    let body = reply.text().await.unwrap();
    (
        status,
        serde_json::from_str(&body).unwrap_or_else(|_| panic!("raw {status}: {body}")),
    )
}

async fn raw_ok(node: &Node, host: &str, operation: Value) -> Value {
    let (status, body) = raw(node, host, operation).await;
    assert_eq!(status, 200, "raw R2: {body}");
    body
}

async fn provision(h: &Harness, app: &str, bucket: &str, label: &str) -> String {
    h.create(app).await;
    h.resource(app, "r2", bucket, json!({})).await;
    let raw_route = r#"
    if (url.pathname === '/raw') {
      try {
        const {op, key, value, options = {}} = await request.json();
        const bucket = env.BLOBS;
        if (op === 'put') {
          const object = await bucket.put(key, value, options);
          return Response.json(object === null ? null : {key: object.key, etag: object.etag});
        }
        if (op === 'get') {
          const object = await bucket.get(key, options);
          return Response.json(object === null ? null : {key: object.key, text: await object.text(), custom: object.customMetadata, type: object.httpMetadata.contentType});
        }
        if (op === 'head') return Response.json(await bucket.head(key));
        if (op === 'delete') { await bucket.delete(key); return Response.json(true); }
        if (op === 'list') {
          const page = await bucket.list(options);
          return Response.json({keys: page.objects.map(object => object.key), prefixes: page.delimitedPrefixes, cursor: page.cursor, truncated: page.truncated});
        }
        throw new Error('unknown raw operation');
      } catch (error) { return Response.json({error: String(error.message)}, {status: 400}); }
    }
    "#;
    let marker = "    const stub = env.COUNTER.getByName(url.searchParams.get('name'));";
    assert_eq!(NODE_SOURCE.matches(marker).count(), 1);
    let source = NODE_SOURCE.replacen(marker, raw_route, 1);
    let digest = h.source(app, &source).await;
    h.stage(
        app,
        &json!({"manifest_version":1,"application":{"app":app},"id":"one",
        "handlers":[{"id":"main","language":"javascript","entrypoint":"main.mjs",
            "artifact_sha256":digest,"runtime_image_sha256":h.runtime_sha256,"http":true,
            "environment":{"LABEL":label},
            "bindings":{"BLOBS":{"kind":"resource","resource":{"kind":"r2","id":bucket}}},
            "worker_config":{"r2_buckets":[{"binding":"BLOBS","bucket_name":bucket}]}}]}),
    )
    .await;
    h.activate(app, "one").await;
    h.hostname(app, "main").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real public API/CLI, pinned MinIO and Firecracker; use the bounded E2E reporter"]
async fn t32_named_buckets() {
    celld::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    let began = Instant::now();
    timeout(Duration::from_secs(600), async {
        let minio = Minio::new().await;
        for bucket in [FLEET, ONE, TWO] {
            assert_eq!(
                minio.oracle("PUT", bucket, "", b"").await.0,
                200,
                "create fixture bucket {bucket}"
            );
        }
        assert_eq!(
            minio
                .oracle("PUT", FLEET, "fleet-canary", b"fleet untouched")
                .await
                .0,
            200
        );
        let before = minio.listed_keys(FLEET).await;
        assert_eq!(before, ["fleet-canary"]);
        let cli_body = minio.directory.join("cli-body");
        std::fs::write(&cli_body, "CLI wrote this").unwrap();
        refused_fleet(&fleet_cli(&minio, &["r2", "put", FLEET, "fleet-canary", "--path", cli_body.to_str().unwrap()]).await);
        let project = minio.directory.join("fleet-worker");
        std::fs::create_dir(&project).unwrap();
        std::fs::write(project.join("main.mjs"), "export default { fetch() { return new Response('never deployed'); } };\n").unwrap();
        std::fs::write(project.join("wrangler.json"), serde_json::to_vec(&json!({"name":"t32-fleet-refusal","main":"main.mjs","compatibility_date":"2026-03-01","r2_buckets":[{"binding":"BLOBS","bucket_name":FLEET}]})).unwrap()).unwrap();
        refused_fleet(&fleet_cli(&minio, &["deploy", project.to_str().unwrap()]).await);
        assert_eq!(minio.listed_keys(FLEET).await, before, "refused CLI/deploy wrote fleet objects");
        assert_eq!(minio.oracle("GET", FLEET, "fleet-canary", b"").await, (200, b"fleet untouched".to_vec()), "refused CLI overwrote the fleet canary");
        let h = Harness::new_remote(
            "r2",
            true,
            RemoteFleet {
                bucket: format!("{FLEET}/fleet-prefix"),
                endpoint: minio.endpoint.clone(),
                region: "us-east-1".into(),
                access_key: ACCESS_KEY.into(),
                secret_key: SECRET_KEY.into(),
            },
        )
        .await;
        let one = provision(&h, "e2e-one", ONE, "one").await;
        let two = provision(&h, "e2e-two", TWO, "two").await;
        let shared = provision(&h, "e2e-shared", ONE, "shared").await;
        let invalid = h.json_file(
            "fleet-resource.json",
            &json!({"resource":{"kind":"r2","id":FLEET},"configuration":{}}),
        );
        let refused = h
            .cli_as(
                &h.token,
                "e2e-one",
                "resource",
                &["--file", invalid.to_str().unwrap()],
            )
            .await
            .expect_err("fleet bucket accepted as an R2 resource");
        assert!(
            refused.contains("fleet bucket"),
            "wrong API refusal: {refused}"
        );
        let node = h.start("r2").await;

        // Full existing R2 semantics, on two independently named remote buckets.
        for (host, label) in [(&one, "one"), (&two, "two")] {
            assert_eq!(json(&node, host, "/r2-peek").await, Value::Null);
            let value = json(&node, host, "/r2").await;
            assert_eq!(value["text"], format!("hello {label}"));
            assert_eq!(value["range"], label);
            assert_eq!(value["custom"], json!({"label":label}));
            assert_eq!(value["type"], "text/plain");
            for flag in [
                "unmet",
                "refused",
                "checksumFailed",
                "cancelled",
                "absent",
                "aborted",
                "deleted",
            ] {
                assert_eq!(value[flag], true, "{flag}: {value}");
            }
            assert_eq!(value["stream"], json!([8 * 1024 * 1024 + 1, 7, 7]));
            assert_eq!(value["produced"], 8 * 1024 * 1024 + 1);
            assert_eq!(value["multipart"], "tail");
            assert_eq!(value["pages"], json!([1, true, 2, false]));
        }
        assert_eq!(json(&node, &shared, "/r2-peek").await, "hello one");
        assert_eq!(
            minio.oracle("GET", ONE, "record", b"").await,
            (200, b"hello one".to_vec())
        );
        assert_eq!(
            minio.oracle("GET", TWO, "record", b"").await,
            (200, b"hello two".to_vec())
        );
        raw_ok(
            &node,
            &shared,
            json!({"op":"put","key":"record","value":"shared update"}),
        )
        .await;
        assert_eq!(json(&node, &one, "/r2-peek").await, "shared update");
        assert_eq!(json(&node, &two, "/r2-peek").await, "hello two");
        raw_ok(&node, &one, json!({"op":"delete","key":"record"})).await;

        let cli_key = "interop/object.txt";
        assert_eq!(minio.oracle("PUT", ONE, cli_key, b"External wrote this").await.0, 200);
        let external = raw_ok(&node, &one, json!({"op":"get","key":cli_key})).await;
        assert_eq!(external["text"], "External wrote this");
        assert_eq!(external["custom"], json!({"oracle":"external"}));
        cli_ok(fleet_cli(&minio, &["r2", "put", ONE, cli_key, "--path", cli_body.to_str().unwrap()]).await);
        assert_eq!(raw_ok(&node, &one, json!({"op":"get","key":cli_key})).await["text"], "CLI wrote this");
        assert_eq!(minio.oracle("GET", ONE, cli_key, b"").await, (200, b"CLI wrote this".to_vec()));
        raw_ok(&node, &shared, json!({"op":"put","key":cli_key,"value":"Worker wrote this"})).await;
        assert_eq!(cli_ok(fleet_cli(&minio, &["r2", "get", ONE, cli_key]).await), b"Worker wrote this".to_vec());
        cli_ok(fleet_cli(&minio, &["r2", "delete", ONE, cli_key]).await);
        assert_eq!(minio.oracle("GET", ONE, cli_key, b"").await.0, 404);

        // Retain Celld's existing escaping while changing only the bucket.
        raw_ok(&node, &one, json!({"op":"put","key":"a//b","value":"escaped"})).await;
        assert_eq!(minio.oracle("GET", ONE, "a/%/b", b"").await, (200, b"escaped".to_vec()));
        assert!(matches!(
            minio.oracle("GET", ONE, "a//b", b"").await.0,
            400 | 404
        ));
        assert_eq!(raw_ok(&node, &shared, json!({"op":"get","key":"a//b"})).await["text"], "escaped");
        assert_eq!(raw_ok(&node, &two, json!({"op":"get","key":"a//b"})).await, Value::Null);
        raw_ok(&node, &one, json!({"op":"delete","key":"a//b"})).await;

        let mut supported = vec!["a".to_string(), vec!["x".repeat(204); 5].join("/")];
        for key in &supported {
            raw_ok(&node, &one, json!({"op":"put","key":key,"value":"bounds"})).await;
            assert_eq!(minio.oracle("GET", ONE, key, b"").await, (200, b"bounds".to_vec()));
        }
        for key in [String::new(), "x".repeat(1025), "é".repeat(513)] {
            let (status, body) = raw(&node, &one, json!({"op":"put","key":key,"value":"invalid"})).await;
            assert_eq!(status, 400, "R2 UTF-8 byte limit not enforced");
            assert!(body["error"].as_str().unwrap().contains("1 to 1024 bytes"), "key bound reached the provider instead of the R2 validator: {body}");
        }
        let mut listed = Vec::new();
        let mut cursor = Value::Null;
        loop {
            let page = raw_ok(
                &node,
                &one,
                json!({"op":"list","options":{"limit":1,"cursor":cursor}}),
            )
            .await;
            listed.extend(
                page["keys"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|key| key.as_str().unwrap().to_string()),
            );
            if page["truncated"] == false {
                break;
            }
            assert!(
                !page["cursor"].is_null() && page["cursor"] != cursor,
                "nonadvancing raw cursor: {page}"
            );
            cursor = page["cursor"].clone();
            assert!(
                listed.len() <= supported.len(),
                "raw listing did not terminate"
            );
        }
        supported.sort();
        assert_eq!(
            listed, supported,
            "named-bucket listing lost or duplicated objects"
        );
        raw_ok(&node, &one, json!({"op":"delete","key":supported})).await;
        for key in ["group/one", "group/two", "plain"] {
            raw_ok(&node, &one, json!({"op":"put","key":key,"value":key})).await;
        }
        let page = raw_ok(
            &node,
            &one,
            json!({"op":"list","options":{"delimiter":"/","limit":1}}),
        )
        .await;
        assert_eq!(page["prefixes"], json!(["group/"]));
        assert!(page["keys"].as_array().unwrap().is_empty());
        assert_eq!(page["truncated"], true);
        let next = raw_ok(
            &node,
            &one,
            json!({"op":"list","options":{"delimiter":"/","cursor":page["cursor"]}}),
        )
        .await;
        assert_eq!(next["keys"], json!(["plain"]));
        assert_eq!(next["truncated"], false);
        assert_eq!(
            minio.oracle("GET", FLEET, "fleet-canary", b"").await,
            (200, b"fleet untouched".to_vec())
        );
        assert_eq!(minio.oracle("GET", ONE, "r2/record", b"").await.0, 404);
        node.drain().await;
        docker(&["rm", "-f", &minio.name]).await;
    })
    .await
    .expect("named R2 bucket scenario exceeded 600 seconds");
    passed("r2::t32_named_buckets", began);
}
