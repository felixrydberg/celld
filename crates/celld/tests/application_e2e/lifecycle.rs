use super::*;

const SOURCE: &str = include_str!("../fixtures/t29-lifecycle/main.mjs");

fn deployment(h: &Harness, app: &str, version: &str, artifact: &str, label: &str) -> Value {
    json!({"manifest_version":1, "application":{"app":app}, "id":version,
        "handlers":[{"id":"main", "language":"javascript", "entrypoint":"main.mjs",
            "artifact_sha256":artifact, "runtime_image_sha256":h.runtime_sha256,
            "http":true, "environment":{"LABEL":label}}]})
}

fn denied(result: Result<Value, String>) {
    let error = result.expect_err("tenant boundary unexpectedly permitted the command");
    assert!(
        error.contains("403 Forbidden"),
        "expected authorization refusal: {error}"
    );
}

async fn eventually(node: &Node, host: &str, path: &str, wanted: &str) {
    timeout(Duration::from_secs(65), async {
        loop {
            if request(node, host, path).await == (200, wanted.to_owned()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{host}{path} never served {wanted:?}"));
}

async fn no_vms(node: &Node) {
    let state: Value = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{}/state", node.internal))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    for field in ["vms", "memoryMib", "starting", "ready", "quarantined"] {
        assert_eq!(state["applicationVms"][field], 0, "{state}");
    }
    assert!(node.vm_pids().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real public API/CLI and Firecracker; run with the T29 runner"]
async fn t29_lifecycle() {
    let entire_began = Instant::now();
    celld::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    timeout(Duration::from_secs(300), async {
        let began = Instant::now();
        let h = Harness::new("lifecycle", true).await;
        h.create("e2e-one").await;
        h.create("e2e-two").await;
        h.resource("e2e-one", "kv", "cache", json!({})).await;
        let artifact = h.source("e2e-one", SOURCE).await;
        let mut first = deployment(&h, "e2e-one", "v1", &artifact, "one");
        first["handlers"][0]["bindings"] =
            json!({"CACHE":{"kind":"resource", "resource":{"kind":"kv", "id":"cache"}}});
        let file = h.json_file("first.json", &first);
        denied(
            h.cli_as(
                &h.reader,
                "e2e-one",
                "stage",
                &["--file", file.to_str().unwrap()],
            )
            .await,
        );
        denied(
            h.cli_as(
                &h.other_token,
                "e2e-one",
                "stage",
                &["--file", file.to_str().unwrap()],
            )
            .await,
        );
        h.stage("e2e-one", &first).await;
        denied(
            h.cli_as(&h.reader, "e2e-one", "activate", &["--deployment", "v1"])
                .await,
        );
        h.activate("e2e-one", "v1").await;
        let host = h.hostname("e2e-one", "main").await;
        assert_eq!(
            h.cli_as(&h.reader, "e2e-one", "get", &[]).await.unwrap()["application"]["active"]
                ["id"],
            "v1"
        );
        denied(h.cli_as(&h.other_token, "e2e-one", "get", &[]).await);
        denied(h.cli_as(&h.reader, "e2e-two", "get", &[]).await);
        denied(h.cli_as(&h.reader, "e2e-one", "deactivate", &[]).await);
        h.stage(
            "e2e-two",
            &deployment(&h, "e2e-two", "foreign", &artifact, "two"),
        )
        .await;
        let error = h
            .cli_as(
                &h.other_token,
                "e2e-two",
                "activate",
                &["--deployment", "foreign"],
            )
            .await
            .unwrap_err();
        assert!(error.contains("artifact"), "{error}");
        h.stage(
            "e2e-one",
            &deployment(&h, "e2e-one", "missing", &"0".repeat(64), "missing"),
        )
        .await;
        let error = h
            .cli_as(
                &h.token,
                "e2e-one",
                "activate",
                &["--deployment", "missing"],
            )
            .await
            .unwrap_err();
        assert!(error.contains("artifact"), "{error}");
        assert_eq!(
            h.cli("e2e-one", "get", &[]).await["application"]["active"]["id"],
            "v1"
        );
        passed("tenant-boundaries-and-artifact-refusals", began);

        let node = h.start("lifecycle").await;
        eventually(&node, &host, "/hello", "hello one").await;
        let mut second = first.clone();
        second["id"] = json!("v2");
        second["handlers"][0]["environment"]["LABEL"] = json!("two");
        h.stage("e2e-one", &second).await;
        assert_eq!(
            get(&node, &host, "/hello").await,
            "hello one",
            "stage published a deployment"
        );
        let began = Instant::now();
        let finished = std::sync::atomic::AtomicBool::new(false);
        let slow = async {
            let response = request(&node, &host, "/slow").await;
            finished.store(true, std::sync::atomic::Ordering::SeqCst);
            response
        };
        let replace = async {
            eventually(&node, &host, "/slow-started", "one").await;
            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let (observed, mut observations) = tokio::sync::mpsc::unbounded_channel();
            let mut clients = tokio::task::JoinSet::new();
            for _ in 0..3 {
                let (host, port, stop) = (host.clone(), node.public, stop.clone());
                let observed = observed.clone();
                clients.spawn(async move {
                    let client = reqwest::Client::new();
                    let mut seen = Vec::new();
                    let (mut old, mut new) = (false, false);
                    while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                        let response = client
                            .get(format!("http://127.0.0.1:{port}/hello"))
                            .header("host", &host)
                            .timeout(Duration::from_secs(30))
                            .send()
                            .await
                            .unwrap();
                        let reply = (response.status().as_u16(), response.text().await.unwrap());
                        if !old && reply == (200, "hello one".into()) {
                            old = true;
                            observed.send("one").unwrap();
                        }
                        if !new && reply == (200, "hello two".into()) {
                            new = true;
                            observed.send("two").unwrap();
                        }
                        seen.push(reply);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    seen
                });
            }
            for _ in 0..3 {
                assert_eq!(observations.recv().await, Some("one"));
            }
            h.activate("e2e-one", "v2").await;
            assert!(
                !finished.load(std::sync::atomic::Ordering::SeqCst),
                "replacement missed the accepted slow request"
            );
            eventually(&node, &host, "/hello", "hello two").await;
            for _ in 0..3 {
                assert_eq!(observations.recv().await, Some("two"));
            }
            stop.store(true, std::sync::atomic::Ordering::SeqCst);
            while let Some(client) = clients.join_next().await {
                let seen = client.unwrap();
                assert!(
                    seen.iter().any(|reply| reply == &(200, "hello one".into())),
                    "client never saw v1: {seen:?}"
                );
                assert!(
                    seen.iter().any(|reply| reply == &(200, "hello two".into())),
                    "client never saw v2: {seen:?}"
                );
                let mut replaced = false;
                for (status, body) in seen {
                    assert_eq!(status, 200, "request failed during replacement: {body}");
                    match body.as_str() {
                        "hello one" => {
                            assert!(!replaced, "old deployment reappeared after replacement")
                        }
                        "hello two" => replaced = true,
                        _ => panic!("unexpected replacement reply: {body}"),
                    }
                }
            }
        };
        let (slow, ()) = tokio::join!(slow, replace);
        assert_eq!(
            slow,
            (200, "slow one".into()),
            "accepted request lost its deployment"
        );
        assert_eq!(h.hostname("e2e-one", "main").await, host);
        passed("stage-and-replace-under-load", began);
        let began = Instant::now();
        h.activate("e2e-one", "v1").await;
        eventually(&node, &host, "/hello", "hello one").await;
        h.cli("e2e-one", "deactivate", &[]).await;
        timeout(Duration::from_secs(65), async {
            while request(&node, &host, "/hello").await.0 != 404 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("deactivated route remained public");
        node.drain().await;
        passed("rollback-and-deactivation", began);
    })
    .await
    .expect("T29 lifecycle exceeded 300 seconds");
    passed("lifecycle::t29_lifecycle", entire_began);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real JS, esbuild TypeScript and Python CLI on Firecracker"]
async fn t29_languages() {
    let entire_began = Instant::now();
    celld::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    timeout(Duration::from_secs(600), async {
        let esbuild = std::env::var_os("CELLD_ESBUILD").expect("CELLD_ESBUILD is required for T29 TypeScript");
        let python = std::path::PathBuf::from(std::env::var_os("CELLD_APPLICATION_PYTHON_TEST_PROJECT")
            .expect("CELLD_APPLICATION_PYTHON_TEST_PROJECT must contain the prepared Python fixture"));
        let h = Harness::new("languages", true).await;
        for app in ["e2e-js", "e2e-ts", "e2e-python"] { h.create(app).await; }
        let javascript = h.source("e2e-js", "export default {fetch(){return Response.json({language:'javascript',answer:42});}};").await;
        h.stage("e2e-js", &deployment(&h, "e2e-js", "v1", &javascript, "js")).await;
        h.activate("e2e-js", "v1").await;
        let typescript = h.directory.join("typescript.mjs");
        let build = Command::new(esbuild)
            .arg(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/t29-lifecycle/main.ts"))
            .args(["--bundle", "--format=esm", "--platform=neutral"])
            .arg(format!("--outfile={}", typescript.display())).kill_on_drop(true).output().await.unwrap();
        assert!(build.status.success(), "TypeScript build: {}", String::from_utf8_lossy(&build.stderr));
        let typescript = h.source("e2e-ts", &std::fs::read_to_string(typescript).unwrap()).await;
        h.stage("e2e-ts", &deployment(&h, "e2e-ts", "v1", &typescript, "ts")).await;
        h.activate("e2e-ts", "v1").await;
        h.resource("e2e-python", "d1", "db", json!({})).await;
        h.resource("e2e-python", "kv", "cache", json!({})).await;
        let config = python.join("wrangler.jsonc");
        let prepared = h.cli("e2e-python", "python", &["--file", config.to_str().unwrap()]).await;
        let mut manifest = deployment(&h, "e2e-python", "v1", prepared["artifact_sha256"].as_str().unwrap(), "py");
        manifest["handlers"][0]["entrypoint"] = prepared["entrypoint"].clone();
        manifest["handlers"][0]["worker_config"] = prepared["worker_config"].clone();
        manifest["handlers"][0]["bindings"] = json!({
            "DB":{"kind":"resource", "resource":{"kind":"d1", "id":"db"}},
            "CACHE":{"kind":"resource", "resource":{"kind":"kv", "id":"cache"}}});
        h.stage("e2e-python", &manifest).await;
        h.activate("e2e-python", "v1").await;
        let node = h.start("languages").await;
        for (app, language) in [("e2e-js", "javascript"), ("e2e-ts", "typescript")] {
            let began = Instant::now();
            let reply = json(&node, &h.hostname(app, "main").await, "/hello").await;
            assert_eq!(reply, json!({"language":language, "answer":42}));
            passed(language, began);
        }
        let began = Instant::now();
        let host = h.hostname("e2e-python", "main").await;
        assert_eq!(json(&node, &host, "/hello").await["hello"], "python");
        assert_eq!(json(&node, &host, "/kv").await, json!({"value":"kv-from-python"}));
        assert_eq!(json(&node, &host, "/d1").await, json!({"count":1}));
        assert!(!node.vm_pids().is_empty(), "language requests did not create real VMs");
        node.drain().await;
        passed("python-cli-and-public-ingress", began);
    }).await.expect("T29 language scenario exceeded 600 seconds");
    passed("lifecycle::t29_languages", entire_began);
}

fn static_manifest(version: &str, assets: &Value) -> Value {
    json!({"manifest_version":1, "application":{"app":"e2e-one"}, "id":version, "assets":assets})
}

async fn upload_assets(h: &Harness, configuration: Value) -> Value {
    let assets = h.directory.join("assets");
    let config = h.json_file("assets-config.json", &configuration);
    h.cli(
        "e2e-one",
        "assets",
        &[
            "--directory",
            assets.to_str().unwrap(),
            "--file",
            config.to_str().unwrap(),
        ],
    )
    .await["assets"]
        .clone()
}

fn asset_files(h: &Harness) {
    let assets = h.directory.join("assets");
    std::fs::create_dir(&assets).unwrap();
    for (name, body) in [
        ("index.html", "<h1>one</h1>"),
        ("a.txt", "abcdef"),
        ("404.html", "custom missing"),
        ("_headers", "/a.txt\n  X-T29: original\n"),
        ("_redirects", "/old /a.txt 302\n"),
    ] {
        std::fs::write(assets.join(name), body).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "public static-only ingress; no VM prerequisites"]
async fn t29_assets() {
    let entire_began = Instant::now();
    celld::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    timeout(Duration::from_secs(180), async {
        let began = Instant::now();
        let h = Harness::new("assets", false).await;
        h.create("e2e-one").await;
        h.create("e2e-two").await;
        asset_files(&h);
        let assets = upload_assets(&h, json!({"assets":{"not_found_handling":"404-page"}})).await;
        h.stage("e2e-one", &static_manifest("v1", &assets)).await;
        h.activate("e2e-one", "v1").await;
        let host = h.asset_hostname("e2e-one").await;
        assert_eq!(
            h.cli("e2e-one", "get", &[]).await["application"]["active"]["handlers"],
            json!([])
        );
        let mut foreign = static_manifest("foreign", &assets);
        foreign["application"] = json!({"app":"e2e-two"});
        h.stage("e2e-two", &foreign).await;
        let error = h
            .cli_as(
                &h.other_token,
                "e2e-two",
                "activate",
                &["--deployment", "foreign"],
            )
            .await
            .unwrap_err();
        assert!(error.contains("asset"), "{error}");
        let mut missing = assets.clone();
        missing["sha256"] = json!("0".repeat(64));
        h.stage("e2e-one", &static_manifest("missing", &missing))
            .await;
        let error = h
            .cli_as(
                &h.token,
                "e2e-one",
                "activate",
                &["--deployment", "missing"],
            )
            .await
            .unwrap_err();
        assert!(error.contains("asset"), "{error}");
        let worker_first = upload_assets(&h, json!({"assets":{"run_worker_first":true}})).await;
        h.stage("e2e-one", &static_manifest("worker-first", &worker_first))
            .await;
        let error = h
            .cli_as(
                &h.token,
                "e2e-one",
                "activate",
                &["--deployment", "worker-first"],
            )
            .await
            .unwrap_err();
        assert!(error.contains("cannot run a worker first"), "{error}");
        assert_eq!(
            h.cli("e2e-one", "get", &[]).await["application"]["active"]["id"],
            "v1"
        );
        passed("asset-only-publication-and-refusals", began);
        let node = h.start("assets").await;
        let began = Instant::now();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let send = |method: reqwest::Method, path: &str| {
            client
                .request(method, format!("http://127.0.0.1:{}{path}", node.public))
                .header("host", &host)
                .timeout(Duration::from_secs(20))
        };
        for _ in 0..2 {
            let reply = send(reqwest::Method::GET, "/a.txt").send().await.unwrap();
            assert_eq!(reply.status(), 200);
            assert!(reply.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/plain"));
            assert_eq!(reply.headers()["x-t29"], "original");
            let etag = reply.headers()["etag"].clone();
            assert_eq!(reply.text().await.unwrap(), "abcdef");
            let reply = send(reqwest::Method::HEAD, "/a.txt").send().await.unwrap();
            assert_eq!(reply.status(), 200);
            assert_eq!(reply.headers()["content-length"], "6");
            assert!(reply.bytes().await.unwrap().is_empty());
            assert_eq!(
                send(reqwest::Method::GET, "/a.txt")
                    .header("if-none-match", etag)
                    .send()
                    .await
                    .unwrap()
                    .status(),
                304
            );
            let reply = send(reqwest::Method::GET, "/a.txt")
                .header("range", "bytes=1-3")
                .send()
                .await
                .unwrap();
            assert_eq!(reply.status(), 206);
            assert_eq!(reply.headers()["content-range"], "bytes 1-3/6");
            assert_eq!(reply.text().await.unwrap(), "bcd");
        }
        assert_eq!(get(&node, &host, "/").await, "<h1>one</h1>");
        let reply = send(reqwest::Method::GET, "/old").send().await.unwrap();
        assert_eq!(reply.status(), 302);
        assert_eq!(reply.headers()["location"], "/a.txt");
        assert_eq!(
            request(&node, &host, "/missing").await,
            (404, "custom missing".into())
        );
        assert_eq!(
            send(reqwest::Method::POST, "/a.txt")
                .body("unused")
                .send()
                .await
                .unwrap()
                .status(),
            405
        );
        assert_eq!(
            request(
                &node,
                "00000000000000000000000000000000.apps.test",
                "/a.txt"
            )
            .await
            .0,
            404
        );
        no_vms(&node).await;
        passed("asset-only-cold-and-cached-http", began);

        let began = Instant::now();
        std::fs::write(h.directory.join("assets/index.html"), "<h1>two</h1>").unwrap();
        let replacement = upload_assets(&h, json!({})).await;
        h.stage("e2e-one", &static_manifest("v2", &replacement))
            .await;
        assert_eq!(get(&node, &host, "/").await, "<h1>one</h1>");
        h.activate("e2e-one", "v2").await;
        eventually(&node, &host, "/", "<h1>two</h1>").await;
        assert_eq!(h.asset_hostname("e2e-one").await, host);
        h.activate("e2e-one", "v1").await;
        eventually(&node, &host, "/", "<h1>one</h1>").await;
        let spa = upload_assets(
            &h,
            json!({"assets":{"not_found_handling":"single-page-application"}}),
        )
        .await;
        h.stage("e2e-one", &static_manifest("spa", &spa)).await;
        h.activate("e2e-one", "spa").await;
        eventually(&node, &host, "/deep/spa", "<h1>two</h1>").await;
        h.cli("e2e-one", "deactivate", &[]).await;
        timeout(Duration::from_secs(65), async {
            while request(&node, &host, "/a.txt").await.0 != 404 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("deactivated static route remained public");
        no_vms(&node).await;
        node.drain().await;
        passed("asset-stable-url-replace-rollback-spa-deactivate", began);
    })
    .await
    .expect("T29 static-only scenario exceeded 180 seconds");
    passed("lifecycle::t29_assets", entire_began);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "mixed static/dynamic publication on a real Firecracker node"]
async fn t29_mixed_assets() {
    let entire_began = Instant::now();
    celld::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    timeout(Duration::from_secs(300), async {
        let h = Harness::new("mixed-assets", true).await;
        h.create("e2e-one").await;
        h.resource("e2e-one", "static_assets", "site", json!({}))
            .await;
        asset_files(&h);
        let assets = upload_assets(&h, json!({})).await;
        let source = h.source("e2e-one", SOURCE).await;
        let mut manifest = deployment(&h, "e2e-one", "v1", &source, "mixed");
        manifest["assets"] = assets.clone();
        manifest["handlers"][0]["bindings"] =
            json!({"ASSETS":{"kind":"resource", "resource":{"kind":"static_assets", "id":"site"}}});
        manifest["handlers"][0]["worker_config"] = json!({"assets":assets});
        h.stage("e2e-one", &manifest).await;
        h.activate("e2e-one", "v1").await;
        let node = h.start("mixed-assets").await;
        let began = Instant::now();
        let handler = h.hostname("e2e-one", "main").await;
        let static_host = h.asset_hostname("e2e-one").await;
        for host in [&handler, &static_host] {
            assert_eq!(get(&node, host, "/a.txt").await, "abcdef");
        }
        no_vms(&node).await;
        passed("mixed-static-does-not-launch-vm", began);
        let began = Instant::now();
        assert_eq!(get(&node, &handler, "/hello").await, "hello mixed");
        assert!(
            !node.vm_pids().is_empty(),
            "explicit dynamic ingress did not launch a VM"
        );
        node.drain().await;
        passed("mixed-public-dynamic-dispatch", began);
    })
    .await
    .expect("T29 mixed assets scenario exceeded 300 seconds");
    passed("lifecycle::t29_mixed_assets", entire_began);
}
