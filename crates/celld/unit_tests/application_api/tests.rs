use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn id(app: &str) -> ApplicationId {
    application(app.into()).unwrap()
}

fn credential(token: &str, permission: Permission) -> Credential {
    Credential {
        token_sha256: content_digest(token.as_bytes()),
        permission,
    }
}

async fn headers_only(address: std::net::SocketAddr, token: Option<&str>, path: &str) -> String {
    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    let auth = token
        .map(|value| format!("Authorization: Bearer {value}\r\n"))
        .unwrap_or_default();
    socket
        .write_all(
            format!(
                "PUT {path} HTTP/1.1\r\nHost: localhost\r\n{auth}Content-Length: 67108864\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    // No body follows. A response proves denial happened before upload
    // extraction; a server awaiting the body would time out here.
    let mut reply = [0; 1024];
    let count = tokio::time::timeout(std::time::Duration::from_secs(3), socket.read(&mut reply))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8_lossy(&reply[..count]).into_owned()
}

/// A request head with a declared body that never arrives, left open. Eight of
/// these used to hold every management permit forever.
async fn stall(address: std::net::SocketAddr, token: &str, path: &str) -> tokio::net::TcpStream {
    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    socket
        .write_all(
            format!(
                "PUT {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nContent-Length: 67108864\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    socket
}

/// Eight stalled authenticated writes must not take the whole API down: the
/// permit's hold is bounded, so a later GET succeeds once the deadline fires,
/// and the in-flight bound stays at eight rather than growing.
async fn check_stalled_permits_are_released(api: &str, address: std::net::SocketAddr, token: &str) {
    let path = "/v1/applications/one/artifacts/";
    let mut stalled = Vec::new();
    for _ in 0..8 {
        // The write above returns once the head is queued, so each socket holds
        // a permit by the time the next one is opened.
        stalled.push(stall(address, token, path).await);
    }
    // Waits for their deadlines to fire. The test deadline is deliberately
    // longer than the router's, so a failure here means the permits never came
    // back rather than that the test was impatient.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut reply = [0u8; 256];
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "stalled requests never released their permits: {api}{path}"
        );
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        socket
            .write_all(
                format!(
                    "GET /v1/applications/one HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        if let Ok(Ok(count)) = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            tokio::io::AsyncReadExt::read(&mut socket, &mut reply),
        )
        .await
        {
            if count > 0 && reply.starts_with(b"HTTP/1.1 ") {
                assert!(
                    !String::from_utf8_lossy(&reply[..count]).starts_with("HTTP/1.1 503"),
                    "a GET was refused while the permits drained"
                );
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    // Dropping the clients must not be needed for the events to settle.
    drop(stalled);
}

/// Eight stalled authenticated writes must not take the whole API down. The
/// permit's hold is bounded, so a GET succeeds once the deadlines fire, and the
/// in-flight bound stays at eight. A short deadline keeps the test honest about
/// real time; the shipped one is `REQUEST_TIMEOUT`.
#[tokio::test]
async fn stalled_management_requests_release_their_permits() {
    let directory = tempfile::tempdir().unwrap();
    let bucket = crate::bucket::Bucket::open_dev(&directory.path().join("permits.sqlite")).unwrap();
    let operator = "o".repeat(40);
    let auth = Authorization {
        credentials: vec![credential(&operator, Permission::Operator {})],
    };
    // A short deadline: the shipped `REQUEST_TIMEOUT` would make this test wait
    // five minutes. The bound being tested is the same either way.
    let app = router_with(
        ApplicationStore::new(bucket),
        auth,
        "apps.test".into(),
        Duration::from_secs(2),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    check_stalled_permits_are_released(&format!("http://{address}"), address, &operator).await;
    server.abort();
}

async fn cli(
    api: &str,
    token: &str,
    command: &str,
    app: Option<&str>,
    extra: &[&str],
) -> anyhow::Result<serde_json::Value> {
    let mut args = vec![command.into(), "--api".into(), api.into()];
    if let Some(app) = app {
        args.extend(["--app".into(), app.into()]);
    }
    args.extend(extra.iter().map(|s| s.to_string()));
    crate::application_cli::execute(args, token).await
}

/// Activation publishes only a staged, fully uploaded deployment through the
/// catalog CAS; every refusal leaves the active deployment and routes alone.
async fn check_activation(
    api: &str,
    backend: &ApplicationStore,
    (reader, manager, operator): (&str, &str, &str),
    digest: &str,
) {
    let (one, two) = (id("one"), id("two"));
    let code = backend.artifact(&one, digest).await.unwrap().unwrap();
    backend.upload_artifact(&two, digest, code).await.unwrap();
    let stage = |owner: &'static str, version: &'static str, artifact: String| {
        let deployment: Deployment = serde_json::from_value(json!({
            "manifest_version": 1, "application": {"app": owner}, "id": version,
            "handlers": [{"id": "main", "language": "javascript", "entrypoint": "index.js",
                "artifact_sha256": artifact, "runtime_image_sha256": "b".repeat(64), "http": true}]
        }))
        .unwrap();
        async move { backend.stage(&deployment).await.unwrap() }
    };
    for (version, artifact) in [
        ("r1", digest),
        ("r2", digest),
        ("r3", digest),
        ("no-artifact", &"d".repeat(64)[..]),
        ("same-handler", digest),
    ] {
        stage("one", version, artifact.into()).await;
    }
    stage("two", "t1", digest.into()).await;
    let state = || async {
        let one_snapshot = backend.read(&one).await.unwrap();
        let two_snapshot = backend.read(&two).await.unwrap();
        let active = |snapshot: &Snapshot, id: &ApplicationId| {
            snapshot
                .application(id)
                .unwrap()
                .active
                .as_ref()
                .map(|deployment| deployment.id.as_str().to_owned())
        };
        let owner = |snapshot: &Snapshot, id: &ApplicationId| {
            let handler = Name::try_from("main".to_owned()).unwrap();
            let host = format!(
                "{}.apps.test",
                snapshot.application(id)?.route_ids.get(&handler)?.as_str()
            );
            snapshot
                .routes()
                .ok()?
                .resolve(&host, "apps.test")
                .map(|target| target.application.app.as_str().to_owned())
        };
        (
            active(&one_snapshot, &one),
            active(&two_snapshot, &two),
            owner(&one_snapshot, &one),
            owner(&two_snapshot, &two),
        )
    };
    let activate = |token: &str, owner: &'static str, version: &'static str| {
        let (token, args) = (token.to_owned(), ["--deployment", version]);
        async move { cli(api, &token, "activate", Some(owner), &args).await }
    };
    assert_eq!(state().await, (None, None, None, None));
    assert!(activate(manager, "one", "never-staged").await.is_err());
    assert!(activate(reader, "one", "r1").await.is_err());
    assert_eq!(state().await, (None, None, None, None));
    activate(manager, "one", "r1").await.unwrap();
    assert_eq!(
        state().await,
        (Some("r1".to_owned()), None, Some("one".to_owned()), None)
    );
    // A missing artifact refuses. Same-named handlers in two applications
    // each answer only at their own assigned hostname.
    activate(operator, "two", "t1").await.unwrap();
    let held = (
        Some("r1".to_owned()),
        Some("t1".to_owned()),
        Some("one".to_owned()),
        Some("two".to_owned()),
    );
    assert_eq!(state().await, held);
    assert!(activate(manager, "one", "no-artifact").await.is_err());
    assert_eq!(state().await, held);
    // Replacement and rollback are both activation of a staged version.
    activate(manager, "one", "r2").await.unwrap();
    assert_eq!(state().await.0.as_deref(), Some("r2"));
    activate(manager, "one", "r1").await.unwrap();
    assert_eq!(state().await, held);
    // Two writers on one observed revision: the catalog CAS picks one.
    let revision = cli(api, reader, "get", Some("one"), &[]).await.unwrap()["revision"].clone();
    let client = reqwest::Client::new();
    let put = |version: &'static str| {
        let request = client
            .put(format!("{api}/v1/applications/one/active"))
            .bearer_auth(manager)
            .json(&json!({"revision": revision, "deployment": version}));
        async move { (version, request.send().await.unwrap().status()) }
    };
    let raced = tokio::join!(put("r2"), put("r3"));
    let winners: Vec<_> = [raced.0, raced.1]
        .into_iter()
        .filter(|(_, status)| status.is_success())
        .collect();
    assert_eq!(winners.len(), 1, "{raced:?}");
    assert_eq!(state().await.0.as_deref(), Some(winners[0].0));
    // Deactivation withdraws the route, and nothing another application
    // activates, even the same manifest, can answer at the freed hostname.
    cli(api, operator, "deactivate", Some("two"), &[])
        .await
        .unwrap();
    activate(manager, "one", "same-handler").await.unwrap();
    assert_eq!(
        state().await,
        (
            Some("same-handler".to_owned()),
            None,
            Some("one".to_owned()),
            None
        )
    );
    cli(api, manager, "deactivate", Some("one"), &[])
        .await
        .unwrap();
    assert_eq!(state().await, (None, None, None, None));
}

/// T10: one handler lifecycle through the CLI. Replacing and removing a
/// handler keeps the application's resource identity, denied edits leave
/// the catalog revision untouched, and `runtime` reports only what a node
/// published, never what the catalog merely declares.
async fn check_lifecycle(
    api: &str,
    backend: &ApplicationStore,
    (reader, manager, owner, operator): (&str, &str, &str, &str),
    digest: &str,
) {
    use crate::applications::{HandlerState, HandlerStatus};
    let one = id("one");
    let directory = tempfile::tempdir().unwrap();
    let file = |name: &str, bytes: &[u8]| {
        let path = directory.path().join(name);
        std::fs::write(&path, bytes).unwrap();
        path.to_str().unwrap().to_owned()
    };
    let replacement = b"export default { fetch() { return new Response('two'); } };";
    let digest2 = content_digest(replacement);
    let manifest = |version: &str, handlers: &[(&str, &str)]| {
        let handlers: Vec<_> = handlers
            .iter()
            .map(|(id, artifact)| {
                json!({
                    "id": id, "language": "javascript", "entrypoint": "index.js",
                    "artifact_sha256": artifact, "runtime_image_sha256": "b".repeat(64),
                    "http": *id == "main",
                    "bindings": {"DB": {"kind": "resource", "resource": {"kind": "d1", "id": "db"}}}
                })
            })
            .collect();
        let body = json!({"manifest_version": 1, "application": one, "id": version,
                "handlers": handlers});
        file(
            &format!("{version}.json"),
            &serde_json::to_vec(&body).unwrap(),
        )
    };
    let inspect = |token: &str, owner: &'static str| {
        let token = token.to_owned();
        async move { cli(api, &token, "get", Some(owner), &[]).await.unwrap() }
    };
    let stage = |token: &str, path: &str| {
        let (token, path) = (token.to_owned(), path.to_owned());
        async move {
            let args = ["--file", &path];
            cli(api, &token, "stage", Some("one"), &args).await
        }
    };
    let activate = |token: &str, owner: &'static str, version: &'static str| {
        let token = token.to_owned();
        async move {
            let args = ["--deployment", version];
            cli(api, &token, "activate", Some(owner), &args).await
        }
    };
    let handlers = |inspected: &serde_json::Value| -> Vec<(String, String)> {
        let none = vec![];
        let active = inspected["application"]["active"]["handlers"].as_array();
        active
            .unwrap_or(&none)
            .iter()
            .map(|h| {
                let text = |field: &str| h[field].as_str().unwrap().to_owned();
                (text("id"), text("artifact_sha256"))
            })
            .collect()
    };
    let pair = |id: &str, artifact: &str| (id.to_owned(), artifact.to_owned());
    let [v1, v2, v3] = [
        ("lifecycle-1", vec![("main", digest), ("extra", digest)]),
        (
            "lifecycle-2",
            vec![("main", &digest2[..]), ("extra", digest)],
        ),
        ("lifecycle-3", vec![("main", &digest2[..])]),
    ]
    .map(|(version, handlers)| manifest(version, &handlers));
    // Denied cross-application/read edits are refused before any side effect.
    let before = inspect(reader, "one").await;
    let artifact = file("replacement.js", replacement);
    let upload = |token: &str, owner: &'static str| {
        let (token, artifact) = (token.to_owned(), artifact.clone());
        async move {
            let args = ["--file", &artifact];
            cli(api, &token, "artifact", Some(owner), &args).await
        }
    };
    assert!(upload(reader, "one").await.is_err());
    assert!(upload(owner, "two").await.is_err());
    assert!(stage(reader, &v1).await.is_err());
    assert!(activate(reader, "one", "lifecycle-1").await.is_err());
    assert!(activate(owner, "two", "lifecycle-1").await.is_err());
    let name: Name = "lifecycle-1".to_owned().try_into().unwrap();
    assert!(backend.deployment(&one, &name).await.unwrap().is_none());
    assert!(backend.artifact(&one, &digest2).await.unwrap().is_none());
    assert!(backend
        .artifact(&id("two"), &digest2)
        .await
        .unwrap()
        .is_none());
    assert_eq!(inspect(reader, "one").await["revision"], before["revision"]);
    // Deploy: staging and uploading publish nothing; activation does, yet
    // no node has reported, so no runtime state is claimed.
    upload(manager, "one").await.unwrap();
    // `assets` builds the original index and uploads bodies, then the
    // index, as this application's artifacts only.
    let site = tempfile::tempdir().unwrap();
    std::fs::write(site.path().join("a.txt"), "asset").unwrap();
    let args = ["--directory", site.path().to_str().unwrap()];
    assert!(cli(api, reader, "assets", Some("one"), &args)
        .await
        .is_err());
    let published = cli(api, manager, "assets", Some("one"), &args)
        .await
        .unwrap();
    let sha = published["assets"]["sha256"].as_str().unwrap();
    assert_eq!(published["assets"]["file_count"], 1);
    assert!(backend.artifact(&one, sha).await.unwrap().is_some());
    assert!(backend
        .artifact(&one, &content_digest(b"asset"))
        .await
        .unwrap()
        .is_some());
    assert!(backend.artifact(&id("two"), sha).await.unwrap().is_none());
    for path in [&v1, &v2, &v3] {
        stage(manager, path).await.unwrap();
    }
    assert!(inspect(reader, "one").await["application"]["active"].is_null());
    activate(manager, "one", "lifecycle-1").await.unwrap();
    let deployed = inspect(reader, "one").await;
    assert_eq!(
        handlers(&deployed),
        [pair("main", digest), pair("extra", digest)]
    );
    assert_eq!(deployed["runtime"], json!([]));
    // Only the `http` handler has a host-assigned hostname.
    let hostname = deployed["hostnames"]["main"].as_str().unwrap().to_owned();
    assert!(hostname.ends_with(".apps.test") && hostname.len() == 32 + ".apps.test".len());
    assert_eq!(deployed["hostnames"].as_object().unwrap().len(), 1);
    // A node observation shows beside, and distinct from, the catalog.
    let main = HandlerStatus {
        deployment: name,
        handler: "main".to_owned().try_into().unwrap(),
        state: HandlerState::Serving,
    };
    let extra = HandlerStatus {
        handler: "extra".to_owned().try_into().unwrap(),
        state: HandlerState::Failed {
            reason: "launch failed".into(),
        },
        ..main.clone()
    };
    backend
        .publish_status(&one, "node-a", vec![main, extra])
        .await
        .unwrap();
    let observed = inspect(reader, "one").await;
    assert_eq!(
        observed["runtime"][0]["handlers"],
        json!([
            {"deployment": "lifecycle-1", "handler": "main", "state": "serving"},
            {"deployment": "lifecycle-1", "handler": "extra", "state": "failed", "reason": "launch failed"}
        ])
    );
    assert!(
        observed["runtime"][0]["observed_at_unix_secs"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(observed["application"], deployed["application"]);
    assert_eq!(observed["runtime"][0]["node"], "node-a");
    assert_eq!(inspect(operator, "two").await["runtime"], json!([]));
    // Replace one handler's code, then remove the other handler; neither
    // touches the registered resource identity.
    let resources = deployed["application"]["resources"].clone();
    activate(manager, "one", "lifecycle-2").await.unwrap();
    let replaced = inspect(reader, "one").await;
    assert_eq!(
        handlers(&replaced),
        [pair("main", &digest2), pair("extra", digest)]
    );
    activate(manager, "one", "lifecycle-3").await.unwrap();
    let removed = inspect(reader, "one").await;
    assert_eq!(handlers(&removed), [pair("main", &digest2)]);
    assert_eq!(replaced["hostnames"]["main"], hostname.as_str());
    assert_eq!(removed["hostnames"]["main"], hostname.as_str());
    assert_eq!(removed["application"]["resources"], resources);
    cli(api, manager, "deactivate", Some("one"), &[])
        .await
        .unwrap();
    let idle = inspect(reader, "one").await;
    assert!(idle["application"]["active"].is_null());
    assert_eq!(idle["hostnames"], json!({}));
    assert_eq!(idle["application"]["resources"], resources);
    eprintln!("application lifecycle: denied read/foreign-application edits left the revision, artifacts and deployments untouched; deploy, handler replacement, handler removal and deactivation kept resource identity; runtime stayed empty until a node published, then showed serving/failed beside the catalog; local bucket, no VM");
}

/// T21: every route and method, for every credential scope and every
/// target (own application, two other applications, an unknown application),
/// is decided from the credential alone and
/// before the body or catalog is read. A restart with a smaller credential
/// set revokes, downgrades and re-scopes the very same tokens.
async fn check_scope_matrix() {
    const NONE: (bool, bool) = (false, false);
    const READ: (bool, bool) = (true, false);
    const ALL: (bool, bool) = (true, true);
    let directory = tempfile::tempdir().unwrap();
    let bucket = crate::bucket::Bucket::open_dev(&directory.path().join("scopes.sqlite")).unwrap();
    let store = ApplicationStore::new(bucket.clone());
    let targets = ["one", "sibling", "two", "absent"];
    let mut revisions = Vec::new();
    for app in &targets[..3] {
        let mut snapshot = store.read(&id(app)).await.unwrap();
        snapshot.create(id(app)).unwrap();
        revisions.push(store.commit(snapshot).await.unwrap());
    }
    let token = |letter: char| letter.to_string().repeat(40);
    let scoped = |app: &str, access: Access| Permission::Application {
        application: id(app),
        access,
    };
    let digest = "a".repeat(64);
    let routes = [
        (reqwest::Method::GET, String::new()),
        (reqwest::Method::POST, String::new()),
        (reqwest::Method::PUT, "/resources".into()),
        (reqwest::Method::GET, "/secrets".into()),
        (reqwest::Method::PUT, "/secrets/s".into()),
        (reqwest::Method::DELETE, "/secrets/s".into()),
        (reqwest::Method::PUT, "/limits".into()),
        (reqwest::Method::DELETE, "/limits".into()),
        (reqwest::Method::PUT, "/active".into()),
        (reqwest::Method::DELETE, "/active".into()),
        (reqwest::Method::GET, "/deployments/v".into()),
        (reqwest::Method::PUT, "/deployments/v".into()),
        (reqwest::Method::PUT, format!("/artifacts/{digest}")),
    ];
    let client = reqwest::Client::new();
    // Credential letter, permission and the (read, write) it holds on each target.
    type Row = (char, Permission, [(bool, bool); 4]);
    let phases: [Vec<Row>; 2] = [
        vec![
            ('o', Permission::Operator {}, [ALL; 4]),
            ('t', scoped("one", Access::Manage), [ALL, NONE, NONE, NONE]),
            ('u', scoped("one", Access::Read), [READ, NONE, NONE, NONE]),
            ('a', scoped("one", Access::Manage), [ALL, NONE, NONE, NONE]),
            ('r', scoped("one", Access::Read), [READ, NONE, NONE, NONE]),
            ('b', scoped("two", Access::Manage), [NONE, NONE, ALL, NONE]),
        ],
        // The restarted server: u and a are gone, t lost write access and r
        // now names the sibling instead of the application it had.
        vec![
            ('o', Permission::Operator {}, [ALL; 4]),
            ('t', scoped("one", Access::Read), [READ, NONE, NONE, NONE]),
            (
                'r',
                scoped("sibling", Access::Read),
                [NONE, READ, NONE, NONE],
            ),
            ('b', scoped("two", Access::Manage), [NONE, NONE, ALL, NONE]),
        ],
    ];
    for (phase, rows) in phases.iter().enumerate() {
        let auth = Authorization {
            credentials: rows
                .iter()
                .map(|(letter, permission, _)| credential(&token(*letter), permission.clone()))
                .collect(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn({
            let app = router(
                ApplicationStore::new(bucket.clone()),
                auth,
                "apps.test".into(),
            )
            .unwrap();
            async move { axum::serve(listener, app).await.unwrap() }
        });
        let send = |method: &reqwest::Method, path: String, bearer: Option<String>| {
            let mut request = client
                .request(method.clone(), format!("{api}{path}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body("{}");
            if let Some(bearer) = bearer {
                request = request.bearer_auth(bearer);
            }
            async move { request.send().await.unwrap().status() }
        };
        // A token the server does not hold, and no token, fail authentication
        // whatever the route.
        for (method, suffix) in &routes {
            for bearer in [None, Some(token('z'))] {
                let path = format!("/v1/applications/one{suffix}");
                assert_eq!(
                    send(method, path, bearer).await,
                    StatusCode::UNAUTHORIZED,
                    "phase {phase} {method} {suffix}"
                );
            }
        }
        for letter in ['o', 't', 'u', 'a', 'r', 'b'] {
            let listed = send(
                &reqwest::Method::GET,
                "/v1/applications".into(),
                Some(token(letter)),
            )
            .await;
            let Some((_, _, grants)) = rows.iter().find(|(held, ..)| *held == letter) else {
                assert_eq!(listed, StatusCode::UNAUTHORIZED, "revoked {letter} listed");
                for (method, suffix) in &routes {
                    let path = format!("/v1/applications/one{suffix}");
                    assert_eq!(
                        send(method, path, Some(token(letter))).await,
                        StatusCode::UNAUTHORIZED,
                        "phase {phase}: revoked {letter} reached {method} {suffix}"
                    );
                }
                continue;
            };
            assert_eq!(listed, StatusCode::OK);
            let listing: serde_json::Value = client
                .get(format!("{api}/v1/applications"))
                .bearer_auth(token(letter))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(
                listing["applications"].as_array().unwrap().len(),
                grants[..3].iter().filter(|(read, _)| *read).count(),
                "phase {phase} {letter}: {listing}"
            );
            for (app, (may_read, may_write)) in targets.iter().zip(grants) {
                for (method, suffix) in &routes {
                    let write = method != reqwest::Method::GET;
                    let path = format!("/v1/applications/{app}{suffix}");
                    let status = send(method, path, Some(token(letter))).await;
                    let denied = !(if write { *may_write } else { *may_read });
                    assert_eq!(
                        status == StatusCode::FORBIDDEN,
                        denied,
                        "phase {phase} {letter} {method} {app}{suffix}: {status}"
                    );
                    assert_ne!(status, StatusCode::UNAUTHORIZED);
                }
            }
        }
        server.abort();
    }
    // Nothing above changed the catalog: every request was refused or failed
    // validation before it reached a write.
    for (app, revision) in targets[..3].iter().zip(revisions) {
        assert_eq!(
            store.read(&id(app)).await.unwrap().revision(),
            Some(revision.as_str())
        );
    }
}

async fn check_listing(api: &str, operator: &str, reader: &str, manager: &str) {
    let client = reqwest::Client::new();
    for query in [
        "limit=0",
        "limit=257",
        "cursor=bad",
        "tenant=one",
        "limit=1&limit=2",
    ] {
        assert_eq!(
            client
                .get(format!("{api}/v1/applications?{query}"))
                .bearer_auth(operator)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST,
            "{query}"
        );
    }
    assert_eq!(
        client
            .get(format!("{api}/v1/applications?cursor=anything"))
            .bearer_auth(reader)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .get(format!("{api}/v1/tenants/one/apps/same"))
            .bearer_auth(operator)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let mut names = std::collections::BTreeSet::new();
    let mut cursor = None;
    for page_number in 0..3 {
        let mut args = vec!["--limit", "1"];
        if let Some(cursor) = cursor.as_deref() {
            args.extend(["--cursor", cursor]);
        }
        let page = cli(api, operator, "list", None, &args).await.unwrap();
        assert!(page.get("revision").is_none());
        let entries = page["applications"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries[0]["revision"]["token"].is_string());
        assert!(names.insert(
            entries[0]["application"]["id"]["app"]
                .as_str()
                .unwrap()
                .to_owned()
        ));
        cursor = page["next_cursor"].as_str().map(str::to_owned);
        assert_eq!(cursor.is_some(), page_number < 2);
        if page_number == 0 {
            assert!(cli(
                api,
                operator,
                "list",
                None,
                &["--limit", "2", "--cursor", cursor.as_deref().unwrap()]
            )
            .await
            .is_err());
            assert!(cli(
                api,
                reader,
                "list",
                None,
                &["--limit", "1", "--cursor", cursor.as_deref().unwrap()]
            )
            .await
            .is_err());
        }
    }
    assert_eq!(
        names,
        ["one", "sibling", "two"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    );
    for args in [
        vec!["--limit", "0"],
        vec!["--limit", "257"],
        vec!["--limit", "bad"],
        vec!["--tenant", "one"],
        vec!["--app", "one"],
    ] {
        assert!(cli(api, manager, "list", None, &args).await.is_err());
    }
    assert!(cli(api, manager, "get", Some("one"), &["--cursor", "bad"])
        .await
        .is_err());
    for permission in [
        json!({"scope":"tenant","tenant":"one","access":"manage"}),
        json!({"scope":"application","application":{"tenant":"one","app":"one"},"access":"manage"}),
    ] {
        assert!(serde_json::from_value::<Permission>(permission).is_err());
    }
}

#[tokio::test]
async fn application_management_authorization_publication_and_cli() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    crate::asyncrt::bind_test_runtime();
    #[cfg(target_os = "linux")]
    crate::application_vm::tests::check_inactive_cron_candidates_keep_ownership_without_arming().await;
    #[cfg(unix)]
    crate::application_guest_transport::tests::check_protocol_boundary().await;
    #[cfg(unix)]
    crate::application_guest_transport::tests::check_guest_condemnation().await;
    #[cfg(unix)]
    crate::application_guest_transport::tests::check_socket_frame_boundary().await;
    #[cfg(unix)]
    crate::application_storage::tests::check_storage_bridge().await;
    #[cfg(unix)]
    crate::application_storage::tests::check_runtime_storage_bridge().await;
    #[cfg(unix)]
    crate::application_storage::tests::check_guest_alarm_cancellation_closes_only_its_native_claim(
    )
    .await;
    #[cfg(target_os = "linux")]
    crate::runtime::check_application_storage_pool().await;
    #[cfg(unix)]
    crate::js::guest_storage::tests::check_original_v8_storage();
    #[cfg(unix)]
    crate::js::guest_storage::tests::check_original_guest_entry().await;
    #[cfg(unix)]
    crate::js::guest_storage::tests::check_original_guest_response().await;
    #[cfg(unix)]
    crate::js::guest_storage::tests::check_guest_socket_upgrade().await;
    #[cfg(unix)]
    crate::js::guest_storage::tests::check_original_guest_loader().await;
    #[cfg(unix)]
    crate::js::guest_storage::tests::check_original_guest_facets().await;
    #[cfg(unix)]
    crate::js::guest_storage::tests::check_original_guest_retirement().await;
    #[cfg(unix)]
    crate::js::guest_storage::tests::check_original_guest_cancellation().await;
    #[cfg(target_os = "linux")]
    crate::application_javascript_guest::tests::check_application_bootstrap();
    #[cfg(target_os = "linux")]
    crate::application_javascript_guest::tests::check_queue_event().await;
    #[cfg(target_os = "linux")]
    crate::js::r2_ops::tests::check_application_broker().await;
    #[cfg(target_os = "linux")]
    crate::js::r2_ops::tests::check_guest_head_override().await;
    #[cfg(target_os = "linux")]
    crate::application_storage::tests::check_javascript_vm().await;
    #[cfg(target_os = "linux")]
    crate::application_storage::tests::check_javascript_vm_cancellation().await;
    #[cfg(target_os = "linux")]
    crate::application_storage::tests::check_javascript_vm_stream().await;
    // Last of the checks that use facets: its VMs install the node's
    // process-wide facet-stream channel, which the in-process fixtures
    // above do without.
    #[cfg(unix)]
    crate::application_storage::tests::check_actor_handler_bridge().await;
    // The original production facade binds one process-wide runtime.
    // Keep bucket and HTTP checks in that same live runtime; separate
    // tokio::test runtimes can shut down its blocking-I/O executor.
    crate::applications::tests::application_publication_cas_route_ids_and_resource_preservation()
        .await;
    crate::applications::tests::application_artifact_cache_hits_tamper_eviction_and_isolation()
        .await;
    crate::applications::tests::application_secret_rotation_isolation_and_limits().await;
    crate::applications::tests::application_static_assets_isolation_and_routing().await;
    check_scope_matrix().await;
    let directory = tempfile::tempdir().unwrap();
    let bucket = crate::bucket::Bucket::open_dev(&directory.path().join("bucket.sqlite")).unwrap();
    let backend = ApplicationStore::new(bucket.clone());
    let operator = "o".repeat(40);
    let owner = "t".repeat(40);
    let reader = "r".repeat(40);
    let manager = "m".repeat(40);
    let auth = Authorization {
        credentials: vec![
            credential(&operator, Permission::Operator {}),
            credential(
                &owner,
                Permission::Application {
                    application: id("one"),
                    access: Access::Manage,
                },
            ),
            credential(
                &reader,
                Permission::Application {
                    application: id("one"),
                    access: Access::Read,
                },
            ),
            credential(
                &manager,
                Permission::Application {
                    application: id("one"),
                    access: Access::Manage,
                },
            ),
        ],
    };
    auth.validate().unwrap();
    // Unknown scope fields cannot silently turn a scoped credential into
    // an operator, and duplicate token hashes are refused at startup.
    assert!(
        serde_json::from_value::<Permission>(json!({"scope":"operator", "tenant":"one"})).is_err()
    );
    assert!(Authorization {
        credentials: vec![
            credential(&operator, Permission::Operator {}),
            credential(&operator, Permission::Operator {})
        ]
    }
    .validate()
    .is_err());
    let app = router(ApplicationStore::new(bucket), auth, "apps.test".into()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let api = format!("http://{address}");
    let client = reqwest::Client::new();
    let path = "/v1/applications/one";
    let other = "/v1/applications/two";
    let upload_path = format!("{other}/artifacts/{}", "a".repeat(64));
    assert!(headers_only(address, None, &upload_path)
        .await
        .starts_with("HTTP/1.1 401"));
    assert!(headers_only(address, Some(&owner), &upload_path)
        .await
        .starts_with("HTTP/1.1 403"));
    assert!(headers_only(
        address,
        Some(&reader),
        &format!("{path}/artifacts/{}", "a".repeat(64))
    )
    .await
    .starts_with("HTTP/1.1 403"));
    assert_eq!(
        client
            .get(format!("{api}/v1/applications"))
            .header(header::AUTHORIZATION, format!("Bearer {operator}"))
            .header(header::AUTHORIZATION, format!("Bearer {owner}"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );

    let empty = cli(&api, &operator, "list", None, &[]).await.unwrap();
    assert!(empty.get("revision").is_none());
    assert_eq!(empty["applications"], json!([]));
    cli(&api, &owner, "create", Some("one"), &[]).await.unwrap();
    // Unrelated app creation uses its own absent revision, even after one exists.
    assert_eq!(
        client
            .post(format!("{api}{other}"))
            .bearer_auth(&operator)
            .json(&json!({"revision":{"token":null}}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert!(cli(&api, &operator, "create", Some("two"), &[])
        .await
        .is_err());
    cli(&api, &operator, "create", Some("sibling"), &[])
        .await
        .unwrap();
    let own_listing = cli(&api, &owner, "list", None, &[]).await.unwrap();
    assert_eq!(own_listing["applications"].as_array().unwrap().len(), 1);
    let reader_list = cli(&api, &reader, "list", None, &[]).await.unwrap();
    assert_eq!(reader_list["applications"].as_array().unwrap().len(), 1);
    assert_eq!(
        reader_list["applications"][0]["application"]["id"],
        json!({"app":"one"})
    );
    assert!(reader_list.get("revision").is_none());
    assert!(reader_list["applications"][0]["revision"]["token"].is_string());
    assert_eq!(
        client
            .get(format!("{api}/v1/applications/sibling"))
            .bearer_auth(&manager)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .get(format!("{api}{other}"))
            .bearer_auth(&owner)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );

    check_listing(&api, &operator, &reader, &manager).await;

    let one_revision =
        cli(&api, &operator, "get", Some("one"), &[]).await.unwrap()["revision"].clone();
    let two_revision =
        cli(&api, &operator, "get", Some("two"), &[]).await.unwrap()["revision"].clone();
    let update = |app: &'static str, revision: serde_json::Value| {
        let request = client
            .delete(format!("{api}/v1/applications/{app}/limits"))
            .bearer_auth(&operator)
            .json(&json!({"revision":revision}));
        async move { request.send().await.unwrap().status() }
    };
    let results = tokio::join!(update("one", one_revision), update("two", two_revision));
    assert_eq!(results, (StatusCode::OK, StatusCode::OK));

    let resource = directory.path().join("resource.json");
    std::fs::write(
        &resource,
        br#"{"resource":{"kind":"d1","id":"db"},"configuration":{}}"#,
    )
    .unwrap();
    cli(
        &api,
        &manager,
        "resource",
        Some("one"),
        &["--file", resource.to_str().unwrap()],
    )
    .await
    .unwrap();
    assert!(cli(
        &api,
        &reader,
        "resource",
        Some("one"),
        &["--file", resource.to_str().unwrap()]
    )
    .await
    .is_err());
    let inspected = cli(&api, &reader, "get", Some("one"), &[]).await.unwrap();
    assert_eq!(
        inspected["application"]["resources"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    for suffix in ["secrets/api-token", "limits"] {
        assert!(
            headers_only(address, Some(&reader), &format!("{path}/{suffix}"))
                .await
                .starts_with("HTTP/1.1 403")
        );
        assert!(
            headers_only(address, Some(&manager), &format!("{other}/{suffix}"))
                .await
                .starts_with("HTTP/1.1 403")
        );
    }
    let secret_file = directory.path().join("private-value.txt");
    std::fs::write(&secret_file, "api-scope-one\n").unwrap();
    cli(
        &api,
        &manager,
        "secret-put",
        Some("one"),
        &[
            "--secret",
            "api-token",
            "--file",
            secret_file.to_str().unwrap(),
        ],
    )
    .await
    .unwrap();
    let first_secret = cli(&api, &reader, "secrets", Some("one"), &[])
        .await
        .unwrap();
    assert!(first_secret["secrets"]["api-token"]
        .as_str()
        .is_some_and(|version| version.len() == 64));
    for command in ["get", "list", "secrets"] {
        let result = if command == "list" {
            cli(&api, &reader, command, None, &[]).await
        } else {
            cli(&api, &reader, command, Some("one"), &[]).await
        }
        .unwrap();
        assert!(!result.to_string().contains("api-scope-one"));
    }
    let secret_handler: celld_applications::Handler = serde_json::from_value(json!({"id":"main","language":"javascript","entrypoint":"main.mjs","artifact_sha256":"a".repeat(64),"runtime_image_sha256":"b".repeat(64),"secrets":{"PRIVATE_TOKEN":"api-token"}})).unwrap();
    let snapshot = backend.read(&id("one")).await.unwrap();
    assert_eq!(
        backend
            .resolve_environment(snapshot.application(&id("one")).unwrap(), &secret_handler)
            .await
            .unwrap()[&"PRIVATE_TOKEN".to_owned().try_into().unwrap()],
        "api-scope-one\n"
    );
    assert!(backend
        .resolve_environment(
            backend
                .read(&id("two"))
                .await
                .unwrap()
                .application(&id("two"))
                .unwrap(),
            &secret_handler
        )
        .await
        .is_err());
    std::fs::write(&secret_file, "api-scope-two").unwrap();
    cli(
        &api,
        &operator,
        "secret-put",
        Some("two"),
        &[
            "--secret",
            "api-token",
            "--file",
            secret_file.to_str().unwrap(),
        ],
    )
    .await
    .unwrap();
    std::fs::write(&secret_file, "api-rotated-one").unwrap();
    cli(
        &api,
        &manager,
        "secret-put",
        Some("one"),
        &[
            "--secret",
            "api-token",
            "--file",
            secret_file.to_str().unwrap(),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        client
            .put(format!("{api}{path}/secrets/api-token"))
            .bearer_auth(&manager)
            .json(&json!({"revision": first_secret["revision"], "value":"stale-secret"}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    let rotated = cli(&api, &reader, "secrets", Some("one"), &[])
        .await
        .unwrap();
    assert_ne!(
        rotated["secrets"]["api-token"],
        first_secret["secrets"]["api-token"]
    );
    for invalid in [
        json!({"revision": rotated["revision"], "value":"x".repeat(MAX_SECRET_BYTES + 1)}),
        json!({"revision": rotated["revision"], "value":"\0"}),
    ] {
        assert_eq!(
            client
                .put(format!("{api}{path}/secrets/invalid"))
                .bearer_auth(&manager)
                .json(&invalid)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        client
            .put(format!("{api}{path}/secrets/forged"))
            .bearer_auth(&manager)
            .json(
                &json!({"revision":rotated["revision"],"value":"value","application":{"app":"two"}})
            )
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let policy_file = directory.path().join("limits.json");
    let policy = json!({"size_class":"lite","invocation_timeout_ms":5000});
    std::fs::write(&policy_file, serde_json::to_vec(&policy).unwrap()).unwrap();
    cli(
        &api,
        &manager,
        "limits",
        Some("one"),
        &["--file", policy_file.to_str().unwrap()],
    )
    .await
    .unwrap();
    let policy_inspect = cli(&api, &reader, "get", Some("one"), &[]).await.unwrap();
    assert_eq!(policy_inspect["application"]["limits"], policy);
    assert_eq!(client.put(format!("{api}{path}/limits")).bearer_auth(&manager).json(&json!({"revision":policy_inspect["revision"],"limits":{"size_class":"lite","invocation_timeout_ms":0}})).send().await.unwrap().status(), StatusCode::BAD_REQUEST);
    for limits in [
        json!({"vcpus":1,"memory_mib":256,"invocation_timeout_ms":5000}),
        json!({"size_class":"dev","invocation_timeout_ms":5000}),
    ] {
        assert_eq!(
            client
                .put(format!("{api}{path}/limits"))
                .bearer_auth(&manager)
                .json(&json!({"revision":policy_inspect["revision"],"limits":limits}))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }
    assert!(cli(&api, &reader, "limits-reset", Some("one"), &[])
        .await
        .is_err());
    cli(&api, &manager, "limits-reset", Some("one"), &[])
        .await
        .unwrap();
    cli(
        &api,
        &manager,
        "secret-delete",
        Some("one"),
        &["--secret", "api-token"],
    )
    .await
    .unwrap();
    assert_eq!(
        cli(&api, &reader, "secrets", Some("one"), &[])
            .await
            .unwrap()["secrets"],
        json!({})
    );
    let snapshot = backend.read(&id("one")).await.unwrap();
    assert!(snapshot.application(&id("one")).unwrap().limits.is_none());
    assert!(backend
        .resolve_environment(snapshot.application(&id("one")).unwrap(), &secret_handler)
        .await
        .is_err());
    assert_eq!(
        backend
            .resolve_environment(
                backend
                    .read(&id("two"))
                    .await
                    .unwrap()
                    .application(&id("two"))
                    .unwrap(),
                &secret_handler
            )
            .await
            .unwrap()[&"PRIVATE_TOKEN".to_owned().try_into().unwrap()],
        "api-scope-two"
    );

    assert_eq!(client.put(format!("{api}{path}/resources")).bearer_auth(&manager).json(&json!({"revision": inspected["revision"], "resource": {"resource":{"kind":"d1","id":"forged","tenant":"two"}}})).send().await.unwrap().status(), StatusCode::UNPROCESSABLE_ENTITY);

    let code = b"export default { fetch() { return new Response('guest'); } };";
    let digest = content_digest(code);
    assert_eq!(
        client
            .put(format!("{api}{path}/artifacts/{digest}"))
            .bearer_auth(&manager)
            .body("wrong bytes")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    let artifact = directory.path().join("index.js");
    std::fs::write(&artifact, code).unwrap();
    let uploaded = cli(
        &api,
        &manager,
        "artifact",
        Some("one"),
        &["--file", artifact.to_str().unwrap()],
    )
    .await
    .unwrap();
    assert_eq!(uploaded["artifact_sha256"], digest);
    assert_eq!(
        backend
            .artifact(&id("one"), &digest)
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        code
    );
    assert!(backend
        .artifact(&id("two"), &digest)
        .await
        .unwrap()
        .is_none());
    let manifest = directory.path().join("manifest.json");
    let mut body = json!({"manifest_version":1, "application": {"app":"one"}, "id":"v1",
            "handlers":[{"id":"main", "language":"javascript", "entrypoint":"index.js", "artifact_sha256":digest, "runtime_image_sha256":"b".repeat(64), "bindings":{"DB":{"kind":"resource", "resource":{"kind":"d1","id":"db"}}}}]});
    std::fs::write(&manifest, serde_json::to_vec(&body).unwrap()).unwrap();
    cli(
        &api,
        &manager,
        "stage",
        Some("one"),
        &["--file", manifest.to_str().unwrap()],
    )
    .await
    .unwrap();
    let observed_manifest = cli(
        &api,
        &reader,
        "manifest",
        Some("one"),
        &["--deployment", "v1"],
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_value::<Deployment>(observed_manifest).unwrap(),
        serde_json::from_value::<Deployment>(body.clone()).unwrap()
    );
    body["application"]["app"] = json!("two");
    assert_eq!(
        client
            .put(format!("{api}{path}/deployments/v1"))
            .bearer_auth(&manager)
            .json(&body)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    body["application"]["app"] = json!("one");
    body["handlers"][0]["artifact_sha256"] = json!("c".repeat(64));
    assert_eq!(
        client
            .put(format!("{api}{path}/deployments/v1"))
            .bearer_auth(&manager)
            .json(&body)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    // Staging never pretends the guest has been qualified or publishes it.
    let stored = backend.read(&id("one")).await.unwrap();
    assert!(stored.application(&id("one")).unwrap().active.is_none());
    check_activation(&api, &backend, (&reader, &manager, &operator), &digest).await;
    cli(&api, &manager, "deactivate", Some("one"), &[])
        .await
        .unwrap();
    assert_eq!(
        backend
            .read(&id("one"))
            .await
            .unwrap()
            .application(&id("one"))
            .unwrap()
            .resources
            .len(),
        1
    );
    check_lifecycle(
        &api,
        &backend,
        (&reader, &manager, &owner, &operator),
        &digest,
    )
    .await;
    assert!(cli("http://example.com", &manager, "list", None, &[])
        .await
        .is_err());
    cli(
        &api,
        &manager,
        "secret-put",
        Some("one"),
        &[
            "--secret",
            "api-token",
            "--file",
            secret_file.to_str().unwrap(),
        ],
    )
    .await
    .unwrap();
    let mut referenced: Deployment = serde_json::from_value(body).unwrap();
    referenced.id = "active-reference".to_owned().try_into().unwrap();
    referenced.handlers[0].secrets.insert(
        "PRIVATE_TOKEN".to_owned().try_into().unwrap(),
        "api-token".to_owned().try_into().unwrap(),
    );
    backend.stage(&referenced).await.unwrap();
    let mut snapshot = backend.read(&id("one")).await.unwrap();
    backend.activate(&mut snapshot, referenced).await.unwrap();
    backend.commit(snapshot).await.unwrap();
    let active = cli(&api, &reader, "secrets", Some("one"), &[])
        .await
        .unwrap();
    assert_eq!(
        client
            .delete(format!("{api}{path}/secrets/api-token"))
            .bearer_auth(&manager)
            .json(&json!({"revision":active["revision"]}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    cli(&api, &manager, "deactivate", Some("one"), &[])
        .await
        .unwrap();
    cli(
        &api,
        &manager,
        "secret-delete",
        Some("one"),
        &["--secret", "api-token"],
    )
    .await
    .unwrap();
    eprintln!("application API/CLI secrets/limits: authenticated scoped rotation/deletion/listing with redacted values, other-application/read edits rejected before body, stale revisions refused, opaque versions and same-name resource isolation across applications, invalid values/identity/limits refused, active-reference deletion conflict and policy reset; execution availability remains false");
    server.abort();
}
