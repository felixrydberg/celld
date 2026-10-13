//! Public application qualification. Run one ignored selector per process.
#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods)]

use celld::application_api::{Access, Authorization, Credential, Permission};
use celld::applications::{content_digest, ApplicationStore};
use celld_applications::ApplicationId;
use serde_json::{json, Value};
use std::io::Write;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};
use tokio::time::timeout;

#[path = "application_e2e/faults.rs"]
mod faults;
#[path = "application_e2e/lifecycle.rs"]
mod lifecycle;
#[path = "application_e2e/r2.rs"]
mod r2;
#[path = "application_e2e/services.rs"]
mod services;

#[path = "application_e2e/cache_wakes.rs"]
mod cache_wakes;

const PLATFORM_DOMAIN: &str = "apps.test";
const NODE_SOURCE: &str = include_str!("fixtures/node-worker.mjs");

struct RemoteFleet {
    bucket: String,
    endpoint: String,
    region: String,
    access_key: String,
    secret_key: String,
}

struct Harness {
    directory: PathBuf,
    runtime_sha256: String,
    api: String,
    token: String,
    reader: String,
    other_token: String,
    dynamic: bool,
    remote: Option<RemoteFleet>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Harness {
    async fn new(label: &str, dynamic: bool) -> Self {
        Self::new_with_fleet(label, dynamic, None).await
    }

    async fn new_remote(label: &str, dynamic: bool, remote: RemoteFleet) -> Self {
        Self::new_with_fleet(label, dynamic, Some(remote)).await
    }

    async fn new_with_fleet(label: &str, dynamic: bool, remote: Option<RemoteFleet>) -> Self {
        let artifacts = std::env::var_os("CELLD_APPLICATION_NODE_ARTIFACTS")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        std::fs::create_dir_all(&artifacts).unwrap();
        let directory = tempfile::Builder::new()
            .prefix(&format!("t29-{label}-"))
            .tempdir_in(&artifacts)
            .unwrap()
            .keep();
        eprintln!("T29 artifacts: {}", directory.display());
        let token = "o".repeat(40);
        let reader = "r".repeat(40);
        let other_token = "b".repeat(40);
        let credential = |token: &str, permission| Credential {
            token_sha256: content_digest(token.as_bytes()),
            permission,
        };
        let application = |app: &str| ApplicationId {
            app: app.to_owned().try_into().unwrap(),
        };
        let auth = Authorization {
            credentials: vec![
                credential(&token, Permission::Operator {}),
                credential(
                    &reader,
                    Permission::Application {
                        application: application("e2e-one"),
                        access: Access::Read,
                    },
                ),
                credential(
                    &other_token,
                    Permission::Application {
                        application: application("e2e-two"),
                        access: Access::Manage,
                    },
                ),
            ],
        };
        let bucket = match &remote {
            Some(remote) => celld::bucket::Bucket::open(
                &remote.bucket,
                Some(&remote.endpoint),
                &remote.region,
                Some(celld::bucket::StaticCredentials {
                    access_key_id: remote.access_key.clone(),
                    secret_access_key: remote.secret_key.clone(),
                    session_token: None,
                }),
                Some("t32-api"),
            )
            .unwrap(),
            None => celld::dev::open_local_bucket(&directory.join("bucket.sqlite")).unwrap(),
        };
        let router = celld::application_api::router(
            ApplicationStore::new(bucket),
            auth,
            PLATFORM_DOMAIN.into(),
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let runtime_sha256 = if dynamic {
            let image = PathBuf::from(
                std::env::var_os("CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE")
                    .expect("runtime image is required"),
            );
            let digest = std::fs::read_to_string(image.with_extension("sha256"))
                .unwrap()
                .trim()
                .to_owned();
            let vms = json!({
                "jailer": "/usr/local/bin/jailer", "firecracker": "/usr/local/bin/firecracker",
                "kernel": {"path": std::env::var("CELLD_APPLICATION_JAVASCRIPT_TEST_KERNEL").unwrap(),
                    "sha256": std::env::var("CELLD_APPLICATION_JAVASCRIPT_TEST_KERNEL_SHA256").unwrap()},
                "runtimes": [{"path": image, "sha256": digest}], "uid":1000, "gid":1000,
                "max_vms":4, "max_memory_mib":12288, "idle_ttl_secs":600,
                "startup_pool":0, "platform_domain":PLATFORM_DOMAIN,
                "container_fence_image":std::env::var("CELLD_APPLICATION_CONTAINER_FENCE_IMAGE").ok(),
            });
            std::fs::write(
                directory.join("vms.json"),
                serde_json::to_vec(&vms).unwrap(),
            )
            .unwrap();
            digest
        } else {
            String::new()
        };
        Self {
            directory,
            runtime_sha256,
            api,
            token,
            reader,
            other_token,
            dynamic,
            remote,
            server,
        }
    }

    async fn cli_as(
        &self,
        token: &str,
        app: &str,
        command: &str,
        args: &[&str],
    ) -> Result<Value, String> {
        let output = timeout(
            Duration::from_secs(300),
            Command::new(env!("CARGO_BIN_EXE_celld"))
                .args(["application", command, "--api", &self.api, "--app", app])
                .args(args)
                .env("CELLD_APPLICATION_TOKEN", token)
                .kill_on_drop(true)
                .output(),
        )
        .await
        .map_err(|_| format!("CLI {command} timed out"))?
        .map_err(|error| error.to_string())?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).into_owned());
        }
        serde_json::from_slice(&output.stdout).map_err(|error| format!("CLI {command}: {error}"))
    }

    async fn cli(&self, app: &str, command: &str, args: &[&str]) -> Value {
        self.cli_as(&self.token, app, command, args)
            .await
            .unwrap_or_else(|error| panic!("{app} {command}: {error}"))
    }

    async fn create(&self, app: &str) {
        self.cli(app, "create", &[]).await;
    }

    fn json_file(&self, name: &str, value: &Value) -> PathBuf {
        let path = self.directory.join(name);
        std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
        path
    }

    async fn resource(&self, app: &str, kind: &str, id: &str, configuration: Value) {
        let path = self.json_file(
            &format!("{app}-{kind}-{id}.json"),
            &json!({"resource":{"kind":kind,"id":id},"configuration":configuration}),
        );
        self.cli(app, "resource", &["--file", path.to_str().unwrap()])
            .await;
    }

    async fn upload(&self, app: &str, path: &Path) -> String {
        self.cli(app, "artifact", &["--file", path.to_str().unwrap()])
            .await;
        content_digest(&std::fs::read(path).unwrap())
    }

    async fn source(&self, app: &str, source: &str) -> String {
        let directory = tempfile::Builder::new()
            .prefix("source-")
            .tempdir_in(&self.directory)
            .unwrap();
        std::fs::write(directory.path().join("main.mjs"), source).unwrap();
        // Keep the output outside the source tree: the builder rejects its own input there.
        let output = directory.path().with_extension("bundle");
        let built = Command::new("python3")
            .arg("-B")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../applications/guest/build-application-image.py"),
            )
            .arg(directory.path())
            .arg(&output)
            .kill_on_drop(true)
            .output()
            .await
            .unwrap();
        assert!(
            built.status.success(),
            "{}",
            String::from_utf8_lossy(&built.stderr)
        );
        self.upload(app, &output).await
    }

    async fn stage(&self, app: &str, manifest: &Value) {
        let path = self.json_file(&format!("{app}-manifest.json"), manifest);
        self.cli(app, "stage", &["--file", path.to_str().unwrap()])
            .await;
    }

    async fn activate(&self, app: &str, version: &str) {
        self.cli(app, "activate", &["--deployment", version]).await;
    }

    async fn hostname(&self, app: &str, handler: &str) -> String {
        self.cli(app, "get", &[]).await["hostnames"][handler]
            .as_str()
            .unwrap()
            .to_owned()
    }

    async fn asset_hostname(&self, app: &str) -> String {
        self.cli(app, "get", &[]).await["asset_hostname"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    async fn start(&self, name: &str) -> Node {
        let name = format!(
            "{}-{name}",
            std::env::var("CELLD_APPLICATION_RUN_ID")
                .unwrap_or_else(|_| format!("t29-{}", std::process::id()))
        );
        let public = free_port();
        let internal = free_port();
        let log = std::fs::File::create(self.directory.join(format!("{name}.log"))).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_celld"));
        command.as_std_mut().process_group(0);
        match &self.remote {
            Some(remote) => {
                command
                    .env_remove("CELLD_INTERNAL_DEV_STORE")
                    .env_remove("CELLD_TEST_BUCKET")
                    .env_remove("AWS_SESSION_TOKEN")
                    .env("AWS_ACCESS_KEY_ID", &remote.access_key)
                    .env("AWS_SECRET_ACCESS_KEY", &remote.secret_key)
                    .env("S3_ENDPOINT", &remote.endpoint)
                    .env("AWS_REGION", &remote.region)
                    .args([
                        "--bucket",
                        &remote.bucket,
                        "--endpoint",
                        &remote.endpoint,
                        "--region",
                        &remote.region,
                    ]);
            }
            None => {
                command
                    .env(
                        "CELLD_INTERNAL_DEV_STORE",
                        self.directory.join("bucket.sqlite"),
                    )
                    .env("CELLD_TEST_BUCKET", "celld-dev")
                    .args(["--bucket", "celld-dev"]);
            }
        }
        command
            .env_remove("CELLD_TEST_SCRIPT_PATH")
            .env_remove("CELLD_TEST_DO_CLASSES")
            .env_remove("CELLD_TEST_KV")
            .env("CELLD_ADDR", format!("127.0.0.1:{public}"))
            .env("CELLD_INTERNAL_ADDR", format!("127.0.0.1:{internal}"))
            .env("CELLD_WATCH", self.directory.join(&name))
            .env("CELLD_NODE", &name)
            .env("CELLD_WAKER_TICK_MS", "1000")
            .env("CELLD_HANDLER_BUDGET_S", "60")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .kill_on_drop(false);
        if self.dynamic {
            command
                .arg("--application-vms")
                .arg(self.directory.join("vms.json"));
        } else {
            command.arg("--application-domain").arg(PLATFORM_DOMAIN);
        }
        let mut node = Node {
            child: command.spawn().unwrap(),
            public,
            internal,
        };
        register_process(node.child.id().unwrap());
        if let Some(directory) = std::env::var_os("CELLD_APPLICATION_NODE_ARTIFACTS") {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(PathBuf::from(directory).join("nodes.jsonl"))
                .unwrap();
            writeln!(
                file,
                "{}",
                json!({"node":name,"pid":node.child.id().unwrap()})
            )
            .unwrap();
        }
        timeout(Duration::from_secs(240), async {
            loop {
                node.vm_pids();
                assert!(
                    node.child.try_wait().unwrap().is_none(),
                    "node exited before readiness"
                );
                if let Ok(reply) = reqwest::get(format!(
                    "http://127.0.0.1:{public}/.well-known/celld/health"
                ))
                .await
                {
                    if reply.status() == 200 {
                        break;
                    }
                    assert_eq!(reply.status(), 503, "unexpected readiness response");
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("node did not become ready");
        node
    }
}

struct Node {
    child: Child,
    public: u16,
    internal: u16,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.vm_pids();
        if let Some(pid) = self.child.id() {
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
        }
    }
}

impl Node {
    fn vm_pids(&self) -> Vec<u32> {
        let Some(pid) = self.child.id() else {
            return vec![];
        };
        let parent = format!("PPid:\t{pid}");
        std::fs::read_dir("/proc")
            .unwrap()
            .flatten()
            .filter_map(|entry| {
                let pid = entry.file_name().to_str()?.parse().ok()?;
                let status = std::fs::read_to_string(entry.path().join("status")).ok()?;
                let command = std::fs::read_to_string(entry.path().join("comm")).ok()?;
                if status.lines().any(|line| line == parent)
                    && matches!(command.trim(), "jailer" | "firecracker")
                {
                    register_process(pid);
                    Some(pid)
                } else {
                    None
                }
            })
            .collect()
    }

    async fn drain(mut self) {
        let pids = self.vm_pids();
        unsafe {
            libc::kill(self.child.id().unwrap() as i32, libc::SIGTERM);
        }
        let status = timeout(Duration::from_secs(90), self.child.wait())
            .await
            .expect("node did not drain")
            .unwrap();
        assert!(status.success(), "node drain failed: {status}");
        for pid in pids {
            assert!(
                !Path::new(&format!("/proc/{pid}")).exists(),
                "VM {pid} outlived drain"
            );
        }
    }

    async fn kill(mut self) {
        let pids = self.vm_pids();
        let parent = pids.first().map(|pid| {
            let membership = std::fs::read_to_string(format!("/proc/{pid}/cgroup")).unwrap();
            let path = membership
                .lines()
                .find_map(|line| line.strip_prefix("0::"))
                .unwrap();
            let leaf = Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/'));
            let parent = leaf.parent().unwrap().to_owned();
            assert!(parent
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("celld-vms-"));
            std::fs::create_dir(parent.join("fixture-stop")).unwrap();
            std::fs::write(parent.join("cgroup.max.descendants"), "0").unwrap();
            parent
        });
        for pid in &pids {
            unsafe {
                libc::kill(*pid as i32, libc::SIGKILL);
            }
        }
        timeout(Duration::from_secs(10), async {
            while pids
                .iter()
                .any(|pid| Path::new(&format!("/proc/{pid}")).exists())
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("VMs were not reaped before host kill");
        self.child.start_kill().unwrap();
        timeout(Duration::from_secs(10), self.child.wait())
            .await
            .expect("node kill did not settle")
            .unwrap();
        if let Some(parent) = parent {
            std::fs::remove_dir(parent.join("fixture-stop")).unwrap();
            std::fs::remove_dir(parent).unwrap();
        }
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn register_process(pid: u32) {
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return;
    };
    let start_ticks: u64 = stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .nth(19)
        .unwrap()
        .parse()
        .unwrap();
    if let Some(directory) = std::env::var_os("CELLD_APPLICATION_NODE_ARTIFACTS") {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(PathBuf::from(directory).join("processes.jsonl"))
            .unwrap();
        writeln!(
            file,
            "{}",
            json!({"pid":pid,"start_ticks":start_ticks,"pgid":unsafe { libc::getpgid(pid as i32) }})
        )
        .unwrap();
    }
}

async fn request(node: &Node, host: &str, path: &str) -> (u16, String) {
    let reply = reqwest::Client::builder()
        .timeout(Duration::from_secs(65))
        .build()
        .unwrap()
        .get(format!("http://127.0.0.1:{}{path}", node.public))
        .header("host", host)
        .send()
        .await
        .unwrap();
    (reply.status().as_u16(), reply.text().await.unwrap())
}

async fn get(node: &Node, host: &str, path: &str) -> String {
    let (status, body) = request(node, host, path).await;
    assert_eq!(status, 200, "{host}{path}: {body}");
    body
}

async fn json(node: &Node, host: &str, path: &str) -> Value {
    serde_json::from_str(&get(node, host, path).await).unwrap()
}

fn passed(name: &str, began: Instant) {
    eprintln!(
        "T29_RESULT {}",
        json!({"scenario":name,"status":"passed","duration_ms":began.elapsed().as_millis()})
    );
}

async fn ws_echo_server() -> u16 {
    use fastwebsockets::{FragmentCollector, OpCode, Payload};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let service = hyper::service::service_fn(
                    |mut request: hyper::Request<hyper::body::Incoming>| async move {
                        let (response, upgrade) =
                            fastwebsockets::upgrade::upgrade(&mut request).unwrap();
                        tokio::spawn(async move {
                            let mut socket = FragmentCollector::new(upgrade.await.unwrap());
                            while let Ok(frame) = socket.read_frame().await {
                                let echo = match frame.opcode {
                                    OpCode::Text => fastwebsockets::Frame::text(Payload::Owned(
                                        [b"echo:".as_slice(), &frame.payload].concat(),
                                    )),
                                    OpCode::Binary => fastwebsockets::Frame::binary(
                                        Payload::Owned(frame.payload.to_vec()),
                                    ),
                                    _ => break,
                                };
                                if socket.write_frame(echo).await.is_err() {
                                    break;
                                }
                            }
                        });
                        Ok::<_, std::convert::Infallible>(response)
                    },
                );
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                    .with_upgrades()
                    .await;
            });
        }
    });
    port
}

/// A real WebSocket client on a node's public port, as a browser would be.
async fn ws_open(port: u16, host: &str, path: &str) -> celld::ws_client::Connection {
    let mut headers = hyper::HeaderMap::new();
    headers.insert(hyper::header::HOST, host.parse().unwrap());
    match celld::ws_client::connect(&format!("ws://127.0.0.1:{port}{path}"), headers).await {
        Ok(connection) => connection,
        Err(celld::ws_client::Error::Declined(declined)) => panic!(
            "WebSocket {path} declined: {} {}",
            declined.status,
            String::from_utf8_lossy(&declined.body)
        ),
        Err(celld::ws_client::Error::Failed(error)) => panic!("WebSocket {path}: {error:#}"),
    }
}

async fn ws_send(connection: &mut celld::ws_client::Connection, text: &str) {
    connection
        .socket
        .write_frame(fastwebsockets::Frame::text(fastwebsockets::Payload::Owned(
            text.as_bytes().to_vec(),
        )))
        .await
        .unwrap();
}

/// The next frame: `Ok(text)` for a text frame, `Err((code, reason))` for a close.
async fn ws_next(connection: &mut celld::ws_client::Connection) -> Result<String, (u16, String)> {
    let frame = timeout(Duration::from_secs(30), connection.socket.read_frame())
        .await
        .expect("no WebSocket frame arrived")
        .expect("WebSocket failed");
    match frame.opcode {
        fastwebsockets::OpCode::Text => Ok(String::from_utf8(frame.payload.to_vec()).unwrap()),
        fastwebsockets::OpCode::Close => {
            let payload = frame.payload.to_vec();
            let code = payload
                .get(..2)
                .map_or(1005, |code| u16::from_be_bytes([code[0], code[1]]));
            Err((
                code,
                String::from_utf8_lossy(payload.get(2..).unwrap_or(&[])).into_owned(),
            ))
        }
        other => panic!("unexpected WebSocket frame {other:?}"),
    }
}
