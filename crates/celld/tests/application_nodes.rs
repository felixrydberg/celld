//! Real `celld` processes hosting application VMs, sharing one development
//! store as their fleet bucket. Needs the actual-VM environment of
//! `application_vm.rs` (`CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE` and the
//! kernel variables, run as root). Explicitly ignored in ordinary test runs.
#![cfg(target_os = "linux")]
// These integration tests drive real node processes, containers and wall-clock
// deadlines from outside the node, so the execution-boundary lint does not apply.
#![allow(clippy::disallowed_methods)]

use celld::applications::{content_digest, ApplicationStore, Resource};
use celld_applications::{ApplicationId, Deployment, ResourceKind, ResourceRef};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::time::timeout;

/// The `platform_domain` of every node's VM configuration.
const PLATFORM_DOMAIN: &str = "apps.test";

/// The checks name each application by a logical label ("route.test",
/// "queues.test"). Activation records the `<route id>.apps.test` hostname the
/// host assigned to that application's handler; every request helper sends that
/// hostname as Host. A name that was never activated ("other.test") is sent
/// as is: no application can claim it.
static HOSTS: std::sync::Mutex<std::collections::BTreeMap<String, String>> =
    std::sync::Mutex::new(std::collections::BTreeMap::new());

fn host_of(label: &str) -> String {
    HOSTS.lock().unwrap().get(label).cloned().unwrap_or_else(|| label.to_owned())
}

const SOURCE: &str = include_str!("fixtures/node-worker.mjs");

struct Node {
    child: Child,
    public: u16,
    internal: u16,
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(pid) = self.child.id() {
            unsafe { libc::kill(pid as i32, libc::SIGTERM) };
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

/// A node on the shared store serving an original one-script Worker, with
/// application VMs only when given a configuration.
/// `kv` is the node's `CELLD_TEST_KV` knob list; empty for none. `extra` is
/// any further environment, such as `CELLD_HANDLER_BUDGET_S`.
async fn start(
    directory: &Path,
    name: &str,
    vms: Option<&Path>,
    application_only: bool,
    kv: &str,
    extra: &[(&str, &str)],
) -> Node {
    let (public, internal) = (free_port(), free_port());
    let log = std::fs::File::create(directory.join(format!("{name}.log"))).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_celld"));
    command
        .env("CELLD_INTERNAL_DEV_STORE", directory.join("bucket.sqlite"))
        .env("CELLD_TEST_BUCKET", "celld-dev")
        .env("CELLD_TEST_SCRIPT_PATH", directory.join("worker.js"))
        .env("CELLD_TEST_DO_CLASSES", "")
        .env("CELLD_ADDR", format!("127.0.0.1:{public}"))
        .env("CELLD_INTERNAL_ADDR", format!("127.0.0.1:{internal}"))
        .env("CELLD_WATCH", directory.join(name))
        .env("CELLD_NODE", name)
        // Exercise wake recovery without waiting out the default three-minute
        // advisory scanner lease. Ownership and durable alarm checks stay intact.
        .env("CELLD_WAKER_TICK_MS", "1000")
        .env("CELLD_TEST_KV", kv)
        .envs(extra.iter().copied())
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .kill_on_drop(false);
    if let Some(vms) = vms {
        command.arg("--application-vms").arg(vms);
    }
    if application_only && vms.is_none() { command.arg("--application-domain").arg(PLATFORM_DOMAIN); }
    if application_only {
        command.arg("--bucket").arg("celld-dev");
        command.env_remove("CELLD_TEST_SCRIPT_PATH");
    }
    let node = Node {
        child: command.spawn().unwrap(),
        public,
        internal,
    };
    let health = format!("http://127.0.0.1:{public}/.well-known/celld/health");
    let mut observed_preparation = false;
    timeout(Duration::from_secs(240), async {
        loop {
            if let Ok(reply) = reqwest::get(&health).await {
                if reply.status() == 200 {
                    break;
                }
                if reply.status() == 503 && vms.is_some() && !observed_preparation {
                    assert_eq!(reply.text().await.unwrap(), "{\"ok\":false}");
                    for (port, path) in [
                        (internal, "/.well-known/celld/health"),
                        (internal, "/peer/log/tail"),
                        (public, "/state"),
                        (public, "/hello"),
                    ] {
                        let refused = reqwest::get(format!("http://127.0.0.1:{port}{path}"))
                            .await
                            .unwrap();
                        assert_eq!(refused.status(), 503, "{name}: startup admitted {path}");
                    }
                    observed_preparation = true;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("node did not become ready");
    assert!(
        vms.is_none() || observed_preparation,
        "{name}: did not observe fail-closed cache preparation"
    );
    node
}

async fn status(port: u16, host: &str, path: &str) -> u16 {
    reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}{path}"))
        .header("host", host_of(host))
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

async fn get(port: u16, host: &str, path: &str) -> String {
    let reply = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}{path}"))
        .header("host", host_of(host))
        .send()
        .await
        .unwrap();
    let status = reply.status();
    let body = reply.text().await.unwrap();
    assert_eq!(status, 200, "{path}: {body}");
    body
}

/// Like `get`, but `(status, body)`, and `(0, error)` when the node cannot
/// answer, as while it recovers or is stopped.
async fn try_get(port: u16, host: &str, path: &str, limit: Duration) -> (u16, String) {
    let reply = reqwest::Client::builder()
        .timeout(limit)
        .build()
        .unwrap()
        .get(format!("http://127.0.0.1:{port}{path}"))
        .header("host", host_of(host))
        .send()
        .await;
    match reply {
        Ok(reply) => (reply.status().as_u16(), reply.text().await.unwrap_or_default()),
        Err(error) => (0, error.to_string()),
    }
}

/// A read retried until the node answers 200, for use while it recovers. Never
/// use it for a write: a failed attempt may have committed.
async fn get_eventually(port: u16, host: &str, path: &str) -> String {
    timeout(Duration::from_secs(120), async {
        loop {
            let (status, body) = try_get(port, host, path, Duration::from_secs(20)).await;
            if status == 200 {
                break body;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{path} never answered on port {port}"))
}

async fn get_json_eventually(node: &Node, path: &str) -> serde_json::Value {
    serde_json::from_str(&get_eventually(node.public, "route.test", path).await).unwrap()
}

/// Whether the node's native state holds a cell whose name contains `needle`.
async fn holds_cell(node: &Node, needle: &str) -> bool {
    reqwest::get(format!("http://127.0.0.1:{}/state", node.internal))
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
        .contains(needle)
}

const MIB: usize = 1024 * 1024;

/// A WebSocket echo peer on loopback: text comes back prefixed, binary as is,
/// and a close is answered. Application handlers dial it through their node.
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
                                    OpCode::Binary => fastwebsockets::Frame::binary(Payload::Owned(
                                        frame.payload.to_vec()),
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
    headers.insert(hyper::header::HOST, host_of(host).parse().unwrap());
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
        .write_frame(fastwebsockets::Frame::text(
            fastwebsockets::Payload::Owned(text.as_bytes().to_vec())))
        .await
        .unwrap();
}

/// The next frame: `Ok(text)` for a text frame, `Err((code, reason))` for a close.
async fn ws_next(
    connection: &mut celld::ws_client::Connection) -> Result<String, (u16, String)> {
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
            Err((code, String::from_utf8_lossy(payload.get(2..).unwrap_or(&[])).into_owned()))
        }
        other => panic!("unexpected WebSocket frame {other:?}"),
    }
}

async fn kv_put(node: &Node, key: &str, size: usize, fill: u8) -> serde_json::Value {
    let path = format!("/kv-put?key={key}&size={size}&fill={fill}");
    serde_json::from_str(&get(node.public, "route.test", &path).await).unwrap()
}

/// `null` for an absent key, otherwise the value's length and edge bytes.
async fn kv_peek(node: &Node, key: &str) -> serde_json::Value {
    let path = format!("/kv-peek?key={key}");
    serde_json::from_str(&get(node.public, "route.test", &path).await).unwrap()
}

fn kv_edges(size: usize, fill: u8) -> serde_json::Value {
    serde_json::json!({"length":size,"head":[1,2,3,4],"tail":[5,6,7,8],"middle":fill})
}

/// Every value the durability steps expect, as `(key, size, fill)`.
const KV_VALUES: [(&str, usize, u8); 3] =
    [("small", 16, 5), ("dup", 2 * MIB, 9), ("keep", 3 * MIB, 3)];

async fn kv_expect_values(node: &Node) {
    for (key, size, fill) in KV_VALUES {
        assert_eq!(kv_peek(node, key).await, kv_edges(size, fill), "{key}");
    }
}

/// An application's queue deliveries, as `[queue, n, attempts, batch size,
/// delivered at, sent at, label]` in delivery order.
async fn queue_log(node: &Node, host: &str) -> Vec<serde_json::Value> {
    serde_json::from_str(&get(node.public, host, "/q-log").await).unwrap()
}

/// The deliveries of one label (see `queue_log`).
async fn queue_rows(node: &Node, host: &str, label: &str) -> Vec<serde_json::Value> {
    let mut rows = queue_log(node, host).await;
    rows.retain(|row| row[6] == label);
    rows
}

/// Like `queue_rows`, but empty while the node cannot answer, as just after
/// its VMs were killed or its database changed owner.
async fn queue_rows_lossy(node: &Node, host: &str, label: &str) -> Vec<serde_json::Value> {
    let fetch = async {
        let reply = reqwest::Client::new()
            .get(format!("http://127.0.0.1:{}/q-log", node.public))
            .header("host", host_of(host))
            .send()
            .await
            .ok()?;
        if reply.status() != 200 {
            return None;
        }
        serde_json::from_str::<Vec<serde_json::Value>>(&reply.text().await.ok()?).ok()
    };
    let mut rows = fetch.await.unwrap_or_default();
    rows.retain(|row| row[6] == label);
    rows
}

/// An application's cron runs, as `[cron, scheduled time, attempt, label]` in
/// run order; empty while the node cannot answer, as just after a replacement.
async fn cron_rows(node: &Node, host: &str) -> Vec<Vec<serde_json::Value>> {
    let fetch = async {
        let reply = reqwest::Client::new()
            .get(format!("http://127.0.0.1:{}/cron-log", node.public))
            .header("host", host_of(host))
            .send()
            .await
            .ok()?;
        if reply.status() != 200 {
            return None;
        }
        serde_json::from_str(&reply.text().await.ok()?).ok()
    };
    fetch.await.unwrap_or_default()
}

/// Publish the staged deployment `id` with each application's own CAS revision.
async fn activate_all(store: &ApplicationStore, applications: [&ApplicationId; 2], id: &str) {
    let id: celld_applications::Name = id.to_owned().try_into().unwrap();
    for application in applications {
        let mut snapshot = store.read(application).await.unwrap();
        let deployment = store.deployment(application, &id).await.unwrap().unwrap();
        store.activate(&mut snapshot, deployment).await.unwrap();
        store.commit(snapshot).await.unwrap();
    }
}

/// Large-value objects in the shared bucket, whichever cell owns them.
async fn blob_count(bucket: &celld::bucket::Bucket) -> usize {
    bucket.list("kv/blobs-v2/").await.unwrap().len()
}

/// The jailer and VMM processes this node started.
fn vm_pids(node: &Node) -> Vec<u32> {
    let parent = format!("PPid:\t{}", node.child.id().unwrap());
    std::fs::read_dir("/proc")
        .unwrap()
        .flatten()
        .filter_map(|item| {
            let pid = item.file_name().to_str()?.parse::<u32>().ok()?;
            let status = std::fs::read_to_string(item.path().join("status")).ok()?;
            let command = std::fs::read_to_string(item.path().join("comm")).ok()?;
            (status.lines().any(|line| line == parent)
                && matches!(command.trim(), "jailer" | "firecracker"))
            .then_some(pid)
        })
        .collect()
}

/// SIGTERM is the node's graceful drain.
async fn drain(mut node: Node) {
    let pids = vm_pids(&node);
    unsafe { libc::kill(node.child.id().unwrap() as i32, libc::SIGTERM) };
    let status = timeout(Duration::from_secs(90), node.child.wait())
        .await
        .expect("node did not drain")
        .unwrap();
    assert!(status.success(), "node exited with {status}");
    for pid in pids {
        assert!(
            !Path::new(&format!("/proc/{pid}")).exists(),
            "VM process {pid} outlived the drain"
        );
    }
}

/// Reap only this fixture's VMs, then lose its native owner without draining.
/// Keeping the host alive for the VM reap avoids orphan VMM zombies.
async fn hard_stop(mut node: Node) -> Vec<u32> {
    let pids = vm_pids(&node);
    assert!(!pids.is_empty(), "no armed owner's VM to kill");
    // Keep the scoped parent present while refusing refill cgroups during this crash.
    let membership = std::fs::read_to_string(format!("/proc/{}/cgroup", pids[0])).unwrap();
    let path = membership
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .unwrap();
    let leaf = Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/'));
    let parent = leaf.parent().unwrap();
    assert!(parent
        .file_name()
        .unwrap()
        .to_string_lossy()
        .starts_with("celld-vms-"));
    let guard = parent.join("fixture-stop");
    std::fs::create_dir(&guard).unwrap();
    std::fs::write(parent.join("cgroup.max.descendants"), "0").unwrap();
    for pid in &pids {
        unsafe { libc::kill(*pid as i32, libc::SIGKILL) };
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
    .expect("killed VMs were not reaped by their owner");
    timeout(Duration::from_secs(10), async {
        while !vm_pids(&node).is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("owner did not reap refill attempts before crash");
    node.child.start_kill().unwrap();
    let status = timeout(Duration::from_secs(10), node.child.wait())
        .await
        .unwrap()
        .unwrap();
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(
        status.signal(),
        Some(libc::SIGKILL),
        "owner did not exit by SIGKILL"
    );
    std::fs::remove_dir(guard).unwrap();
    std::fs::remove_dir(parent).unwrap();
    pids
}

async fn wait_node_fence(bucket: &celld::bucket::Bucket, name: &str) {
    timeout(Duration::from_secs(30), async {
        loop {
            let Some((bytes, _)) = bucket.get(&format!("nodes/{name}.json")).await.unwrap() else {
                break;
            };
            let lease: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            if u128::from(lease["expires_ms"].as_u64().unwrap()) <= now {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("hard-lost owner's real node lease did not expire");
}

async fn alarm_fired_once(node: &Node, name: &str) {
    let path = format!("/alarm-state?name={name}");
    let expected =
        serde_json::json!({"attempts":1,"hidden":true,"retryCount":0,"isRetry":false,"alarm":null});
    timeout(Duration::from_secs(90), async {
        loop {
            let state: serde_json::Value =
                serde_json::from_str(&get(node.public, "route.test", &path).await).unwrap();
            if state["attempts"].as_u64().unwrap() > 0 && state["alarm"].is_null() {
                assert_eq!(state, expected, "{name}");
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("committed alarm did not recover after losing its guest or owner");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let state: serde_json::Value =
        serde_json::from_str(&get(node.public, "route.test", &path).await).unwrap();
    assert_eq!(state, expected, "{name}: settled alarm fired again");
}

async fn alarm_state(node: &Node, host: &str, name: &str) -> serde_json::Value {
    serde_json::from_str(&get(node.public, host, &format!("/alarm-state?name={name}")).await)
        .unwrap()
}

async fn assert_alarm_unfired(node: &Node, name: &str, pending: bool) {
    let state = alarm_state(node, "route.test", name).await;
    assert_eq!(state["attempts"], 0, "{name}: fired: {state}");
    assert_eq!(state["alarm"].is_i64(), pending, "{name}: {state}");
}

async fn arm_pending_alarm(node: &Node, name: &str) {
    assert_eq!(
        get(
            node.public,
            "route.test",
            &format!("/alarm-arm?name={name}&delay=30000")
        )
        .await,
        "armed"
    );
    let state: serde_json::Value = serde_json::from_str(
        &get(
            node.public,
            "route.test",
            &format!("/alarm-state?name={name}"),
        )
        .await,
    )
    .unwrap();
    assert_eq!(state["attempts"], 0, "{name}: alarm fired before the loss");
    assert!(
        state["alarm"].is_i64(),
        "{name}: installation was not committed"
    );
}

/// A request that may fail: the body of a 200, otherwise `None`. A frozen,
/// fenced or recovering node answers late, wrongly or not at all.
async fn try_get_opt(port: u16, host: &str, path: &str) -> Option<String> {
    let reply = reqwest::Client::builder()
        .timeout(Duration::from_secs(90))
        .build()
        .unwrap()
        .get(format!("http://127.0.0.1:{port}{path}"))
        .header("host", host_of(host))
        .send()
        .await
        .ok()?;
    if reply.status() != 200 {
        return None;
    }
    reply.text().await.ok()
}

/// One `/t22` write to the object, D1, KV and the queue. `Some(elapsed)` only
/// when the node acknowledged it.
async fn t22_write(node: &Node, n: u32) -> Option<Duration> {
    let started = std::time::Instant::now();
    try_get_opt(node.public, "route.test", &format!("/t22?name=t22-obj&n={n}")).await?;
    Some(started.elapsed())
}

async fn t22_read(node: &Node) -> Option<serde_json::Value> {
    serde_json::from_str(&try_get_opt(node.public, "route.test", "/t22-read?name=t22-obj").await?).ok()
}

/// Everything acknowledged is visible and nothing unattempted is. An attempt
/// that was not acknowledged may or may not have landed. Queue messages are
/// delivered at least once, so they are checked separately by `t22_queued`.
async fn t22_expect(node: &Node, acknowledged: &[u32], attempted: &[u32]) {
    let read = t22_read(node).await.expect("acknowledged state was not readable");
    let rows: Vec<u32> = serde_json::from_value(read["rows"].clone()).unwrap();
    let keys: Vec<String> = serde_json::from_value(read["keys"].clone()).unwrap();
    let count = read["count"].as_u64().unwrap() as usize;
    for n in acknowledged {
        assert!(rows.contains(n), "acknowledged D1 write {n} was lost: {read}");
        assert!(keys.contains(&format!("t22:{n}")), "acknowledged KV write {n} was lost: {read}");
    }
    assert!(rows.iter().all(|n| attempted.contains(n)), "unattempted D1 row: {read}");
    assert!(count >= acknowledged.len() && count <= attempted.len(), "object count {count}: {read}");
}

/// Every acknowledged send is delivered to the consumer at least once.
async fn t22_queued(node: &Node, acknowledged: &[u32]) {
    timeout(Duration::from_secs(120), async {
        loop {
            let rows = queue_rows_lossy(node, "route.test", "t22").await;
            if acknowledged.iter().all(|n| rows.iter().any(|row| row[1] == *n)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("an acknowledged queue send was lost");
}

/// One facet write of `n` (`f-both` also writes the object's root storage;
/// `f-only` only the facet). `pad` incompressible bytes ride in the facet's row.
/// `Some(elapsed)` only when the node acknowledged it.
async fn facet_write(node: &Node, route: &str, n: u32, pad: usize) -> Option<Duration> {
    let started = std::time::Instant::now();
    try_get_opt(node.public, "route.test", &format!("/{route}?name=t28-obj&n={n}&pad={pad}")).await?;
    Some(started.elapsed())
}

/// The object's root keys and the facet's rows, as the node serving them sees them.
async fn facet_state(node: &Node) -> Option<(Vec<u32>, Vec<u32>)> {
    let state: serde_json::Value =
        serde_json::from_str(&try_get_opt(node.public, "route.test", "/f-read?name=t28-obj").await?).ok()?;
    Some((
        serde_json::from_value(state["root"].clone()).ok()?,
        serde_json::from_value(state["facet"].clone()).ok()?,
    ))
}

/// Every acknowledged facet and root write is visible, nothing unattempted is.
/// An attempt that was not acknowledged may or may not have landed.
async fn facet_expect(node: &Node, root: &[u32], facet: &[u32], attempted: &[u32]) {
    let (seen_root, seen_facet) = facet_state(node).await.expect("acknowledged state was not readable");
    for n in root {
        assert!(seen_root.contains(n), "acknowledged root write {n} was lost: {seen_root:?} {seen_facet:?}");
    }
    for n in facet {
        assert!(seen_facet.contains(n), "acknowledged facet write {n} was lost: {seen_root:?} {seen_facet:?}");
    }
    assert!(
        seen_root.iter().chain(&seen_facet).all(|n| attempted.contains(n)),
        "unattempted write: {seen_root:?} {seen_facet:?}"
    );
}

/// A node's folded log in its lease record: whether its fleet ensemble is
/// serving acknowledgements, and the members. `None` before the log opens.
async fn log_posture(bucket: &celld::bucket::Bucket, node: &str) -> Option<(bool, Vec<String>)> {
    let (bytes, _) = bucket.get(&format!("nodes/{node}.json")).await.unwrap()?;
    let lease: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let log = lease.get("log").filter(|log| !log.is_null())?;
    let members = log["ensemble"].as_array()?;
    Some((
        log["active"].as_bool().unwrap_or(false),
        members.iter().map(|member| member.as_str().unwrap().to_owned()).collect(),
    ))
}

/// Bucket tiering flushes (node-log bundles) a node has completed. A flush
/// uploads every write acknowledged before it started, then the bundle is
/// collected into per-cell objects, so the node's log is the lasting record.
fn tier_flushes(directory: &Path, node: &str) -> usize {
    let log = std::fs::read(directory.join(format!("{node}.log"))).unwrap();
    String::from_utf8_lossy(&log).matches("event=\"log_bundle_flush\"").count()
}

/// Wait until each node completes a tiering flush after its last acknowledged
/// write, so the write no longer depends on a follower's disk.
// ponytail: a flush already running at the baseline could predate the write
// (a millisecond window against the 2 s tick); a node with nothing left dirty
// never flushes again, so waiting for a second flush is not an option.
async fn wait_bucket_tiered(directory: &Path, nodes: &[&str]) {
    let before: Vec<usize> = nodes.iter().map(|node| tier_flushes(directory, node)).collect();
    timeout(Duration::from_secs(30), async {
        for (node, before) in nodes.iter().zip(before) {
            while tier_flushes(directory, node) <= before {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    })
    .await
    .expect("acknowledged writes were not tiered to the bucket");
}

fn tree_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| match entry.metadata() {
            Ok(meta) if meta.is_dir() => tree_bytes(&entry.path()),
            Ok(meta) => meta.len(),
            Err(_) => 0,
        })
        .sum()
}

fn signal(node: &Node, signal: i32) {
    unsafe { libc::kill(node.child.id().unwrap() as i32, signal) };
}

/// Freeze a node outside any write to the development bucket. The bucket is one
/// SQLite file, so a process stopped inside its write transaction blocks every
/// other node's bucket writes, lease renewals included, until it resumes.
async fn freeze(node: &Node, bucket: &celld::bucket::Bucket) {
    loop {
        signal(node, libc::SIGSTOP);
        if timeout(Duration::from_secs(1), bucket.put("t22/freeze-probe", "probe")).await.is_ok() {
            return;
        }
        signal(node, libc::SIGCONT);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Stop a node that cannot be drained (frozen, fenced or already gone) after
/// reaping its own VMs, which only the live host can wait for.
async fn kill_node(mut node: Node) {
    let Some(host) = node.child.id() else { return };
    unsafe { libc::kill(host as i32, libc::SIGCONT) };
    let pids = vm_pids(&node);
    for pid in &pids {
        unsafe { libc::kill(*pid as i32, libc::SIGKILL) };
    }
    let _ = timeout(Duration::from_secs(10), async {
        while pids.iter().any(|pid| Path::new(&format!("/proc/{pid}")).exists()) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    let _ = node.child.start_kill();
    let _ = timeout(Duration::from_secs(10), node.child.wait()).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires root, KVM and the approved application guest image/kernel"]
async fn nodes_host_application_vms_and_call_remote_objects() {
    check_application_nodes(false, false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires root, KVM and the approved application guest image/kernel"]
async fn t30_indexed_application_node_smoke() {
    check_application_nodes(true, false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires root, KVM and the approved application guest image/kernel"]
async fn t31_mixed_application_node_smoke() {
    check_application_nodes(false, true).await;
}

async fn check_application_nodes(focused: bool, static_mixed: bool) {
    celld::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    let image = std::env::var_os("CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE")
        .expect("CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE is required");
    let image = Path::new(&image);
    let runtime_sha256 = std::fs::read_to_string(image.with_extension("sha256"))
        .unwrap()
        .trim()
        .to_owned();
    // A Windows-mounted dev bucket can spend the native deadline on metadata
    // scans. Let operators retain this real-disk fixture on Linux's filesystem.
    let logs = std::env::var_os("CELLD_APPLICATION_NODE_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/application-guest")
        });
    std::fs::create_dir_all(&logs).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("nodes-t11a-")
        .tempdir_in(logs)
        .unwrap()
        .keep();
    eprintln!("application node artifacts: {}", directory.display());
    let directory = directory.as_path();

    // The application, published into the store the nodes share.
    let source = directory.join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("main.mjs"), SOURCE).unwrap();
    let built = Command::new("/usr/bin/python3")
        .arg(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../applications/guest/build-application-image.py"),
        )
        .arg(&source)
        .arg(directory.join("app.bundle"))
        .env_clear()
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .await
        .unwrap();
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let artifact = std::fs::read(directory.join("app.bundle")).unwrap();
    let digest = content_digest(&artifact);
    let store = ApplicationStore::new(
        celld::dev::open_local_bucket(&directory.join("bucket.sqlite")).unwrap(),
    );
    let application = ApplicationId {
        app: "nodes".to_owned().try_into().unwrap(),
    };
    let counter = ResourceRef {
        kind: ResourceKind::DurableObject,
        id: "counter".to_owned().try_into().unwrap(),
    };
    let database = ResourceRef {
        kind: ResourceKind::D1,
        id: "db".to_owned().try_into().unwrap(),
    };
    let cache = ResourceRef {
        kind: ResourceKind::Kv,
        id: "cache".to_owned().try_into().unwrap(),
    };
    let workflow = ResourceRef {
        kind: ResourceKind::Workflow,
        id: "flow".to_owned().try_into().unwrap(),
    };
    let r2 = ResourceRef {
        kind: ResourceKind::R2,
        id: "blobs".to_owned().try_into().unwrap(),
    };
    let site = ResourceRef {
        kind: ResourceKind::StaticAssets,
        id: "site".to_owned().try_into().unwrap(),
    };
    let queue = |id: &str| ResourceRef {
        kind: ResourceKind::Queue,
        id: id.to_owned().try_into().unwrap(),
    };
    let loader = ResourceRef {
        kind: ResourceKind::WorkerLoader,
        id: "loader".to_owned().try_into().unwrap(),
    };
    let tick = ResourceRef {
        kind: ResourceKind::Cron,
        id: "tick".to_owned().try_into().unwrap(),
    };
    let room = ResourceRef {
        kind: ResourceKind::DurableObject,
        id: "room".to_owned().try_into().unwrap(),
    };
    // A second application with the same resource and queue names.
    let other = ApplicationId {
        app: "queues".to_owned().try_into().unwrap(),
    };
    for (application, host, label) in [
        (&application, "route.test", "one"),
        (&other, "queues.test", "other"),
    ] {
        store
            .upload_artifact(application, &digest, artifact.clone().into())
            .await
            .unwrap();
        // Static assets as `celld application assets` publishes them: every
        // body, then the index, as this application's artifacts. `/hello` is
        // also an asset, but `run_worker_first` sends it to the guest.
        let mut entries = serde_json::Map::new();
        for (path, body) in [
            ("/a.txt".to_owned(), format!("asset {label}")),
            ("/hello".to_owned(), format!("asset hello {label}")),
            (format!("/only-{label}.txt"), format!("private {label}")),
        ] {
            let sha256 = content_digest(body.as_bytes());
            store
                .upload_artifact(application, &sha256, body.clone().into_bytes().into())
                .await
                .unwrap();
            entries.insert(
                path,
                serde_json::json!({"sha256": sha256, "bytes": body.len(), "content_type": "text/plain"}),
            );
        }
        let total_bytes: u64 = entries.values().map(|entry| entry["bytes"].as_u64().unwrap()).sum();
        let file_count = entries.len();
        let index = serde_json::json!({"schema_version": 1, "entries": entries,
            "config": {"binding": "ASSETS", "run_worker_first": ["/hello"]}})
        .to_string()
        .into_bytes();
        let index_sha256 = content_digest(&index);
        store
            .upload_artifact(application, &index_sha256, index.into())
            .await
            .unwrap();
        let assets = serde_json::json!({"index": "assets.json", "sha256": index_sha256,
            "file_count": file_count, "total_bytes": total_bytes});
        let static_assets = if static_mixed {
            let mut index: serde_json::Value = serde_json::from_slice(&store.artifact(application, &index_sha256).await.unwrap().unwrap()).unwrap();
            index["config"]["run_worker_first"] = serde_json::json!(false);
            let body = serde_json::to_vec(&index).unwrap();
            let digest = content_digest(&body);
            store.upload_artifact(application, &digest, body.into()).await.unwrap();
            Some(celld_applications::AssetManifestRef { index: "assets.json".into(), sha256: digest,
                file_count: file_count as u32, total_bytes })
        } else { None };
        let mut deployment: Deployment = serde_json::from_value(serde_json::json!({
            "manifest_version": 1,
            "application": application,
            "id": "one",
            "handlers": [{
                "id": "javascript", "language": "javascript", "entrypoint": "main.mjs",
                "artifact_sha256": digest, "runtime_image_sha256": runtime_sha256,
                "http": true, "environment": {"LABEL": label},
                "bindings": {"COUNTER": {"kind": "resource", "resource": counter}, "ROOM": {"kind": "resource", "resource": room}, "DB": {"kind": "resource", "resource": database}, "CACHE": {"kind": "resource", "resource": cache}, "JOBS": {"kind": "resource", "resource": queue("jobs")}, "FLOW": {"kind": "resource", "resource": workflow}, "BLOBS": {"kind": "resource", "resource": r2}, "ASSETS": {"kind": "resource", "resource": site}, "LOADER": {"kind": "resource", "resource": loader}},
                "worker_config": {"assets": assets, "worker_loaders": [{"binding": "LOADER"}], "do_classes": ["Counter", "Room"], "durable_objects": {"bindings": [{"name":"COUNTER","class_name":"Counter"},{"name":"ROOM","class_name":"Room"}]}, "compatibility_date": "2026-03-01", "compatibility_flags": ["js_rpc"], "queue_consumers": [
                    {"queue": "jobs", "max_batch_size": 10, "max_batch_timeout": 1, "max_retries": 1, "dead_letter_queue": "dead", "retry_delay": 1},
                    {"queue": "dead", "max_batch_size": 10, "max_batch_timeout": 1, "max_retries": 0}
                ], "workflows": [{"binding":"FLOW", "name":"flow", "class_name":"Flow"}], "r2_buckets": [{"binding":"BLOBS","bucket_name":"blobs"}]}
            }]
        }))
        .unwrap();
        deployment.assets = static_assets;
        store.stage(&deployment).await.unwrap();
        // The same code with a cron trigger, activated by the cron phase below.
        let mut triggered = deployment.clone();
        triggered.id = "cron".to_owned().try_into().unwrap();
        triggered.triggers = serde_json::from_value(serde_json::json!([
            {"kind": "cron", "id": "tick", "expression": "* * * * *", "handler": "javascript"}
        ]))
        .unwrap();
        store.stage(&triggered).await.unwrap();
        let mut snapshot = store.read(application).await.unwrap();
        snapshot.create(application.clone()).unwrap();
        for resource in [
            &counter,
            &room,
            &database,
            &cache,
            &queue("jobs"),
            &queue("dead"),
            &workflow,
            &r2,
            &site,
            &loader,
            &tick,
        ] {
            snapshot
                .register_resource(
                    application,
                    Resource {
                        resource: resource.clone(),
                        configuration: serde_json::json!({}),
                    },
                )
                .unwrap();
        }
        store.activate(&mut snapshot, deployment).await.unwrap();
        let handler: celld_applications::Name = "javascript".to_owned().try_into().unwrap();
        let id = &snapshot.application(application).unwrap().route_ids[&handler];
        HOSTS
            .lock()
            .unwrap()
            .insert(host.to_owned(), format!("{}.{PLATFORM_DOMAIN}", id.as_str()));
        store.commit(snapshot).await.unwrap();
    }

    // The trusted node configuration.
    // The original Worker exports no application class. Only authenticated
    // active application namespaces can activate native host storage.
    std::fs::write(
        directory.join("worker.js"),
        "export default { fetch() { return new Response('worker'); } }",
    )
    .unwrap();
    let vms = directory.join("vms.json");
    std::fs::write(
        &vms,
        serde_json::json!({
            "jailer": "/usr/local/bin/jailer",
            "firecracker": "/usr/local/bin/firecracker",
            "kernel": {
                "path": std::env::var("CELLD_APPLICATION_JAVASCRIPT_TEST_KERNEL").unwrap(),
                "sha256": std::env::var("CELLD_APPLICATION_JAVASCRIPT_TEST_KERNEL_SHA256").unwrap(),
            },
            "runtimes": [{"path": image, "sha256": runtime_sha256}],
            "uid": 1000, "gid": 1000,
            "max_vms": 2, "max_memory_mib": 12288, "idle_ttl_secs": 600,
            "startup_pool": std::env::var("CELLD_APPLICATION_STARTUP_POOL").unwrap_or_else(|_| "0".into()).parse::<u32>().unwrap(),
            "platform_domain": PLATFORM_DOMAIN
        })
        .to_string(),
    )
    .unwrap();

    if static_mixed {
        assert_eq!(std::env::var("CELLD_APPLICATION_STARTUP_POOL").unwrap_or_else(|_| "0".into()), "0");
        let node = start(directory, "t31-mixed", Some(&vms), true, "", &[]).await;
        let snapshot = store.read(&application).await.unwrap();
        let asset_host = format!("{}.{}", snapshot.application(&application).unwrap().asset_route_id.as_ref().unwrap().as_str(), PLATFORM_DOMAIN);
        assert!(vm_pids(&node).is_empty());
        for _ in 0..2 {
            assert_eq!(get(node.public, "route.test", "/a.txt").await, "asset one");
            assert_eq!(get(node.public, &asset_host, "/hello").await, "asset hello one");
            assert_eq!(status(node.public, &asset_host, "/missing").await, 404);
        }
        let state: serde_json::Value = reqwest::get(format!("http://127.0.0.1:{}/state", node.internal)).await.unwrap().json().await.unwrap();
        assert_eq!(state["applicationVms"]["vms"], 0, "{state}");
        assert!(vm_pids(&node).is_empty());
        // The original worker-first rule on the explicit handler route enters Firecracker.
        assert_eq!(get(node.public, "route.test", "/hello").await, "hello one");
        assert!(!vm_pids(&node).is_empty());
        assert_eq!(get(node.public, &asset_host, "/a.txt").await, "asset one");
        drain(node).await;
        eprintln!("T31 mixed node PASS: cold/cached asset and terminal miss zero VMs; explicit dynamic worker-first route executes in Firecracker; drain reaps every VM");
        return;
    }

    // Without a configuration, a node ignores the committed catalog.
    let plain = start(directory, "plain", None, false, "", &[]).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(get(plain.public, "route.test", "/hello").await, "worker");
    assert!(vm_pids(&plain).is_empty());
    drain(plain).await;

    // With one, an activated route is served from public ingress in a VM, and
    // a URL no route claims reaches the Worker as before.
    let (one, two) = (
        start(directory, "one", Some(&vms), false, "", &[]).await,
        start(directory, "two", Some(&vms), false, "", &[]).await,
    );
    timeout(Duration::from_secs(30), async {
        while get(one.public, "route.test", "/hello").await != "hello one" {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the activated route was not served");
    assert_eq!(get(one.public, "other.test", "/hello").await, "worker");
    assert!(!vm_pids(&one).is_empty());

    // Objects are owned by whichever node first used them. The other node's
    // handler reaches them through the original peer path, in the owner's VM.
    for (first, second, object) in [(&one, &two, "o1"), (&two, &one, "o2")] {
        for (turn, node) in [first, second, first, second].into_iter().enumerate() {
            let bump = get(node.public, "route.test", &format!("/bump?name={object}")).await;
            assert_eq!(bump, format!("{object}:{}", turn + 1));
        }
        let rpc = get(second.public, "route.test", &format!("/rpc?name={object}")).await;
        assert_eq!(
            rpc,
            format!(r#"{{"add":{{"sum":5,"name":"{object}"}},"count":4}}"#)
        );
        let fail = get(second.public, "route.test", &format!("/fail?name={object}")).await;
        assert_eq!(fail, r#"{"name":"RangeError","code":7,"remote":true}"#);
        // Only the owner holds the object's cell.
        let holds = |node: &Node| {
            let url = format!("http://127.0.0.1:{}/state", node.internal);
            async move {
                reqwest::get(url)
                    .await
                    .unwrap()
                    .text()
                    .await
                    .unwrap()
                    .contains("Counter:")
            }
        };
        if object == "o1" {
            assert!(holds(first).await && !holds(second).await);
        }
        let path = format!("/bump?name={object}");
        let (a, b) = tokio::join!(
            get(first.public, "route.test", &path),
            get(second.public, "route.test", &path)
        );
        let mut replies = vec![a, b];
        replies.sort();
        assert_eq!(replies, vec![format!("{object}:5"), format!("{object}:6")]);
    }
    if focused {
        for (host, node) in [("route.test", &one), ("queues.test", &two)] {
            assert_eq!(get(node.public, host, "/d1-setup").await, "\"ready\"");
        }
        assert_eq!(get(two.public, "route.test", "/d1-next").await, "1");
        assert_eq!(get(one.public, "queues.test", "/d1-read").await, "0");
        assert_eq!(get(one.public, "route.test", "/kv-seed").await, "\"one\"");
        assert_eq!(get(two.public, "queues.test", "/kv-peek?key=seed").await, "null");
        let kv: serde_json::Value = serde_json::from_str(&get(two.public, "route.test", "/kv").await).unwrap();
        assert_eq!(kv["text"], "hello");
        assert_eq!(get(one.public, "route.test", "/a.txt").await, "asset one");
        assert_eq!(get(two.public, "queues.test", "/a.txt").await, "asset other");

        for (host, label) in [("route.test", "one"), ("queues.test", "other")] {
            assert_eq!(get(one.public, host, "/q-setup").await, "\"ready\"");
            assert_eq!(get(two.public, host, &format!("/q-produce?label={label}")).await, "\"sent\"");
            let rows = timeout(Duration::from_secs(60), async {
                loop {
                    let rows = queue_log(&one, host).await;
                    if rows.iter().any(|row| row[0] == "dead" && row[1] == 3)
                        && rows.iter().any(|row| row[0] == "jobs" && row[1] == 5)
                        && rows.iter().any(|row| row[0] == "jobs" && row[1] == 4 && row[2] == 2)
                    {
                        break rows;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }).await.expect("focused queue delivery/retry did not settle");
            assert!(rows.iter().all(|row| row[6] == label), "{host}: {rows:?}");
            for (message, attempts) in [(1, vec![1]), (2, vec![1]), (3, vec![1, 2]), (4, vec![1, 2]), (5, vec![1])] {
                let observed: Vec<_> = rows.iter().filter(|row| row[0] == "jobs" && row[1] == message)
                    .map(|row| row[2].as_i64().unwrap()).collect();
                assert_eq!(observed, attempts, "{host}: message {message}: {rows:?}");
            }
        }
        for (host, node) in [("route.test", &one), ("queues.test", &two)] {
            assert_eq!(get(node.public, host, "/alarm-arm?name=focused&delay=100").await, "armed");
            timeout(Duration::from_secs(20), async {
                while alarm_state(node, host, "focused").await["attempts"] != 1 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }).await.expect("focused indexed alarm did not fire");
        }
        let handler: celld_applications::Name = "javascript".to_owned().try_into().unwrap();
        let mut snapshot = store.read(&application).await.unwrap();
        let route_id = snapshot.application(&application).unwrap().route_ids[&handler].clone();
        let mut replacement = snapshot.application(&application).unwrap().active.clone().unwrap();
        replacement.id = "focused-v2".to_owned().try_into().unwrap();
        replacement.handlers[0].environment.insert("LABEL".to_owned().try_into().unwrap(), "replacement".into());
        store.stage(&replacement).await.unwrap();
        store.activate(&mut snapshot, replacement).await.unwrap();
        assert_eq!(snapshot.application(&application).unwrap().route_ids[&handler], route_id);
        store.commit(snapshot).await.unwrap();
        timeout(Duration::from_secs(20), async {
            while get(one.public, "route.test", "/hello").await != "hello replacement" {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }).await.expect("focused replacement route did not refresh");
        assert_eq!(get(two.public, "route.test", "/d1-read").await, "1");
        assert_eq!(get(one.public, "queues.test", "/hello").await, "hello other");
        let mut snapshot = store.read(&application).await.unwrap();
        snapshot.deactivate(&application).unwrap();
        store.commit(snapshot).await.unwrap();
        timeout(Duration::from_secs(20), async {
            while status(one.public, "route.test", "/hello").await != 404 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }).await.expect("focused deactivation did not withdraw route");
        assert_eq!(get(two.public, "queues.test", "/hello").await, "hello other");
        activate_all(&store, [&application, &other], "one").await;
        drain(two).await;
        drain(one).await;
        let reopened = start(directory, "one", Some(&vms), false, "", &[]).await;
        assert_eq!(get(reopened.public, "route.test", "/hello").await, "hello one");
        assert_eq!(get(reopened.public, "route.test", "/d1-read").await, "1");
        assert_eq!(get(reopened.public, "route.test", "/bump?name=o1").await, "o1:7");
        drain(reopened).await;
        eprintln!("T30 focused nodes PASS: indexed ingress/peer DO and RPC, D1/KV/assets isolation, queue acknowledgements/retries/dead-letter, alarm wake, stable replacement URL, independent deactivation/reactivation, graceful local-disk reopen and physical drain; full machine-loss durability and minute-boundary cron not rerun");
        return;
    }

    // T19: hibernatable WebSockets through public ingress. The upgrade runs the
    // route's handler in a VM, which returns the Room object's 101; the host
    // owns the socket, so messages, tags, attachments and the auto-response
    // survive the loss of every guest VM on the node.
    {
        let (host, name) = ("route.test", "room-a");
        let mut a = ws_open(one.public, host, &format!("/ws?name={name}&tag=a")).await;
        assert_eq!(ws_next(&mut a).await, Ok("welcome:1".to_owned()));
        let mut b = ws_open(one.public, host, &format!("/ws?name={name}&tag=b")).await;
        assert_eq!(ws_next(&mut b).await, Ok("welcome:2".to_owned()));
        // Both directions, with the object's own attachment, storage and tags.
        ws_send(&mut a, "hi").await;
        assert_eq!(ws_next(&mut a).await, Ok("echo:hi:1:1:a".to_owned()));
        ws_send(&mut b, "all").await;
        assert_eq!(ws_next(&mut b).await, Ok("broadcast:b".to_owned()));
        // The auto-response is the shell's: the object's counters do not move.
        ws_send(&mut a, "ping").await;
        assert_eq!(ws_next(&mut a).await, Ok("pong".to_owned()));
        assert_eq!(get(one.public, host, &format!("/ws-peek?name={name}")).await, "2::2");
        // The same object name in the other application is another object.
        let mut other = ws_open(one.public, "queues.test", &format!("/ws?name={name}&tag=a")).await;
        assert_eq!(ws_next(&mut other).await, Ok("welcome:1".to_owned()));
        ws_send(&mut other, "hi").await;
        assert_eq!(ws_next(&mut other).await, Ok("echo:hi:1:1:a".to_owned()));
        // Guest loss: every VM on the node dies; the sockets do not, and the
        // next message wakes a cold VM that rebuilds the socket from the host.
        let pids = vm_pids(&one);
        for pid in &pids {
            let killed = Command::new("kill").arg("-9").arg(pid.to_string()).status().await.unwrap();
            assert!(killed.success());
        }
        // Once the owner has reaped them, a message cannot race into a VM that
        // is already dead but not yet noticed; a frame sent in that window
        // fails its event and closes the socket, as an original handler
        // failure does.
        timeout(Duration::from_secs(10), async {
            while pids.iter().any(|pid| Path::new(&format!("/proc/{pid}")).exists()) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("killed VMs were not reaped by their owner");
        ws_send(&mut a, "again").await;
        assert_eq!(ws_next(&mut a).await, Ok("echo:again:2:3:a".to_owned()));
        // The object closes its own socket; the client sees the close frame.
        ws_send(&mut a, "close").await;
        assert_eq!(ws_next(&mut a).await, Err((1000, "bye".to_owned())));
        // A client that goes away is a close the object is told about.
        drop(b);
        timeout(Duration::from_secs(30), async {
            loop {
                let peek = get(one.public, host, &format!("/ws-peek?name={name}")).await;
                if peek.split(':').nth(1).is_some_and(|closed| {
                    closed.starts_with("1005") || closed.starts_with("1006") || closed.starts_with("1000")
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the object was never told its socket closed");
        drop(other);

        // Through a peer: node two's handler forwards the upgrade to the
        // object's owner, node one, whose host registry owns the socket; node
        // two splices its client to the owner's connection, and every message
        // is a socket event of the owner's guest.
        let name = "room-b";
        assert_eq!(get(one.public, host, &format!("/ws-peek?name={name}")).await, "0::0");
        let mut remote = ws_open(two.public, host, &format!("/ws?name={name}&tag=r")).await;
        assert_eq!(ws_next(&mut remote).await, Ok("welcome:1".to_owned()));
        ws_send(&mut remote, "hi").await;
        assert_eq!(ws_next(&mut remote).await, Ok("echo:hi:1:1:r".to_owned()));
        ws_send(&mut remote, "ping").await;
        assert_eq!(ws_next(&mut remote).await, Ok("pong".to_owned()));
        // A client on the owner and one through the peer share the object.
        let mut local = ws_open(one.public, host, &format!("/ws?name={name}&tag=r")).await;
        assert_eq!(ws_next(&mut local).await, Ok("welcome:2".to_owned()));
        ws_send(&mut local, "all").await;
        assert_eq!(ws_next(&mut local).await, Ok("broadcast:r".to_owned()));
        assert_eq!(ws_next(&mut remote).await, Ok("broadcast:r".to_owned()));
        // The object closes the peer's socket; the client sees the close frame.
        ws_send(&mut remote, "close").await;
        assert_eq!(ws_next(&mut remote).await, Err((1000, "bye".to_owned())));
        drop(local);

        // Outbound: a handler's `new WebSocket(url)` is dialed by its node, from
        // either node, and the peer's frames reach it one credit at a time.
        let echo = ws_echo_server().await;
        for node in [&one, &two] {
            let dialed: serde_json::Value = serde_json::from_str(
                &get(node.public, host, &format!("/dial?url=ws://127.0.0.1:{echo}/")).await,
            )
            .unwrap();
            assert_eq!(
                dialed,
                serde_json::json!({"protocol": "", "seen": ["echo:hello", 100000], "code": 1000})
            );
        }
    }
    // Original R2 API uses only the registered application bucket. Streamed
    // reads and uploads cross VM credits; caller errors abort the native write.
    for (host, label, node) in [("route.test", "one", &one), ("queues.test", "other", &two)] {
        assert_eq!(get(node.public, host, "/r2-peek").await, "null");
        let value: serde_json::Value =
            serde_json::from_str(&get(node.public, host, "/r2").await).unwrap();
        assert_eq!(value["text"], format!("hello {label}"));
        assert_eq!(value["range"], label);
        assert_eq!(value["custom"], serde_json::json!({"label":label}));
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
            assert_eq!(value[flag], true, "R2 {host} {flag}: {value}");
        }
        assert_eq!(
            value["stream"],
            serde_json::json!([8 * 1024 * 1024 + 1, 7, 7])
        );
        assert_eq!(value["produced"], 8 * 1024 * 1024 + 1);
        assert_eq!(value["multipart"], "tail");
        assert_eq!(value["pages"], serde_json::json!([1, true, 2, false]));
    }
    assert_eq!(
        get(two.public, "route.test", "/r2-peek").await,
        r#""hello one""#
    );
    assert_eq!(
        get(one.public, "queues.test", "/r2-peek").await,
        r#""hello other""#
    );

    // Original static assets: ingress serves them before the handler (the
    // guest cannot produce these bodies for these paths), `run_worker_first`
    // reaches the guest, the guest has the original ASSETS binding, and each
    // application sees only its own assets from either node.
    for (host, label, foreign) in [("route.test", "one", "other"), ("queues.test", "other", "one")] {
        for node in [&one, &two] {
            assert_eq!(get(node.public, host, "/a.txt").await, format!("asset {label}"));
            assert_eq!(
                get(node.public, host, &format!("/only-{label}.txt")).await,
                format!("private {label}")
            );
            assert_eq!(get(node.public, host, "/hello").await, format!("hello {label}"));
            assert_eq!(get(node.public, host, "/asset-fetch").await, format!("asset {label}"));
            assert_eq!(
                get(node.public, host, "/asset-fetch?path=hello").await,
                format!("asset hello {label}")
            );
            assert_eq!(
                get(node.public, host, &format!("/only-{foreign}.txt")).await,
                "objects",
                "{host} served {foreign}'s asset"
            );
            assert_eq!(
                status(node.public, host, &format!("/asset-fetch?path=only-{foreign}.txt")).await,
                404,
                "{host} guest fetched {foreign}'s asset"
            );
        }
    }

    // Original Workflow execution stays in the application's VM. Each sleep
    // and retry replays the native ledger through an authenticated alarm;
    // `once` remains exactly once across those drives and the peer ingress.
    async fn workflow_status(node: &Node, host: &str, id: &str) -> serde_json::Value {
        serde_json::from_str(&get(node.public, host, &format!("/wf-status?id={id}")).await).unwrap()
    }
    async fn workflow_wait(node: &Node, host: &str, id: &str, wanted: &str) -> serde_json::Value {
        timeout(Duration::from_secs(30), async {
            loop {
                let status = workflow_status(node, host, id).await;
                assert!(
                    status.get("error").is_none() || wanted == "errored",
                    "{status}"
                );
                if status["status"] == wanted {
                    break status;
                }
                assert!(
                    !matches!(
                        status["status"].as_str(),
                        Some("complete" | "errored" | "terminated")
                    ),
                    "workflow reached unexpected terminal state: {status}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("workflow did not reach expected state")
    }
    let missing = workflow_status(&two, "queues.test", "shared").await;
    assert!(missing["error"]
        .as_str()
        .unwrap()
        .contains("does not exist"), "missing Workflow response: {missing}");
    assert_eq!(
        get(one.public, "route.test", "/wf-create?id=shared").await,
        r#"{"id":"shared"}"#
    );
    let completed = workflow_wait(&two, "route.test", "shared", "complete").await;
    assert_eq!(
        completed["output"],
        serde_json::json!({"first":1,"attempts":2,"label":"one"})
    );
    let counts: serde_json::Value =
        serde_json::from_str(&get(one.public, "route.test", "/wf-counts?id=shared").await).unwrap();
    assert_eq!(counts, serde_json::json!({"first":1,"retry":2,"after":0}));
    assert_eq!(
        get(two.public, "queues.test", "/wf-create?id=shared").await,
        r#"{"id":"shared"}"#
    );
    let isolated = workflow_wait(&one, "queues.test", "shared", "complete").await;
    assert_eq!(
        isolated["output"],
        serde_json::json!({"first":1,"attempts":2,"label":"other"})
    );
    assert_eq!(
        get(one.public, "route.test", "/wf-create?id=failed&mode=fail").await,
        r#"{"id":"failed"}"#
    );
    let failed = workflow_wait(&two, "route.test", "failed", "errored").await;
    assert!(failed["error"]
        .to_string()
        .contains("workflow permanent failure"));
    assert_eq!(
        get(
            one.public,
            "route.test",
            "/wf-create?id=cancelled&mode=cancel"
        )
        .await,
        r#"{"id":"cancelled"}"#
    );
    workflow_wait(&two, "route.test", "cancelled", "waiting").await;
    let terminated: serde_json::Value =
        serde_json::from_str(&get(two.public, "route.test", "/wf-terminate?id=cancelled").await)
            .unwrap();
    assert_eq!(terminated["status"], "terminated");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let counts: serde_json::Value =
        serde_json::from_str(&get(one.public, "route.test", "/wf-counts?id=cancelled").await)
            .unwrap();
    assert_eq!(counts, serde_json::json!({"first":1,"retry":0,"after":0}));
    assert_eq!(
        get(one.public, "route.test", "/wf-create?id=event&mode=event").await,
        r#"{"id":"event"}"#
    );
    workflow_wait(&two, "route.test", "event", "waiting").await;
    get(two.public, "route.test", "/wf-event?id=event").await;
    let event = workflow_wait(&one, "route.test", "event", "complete").await;
    assert_eq!(
        event["output"],
        serde_json::json!({"first":1,"attempts":2,"label":"one","payload":"one"})
    );

    // The firing alarm admits its own guest storage event while this RPC is
    // still waiting. A reply before joined alarm settlement cannot pass here.
    let waited: serde_json::Value =
        serde_json::from_str(&get(one.public, "route.test", "/alarm-wait?name=alarm-wait").await)
            .unwrap();
    assert_eq!(waited["attempts"], 1);
    assert_eq!(waited["hidden"], true);
    assert_eq!(waited["retryCount"], 0);
    assert_eq!(waited["isRetry"], false);
    for route in ["alarm-rollback"] {
        let rollback: serde_json::Value = serde_json::from_str(
            &get(two.public, "route.test", &format!("/{route}?name={route}")).await,
        )
        .unwrap();
        assert_eq!(rollback["alarm"], serde_json::Value::Null, "{route}");
    }
    // The rolled-back deadline is 100 ms; observe well past a wake scan.
    tokio::time::sleep(Duration::from_secs(3)).await;
    for route in ["alarm-rollback"] {
        assert_alarm_unfired(&one, route, false).await;
    }
    assert_eq!(
        get(one.public, "route.test", "/alarm-arm?name=alarm-retry&fail").await,
        "armed"
    );
    timeout(Duration::from_secs(20), async {
        loop {
            let result: serde_json::Value = serde_json::from_str(
                &get(two.public, "route.test", "/alarm-state?name=alarm-retry").await,
            )
            .unwrap();
            if result["attempts"] == 2 && result["alarm"].is_null() {
                assert_eq!(result["retryCount"], 1);
                assert_eq!(result["isRetry"], true);
                assert_eq!(result["hidden"], true);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("guest alarm did not retry and durably consume");

    // The same class and name in two applications are distinct cells: one
    // application's alarm neither fires nor appears in the other.
    for (host, other, node) in [("route.test", "queues.test", &one), ("queues.test", "route.test", &two)] {
        assert_eq!(
            get(node.public, host, "/alarm-arm?name=alarm-xapp&delay=100").await,
            "armed"
        );
        timeout(Duration::from_secs(20), async {
            while alarm_state(node, host, "alarm-xapp").await["attempts"] != 1 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("same-name alarm did not fire in its own application");
        let state = alarm_state(&one, other, "alarm-xapp").await;
        // The first pass leaves `queues.test` untouched; the second leaves
        // `route.test` at its single earlier firing.
        let expected = u64::from(host == "queues.test");
        assert_eq!(state["attempts"], expected, "{host} -> {other}: {state}");
        assert!(state["alarm"].is_null(), "{host} -> {other}: {state}");
    }

    // The first D1 use fixes its owner to node one. Node two must reach the
    // same resource via the peer path, including batch errors and bookmarks.
    assert_eq!(
        get(one.public, "route.test", "/d1-setup").await,
        "\"ready\""
    );
    assert_eq!(get(two.public, "route.test", "/d1-next").await, "1");
    assert_eq!(get(one.public, "route.test", "/d1-read").await, "1");
    let input: serde_json::Value =
        serde_json::from_str(&get(two.public, "route.test", "/d1-large-input").await).unwrap();
    assert_eq!(
        input,
        serde_json::json!({"values":vec![1800000;5],"larger":vec![1800000;10]})
    );
    let large: serde_json::Value =
        serde_json::from_str(&get(two.public, "route.test", "/d1-large").await).unwrap();
    assert_eq!(
        large,
        serde_json::json!((1..=20)
            .map(|i| serde_json::json!([i, 900000, "xxxx", "xxxx"]))
            .collect::<Vec<_>>())
    );
    let batch: serde_json::Value =
        serde_json::from_str(&get(two.public, "route.test", "/d1-batch").await).unwrap();
    assert_eq!(batch["changes"], serde_json::json!([1, 1]));
    assert_eq!(batch["rows"], serde_json::json!([[1, "one"], [2, "two"]]));
    assert_eq!(batch["bookmark"], "celld:primary");
    assert_eq!(batch["failed"]["name"], "Error");
    let message = batch["failed"]["message"].as_str().unwrap();
    assert!(message.starts_with("D1_ERROR: ") && message.contains("UNIQUE constraint failed"));
    assert_eq!(
        message.strip_prefix("D1_ERROR: ").unwrap(),
        batch["failed"]["cause"].as_str().unwrap()
    );
    let counts = futures_util::future::join_all((0..12).map(|index| {
        get(
            if index % 2 == 0 {
                one.public
            } else {
                two.public
            },
            "route.test",
            "/d1-next",
        )
    }))
    .await;
    let mut counts: Vec<i64> = counts.iter().map(|value| value.parse().unwrap()).collect();
    counts.sort();
    assert_eq!(counts, (2..=13).collect::<Vec<_>>());
    assert_eq!(get(two.public, "route.test", "/d1-read").await, "13");
    // The first KV use fixes its owner to node one. Node two reaches the same
    // namespace through the peer path, including a maximum-size value.
    assert_eq!(get(one.public, "route.test", "/kv-seed").await, "\"one\"");
    let kv: serde_json::Value =
        serde_json::from_str(&get(two.public, "route.test", "/kv").await).unwrap();
    assert_eq!(kv["text"], "hello");
    assert_eq!(kv["json"], serde_json::json!({"a":[1,2]}));
    assert_eq!(
        kv["meta"],
        serde_json::json!({"value":{"a":[1,2]},"metadata":{"tag":"m"},"cacheStatus":null})
    );
    assert_eq!(kv["missing"], serde_json::Value::Null);
    assert_eq!(
        kv["pages"],
        serde_json::json!([["p:one", "p:three"], false, ["p:two"], true])
    );
    assert_eq!(kv["ttl"][0], "soon");
    assert!((59..=60).contains(&kv["ttl"][1].as_i64().unwrap()));
    assert_eq!(kv["deleted"], serde_json::Value::Null);
    let large =
        serde_json::json!({"length":25*1024*1024,"head":[1,2,3,4],"tail":[5,6,7,8],"middle":7});
    for node in [&two, &one] {
        let reply = get(node.public, "route.test", "/kv-large").await;
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&reply).unwrap(),
            large
        );
    }
    for (node, expected) in [(&one, true), (&two, false)] {
        let state = reqwest::get(format!("http://127.0.0.1:{}/state", node.internal))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(
            state.contains("__D1Database:"),
            expected,
            "D1 ownership: {state}"
        );
        assert_eq!(
            state.contains("__KvNamespace:"),
            expected,
            "KV ownership: {state}"
        );
    }
    // T21: the other application names the same object, database and key
    // namespace, and shares none of their state. Its object starts at 1 beside
    // route.test's, its KV namespace is empty, and it can run the same setup
    // statements route.test already ran, in its own empty database.
    for (first, second) in [(&one, &two), (&two, &one)] {
        let host = "queues.test";
        let name = if first.public == one.public { "o1" } else { "o2" };
        assert_eq!(get(first.public, host, &format!("/bump?name={name}")).await, format!("{name}:1"));
        assert_eq!(get(second.public, host, &format!("/bump?name={name}")).await, format!("{name}:2"));
    }
    assert_eq!(get(two.public, "queues.test", "/kv-peek?key=seed").await, "null");
    assert_eq!(get(one.public, "queues.test", "/kv-keys?prefix=").await, "[]");
    assert_eq!(get(two.public, "queues.test", "/d1-setup").await, "\"ready\"");
    assert_eq!(get(one.public, "queues.test", "/d1-read").await, "0");
    assert_eq!(get(two.public, "route.test", "/d1-read").await, "13");
    // The unauthenticated `/do/` route (internal port) reaches no application
    // cell: reserved classes are refused outright and an application object
    // has no handler there, whatever its authentic id.
    let object = celld_applications::ResourceAddress {
        application: application.clone(),
        resource: counter.clone(),
    }
    .cell_scope("Counter", "o1")
    .unwrap();
    for scope in [
        "__D1Database",
        "__KvNamespace",
        "__Queue",
        "__Workflow.flow",
    ]
    .map(|class| format!("{class}:{}", "0".repeat(64)))
    {
        assert_eq!(status(one.internal, "other.test", &format!("/do/{scope}")).await, 403, "{scope}");
    }
    assert_ne!(status(one.internal, "other.test", &format!("/do/{object}")).await, 200);
    assert_eq!(get(one.public, "route.test", "/bump?name=o1").await, "o1:7");
    // T14: application queues. The first use fixes each queue's owner, here
    // node two; its alarm dispatches batches to the consumer handler's VM there,
    // and the log lives in the application's own D1 database. A second
    // application has queues of the same names.
    for (host, label) in [("route.test", "one"), ("queues.test", "other")] {
        assert_eq!(get(one.public, host, "/q-setup").await, "\"ready\"");
        let produce = format!("/q-produce?label={label}");
        assert_eq!(get(two.public, host, &produce).await, "\"sent\"");
    }
    let delivered = |rows: &[serde_json::Value], queue: &str, n: i64| -> Vec<i64> {
        rows.iter()
            .filter(|row| row[0] == queue && row[1] == n)
            .map(|row| row[2].as_i64().unwrap())
            .collect()
    };
    for host in ["route.test", "queues.test"] {
        timeout(Duration::from_secs(60), async {
            loop {
                let rows = queue_log(&one, host).await;
                if !delivered(&rows, "dead", 3).is_empty()
                    && !delivered(&rows, "jobs", 5).is_empty()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{host}: the queue did not deliver"));
    }
    // Any wrongful redelivery would arrive within a retry delay and batch timeout.
    tokio::time::sleep(Duration::from_secs(4)).await;
    for (host, label) in [("route.test", "one"), ("queues.test", "other")] {
        let rows = queue_log(&one, host).await;
        // Nothing of the other application, and the retry settings applied.
        assert!(rows.iter().all(|row| row[6] == label), "{host}: {rows:?}");
        // Acknowledged before the handler threw: delivered once, never again.
        assert_eq!(delivered(&rows, "jobs", 1), [1], "{host}: {rows:?}");
        assert_eq!(delivered(&rows, "jobs", 2), [1], "{host}: {rows:?}");
        // A retry and a thrown handler both redeliver, and a spent poison
        // message goes to the dead-letter queue once.
        assert_eq!(delivered(&rows, "jobs", 4), [1, 2], "{host}: {rows:?}");
        assert_eq!(delivered(&rows, "jobs", 3), [1, 2], "{host}: {rows:?}");
        assert_eq!(delivered(&rows, "dead", 3).len(), 1, "{host}: {rows:?}");
        assert_eq!(rows.iter().filter(|row| row[0] == "dead").count(), 1);
        // A batch send arrives as one batch, and a delayed message is late.
        assert!(rows
            .iter()
            .any(|row| row[0] == "jobs" && row[3].as_i64().unwrap() >= 2));
        assert_eq!(delivered(&rows, "jobs", 5), [1], "{host}: {rows:?}");
        let late = rows.iter().find(|row| row[1] == 5).unwrap();
        assert!(
            late[4].as_i64().unwrap() - late[5].as_i64().unwrap() >= 1500,
            "{host}: delayed message arrived early: {late}"
        );
    }
    // T17: cron triggers in the management catalog. Both applications name the
    // same trigger id and expression; activating the triggered deployment makes
    // every node arm the one cron cell of each, and ownership picks one runner.
    // The first attempt of a tick throws and the original retry (a few seconds
    // later, same scheduledTime) runs it again; each application's own VM logs
    // only its own label.
    activate_all(&store, [&application, &other], "cron").await;
    let hosts = [("route.test", "one"), ("queues.test", "other")];
    timeout(Duration::from_secs(150), async {
        loop {
            let mut ready = true;
            for (host, _) in hosts {
                let rows = cron_rows(&one, host).await;
                ready &= rows.iter().any(|row| row[2] == 2);
            }
            if ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("the cron trigger did not run and retry");
    for (host, label) in hosts {
        let rows = cron_rows(&one, host).await;
        let mut ticks = std::collections::BTreeMap::<i64, Vec<i64>>::new();
        for row in &rows {
            assert_eq!((&row[0], &row[3]), (&serde_json::json!("* * * * *"), &serde_json::json!(label)), "{host}: {rows:?}");
            ticks.entry(row[1].as_i64().unwrap()).or_default().push(row[2].as_i64().unwrap());
        }
        // One run per node would show as a repeated first attempt.
        for (at, attempts) in &ticks {
            assert_eq!(at % 60_000, 0, "{host}: the occurrence, not the attempt time: {rows:?}");
            assert_eq!(attempts, &[1, 2], "{host}: {rows:?}");
        }
    }
    // Withdrawing the trigger stops it: no row follows the next minute boundary.
    activate_all(&store, [&application, &other], "one").await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let removed_at = std::time::Instant::now();
    let mut baseline = Vec::new();
    for (host, _) in hosts {
        baseline.push(cron_rows(&one, host).await);
    }
    // T13 durability, part one. A large value is a bucket blob plus an index
    // row, and 2 MiB is above the 1 MiB inline limit. Putting equal bytes
    // again, through the peer and then the owner, reuses the one
    // content-addressed blob and leaves one key. Nodes one and two run without
    // replication, so only blob objects are durable here; the evidence that
    // needs durable rows (ownership movement, failed puts) runs below on
    // replicated nodes.
    let bucket = celld::dev::open_local_bucket(&directory.join("bucket.sqlite")).unwrap();
    let blobs = blob_count(&bucket).await;
    for node in [&two, &one] {
        assert_eq!(
            kv_put(node, "dup", 2 * MIB, 9).await,
            serde_json::json!({"ok":true})
        );
    }
    assert_eq!(
        blob_count(&bucket).await,
        blobs + 1,
        "equal retry leaked a blob"
    );
    assert_eq!(
        get(two.public, "route.test", "/kv-keys?prefix=dup").await,
        r#"["dup"]"#
    );
    // The minute after the trigger was withdrawn passed with no further run.
    tokio::time::sleep_until(tokio::time::Instant::from_std(removed_at + Duration::from_secs(65))).await;
    for ((host, _), before) in hosts.into_iter().zip(&baseline) {
        assert_eq!(&cron_rows(&one, host).await, before, "{host}: a withdrawn trigger ran");
    }
    drain(two).await;
    drain(one).await;
    // The script-path fixtures cannot durably hand armed alarms over. Exit
    // does not revoke their last node lease; wait for the real fence before
    // the replicated phase can acquire their old scopes.
    timeout(Duration::from_secs(30), async {
        for name in ["one", "two"] {
            loop {
                let Some((bytes, _)) = bucket.get(&format!("nodes/{name}.json")).await.unwrap()
                else {
                    break;
                };
                let lease: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis();
                if u128::from(lease["expires_ms"].as_u64().unwrap()) <= now {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    })
    .await
    .expect("the script-path nodes' leases did not expire");
    // Standalone production bootstrap needs only the application catalog,
    // approved VM configuration and bucket; no legacy deployment or test script.
    let application_only = start(
        directory,
        "application-only",
        Some(&vms),
        true,
        "blob-sweep-ms=5000",
        &[],
    )
    .await;
    assert_eq!(
        get(application_only.public, "route.test", "/hello").await,
        "hello one"
    );
    assert_eq!(
        get(
            application_only.public,
            "route.test",
            "/bump?name=standalone"
        )
        .await,
        "standalone:1"
    );
    let missing = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{}/", application_only.public))
        .header("host", "other.test")
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
    // T13 durability, part two, on replicated production-bootstrap nodes: only
    // they hold acknowledged rows durably and can hand an armed alarm over.
    // The first KV use fixes the owner to this node. Its short collector delay
    // also collects the blobs nodes one and two left behind, whose rows were
    // never durable, so exactly the two live blobs remain.
    for (key, size, fill) in KV_VALUES {
        assert_eq!(
            kv_put(&application_only, key, size, fill).await,
            serde_json::json!({"ok":true})
        );
    }
    kv_expect_values(&application_only).await;
    timeout(Duration::from_secs(60), async {
        while blob_count(&bucket).await != 2 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("the unreplicated owners' blobs were not collected");
    // T14 durability on the same replicated owner. The first use fixes the
    // queue's and the log database's owner to this node. Five messages delayed
    // past the drain are acknowledged sends it never delivers; they must
    // still arrive, once each, from whichever node takes the queue over.
    let host = "route.test";
    assert_eq!(
        get(application_only.public, host, "/q-setup").await,
        "\"ready\""
    );
    let backlog = "/q-backlog?label=backlog&kind=ok&ns=101,102,103,104,105&delay=20";
    assert_eq!(
        get(application_only.public, host, backlog).await,
        "\"sent\""
    );
    let produced = std::time::Instant::now();
    // Ownership moves. The owner drains; a new node, which starts with the
    // blob-then-fail test knob, takes over the namespace on first use, and a
    // third node reaches it across the peer path. Every value reads back
    // unchanged from both. Their handler budget also bounds the queue batch's
    // lease for the killed-VM redelivery below.
    // A committed user alarm outlives its guest VM, host and ownership epoch.
    assert_eq!(
        get(
            application_only.public,
            "route.test",
            "/alarm-arm?name=alarm-handoff&delay=50000"
        )
        .await,
        "armed"
    );
    drain(application_only).await;
    // Cold ownership recovery can take tens of seconds; this fixture budget
    // must cover it as well as the queue handler. Production defaults stay intact.
    let budget = [("CELLD_HANDLER_BUDGET_S", "60")];
    let (moved, peer) = (
        start(
            directory,
            "application-moved",
            Some(&vms),
            true,
            "fail-after-blob,blob-sweep-ms=5000",
            &budget,
        )
        .await,
        start(directory, "application-peer", Some(&vms), true, "", &budget).await,
    );
    for node in [&moved, &peer] {
        timeout(Duration::from_secs(30), async {
            while get(node.public, "route.test", "/hello").await != "hello one" {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .expect("the activated route was not served");
    }
    // Nothing was delivered while the queue had no live owner: the backlog
    // is still pending now (unless this setup was slow enough to be due).
    if produced.elapsed() < Duration::from_secs(15) {
        let rows = queue_rows(&moved, host, "backlog").await;
        assert!(rows.is_empty(), "delivered before the delay: {rows:?}");
    }
    kv_expect_values(&moved).await;
    kv_expect_values(&peer).await;
    timeout(Duration::from_secs(120), async {
        loop {
            let result: serde_json::Value = serde_json::from_str(
                &get(peer.public, "route.test", "/alarm-state?name=alarm-handoff").await,
            )
            .unwrap();
            if result["attempts"] == 1 && result["alarm"].is_null() {
                assert_eq!(result["hidden"], true);
                assert_eq!(result["retryCount"], 0);
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("committed guest alarm was lost on host/VM retirement and owner handoff");
    for (node, expected) in [(&moved, true), (&peer, false)] {
        let state = reqwest::get(format!("http://127.0.0.1:{}/state", node.internal))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(
            state.contains("__KvNamespace:"),
            expected,
            "KV ownership after drain: {state}"
        );
    }
    // A put that dies between its blob write and its row commit fails to the
    // caller, from the owner and across the peer path. The previous value (or
    // absence) is unchanged, and the orphan blobs, which no row names, are
    // collected after the sweep delay while every live blob survives.
    let blobs = blob_count(&bucket).await;
    for (node, key, fill) in [(&peer, "fresh", 11), (&moved, "keep", 12)] {
        let failed = kv_put(node, key, 4 * MIB, fill).await;
        let message = failed["failed"].as_str().unwrap_or_default();
        assert!(
            message.contains("CELLD_TEST_KV_FAIL_AFTER_BLOB_WRITE"),
            "{failed}"
        );
    }
    assert_eq!(kv_peek(&peer, "fresh").await, serde_json::Value::Null);
    assert_eq!(
        get(peer.public, "route.test", "/kv-keys?prefix=fresh").await,
        "[]"
    );
    kv_expect_values(&moved).await;
    assert_eq!(
        blob_count(&bucket).await,
        blobs + 2,
        "no orphan blobs to collect"
    );
    timeout(Duration::from_secs(60), async {
        while blob_count(&bucket).await != blobs {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("the orphan blobs were not collected");
    kv_expect_values(&peer).await;
    // The backlog sent through the drained owner arrives from the new owner,
    // each message once, after its full delay.
    timeout(Duration::from_secs(90), async {
        loop {
            let rows = queue_rows_lossy(&moved, host, "backlog").await;
            if (101..=105).all(|n| !delivered(&rows, "jobs", n).is_empty()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("the backlog was not delivered after the owner drained");
    // A duplicate would follow within a retry delay and batch timeout.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let rows = queue_rows(&peer, host, "backlog").await;
    assert_eq!(rows.len(), 5, "{rows:?}");
    for n in 101..=105 {
        assert_eq!(delivered(&rows, "jobs", n), [1], "{rows:?}");
    }
    assert!(
        rows.iter()
            .all(|row| row[4].as_i64().unwrap() - row[5].as_i64().unwrap() >= 18_000),
        "a delayed message arrived early: {rows:?}"
    );
    // A VM killed mid-handler: the first attempt of message 200 blocks in the
    // handler of whichever node owns the queue. Once it has logged its
    // delivery, every VM of both nodes is killed. The lease (about the
    // handler budget plus the other waits) expires and the message is
    // delivered again, as attempt 2 and, being acknowledged, only then. The
    // pool must start a fresh VM for it.
    let kill_started = std::time::Instant::now();
    assert_eq!(
        get(
            moved.public,
            host,
            "/q-backlog?label=kill&kind=hang&ns=200&delay=0"
        )
        .await,
        "\"sent\""
    );
    timeout(Duration::from_secs(60), async {
        while delivered(&queue_rows_lossy(&moved, host, "kill").await, "jobs", 200) != [1] {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("the first attempt never started");
    let killed: Vec<u32> = [&moved, &peer].into_iter().flat_map(vm_pids).collect();
    assert!(!killed.is_empty(), "no VM to kill");
    // Kill before the guest's fixed 35-second event deadline and the host's
    // 60-second budget, so timeout alone cannot explain this first failure.
    assert!(kill_started.elapsed() < Duration::from_secs(30));
    for pid in &killed {
        unsafe { libc::kill(*pid as i32, libc::SIGKILL) };
    }
    timeout(Duration::from_secs(120), async {
        while delivered(&queue_rows_lossy(&peer, host, "kill").await, "jobs", 200) != [1, 2] {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("the message was not redelivered after its VM was killed");
    let fresh: Vec<u32> = [&moved, &peer].into_iter().flat_map(vm_pids).collect();
    assert!(
        fresh.iter().any(|pid| !killed.contains(pid)),
        "no new VM served the redelivery: {fresh:?}"
    );
    // Acknowledged on attempt 2: nothing follows, neither another attempt nor
    // the dead-letter queue.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let rows = queue_rows(&peer, host, "kill").await;
    assert_eq!(delivered(&rows, "jobs", 200), [1, 2], "{rows:?}");
    assert_eq!(rows.len(), 2, "{rows:?}");
    drain(peer).await;
    drain(moved).await;
    // T20: replacement under load on a real node, then a drain with retained
    // work. The same code under another LABEL replaces `one` while a slow
    // request, a retained waitUntil and a delayed object write started on `one`
    // are in flight and three clients keep requesting.
    let mut swap = store
        .deployment(&application, &"one".to_owned().try_into().unwrap())
        .await
        .unwrap()
        .unwrap();
    swap.id = "swap".to_owned().try_into().unwrap();
    swap.handlers[0]
        .environment
        .insert("LABEL".to_owned().try_into().unwrap(), "swap".into());
    store.stage(&swap).await.unwrap();
    let mut roomy: serde_json::Value = serde_json::from_slice(&std::fs::read(&vms).unwrap()).unwrap();
    roomy["max_vms"] = serde_json::json!(4);
    roomy["max_memory_mib"] = serde_json::json!(12288);
    let roomy_vms = directory.join("lifecycle-vms.json");
    std::fs::write(&roomy_vms, roomy.to_string()).unwrap();
    let lifecycle = start(directory, "lifecycle", Some(&roomy_vms), true, "", &budget).await;
    let port = lifecycle.public;
    let request = |port: u16, path: String| async move {
        let reply = reqwest::Client::new()
            .get(format!("http://127.0.0.1:{port}{path}"))
            .header("host", host_of("route.test"))
            .send()
            .await
            .unwrap();
        (reply.status().as_u16(), reply.text().await.unwrap())
    };
    timeout(Duration::from_secs(60), async {
        while get(port, "route.test", "/hello").await != "hello one" {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the activated route was not served");
    for count in 1..=2 {
        assert_eq!(
            get(port, "route.test", "/bump?name=lifecycle-obj").await,
            format!("lifecycle-obj:{count}")
        );
    }
    let slow = tokio::spawn(request(port, "/slow?ms=6000".into()));
    let slow_bump = tokio::spawn(request(port, "/slow-bump?name=lifecycle-obj&ms=6000".into()));
    assert_eq!(
        request(port, "/bg?ms=6000".into()).await,
        (200, "bg one".to_owned())
    );
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let loaders: Vec<_> = (0..3)
        .map(|_| {
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut seen = Vec::new();
                while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                    seen.push(request(port, "/hello".into()).await);
                }
                seen
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let old_vm_pid = vm_pids(&lifecycle)
        .into_iter()
        .next()
        .expect("the old deployment had no VM before replacement");
    let mut snapshot = store.read(&swap.application).await.unwrap();
    store.activate(&mut snapshot, swap).await.unwrap();
    store.commit(snapshot).await.unwrap();
    assert!(
        !slow.is_finished() && !slow_bump.is_finished(),
        "the replacement did not happen under in-flight work"
    );
    timeout(Duration::from_secs(60), async {
        while get(port, "route.test", "/hello").await != "hello swap" {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the replacement was not served");
    // A write through the new deployment while the old one's is still pending.
    let mid = get(port, "route.test", "/bump?name=lifecycle-obj").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let (mut before, mut after) = (0, 0);
    for seen in loaders {
        let mut swapped = false;
        for (status, body) in seen.await.unwrap() {
            assert_eq!(status, 200, "a request failed across the replacement: {body}");
            match body.as_str() {
                "hello one" => {
                    assert!(!swapped, "a request reached the old deployment after the new");
                    before += 1;
                }
                "hello swap" => {
                    swapped = true;
                    after += 1;
                }
                other => panic!("unexpected reply {other:?}"),
            }
        }
    }
    assert!(before > 0 && after > 0, "load did not span the swap: {before}/{after}");
    // Accepted work finished on the pinned old deployment, and its object write
    // landed beside the new deployment's: two writes, neither lost nor repeated.
    assert_eq!(slow.await.unwrap(), (200, "slow one".to_owned()));
    let late = slow_bump.await.unwrap();
    assert_eq!(late.0, 200, "{}", late.1);
    let mut bumps = [mid, late.1];
    bumps.sort();
    assert_eq!(bumps, ["lifecycle-obj:3", "lifecycle-obj:4"]);
    // The old deployment's waitUntil outlived the replacement.
    timeout(Duration::from_secs(60), async {
        loop {
            let rows = get(port, "route.test", "/bg-read").await;
            if rows != "[]" {
                assert_eq!(rows, r#"[["one"]]"#);
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("the retained waitUntil work was lost with the replaced deployment");
    // Its VM goes once that work has settled, not at the 600 s idle TTL.
    timeout(Duration::from_secs(30), async {
        while Path::new(&format!("/proc/{old_vm_pid}")).exists() {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("the replaced deployment's VM was not reaped once its work settled");
    // A drain settles accepted background work before it hands ownership over.
    assert_eq!(
        request(port, "/bg?ms=3000".into()).await,
        (200, "bg swap".to_owned())
    );
    let handed_off = vm_pids(&lifecycle);
    assert!(!handed_off.is_empty(), "no VM held the retained work");
    drain(lifecycle).await;
    // The successor adopts the resources on a fresh VM: nothing the old owner's
    // VM held is still running, and the object continues from its last write.
    let adopted = start(directory, "lifecycle-successor", Some(&roomy_vms), true, "", &budget).await;
    timeout(Duration::from_secs(60), async {
        while get(adopted.public, "route.test", "/hello").await != "hello swap" {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the activated route was not served");
    assert_eq!(
        get(adopted.public, "route.test", "/bg-read").await,
        r#"[["one"],["swap"]]"#,
        "the drain did not settle the retained work"
    );
    assert_eq!(
        get(adopted.public, "route.test", "/bump?name=lifecycle-obj").await,
        "lifecycle-obj:5"
    );
    let fresh = vm_pids(&adopted);
    assert!(
        !fresh.is_empty() && fresh.iter().all(|pid| !handed_off.contains(pid)),
        "the successor reused the old owner's VM: {fresh:?}"
    );
    drain(adopted).await;
    let one = store
        .deployment(&application, &"one".to_owned().try_into().unwrap())
        .await
        .unwrap()
        .unwrap();
    let mut snapshot = store.read(&one.application).await.unwrap();
    store.activate(&mut snapshot, one).await.unwrap();
    store.commit(snapshot).await.unwrap();
    eprintln!("application lifecycle: replacing a deployment on a real node under three clients, a slow request, a retained waitUntil and a delayed object write failed no request, served each client the old deployment then the new, finished the accepted work on the old deployment with both object writes kept, and retired the old VM once its work settled; a drain then settled retained background work and a successor node adopted the resources and object on a fresh VM");
    // T22: follower-disk replication and the bucket fallback, with the original
    // node log, shipper and ownership fence. The bucket tiering interval is a
    // minute, so a write acknowledged sooner was proved by the follower's disk.
    // Applications never supply proof: each `/t22` request writes the object,
    // D1, KV and the queue, and its reply leaves only through native proof.
    let replicated = [
        ("CELLD_HANDLER_BUDGET_S", "60"),
        ("CELLD_LOG_FLUSH_MS", "60000"),
    ];
    // Drained nodes keep their last lease until it expires; only live peers may
    // be recruited as followers.
    for name in [
        "application-only",
        "application-moved",
        "application-peer",
        "lifecycle",
        "lifecycle-successor",
    ] {
        wait_node_fence(&bucket, name).await;
    }
    let leader = start(directory, "t22-leader", Some(&vms), true, "", &replicated).await;
    let follower = start(directory, "t22-follower", Some(&vms), true, "", &replicated).await;
    for node in [&leader, &follower] {
        timeout(Duration::from_secs(30), async {
            while get(node.public, "route.test", "/hello").await != "hello one" {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .expect("the activated route was not served");
    }
    // The first use fixes every owner to the leader, which then recruits the
    // follower. The ensemble turns `active` only on its first fleet-acknowledged
    // write, which the writes below make and the posture is checked for after.
    assert_eq!(get(leader.public, "route.test", "/q-setup").await, "\"ready\"");
    timeout(Duration::from_secs(90), async {
        while !matches!(
            log_posture(&bucket, "t22-leader").await,
            Some((_, members)) if members.iter().any(|member| member == "t22-follower")
        ) {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("the leader never opened a fleet ensemble on the follower");
    let (mut attempted, mut acknowledged) = (Vec::new(), Vec::new());
    for n in 1..=6 {
        attempted.push(n);
        let node = if n % 2 == 0 { &follower } else { &leader };
        let elapsed = t22_write(node, n).await.expect("healthy fleet refused a write");
        // The first write includes cold activation of every cell.
        if n > 1 {
            assert!(elapsed < Duration::from_secs(30), "write {n} waited for the bucket: {elapsed:?}");
        }
        acknowledged.push(n);
    }
    assert!(
        matches!(log_posture(&bucket, "t22-leader").await, Some((true, _))),
        "the leader's ensemble never began acknowledging from the follower's disk"
    );
    assert!(
        tree_bytes(&directory.join("t22-follower/peerlog/t22-leader")) > 0,
        "the follower's disk holds none of the leader's log"
    );
    // A follower that stops answering (its replies are late or lost): the reply
    // is held until the ensemble member is judged dead, then the unpaced
    // bucket upload proves it. An error is also safe, an acknowledgement is not
    // unless the write is durable, which the recovery below checks.
    freeze(&follower, &bucket).await;
    attempted.push(7);
    if let Some(elapsed) = t22_write(&leader, 7).await {
        assert!(
            elapsed >= Duration::from_secs(1),
            "a reply left before native proof: {elapsed:?}"
        );
        acknowledged.push(7);
    }
    let mut fallback = false;
    for n in 8..=10 {
        attempted.push(n);
        if t22_write(&leader, n).await.is_some() {
            acknowledged.push(n);
            fallback = true;
            break;
        }
    }
    assert!(fallback, "no write was acknowledged through the bucket while the follower was frozen");
    // Its late answers arrive; both paths keep acknowledging.
    signal(&follower, libc::SIGCONT);
    for (n, node) in [(11, &follower), (12, &leader)] {
        attempted.push(n);
        t22_write(node, n).await.expect("write after the follower resumed");
        acknowledged.push(n);
    }
    // Stale lease: the leader freezes until its node lease expires, so the
    // surviving node recovers its log and takes the cells over. Everything
    // acknowledged survives, and the resumed leader cannot acknowledge a
    // write the successor does not hold.
    freeze(&leader, &bucket).await;
    wait_node_fence(&bucket, "t22-leader").await;
    timeout(Duration::from_secs(150), async {
        while t22_read(&follower).await.is_none() {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .expect("the successor did not recover the frozen leader's cells");
    t22_expect(&follower, &acknowledged, &attempted).await;
    t22_queued(&follower, &acknowledged).await;
    attempted.push(13);
    t22_write(&follower, 13).await.expect("successor refused a write");
    acknowledged.push(13);
    signal(&leader, libc::SIGCONT);
    attempted.push(14);
    if t22_write(&leader, 14).await.is_some() {
        acknowledged.push(14);
    }
    t22_expect(&follower, &acknowledged, &attempted).await;
    t22_queued(&follower, &acknowledged).await;
    kill_node(leader).await;
    drain(follower).await;
    eprintln!("application replication: app-scoped object, D1, KV and queue writes were acknowledged from the follower's disk inside the bucket tiering interval; a frozen follower held the first reply until it was judged dead and the bucket fallback acknowledged the rest; after its owner froze past its node lease the survivor recovered every acknowledged write, and the resumed owner acknowledged nothing the successor lacked");
    // T28: Durable Object facets on the same replicated fleet. A facet is a
    // database file and replication stream of its own below its object, so its
    // writes commit apart from the object's root writes and must be proved
    // apart. `f-both` writes the root and the facet, `f-only` only the facet
    // (whose reply therefore waits on nothing but the facet's stream).
    for name in ["t22-leader", "t22-follower"] {
        wait_node_fence(&bucket, name).await;
    }
    let leader = start(directory, "t28-leader", Some(&vms), true, "", &replicated).await;
    let follower = start(directory, "t28-follower", Some(&vms), true, "", &replicated).await;
    for node in [&leader, &follower] {
        timeout(Duration::from_secs(30), async {
            while get(node.public, "route.test", "/hello").await != "hello one" {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .expect("the activated route was not served");
    }
    // The first use fixes the object's owner to the leader, which recruits the
    // follower. The first write includes cold activation of the facet's stream.
    let (mut attempted, mut root_acknowledged, mut acknowledged) = (vec![1], vec![1], vec![1]);
    facet_write(&leader, "f-both", 1, 0).await.expect("healthy fleet refused a facet write");
    timeout(Duration::from_secs(90), async {
        while !matches!(
            log_posture(&bucket, "t28-leader").await,
            Some((_, members)) if members.iter().any(|member| member == "t28-follower")
        ) {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("the leader never opened a fleet ensemble on the follower");
    // Writes through either node (the follower's reach the leader's VM through
    // the peer path), all acknowledged from the follower's disk well inside the
    // bucket tiering interval.
    for (n, node, route) in [
        (2, &follower, "f-both"),
        (3, &leader, "f-only"),
        (4, &follower, "f-only"),
        (5, &leader, "f-both"),
        (6, &follower, "f-only"),
    ] {
        attempted.push(n);
        let elapsed = facet_write(node, route, n, 0).await.expect("healthy fleet refused a facet write");
        assert!(elapsed < Duration::from_secs(30), "facet write {n} waited for the bucket: {elapsed:?}");
        acknowledged.push(n);
        if route == "f-both" {
            root_acknowledged.push(n);
        }
    }
    assert!(
        matches!(log_posture(&bucket, "t28-leader").await, Some((true, _))),
        "the leader's ensemble never began acknowledging from the follower's disk"
    );
    // A facet write alone, 200 kB of incompressible rows, reaches the follower's
    // disk before its reply: the root wrote nothing, so the facet's own stream
    // is what replicated.
    let replica = directory.join("t28-follower/peerlog/t28-leader");
    let before = tree_bytes(&replica);
    attempted.push(7);
    let elapsed = facet_write(&leader, "f-only", 7, 200_000).await.expect("healthy fleet refused a facet write");
    assert!(elapsed < Duration::from_secs(30), "facet write 7 waited for the bucket: {elapsed:?}");
    acknowledged.push(7);
    let grown = tree_bytes(&replica) - before.min(tree_bytes(&replica));
    assert!(grown >= 100_000, "the follower's disk did not receive the facet's write: grew {grown} bytes");
    facet_expect(&follower, &root_acknowledged, &acknowledged, &attempted).await;
    // A follower that stops answering: the facet-only reply is held until the
    // ensemble member is judged dead, then the bucket upload proves it. A reply
    // never leaves before its facet write is proved.
    freeze(&follower, &bucket).await;
    attempted.push(8);
    if let Some(elapsed) = facet_write(&leader, "f-only", 8, 0).await {
        assert!(
            elapsed >= Duration::from_secs(1),
            "a facet reply left before native proof: {elapsed:?}"
        );
        acknowledged.push(8);
    }
    let mut fallback = false;
    for n in 9..=11 {
        attempted.push(n);
        if facet_write(&leader, "f-both", n, 0).await.is_some() {
            acknowledged.push(n);
            root_acknowledged.push(n);
            fallback = true;
            break;
        }
    }
    assert!(fallback, "no facet write was acknowledged through the bucket while the follower was frozen");
    signal(&follower, libc::SIGCONT);
    for (n, node, route) in [(12, &follower, "f-only"), (13, &leader, "f-both")] {
        attempted.push(n);
        facet_write(node, route, n, 0).await.expect("facet write after the follower resumed");
        acknowledged.push(n);
        if route == "f-both" {
            root_acknowledged.push(n);
        }
    }
    // Owner loss: the leader freezes past its node lease. The successor
    // restores the object and its facet from the replicated streams and holds
    // every acknowledged write of both.
    freeze(&leader, &bucket).await;
    wait_node_fence(&bucket, "t28-leader").await;
    timeout(Duration::from_secs(150), async {
        while facet_state(&follower).await.is_none() {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .expect("the successor did not recover the frozen leader's object and facet");
    facet_expect(&follower, &root_acknowledged, &acknowledged, &attempted).await;
    attempted.push(14);
    facet_write(&follower, "f-both", 14, 0).await.expect("successor refused a facet write");
    acknowledged.push(14);
    root_acknowledged.push(14);
    // The resumed leader is fenced: whatever it acknowledges the successor holds.
    signal(&leader, libc::SIGCONT);
    attempted.push(15);
    if facet_write(&leader, "f-only", 15, 0).await.is_some() {
        acknowledged.push(15);
    }
    facet_expect(&follower, &root_acknowledged, &acknowledged, &attempted).await;
    kill_node(leader).await;
    drain(follower).await;
    for name in ["t28-leader", "t28-follower"] {
        wait_node_fence(&bucket, name).await;
    }
    eprintln!("application facets (T28): facet writes, with and without a root write, were acknowledged from the follower's disk on their own stream inside the bucket tiering interval and a facet-only write reached the follower's log before its reply; a frozen follower held a facet-only reply until it was judged dead and the bucket fallback acknowledged the rest; after the owner froze past its node lease the successor restored the object and its facet and held every acknowledged root write and facet write, took more, and the resumed owner acknowledged nothing the successor lacked");
    // Use the existing idle TTL mechanism to evict an actual guest while its
    // alarm is durably armed. Do not touch the guest until its PID is gone.
    let mut idle_config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&vms).unwrap()).unwrap();
    idle_config["idle_ttl_secs"] = serde_json::json!(1);
    let idle_vms = directory.join("alarm-idle-vms.json");
    std::fs::write(&idle_vms, idle_config.to_string()).unwrap();
    let restart = start(
        directory,
        "alarm-restart",
        Some(&idle_vms),
        true,
        "",
        &budget,
    )
    .await;
    arm_pending_alarm(&restart, "alarm-eviction").await;
    // A later setAlarm replaces an earlier, shorter deadline; the stale 2 s
    // one would start a VM below.
    for delay in [2000, 90000] {
        let path = format!("/alarm-arm?name=alarm-rearm&delay={delay}");
        assert_eq!(get(restart.public, "route.test", &path).await, "armed");
    }
    let evicted = vm_pids(&restart);
    assert!(!evicted.is_empty(), "alarm was not installed by a real VM");
    timeout(Duration::from_secs(15), async {
        while evicted
            .iter()
            .any(|pid| Path::new(&format!("/proc/{pid}")).exists())
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("idle guest was not evicted before its committed alarm became due");
    let state: serde_json::Value =
        reqwest::get(format!("http://127.0.0.1:{}/state", restart.internal)).await
            .unwrap()
            .json()
            .await
            .unwrap();
    assert_eq!(
        state["applicationVms"]["ready"], 0,
        "another assigned VM appeared before the alarm deadline"
    );
    assert_eq!(
        state["applicationVms"]["starting"], 0,
        "another assigned VM started before the alarm deadline"
    );
    // Observed after eviction and before the deadline: unfired, still armed.
    assert_alarm_unfired(&restart, "alarm-eviction", true).await;
    alarm_fired_once(&restart, "alarm-eviction").await;
    assert_alarm_unfired(&restart, "alarm-rearm", true).await;
    assert!(
        vm_pids(&restart).iter().all(|pid| !evicted.contains(pid)),
        "evicted guest served its alarm"
    );
    // An acknowledged installation survives SIGKILL and a new process using
    // the same native data directory. No graceful seal or ownership transfer.
    arm_pending_alarm(&restart, "alarm-process-restart").await;
    // Rolled-back installations (3 s deadline) must not reappear after restart.
    for route in ["alarm-rollback"] {
        let path = format!("/{route}?name={route}-restart&delay=3000");
        let state: serde_json::Value =
            serde_json::from_str(&get(restart.public, "route.test", &path).await).unwrap();
        assert!(state["alarm"].is_null(), "{route}: {state}");
    }
    let killed = hard_stop(restart).await;
    wait_node_fence(&bucket, "alarm-restart").await;
    let restart = start(directory, "alarm-restart", Some(&vms), true, "", &budget).await;
    alarm_fired_once(&restart, "alarm-process-restart").await;
    for route in ["alarm-rollback"] {
        assert_alarm_unfired(&restart, &format!("{route}-restart"), false).await;
    }
    assert!(
        vm_pids(&restart).iter().all(|pid| !killed.contains(pid)),
        "old guest survived process restart"
    );
    // A second hard loss is recovered by a distinct owner with a fresh native
    // directory. Its only source of the alarm is the replicated bucket state.
    arm_pending_alarm(&restart, "alarm-owner-loss").await;
    // An explicit deleteAlarm is as durable as an installation.
    arm_pending_alarm(&restart, "alarm-deleted").await;
    let cancel = get(restart.public, "route.test", "/alarm-cancel?name=alarm-deleted").await;
    assert!(serde_json::from_str::<serde_json::Value>(&cancel).unwrap()["alarm"].is_null());
    let killed = hard_stop(restart).await;
    wait_node_fence(&bucket, "alarm-restart").await;
    let successor = start(directory, "alarm-successor", Some(&vms), true, "", &budget).await;
    alarm_fired_once(&successor, "alarm-owner-loss").await;
    // Armed with the same deadline, it would have fired by now (settle window).
    assert_alarm_unfired(&successor, "alarm-deleted", false).await;
    assert!(
        vm_pids(&successor).iter().all(|pid| !killed.contains(pid)),
        "lost owner's guest served the successor"
    );
    drain(successor).await;
    // T23: recovery and one-writer fencing on replicated production-bootstrap
    // nodes, for every durable service. Two nodes hold acknowledged state: an
    // object's storage, D1 rows, KV values (inline and blob), a delayed queue
    // backlog, an armed alarm and a Workflow waiting for an event.
    let (a, b) = (
        start(directory, "t23-a", Some(&vms), true, "", &budget).await,
        start(directory, "t23-b", Some(&vms), true, "", &budget).await,
    );
    for node in [&a, &b] {
        assert_eq!(get_eventually(node.public, host, "/hello").await, "hello one");
    }
    assert_eq!(get(a.public, host, "/t23-d1-setup").await, "\"ready\"");
    assert_eq!(get(a.public, host, "/q-setup").await, "\"ready\"");
    const T23_KV: [(&str, usize, u8); 2] = [("t23-small", 16, 6), ("t23-big", 2 * MIB, 4)];
    for (key, size, fill) in T23_KV {
        assert_eq!(kv_put(&a, key, size, fill).await, serde_json::json!({"ok":true}));
    }
    for (turn, node) in [&a, &b, &a, &b, &a].into_iter().enumerate() {
        assert_eq!(get(node.public, host, "/t23-d1-next").await, (turn + 1).to_string());
    }
    for (name, order) in [("t23-a", [&a, &b, &a]), ("t23-b", [&b, &a, &b])] {
        for (turn, node) in order.into_iter().enumerate() {
            let bump = get(node.public, host, &format!("/bump?name={name}")).await;
            assert_eq!(bump, format!("{name}:{}", turn + 1));
        }
    }
    // Due long after the hard loss below, so only a successor can deliver.
    let backlog = "/q-backlog?label=t23&kind=ok&ns=301,302&delay=90";
    assert_eq!(get(a.public, host, backlog).await, "\"sent\"");
    assert_eq!(get(b.public, host, "/alarm-arm?name=t23-alarm&delay=110000").await, "armed");
    assert_eq!(
        get(a.public, host, "/wf-create?id=t23&mode=long").await,
        r#"{"id":"t23"}"#
    );
    workflow_wait(&b, host, "t23", "waiting").await;

    // Guest crash: each node's object holds a committed write and an open
    // transaction. Once both are in flight, every VM of both nodes is killed.
    // The in-flight requests fail; the committed writes remain, the open
    // transaction's write does not, and fresh VMs serve what follows.
    let mut holds = Vec::new();
    for (node, name) in [(&a, "t23-hold-a"), (&b, "t23-hold-b")] {
        let (port, path) = (node.public, format!("/crash-hold?name={name}"));
        holds.push(tokio::spawn(async move {
            try_get(port, "route.test", &path, Duration::from_secs(90)).await
        }));
    }
    timeout(Duration::from_secs(60), async {
        for (node, name) in [(&a, "t23-hold-a"), (&b, "t23-hold-b")] {
            while get_json_eventually(node, &format!("/rpc?name={name}-sig")).await["count"] != 1 {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    })
    .await
    .expect("the held requests never reached their open transactions");
    let crashed: Vec<u32> = [&a, &b].into_iter().flat_map(vm_pids).collect();
    assert!(!crashed.is_empty(), "no VM to crash");
    for pid in &crashed {
        unsafe { libc::kill(*pid as i32, libc::SIGKILL) };
    }
    for held in holds {
        let (status, body) = timeout(Duration::from_secs(80), held).await.unwrap().unwrap();
        assert_ne!(status, 200, "a request survived its crashed VM: {body}");
    }
    for node in [&b, &a] {
        for name in ["t23-hold-a", "t23-hold-b"] {
            let state = get_json_eventually(node, &format!("/crash-state?name={name}")).await;
            assert_eq!(state, serde_json::json!({"acked":1,"uncommitted":0}), "{name}");
            let sig = get_json_eventually(node, &format!("/rpc?name={name}-sig")).await;
            assert_eq!(sig["count"], 1, "{name}: acknowledged cross-object write lost");
        }
    }
    let fresh: Vec<u32> = [&a, &b].into_iter().flat_map(vm_pids).collect();
    assert!(
        fresh.iter().any(|pid| !crashed.contains(pid)),
        "no new VM served after the crash: {fresh:?}"
    );
    for node in [&a, &b] {
        assert_eq!(get_eventually(node.public, host, "/t23-d1-read").await, "5");
        for (key, size, fill) in T23_KV {
            assert_eq!(kv_peek(node, key).await, kv_edges(size, fill), "{key}");
        }
        for name in ["t23-a", "t23-b"] {
            let rpc = get_json_eventually(node, &format!("/rpc?name={name}")).await;
            assert_eq!(rpc["count"], 3, "{name}");
        }
    }
    assert_eq!(workflow_status(&a, host, "t23").await["status"], "waiting");
    // The owners keep accepting writes after the crash, through either node.
    assert_eq!(get(b.public, host, "/t23-d1-next").await, "6");
    assert_eq!(get(b.public, host, "/bump?name=t23-a").await, "t23-a:4");
    assert_eq!(get(a.public, host, "/bump?name=t23-b").await, "t23-b:4");

    // Simultaneous host death: both nodes are lost at once, without a drain,
    // and two fresh nodes with empty native directories restart together. They
    // recover every service from the bucket alone and race for the same cells.
    // Each node acknowledged from its peer's disk, and both disks are lost
    // here, so the writes must first be tiered to the bucket (the flush
    // interval) or the documented bounded loss would drop them.
    wait_bucket_tiered(directory, &["t23-a", "t23-b"]).await;
    let (lost_a, lost_b) = tokio::join!(hard_stop(a), hard_stop(b));
    let lost: Vec<u32> = lost_a.into_iter().chain(lost_b).collect();
    wait_node_fence(&bucket, "t23-a").await;
    wait_node_fence(&bucket, "t23-b").await;
    let (c, d) = tokio::join!(
        start(directory, "t23-c", Some(&vms), true, "", &budget),
        start(directory, "t23-d", Some(&vms), true, "", &budget),
    );
    for node in [&c, &d] {
        assert_eq!(get_eventually(node.public, host, "/hello").await, "hello one");
        // Every acknowledged write survived, read through its owner or a peer.
        assert_eq!(get_eventually(node.public, host, "/t23-d1-read").await, "6");
        for (key, size, fill) in T23_KV {
            assert_eq!(kv_peek(node, key).await, kv_edges(size, fill), "{key}");
        }
        for (name, count) in [("t23-a", 4), ("t23-b", 4), ("t23-hold-a", 0), ("t23-hold-b", 0)] {
            let rpc = get_json_eventually(node, &format!("/rpc?name={name}")).await;
            assert_eq!(rpc["count"], count, "{name}");
        }
        for name in ["t23-hold-a", "t23-hold-b"] {
            let state = get_json_eventually(node, &format!("/crash-state?name={name}")).await;
            assert_eq!(state, serde_json::json!({"acked":1,"uncommitted":0}), "{name}");
        }
    }
    // One owner per cell after the race, and concurrent writers through both
    // nodes publish consecutive, distinct values.
    for needle in ["__D1Database:", "__KvNamespace:"] {
        let holders = [holds_cell(&c, needle).await, holds_cell(&d, needle).await];
        assert_eq!(holders.iter().filter(|held| **held).count(), 1, "{needle}: {holders:?}");
    }
    let next = futures_util::future::join_all((0..8).map(|index| {
        get(if index % 2 == 0 { c.public } else { d.public }, host, "/t23-d1-next")
    }))
    .await;
    let mut next: Vec<i64> = next.iter().map(|value| value.parse().unwrap()).collect();
    next.sort();
    assert_eq!(next, (7..=14).collect::<Vec<_>>());
    let bumps = futures_util::future::join_all((0..6).map(|index| {
        get(if index % 2 == 0 { c.public } else { d.public }, host, "/bump?name=t23-a")
    }))
    .await;
    let mut bumps: Vec<i64> = bumps
        .iter()
        .map(|value| value.strip_prefix("t23-a:").unwrap().parse().unwrap())
        .collect();
    bumps.sort();
    assert_eq!(bumps, (5..=10).collect::<Vec<_>>());
    // The Workflow resumes where it waited: its completed steps do not rerun.
    assert_eq!(workflow_status(&c, host, "t23").await["status"], "waiting");
    get(d.public, host, "/wf-event?id=t23").await;
    let resumed = workflow_wait(&c, host, "t23", "complete").await;
    assert_eq!(
        resumed["output"],
        serde_json::json!({"first":1,"attempts":2,"label":"one","payload":"one"})
    );
    let counts: serde_json::Value =
        serde_json::from_str(&get(d.public, host, "/wf-counts?id=t23").await).unwrap();
    assert_eq!(counts, serde_json::json!({"first":1,"retry":2,"after":0}));
    // The committed alarm fires once, the backlog arrives once each after its
    // full delay, and no lost owner's guest serves a successor.
    alarm_fired_once(&c, "t23-alarm").await;
    timeout(Duration::from_secs(120), async {
        loop {
            let rows = queue_rows_lossy(&d, host, "t23").await;
            if (301..=302).all(|n| !delivered(&rows, "jobs", n).is_empty()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("the backlog was not delivered after both owners were lost");
    tokio::time::sleep(Duration::from_secs(4)).await;
    let rows = queue_rows(&c, host, "t23").await;
    assert_eq!(rows.len(), 2, "{rows:?}");
    for n in 301..=302 {
        assert_eq!(delivered(&rows, "jobs", n), [1], "{rows:?}");
    }
    assert!(
        rows.iter().all(|row| row[4].as_i64().unwrap() - row[5].as_i64().unwrap() >= 89_000),
        "a delayed message arrived early: {rows:?}"
    );
    assert!(
        [&c, &d].into_iter().flat_map(vm_pids).all(|pid| !lost.contains(&pid)),
        "a lost owner's guest served the successors"
    );
    drain(c).await;
    drain(d).await;

    // Lease loss and takeover: a node is frozen after acknowledging writes, its
    // lease expires and a successor takes over; the frozen node then resumes
    // and races the successor. No value is published twice, and every
    // acknowledged value is in the successor's state.
    let (e, f) = tokio::join!(
        start(directory, "t23-e", Some(&vms), true, "", &budget),
        start(directory, "t23-f", Some(&vms), true, "", &budget),
    );
    for node in [&e, &f] {
        assert_eq!(get_eventually(node.public, host, "/hello").await, "hello one");
    }
    assert_eq!(get(e.public, host, "/t23-d1-read").await, "14");
    assert!(holds_cell(&e, "__D1Database:").await, "the first user did not become owner");
    assert_eq!(get(e.public, host, "/t23-d1-next").await, "15");
    assert_eq!(get(e.public, host, "/bump?name=t23-z").await, "t23-z:1");
    assert_eq!(get(e.public, host, "/bump?name=t23-z").await, "t23-z:2");
    let frozen = e.child.id().unwrap() as i32;
    let frozen_vms = vm_pids(&e);
    freeze(&e, &bucket).await;
    wait_node_fence(&bucket, "t23-e").await;
    assert_eq!(get_eventually(f.public, host, "/t23-d1-read").await, "15");
    assert_eq!(get(f.public, host, "/t23-d1-next").await, "16");
    assert_eq!(get(f.public, host, "/bump?name=t23-z").await, "t23-z:3");
    unsafe { libc::kill(frozen, libc::SIGCONT) };
    let race = futures_util::future::join_all((0..12).map(|index| {
        let port = if index % 2 == 0 { e.public } else { f.public };
        let (path, key) = if index % 4 < 2 {
            ("/t23-d1-next", "d1")
        } else {
            ("/bump?name=t23-z", "z")
        };
        async move {
            let (status, body) = try_get(port, host, path, Duration::from_secs(30)).await;
            (key, status, body)
        }
    }))
    .await;
    for (key, first, read) in [("d1", 17, "/t23-d1-read"), ("z", 4, "/rpc?name=t23-z")] {
        // Whatever either node acknowledged, distinct values only...
        let mut values: Vec<i64> = race
            .iter()
            .filter(|(race_key, status, _)| *race_key == key && *status == 200)
            .map(|(_, _, body)| body.rsplit(':').next().unwrap().parse().unwrap())
            .collect();
        values.sort();
        let distinct: std::collections::BTreeSet<_> = values.iter().collect();
        assert_eq!(distinct.len(), values.len(), "{key}: a value was published twice: {values:?}");
        assert!(values.iter().all(|value| *value >= first), "{key}: {values:?}");
        // ...and all of it is in the one successor state.
        let state = get_eventually(f.public, host, read).await;
        let last = match key {
            "d1" => state.parse::<i64>().unwrap(),
            _ => serde_json::from_str::<serde_json::Value>(&state).unwrap()["count"]
                .as_i64()
                .unwrap(),
        };
        assert!(last >= values.last().copied().unwrap_or(first - 1), "{key}: {last} < {values:?}");
    }
    // Reap the frozen node and every guest it started, however it ended.
    for pid in frozen_vms {
        unsafe { libc::kill(pid as i32, libc::SIGKILL) };
    }
    let mut e = e;
    let _ = e.child.start_kill();
    let _ = timeout(Duration::from_secs(10), e.child.wait()).await;
    drain(f).await;
    eprintln!("application recovery (T23): a guest crash in every live VM kept acknowledged object, D1, KV and cross-object writes and dropped an open transaction; both nodes were hard-killed at once and two fresh nodes recovered object, D1, KV, queue backlog, alarm and Workflow state from the bucket with one owner per cell and distinct consecutive concurrent writes; a node frozen past its lease was succeeded and its resumed writes never repeated a published value");
    eprintln!("application alarms: committed alarms fired once after idle guest eviction, native process SIGKILL and local reopen, and hard owner loss with a fresh native directory; real node lease fences were observed and every fixture VM was reaped");
    eprintln!("application nodes: a node without a configuration ignored the catalog; with one it served an activated route from public ingress in a VM and left unclaimed URLs to the Worker; stateless handlers on two real nodes called objects owned by each other through the peer path (fetch, RPC value and error), and drain reaped every VM; large KV values survived equal-value retries on two nodes through the peer path; two applications with same-named queues each produced through a peer and consumed in the owner's VM with batching, a delayed send, a retry, explicit acknowledgements kept across a thrown handler and dead-lettering, and neither saw the other's messages; on replicated production-bootstrap nodes, queue messages sent through an owner and delayed past its drain were delivered once each by the node that took the queue over, and a message whose first attempt was running when every VM was killed was delivered again as attempt 2 from a fresh VM and then left alone; also on replicated production-bootstrap nodes, a previous owner's unreplicated blobs were collected, every value survived KV ownership moving to a new node and read back through a third, and an injected blob-then-row failure changed nothing visible and its orphan blobs were collected");
}

/// Static publication through the real CLI/API and public node ingress. No
/// legacy CURRENT, handler artifact, runtime image or VM configuration exists.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts a real node; run by the focused T31 gate"]
async fn t31_static_application_node_smoke() {
    use celld::application_api::{Access, Authorization, Credential, Permission};
    use serde_json::{json, Value};
    celld::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    let artifacts = std::env::var_os("CELLD_APPLICATION_NODE_ARTIFACTS").map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    std::fs::create_dir_all(&artifacts).unwrap();
    let directory = tempfile::Builder::new().prefix("t31-static-").tempdir_in(artifacts).unwrap().keep();
    eprintln!("T31 node artifacts: {}", directory.display());
    let directory = directory.as_path();
    let bucket = celld::dev::open_local_bucket(&directory.join("bucket.sqlite")).unwrap();
    let token = "a".repeat(40);
    let reader = "r".repeat(40);
    let other_token = "b".repeat(40);
    let app = |name: &str| ApplicationId { app: name.to_owned().try_into().unwrap() };
    let auth = Authorization { credentials: vec![
        Credential { token_sha256: content_digest(token.as_bytes()), permission: Permission::Operator {} },
        Credential { token_sha256: content_digest(reader.as_bytes()), permission: Permission::Application { application: app("static-one"), access: Access::Read } },
        Credential { token_sha256: content_digest(other_token.as_bytes()), permission: Permission::Application { application: app("static-two"), access: Access::Manage } },
    ] };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = format!("http://{}", listener.local_addr().unwrap());
    let router = celld::application_api::router(ApplicationStore::new(bucket.clone()), auth, PLATFORM_DOMAIN.into()).unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap(); });
    async fn cli(api: &str, token: &str, app: &str, command: &str, args: &[&str]) -> Result<Value, String> {
        let output = Command::new(env!("CARGO_BIN_EXE_celld"))
            .args(["application", command, "--api", api, "--app", app]).args(args)
            .env("CELLD_APPLICATION_TOKEN", token).output().await.unwrap();
        if !output.status.success() { return Err(String::from_utf8_lossy(&output.stderr).into()); }
        Ok(serde_json::from_slice(&output.stdout).unwrap())
    }
    for name in ["static-one", "static-two"] { cli(&api, &token, name, "create", &[]).await.unwrap(); }
    let assets = directory.join("assets");
    std::fs::create_dir(&assets).unwrap();
    std::fs::write(assets.join("index.html"), "<h1>one</h1>").unwrap();
    std::fs::write(assets.join("a.txt"), "abcdef").unwrap();
    std::fs::write(assets.join("404.html"), "custom missing").unwrap();
    std::fs::write(assets.join("_headers"), "/a.txt\n  X-T31: original\n").unwrap();
    std::fs::write(assets.join("_redirects"), "/old /a.txt 302\n").unwrap();
    let asset_dir = assets.to_str().unwrap();
    let not_found_config = directory.join("not-found.json");
    std::fs::write(&not_found_config, r#"{"assets":{"not_found_handling":"404-page"}}"#).unwrap();
    let uploaded = cli(&api, &token, "static-one", "assets", &["--directory", asset_dir, "--file", not_found_config.to_str().unwrap()]).await.unwrap();
    let manifest = directory.join("manifest.json");
    let manifest_path = manifest.to_str().unwrap();
    let staged = |version: &str, assets: Value| json!({"manifest_version":1, "application":app("static-one"), "id":version, "assets":assets});
    std::fs::write(&manifest, serde_json::to_vec(&staged("v1", uploaded["assets"].clone())).unwrap()).unwrap();
    assert!(cli(&api, &reader, "static-one", "stage", &["--file", manifest_path]).await.is_err());
    assert!(cli(&api, &other_token, "static-one", "stage", &["--file", manifest_path]).await.is_err());
    cli(&api, &token, "static-one", "stage", &["--file", manifest_path]).await.unwrap();
    assert!(cli(&api, &reader, "static-one", "activate", &["--deployment", "v1"]).await.is_err());
    cli(&api, &token, "static-one", "activate", &["--deployment", "v1"]).await.unwrap();
    let inspected = cli(&api, &reader, "static-one", "get", &[]).await.unwrap();
    let host = inspected["asset_hostname"].as_str().unwrap().to_owned();
    assert_eq!(inspected["application"]["active"]["handlers"], json!([]));
    // A foreign index is absent in the second application's own namespace.
    let mut stolen = staged("stolen", uploaded["assets"].clone());
    stolen["application"] = json!(app("static-two"));
    std::fs::write(&manifest, serde_json::to_vec(&stolen).unwrap()).unwrap();
    cli(&api, &other_token, "static-two", "stage", &["--file", manifest_path]).await.unwrap();
    assert!(cli(&api, &other_token, "static-two", "activate", &["--deployment", "stolen"]).await.is_err());
    let mut missing = uploaded["assets"].clone();
    missing["sha256"] = json!("0".repeat(64));
    std::fs::write(&manifest, serde_json::to_vec(&staged("missing", missing)).unwrap()).unwrap();
    cli(&api, &token, "static-one", "stage", &["--file", manifest_path]).await.unwrap();
    assert!(cli(&api, &token, "static-one", "activate", &["--deployment", "missing"]).await.is_err());
    let first_config = directory.join("worker-first.json");
    std::fs::write(&first_config, r#"{"assets":{"run_worker_first":true}}"#).unwrap();
    let first = cli(&api, &token, "static-one", "assets", &["--directory", asset_dir, "--file", first_config.to_str().unwrap()]).await.unwrap();
    std::fs::write(&manifest, serde_json::to_vec(&staged("worker-first", first["assets"].clone())).unwrap()).unwrap();
    cli(&api, &token, "static-one", "stage", &["--file", manifest_path]).await.unwrap();
    assert!(cli(&api, &token, "static-one", "activate", &["--deployment", "worker-first"]).await.is_err());
    assert_eq!(cli(&api, &reader, "static-one", "get", &[]).await.unwrap()["application"]["active"]["id"], "v1");
    let node = start(directory, "t31-static", None, true, "", &[]).await;
    let client = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
    let request = |method: reqwest::Method, path: &str| client.request(method, format!("http://127.0.0.1:{}{path}", node.public)).header("host", &host);
    // Cold and cached paths share the original HTTP representation semantics.
    for _ in 0..2 {
        let reply = request(reqwest::Method::GET, "/a.txt").send().await.unwrap();
        assert_eq!(reply.status(), 200);
        assert!(reply.headers()["content-type"].to_str().unwrap().starts_with("text/plain"));
        assert_eq!(reply.headers()["x-t31"], "original");
        let etag = reply.headers()["etag"].clone();
        assert_eq!(reply.text().await.unwrap(), "abcdef");
        let reply = request(reqwest::Method::HEAD, "/a.txt").send().await.unwrap();
        assert_eq!(reply.status(), 200); assert_eq!(reply.headers()["content-length"], "6"); assert_eq!(reply.bytes().await.unwrap().len(), 0);
        assert_eq!(request(reqwest::Method::GET, "/a.txt").header("if-none-match", etag).send().await.unwrap().status(), 304);
        let reply = request(reqwest::Method::GET, "/a.txt").header("range", "bytes=1-3").send().await.unwrap();
        assert_eq!(reply.status(), 206); assert_eq!(reply.headers()["content-range"], "bytes 1-3/6"); assert_eq!(reply.text().await.unwrap(), "bcd");
    }
    assert_eq!(request(reqwest::Method::GET, "/").send().await.unwrap().text().await.unwrap(), "<h1>one</h1>");
    let reply = request(reqwest::Method::GET, "/old").send().await.unwrap(); assert_eq!(reply.status(), 302); assert_eq!(reply.headers()["location"], "/a.txt");
    let reply = request(reqwest::Method::GET, "/missing").send().await.unwrap(); assert_eq!(reply.status(), 404); assert_eq!(reply.text().await.unwrap(), "custom missing");
    assert_eq!(request(reqwest::Method::POST, "/a.txt").body("unused").send().await.unwrap().status(), 405);
    assert_eq!(status(node.public, "00000000000000000000000000000000.apps.test", "/a.txt").await, 404);
    let digest = content_digest(b"abcdef");
    bucket.delete(&format!("{}artifacts/{digest}", app("static-one").storage_root())).await.unwrap();
    std::fs::write(&manifest, serde_json::to_vec(&staged("missing-body", uploaded["assets"].clone())).unwrap()).unwrap();
    cli(&api, &token, "static-one", "stage", &["--file", manifest_path]).await.unwrap();
    assert!(cli(&api, &token, "static-one", "activate", &["--deployment", "missing-body"]).await.is_err());
    // Same-size corruption bypasses a HEAD size check; activation must hash bytes.
    let digest = content_digest(b"abcdef");
    bucket.put(&format!("{}artifacts/{digest}", app("static-one").storage_root()), bytes::Bytes::from_static(b"xxxxxx")).await.unwrap();
    std::fs::write(&manifest, serde_json::to_vec(&staged("corrupt", uploaded["assets"].clone())).unwrap()).unwrap();
    cli(&api, &token, "static-one", "stage", &["--file", manifest_path]).await.unwrap();
    assert!(cli(&api, &token, "static-one", "activate", &["--deployment", "corrupt"]).await.is_err());
    assert_eq!(cli(&api, &reader, "static-one", "get", &[]).await.unwrap()["application"]["active"]["id"], "v1");
    // Restore only fixture data, then replace and roll back on the same URL.
    let bucket = celld::dev::open_local_bucket(&directory.join("bucket.sqlite")).unwrap();
    bucket.put(&format!("{}artifacts/{digest}", app("static-one").storage_root()), bytes::Bytes::from_static(b"abcdef")).await.unwrap();
    std::fs::write(assets.join("index.html"), "<h1>two</h1>").unwrap();
    let uploaded2 = cli(&api, &token, "static-one", "assets", &["--directory", asset_dir]).await.unwrap();
    std::fs::write(&manifest, serde_json::to_vec(&staged("v2", uploaded2["assets"].clone())).unwrap()).unwrap();
    cli(&api, &token, "static-one", "stage", &["--file", manifest_path]).await.unwrap();
    cli(&api, &token, "static-one", "activate", &["--deployment", "v2"]).await.unwrap();
    assert_eq!(cli(&api, &reader, "static-one", "get", &[]).await.unwrap()["asset_hostname"], host);
    assert_eq!(request(reqwest::Method::GET, "/").send().await.unwrap().text().await.unwrap(), "<h1>two</h1>");
    cli(&api, &token, "static-one", "activate", &["--deployment", "v1"]).await.unwrap();
    assert_eq!(request(reqwest::Method::GET, "/").send().await.unwrap().text().await.unwrap(), "<h1>one</h1>");
    // SPA configuration comes from the same CLI assets project reader.
    let config = directory.join("spa.json");
    std::fs::write(&config, r#"{"assets":{"not_found_handling":"single-page-application"}}"#).unwrap();
    let spa = cli(&api, &token, "static-one", "assets", &["--directory", asset_dir, "--file", config.to_str().unwrap()]).await.unwrap();
    std::fs::write(&manifest, serde_json::to_vec(&staged("spa", spa["assets"].clone())).unwrap()).unwrap();
    cli(&api, &token, "static-one", "stage", &["--file", manifest_path]).await.unwrap();
    cli(&api, &token, "static-one", "activate", &["--deployment", "spa"]).await.unwrap();
    assert_eq!(request(reqwest::Method::GET, "/deep/spa").send().await.unwrap().text().await.unwrap(), "<h1>two</h1>");
    cli(&api, &token, "static-one", "deactivate", &[]).await.unwrap();
    assert_eq!(request(reqwest::Method::GET, "/a.txt").send().await.unwrap().status(), 404);
    let state: Value = reqwest::get(format!("http://127.0.0.1:{}/state", node.internal)).await.unwrap().json().await.unwrap();
    for field in ["vms", "memoryMib", "starting", "ready", "quarantined"] { assert_eq!(state["applicationVms"][field], 0, "{state}"); }
    assert!(vm_pids(&node).is_empty());
    drain(node).await;
    server.abort();
    eprintln!("T31 static node PASS: real API/CLI, no legacy CURRENT or VM config, cold/cached HTTP, corrupt/cross-app refusal, stable replacement/rollback/deactivation; zero VM capacity/claims");
}
