//! Real `celld` nodes in containers on user-defined Docker networks, with MinIO
//! as the fleet bucket, so a test can cut the network between nodes and to the
//! bucket. Partial T24 evidence: the containers share one kernel, KVM, disk,
//! clock and power supply, so only network faults and process crashes are
//! independent. Needs the actual-VM environment of `application_nodes.rs`, root,
//! Docker (`DOCKER_HOST`, default the native `/run/celld-docker.sock`) and the
//! `celld` binary. Each network-fault scenario runs as its own ignored test.
//! `CELLD_CONTAINER_SCENARIOS` (default `a,d,b,c`) selects scenarios.
#![cfg(target_os = "linux")]
// These integration tests drive real node processes, containers and wall-clock
// deadlines from outside the node, so the execution-boundary lint does not apply.
#![allow(clippy::disallowed_methods)]

use celld::application_api::{Authorization, Credential, Permission};
use celld::applications::{content_digest, ApplicationStore};
use celld::bucket::{Bucket, StaticCredentials};
use celld_applications::{ApplicationId, Deployment};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::process::Command;
use tokio::time::timeout;

const PLATFORM_DOMAIN: &str = "apps.test";
// Test-only credentials for the throwaway MinIO of each scenario.
const ACCESS_KEY: &str = "celldtest";
const SECRET_KEY: &str = "celldtest-secret-key";
const NODE_IMAGE: &str = "celld-t24-node";

/// The part of the node fixture's worker these scenarios use: the `/t22` write
/// of the object, D1, KV and the queue, and the reads that check them.
const SOURCE: &str = r#"import { DurableObject, env } from 'cloudflare:workers';
export class Counter extends DurableObject {
  async fetch() {
    const count = (await this.ctx.storage.get('count') ?? 0) + 1;
    await this.ctx.storage.put('count', count);
    return new Response(`${this.ctx.id.name}:${count}`);
  }
  async count() { return (await this.ctx.storage.get('count')) ?? 0; }
}
export default {
  async fetch(request) {
    const url = new URL(request.url);
    const stub = env.COUNTER.getByName(url.searchParams.get('name'));
    switch (url.pathname) {
      case '/hello': return new Response('hello');
      case '/q-setup':
        await env.DB.exec('CREATE TABLE IF NOT EXISTS q (queue TEXT, n INTEGER, attempts INTEGER, size INTEGER, at INTEGER, sent INTEGER, label TEXT)');
        return Response.json('ready');
      case '/q-log':
        return Response.json(await env.DB.prepare('SELECT queue, n, attempts, size, at, sent, label FROM q ORDER BY rowid').raw());
      case '/t22': {
        const n = +url.searchParams.get('n');
        await env.DB.exec('CREATE TABLE IF NOT EXISTS t22 (n INTEGER)');
        await env.DB.prepare('INSERT INTO t22 VALUES (?)').bind(n).run();
        await env.CACHE.put('t22:' + n, 'v' + n);
        await stub.fetch('http://object/bump');
        await env.JOBS.send({ kind: 'ok', n, label: 't22' });
        return Response.json(n);
      }
      case '/t22-read':
        return Response.json({
          rows: (await env.DB.prepare('SELECT n FROM t22 ORDER BY n').raw()).flat(),
          keys: (await env.CACHE.list({ prefix: 't22:' })).keys.map(key => key.name),
          count: await stub.count(),
        });
    }
    return new Response('objects');
  },
  async queue(batch) {
    for (const message of batch.messages) {
      const { n, sent, label } = message.body;
      await env.DB.prepare('INSERT INTO q VALUES (?, ?, ?, ?, ?, ?, ?)')
        .bind(batch.queue, n, message.attempts, batch.messages.length, Date.now(), sent ?? null, label).run();
      message.ack();
    }
  },
};
"#;

fn docker_host() -> String {
    std::env::var("DOCKER_HOST").unwrap_or_else(|_| "unix:///run/celld-docker.sock".to_owned())
}

/// `docker ARGS`, its trimmed stdout; panics when it fails.
async fn docker(args: &[&str]) -> String {
    let output = Command::new("docker")
        .args(args)
        .env("DOCKER_HOST", docker_host())
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "docker {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// Remove a scenario's containers and network, as on drop and before a start.
fn cleanup(label: &str) {
    let docker = |args: &[&str]| {
        std::process::Command::new("docker")
            .args(args)
            .env("DOCKER_HOST", docker_host())
            .stdin(Stdio::null())
            .output()
    };
    if let Ok(ids) = docker(&["ps", "-aq", "--filter", &format!("label=celld-t24={label}")]) {
        for id in String::from_utf8_lossy(&ids.stdout).split_whitespace() {
            let _ = docker(&["rm", "-f", id]);
        }
    }
    let _ = docker(&["network", "rm", label]);
    fn remove_cgroups(path: &Path) {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries
                .flatten()
                .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            {
                remove_cgroups(&entry.path());
            }
        }
        let _ = std::fs::remove_dir(path);
    }
    if let Ok(entries) = std::fs::read_dir("/sys/fs/cgroup") {
        let prefix = format!("celld-fixture-{label}-");
        for entry in entries
            .flatten()
            .filter(|entry| entry.file_name().to_string_lossy().starts_with(&prefix))
        {
            remove_cgroups(&entry.path());
        }
    }
}

struct Node {
    /// The container name; the fleet's `CELLD_NODE` is `fleet`.
    name: String,
    fleet: String,
    ip: String,
    dir: PathBuf,
}

impl Node {
    async fn exec(&self, args: &[&str]) -> String {
        let mut command = vec!["exec", &self.name];
        command.extend_from_slice(args);
        docker(&command).await
    }

    /// Drop all traffic to and from `peers` (this node's side of a partition).
    async fn cut(&self, peers: &[&str]) {
        for ip in peers {
            self.exec(&["iptables", "-I", "INPUT", "-s", ip, "-j", "DROP"]).await;
            self.exec(&["iptables", "-I", "OUTPUT", "-d", ip, "-j", "DROP"]).await;
        }
    }

    async fn heal(&self) {
        self.exec(&["iptables", "-F"]).await;
    }

    /// Delay and loss on this node's traffic to `peers` only.
    async fn netem(&self, peers: &[&str]) {
        self.exec(&["tc", "qdisc", "add", "dev", "eth0", "root", "handle", "1:", "prio", "bands", "3"]).await;
        self.exec(&["tc", "qdisc", "add", "dev", "eth0", "parent", "1:3", "handle", "30:", "netem",
            "delay", "100ms", "20ms", "loss", "5%"]).await;
        for ip in peers {
            let destination = format!("{ip}/32");
            self.exec(&["tc", "filter", "add", "dev", "eth0", "protocol", "ip", "parent", "1:", "prio", "1",
                "u32", "match", "ip", "dst", &destination, "flowid", "1:3"]).await;
        }
    }

    async fn unnetem(&self) {
        self.exec(&["tc", "qdisc", "del", "dev", "eth0", "root"]).await;
    }

    /// SIGKILL of the whole container: the node and its VMMs, no drain.
    async fn kill(&self) {
        docker(&["kill", &self.name]).await;
    }

    async fn ready(&self) {
        timeout(Duration::from_secs(240), async {
            while !matches!(reqwest::get(format!("http://{}:8080/.well-known/celld/health", self.ip)).await,
                Ok(reply) if reply.status() == 200)
            {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{} did not become ready\n{}", self.name, self.log()));
    }

    /// Reconnect a node that was cut off. A node that lost its lease has exited:
    /// its container is started again (a fresh network namespace has no rules).
    /// Returns whether the node had exited.
    async fn reconnect(&self) -> bool {
        if docker(&["inspect", "-f", "{{.State.Running}}", &self.name]).await == "true" {
            self.heal().await;
            return false;
        }
        docker(&["start", &self.name]).await;
        self.ready().await;
        true
    }

    fn log(&self) -> String {
        let log = std::fs::read_to_string(self.dir.join("node.log")).unwrap_or_default();
        log.lines().rev().take(15).collect::<Vec<_>>().join("\n")
    }
}

struct Prep {
    directory: PathBuf,
    image: PathBuf,
    runtime_sha256: String,
    digest: String,
    run_id: String,
}

/// One scenario's network, MinIO and three nodes. Everything is removed on drop.
struct Cluster {
    label: String,
    bucket: Bucket,
    host: String,
    minio: String,
    nodes: Vec<Node>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        cleanup(&self.label);
    }
}

impl Cluster {
    async fn start(prep: &Prep, index: u8, id: &str) -> Cluster {
        let label = format!("celld-{}-{id}", prep.run_id);
        let directory = prep.directory.join(id);
        std::fs::create_dir_all(directory.join("minio/celld")).unwrap();
        let subnet = format!("10.213.{}", 100 + index);
        let tag = format!("celld-t24={label}");
        let run_tag = format!("celld-t29-run={}", prep.run_id);
        // Establish drop cleanup before the first Docker mutation can fail.
        let mut cluster = Cluster {
            label: label.clone(),
            bucket: open_bucket(&format!("http://{subnet}.2:9000"), id),
            host: String::new(),
            minio: format!("{subnet}.2"),
            nodes: Vec::new(),
        };
        docker(&["network", "create", "--subnet", &format!("{subnet}.0/24"), "--label", &tag, "--label", &run_tag, &label]).await;
        let minio = format!("{subnet}.2");
        let minio_image = std::env::var("CELLD_CONTAINER_MINIO_IMAGE")
            .unwrap_or_else(|_| "chainguard/minio:latest".to_owned());
        docker(&["run", "-d", "--name", &format!("{label}-minio"), "--network", &label, "--ip", &minio,
            "--user", "root", "--label", &tag, "--label", &run_tag,
            "-e", &format!("MINIO_ROOT_USER={ACCESS_KEY}"), "-e", &format!("MINIO_ROOT_PASSWORD={SECRET_KEY}"),
            "-v", &format!("{}:/data", directory.join("minio").display()),
            &minio_image, "server", "/data", "--address", ":9000"]).await;
        timeout(Duration::from_secs(60), async {
            while !matches!(reqwest::get(format!("http://{minio}:9000/minio/health/live")).await,
                Ok(reply) if reply.status() == 200)
            {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .expect("MinIO did not become ready");
        cluster.host = deploy(prep, id, &minio).await;
        for number in 1..=3 {
            let node = cluster.run_node(prep, &directory, &subnet, id, number).await;
            cluster.nodes.push(node);
        }
        for node in &cluster.nodes {
            timeout(Duration::from_secs(240), async {
                while get(node, &cluster.host, "/hello", Duration::from_secs(10)).await.as_deref() != Some("hello") {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            })
            .await
            .unwrap_or_else(|_| {
                panic!("{}: the activated route was not served\n{}", node.name, node.log())
            });
        }
        cluster
    }

    async fn run_node(&self, prep: &Prep, directory: &Path, subnet: &str, id: &str, number: u8) -> Node {
        let fleet = format!("{id}-n{number}");
        let ip = format!("{subnet}.{}", 10 + number);
        let dir = directory.join(&fleet);
        std::fs::create_dir_all(&dir).unwrap();
        let cgroup = PathBuf::from(format!(
            "/sys/fs/cgroup/celld-fixture-{}-{fleet}",
            self.label
        ));
        std::fs::create_dir(&cgroup).unwrap();
        std::fs::write(cgroup.join("cgroup.subtree_control"), "+cpu").unwrap();
        let mounts = [
            (env!("CARGO_BIN_EXE_celld").to_owned(), "/usr/local/bin/celld:ro"),
            ("/usr/local/bin/firecracker".to_owned(), "/usr/local/bin/firecracker:ro"),
            ("/usr/local/bin/jailer".to_owned(), "/usr/local/bin/jailer:ro"),
            (std::env::var("CELLD_APPLICATION_JAVASCRIPT_TEST_KERNEL").unwrap(), "/opt/celld/vmlinux:ro"),
            (prep.image.display().to_string(), "/fixture/runtime.ext4:ro"),
            (prep.directory.join("vms.json").display().to_string(), "/fixture/vms.json:ro"),
            (dir.display().to_string(), "/data"),
            (cgroup.display().to_string(), "/sys/fs/cgroup:rw"),
            (
                std::env::var("CELLD_PYTHON_RUNTIME_DIR")
                    .expect("the eager startup fixture needs the pinned Pyodide runtime"),
                "/fixture/pyodide:ro",
            ),
        ];
        let name = format!("{}-{fleet}", self.label);
        let mut args = vec![
            "run".to_owned(), "-d".to_owned(), "--name".to_owned(), name.clone(),
            "--network".to_owned(), self.label.clone(), "--ip".to_owned(), ip.clone(),
            "--label".to_owned(), format!("celld-t24={}", self.label),
            "--label".to_owned(), format!("celld-t29-run={}", prep.run_id),
            // Not --privileged: the jailer's mount/pivot_root needs SYS_ADMIN and no
            // seccomp or AppArmor profile; `iptables`/`tc` need NET_ADMIN; Firecracker
            // needs /dev/kvm. (Docker's default capabilities cover chroot, mknod, setuid.)
            "--cap-add".to_owned(), "SYS_ADMIN".to_owned(), "--cap-add".to_owned(), "NET_ADMIN".to_owned(),
            "--security-opt".to_owned(), "seccomp=unconfined".to_owned(),
            "--security-opt".to_owned(), "apparmor=unconfined".to_owned(),
            "--device".to_owned(), "/dev/kvm".to_owned(),
            "--cgroupns".to_owned(),
            "host".to_owned(),
        ];
        for (source, target) in mounts {
            args.extend(["-v".to_owned(), format!("{source}:{target}")]);
        }
        for (key, value) in [
            ("AWS_ACCESS_KEY_ID", ACCESS_KEY), ("AWS_SECRET_ACCESS_KEY", SECRET_KEY),
            ("AWS_REGION", "us-east-1"), ("CELLD_NODE", &fleet), ("CELLD_WATCH", "/data"),
            ("CELLD_WAKER_TICK_MS", "1000"), ("CELLD_HANDLER_BUDGET_S", "60"),
            ("CELLD_LOG_FLUSH_MS", "60000"),
            ("CELLD_PYTHON_RUNTIME_DIR", "/fixture/pyodide"),
        ] {
            args.extend(["-e".to_owned(), format!("{key}={value}")]);
        }
        args.extend(
            [NODE_IMAGE, "sh", "-c", "exec celld \"$@\" >/data/node.log 2>&1", "sh",
                "--bucket", &format!("celld/{id}"), "--endpoint", &format!("http://{}:9000", self.minio),
                "--listen", "0.0.0.0:8080", "--internal-listen", "0.0.0.0:9000",
                "--advertise", &format!("{ip}:9000"), "--application-vms", "/fixture/vms.json"]
                .map(str::to_owned),
        );
        docker(&args.iter().map(String::as_str).collect::<Vec<_>>()).await;
        let node = Node { name, fleet, ip, dir };
        node.ready().await;
        node
    }
}

fn open_bucket(endpoint: &str, id: &str) -> Bucket {
    Bucket::open(
        &format!("celld/{id}"),
        Some(endpoint),
        "us-east-1",
        Some(StaticCredentials {
            access_key_id: ACCESS_KEY.to_owned(),
            secret_access_key: SECRET_KEY.to_owned(),
            session_token: None,
        }),
        None,
    )
    .unwrap()
}

/// Provision with the real CLI against the production management HTTP router.
async fn deploy(prep: &Prep, id: &str, minio: &str) -> String {
    let store = ApplicationStore::new(open_bucket(&format!("http://{minio}:9000"), id));
    let application = ApplicationId {
        app: "containers".to_owned().try_into().unwrap(),
    };
    let token = "t".repeat(40);
    let auth = Authorization { credentials: vec![Credential {
        token_sha256: content_digest(token.as_bytes()), permission: Permission::Operator {},
    }] };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = format!("http://{}", listener.local_addr().unwrap());
    let router = celld::application_api::router(store, auth, PLATFORM_DOMAIN.into()).unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap(); });
    struct Api(tokio::task::JoinHandle<()>);
    impl Drop for Api { fn drop(&mut self) { self.0.abort(); } }
    let _api = Api(server);
    async fn cli(api: &str, token: &str, command: &str, args: &[&str]) -> serde_json::Value {
        let output = Command::new(env!("CARGO_BIN_EXE_celld"))
            .args(["application", command, "--api", api, "--app", "containers"])
            .args(args).env("CELLD_APPLICATION_TOKEN", token).output().await.unwrap();
        assert!(output.status.success(), "CLI {command}: {}", String::from_utf8_lossy(&output.stderr));
        serde_json::from_slice(&output.stdout).unwrap()
    }
    cli(&api, &token, "create", &[]).await;
    let resource = |kind: &str, id: &str| serde_json::json!({"kind":kind,"id":id});
    let counter = resource("durable_object", "counter");
    let database = resource("d1", "db");
    let cache = resource("kv", "cache");
    let jobs = resource("queue", "jobs");
    let dead = resource("queue", "dead");
    let resource_path = prep.directory.join(id).join("resource.json");
    for resource in [&counter, &database, &cache, &jobs, &dead] {
        std::fs::write(&resource_path, serde_json::json!({"resource":resource,"configuration":{}}).to_string()).unwrap();
        cli(&api, &token, "resource", &["--file", resource_path.to_str().unwrap()]).await;
    }
    let artifact_path = prep.directory.join("app.bundle");
    let uploaded = cli(&api, &token, "artifact", &["--file", artifact_path.to_str().unwrap()]).await;
    assert_eq!(uploaded["artifact_sha256"], prep.digest);
    let deployment: Deployment = serde_json::from_value(serde_json::json!({
        "manifest_version": 1,
        "application": application,
        "id": "one",
        "handlers": [{
            "id": "javascript", "language": "javascript", "entrypoint": "main.mjs",
            "artifact_sha256": prep.digest, "runtime_image_sha256": prep.runtime_sha256,
            "http": true, "environment": {},
            "bindings": {"COUNTER": {"kind": "resource", "resource": counter}, "DB": {"kind": "resource", "resource": database}, "CACHE": {"kind": "resource", "resource": cache}, "JOBS": {"kind": "resource", "resource": jobs}},
            "worker_config": {"do_classes": ["Counter"], "durable_objects": {"bindings": [{"name":"COUNTER","class_name":"Counter"}]}, "compatibility_date": "2026-03-01", "compatibility_flags": ["js_rpc"], "queue_consumers": [
                {"queue": "jobs", "max_batch_size": 10, "max_batch_timeout": 1, "max_retries": 1, "dead_letter_queue": "dead", "retry_delay": 1},
                {"queue": "dead", "max_batch_size": 10, "max_batch_timeout": 1, "max_retries": 0}
            ]}
        }]
    }))
    .unwrap();
    let manifest_path = prep.directory.join(id).join("manifest.json");
    std::fs::write(&manifest_path, serde_json::to_vec(&deployment).unwrap()).unwrap();
    cli(&api, &token, "stage", &["--file", manifest_path.to_str().unwrap()]).await;
    cli(&api, &token, "activate", &["--deployment", "one"]).await;
    let inspected = cli(&api, &token, "get", &[]).await;
    inspected["hostnames"]["javascript"].as_str().unwrap().to_owned()
}

/// The body of a 200 from the node's public port; `None` for anything else, as
/// from a node that is partitioned, fenced, recovering or gone.
async fn get(node: &Node, host: &str, path: &str, limit: Duration) -> Option<String> {
    let reply = reqwest::Client::builder()
        .timeout(limit)
        .build()
        .unwrap()
        .get(format!("http://{}:8080{path}", node.ip))
        .header("host", host)
        .send()
        .await
        .ok()?;
    if reply.status() != 200 {
        return None;
    }
    reply.text().await.ok()
}

/// One `/t22` write; `Some(elapsed)` only when the node acknowledged it.
async fn write(node: &Node, host: &str, n: u32, limit: Duration) -> Option<Duration> {
    let started = Instant::now();
    get(node, host, &format!("/t22?name=t22-obj&n={n}"), limit).await?;
    Some(started.elapsed())
}

async fn read(node: &Node, host: &str) -> Option<serde_json::Value> {
    serde_json::from_str(&get(node, host, "/t22-read?name=t22-obj", Duration::from_secs(30)).await?).ok()
}

/// Everything acknowledged is visible on `node` and nothing unattempted is, and
/// no write was applied twice. An attempt that was not acknowledged may or may
/// not have landed.
async fn expect(node: &Node, host: &str, acknowledged: &[u32], attempted: &[u32]) {
    let read = timeout(Duration::from_secs(150), async {
        loop {
            if let Some(read) = read(node, host).await {
                break read;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("{} never served the acknowledged state\n{}", node.name, node.log())
    });
    let rows: Vec<u32> = serde_json::from_value(read["rows"].clone()).unwrap();
    let keys: Vec<String> = serde_json::from_value(read["keys"].clone()).unwrap();
    let count = read["count"].as_u64().unwrap() as usize;
    for n in acknowledged {
        assert!(rows.contains(n), "{}: acknowledged D1 write {n} was lost: {read}", node.name);
        assert!(keys.contains(&format!("t22:{n}")), "{}: acknowledged KV write {n} was lost: {read}", node.name);
    }
    assert!(rows.iter().all(|n| attempted.contains(n)), "{}: unattempted D1 row: {read}", node.name);
    let mut unique = rows.clone();
    unique.dedup();
    assert_eq!(unique, rows, "{}: a write was applied twice: {read}", node.name);
    assert!(count >= acknowledged.len() && count <= attempted.len(), "{}: object count {count}: {read}", node.name);
}

/// Every acknowledged send reaches the queue consumer at least once.
async fn queued(node: &Node, host: &str, acknowledged: &[u32]) {
    timeout(Duration::from_secs(120), async {
        loop {
            let rows: Vec<serde_json::Value> = match get(node, host, "/q-log", Duration::from_secs(20)).await {
                Some(body) => serde_json::from_str(&body).unwrap_or_default(),
                None => Vec::new(),
            };
            if acknowledged.iter().all(|n| rows.iter().any(|row| row[6] == "t22" && row[1] == *n)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("an acknowledged queue send was lost");
}

/// A node's folded log in its lease record: whether its ensemble is serving
/// acknowledgements, and the members. `None` before the log opens.
async fn log_posture(bucket: &Bucket, node: &str) -> Option<(bool, Vec<String>)> {
    let (bytes, _) = bucket.get(&format!("nodes/{node}.json")).await.unwrap()?;
    let lease: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let log = lease.get("log").filter(|log| !log.is_null())?;
    let members = log["ensemble"].as_array()?;
    Some((
        log["active"].as_bool().unwrap_or(false),
        members.iter().map(|member| member.as_str().unwrap().to_owned()).collect(),
    ))
}

/// Wait until the node's lease record has expired, so no peer honours it.
async fn wait_fence(bucket: &Bucket, name: &str) {
    timeout(Duration::from_secs(60), async {
        loop {
            let Some((bytes, _)) = bucket.get(&format!("nodes/{name}.json")).await.unwrap() else { break;
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
    .expect("the owner's node lease did not expire");
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

/// What the other nodes' disks hold of `owner`'s log.
fn peer_disk(cluster: &Cluster, owner: &Node) -> u64 {
    cluster
        .nodes
        .iter()
        .filter(|node| node.name != owner.name)
        .map(|node| tree_bytes(&node.dir.join("peerlog").join(&owner.fleet)))
        .sum()
}

/// The addresses of the nodes named in a log posture.
fn ips<'a>(cluster: &'a Cluster, members: &[String]) -> Vec<&'a str> {
    cluster
        .nodes
        .iter()
        .filter(|node| members.contains(&node.fleet))
        .map(|node| node.ip.as_str())
        .collect()
}

/// Ledger of one scenario's writes.
#[derive(Default)]
struct Writes {
    attempted: Vec<u32>,
    acknowledged: Vec<u32>,
}

impl Writes {
    async fn write(&mut self, node: &Node, host: &str, n: u32, limit: Duration) -> Option<Duration> {
        self.attempted.push(n);
        let elapsed = write(node, host, n, limit).await?;
        self.acknowledged.push(n);
        Some(elapsed)
    }

    async fn check(&self, node: &Node, host: &str) {
        expect(node, host, &self.acknowledged, &self.attempted).await;
        queued(node, host, &self.acknowledged).await;
    }
}

/// The owner (first use fixes every owner to the first node), its ensemble
/// recruited, and six healthy writes acknowledged.
async fn healthy(cluster: &Cluster, writes: &mut Writes) {
    let owner = &cluster.nodes[0];
    assert_eq!(get(owner, &cluster.host, "/q-setup", Duration::from_secs(60)).await.as_deref(), Some("\"ready\""));
    timeout(Duration::from_secs(90), async {
        while !matches!(log_posture(&cluster.bucket, &owner.fleet).await, Some((_, members)) if !members.is_empty()) {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the owner never opened a fleet ensemble\n{}", owner.log()));
    for n in 1..=6 {
        // The first use of each cell fixes its owner, so the owner serves it first.
        let node = &cluster.nodes[if n <= 2 { 0 } else { (n % 3) as usize }];
        writes.write(node, &cluster.host, n, Duration::from_secs(90)).await.expect("healthy fleet refused a write");
    }
    assert!(
        matches!(log_posture(&cluster.bucket, &owner.fleet).await, Some((true, _))),
        "the owner's ensemble never began acknowledging"
    );
    assert!(peer_disk(cluster, owner) > 0, "no follower's disk holds the owner's log");
}

/// The survivors hold every acknowledged write, accept a new one, and the
/// queue delivered every acknowledged send.
async fn survivors_hold(cluster: &Cluster, writes: &mut Writes, next: u32) {
    for node in &cluster.nodes[1..] {
        writes.check(node, &cluster.host).await;
    }
    writes
        .write(&cluster.nodes[1], &cluster.host, next, Duration::from_secs(90))
        .await
        .expect("a survivor refused a write");
    for node in &cluster.nodes[1..] {
        writes.check(node, &cluster.host).await;
    }
}

/// a: the owner is cut off from its log ensemble while writes continue.
async fn scenario_a(prep: &Prep) {
    let cluster = Cluster::start(prep, 0, "a").await;
    let (owner, host) = (&cluster.nodes[0], cluster.host.as_str());
    let mut writes = Writes::default();
    healthy(&cluster, &mut writes).await;
    // Cut off the follower(s): the reply is held until they are judged dead, then
    // the bucket (or a newly recruited follower's disk) proves the write.
    let (_, members) = log_posture(&cluster.bucket, &owner.fleet).await.unwrap();
    let before = writes.acknowledged.len();
    owner.cut(&ips(&cluster, &members)).await;
    let mut elapsed = Vec::new();
    for n in 7..=10 {
        if let Some(took) = writes.write(owner, host, n, Duration::from_secs(90)).await {
            elapsed.push(took);
        }
    }
    assert!(writes.acknowledged.len() > before, "no write was acknowledged through the bucket while the follower was cut off\n{}", owner.log());
    assert!(elapsed[0] >= Duration::from_secs(1), "a reply left before any proof: {:?}", elapsed[0]);
    expect(owner, host, &writes.acknowledged, &writes.attempted).await;
    // Heal: the ensemble serves acknowledgements again from a disk that grows.
    let disk = peer_disk(&cluster, owner);
    owner.heal().await;
    let mut n = 10;
    timeout(Duration::from_secs(150), async {
        loop {
            n += 1;
            let took = writes.write(owner, host, n, Duration::from_secs(60)).await;
            if let (Some(took), Some((true, members))) = (took, log_posture(&cluster.bucket, &owner.fleet).await) {
                if !members.is_empty() && took < Duration::from_secs(10) && peer_disk(&cluster, owner) > disk {
                    break;
                }
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("the ensemble did not recover after the partition healed\n{}", owner.log())
    });
    // Cut off from every peer, the owner can only be proved by the bucket. Lose
    // it there: what the survivors hold can only have come from the bucket.
    owner.cut(&[cluster.nodes[1].ip.as_str(), cluster.nodes[2].ip.as_str()]).await;
    let mut acknowledged = false;
    for _ in 0..4 {
        n += 1;
        if writes.write(owner, host, n, Duration::from_secs(90)).await.is_some() {
            acknowledged = true;
            break;
        }
    }
    assert!(acknowledged, "no write was acknowledged through the bucket after the second cut");
    owner.kill().await;
    wait_fence(&cluster.bucket, &owner.fleet).await;
    survivors_hold(&cluster, &mut writes, n + 1).await;
    eprintln!("containers a: with the owner cut off from its log ensemble, the first held write waited for proof and every acknowledged write survived; the healed ensemble served acknowledgements from a growing disk again; after the owner was cut off from both peers (bucket proof only) and killed, both survivors held every acknowledged write ({} of {} attempts)", writes.acknowledged.len(), writes.attempted.len());
}

/// d: SIGKILL of the owner's container in the middle of a write stream.
async fn scenario_d(prep: &Prep) {
    let cluster = Cluster::start(prep, 1, "d").await;
    let host = cluster.host.clone();
    let mut writes = Writes::default();
    healthy(&cluster, &mut writes).await;
    let writer = {
        let owner = &cluster.nodes[0];
        let (fleet, ip, dir, host) = (owner.fleet.clone(), owner.ip.clone(), owner.dir.clone(), host.clone());
        tokio::spawn(async move {
            let node = Node { name: String::new(), fleet, ip, dir };
            let mut stream = Writes::default();
            for n in 7.. {
                if stream.write(&node, &host, n, Duration::from_secs(20)).await.is_none() {
                    break;
                }
            }
            stream
        })
    };
    tokio::time::sleep(Duration::from_secs(4)).await;
    cluster.nodes[0].kill().await;
    let stream = writer.await.unwrap();
    assert!(!stream.acknowledged.is_empty(), "no write was acknowledged before the kill");
    writes.attempted.extend(&stream.attempted);
    writes.acknowledged.extend(&stream.acknowledged);
    wait_fence(&cluster.bucket, &cluster.nodes[0].fleet).await;
    survivors_hold(&cluster, &mut writes, 1000).await;
    eprintln!("containers d: after SIGKILL of the owner's container during a write stream, both survivors held all {} acknowledged writes of {} attempts and accepted a new one", writes.acknowledged.len(), writes.attempted.len());
}

/// b: the owner is cut off from the bucket and every peer.
async fn scenario_b(prep: &Prep) {
    let cluster = Cluster::start(prep, 2, "b").await;
    let (owner, host) = (&cluster.nodes[0], cluster.host.as_str());
    let mut writes = Writes::default();
    healthy(&cluster, &mut writes).await;
    let everything = [cluster.minio.as_str(), cluster.nodes[1].ip.as_str(), cluster.nodes[2].ip.as_str()];
    owner.cut(&everything).await;
    // Neither a follower's disk nor the bucket can prove a write now.
    assert!(
        writes.write(owner, host, 7, Duration::from_secs(45)).await.is_none(),
        "the isolated owner acknowledged a write it could not prove"
    );
    wait_fence(&cluster.bucket, &owner.fleet).await;
    for node in &cluster.nodes[1..] {
        expect(node, host, &writes.acknowledged, &writes.attempted).await;
    }
    writes.write(&cluster.nodes[1], host, 8, Duration::from_secs(90)).await.expect("the successor refused a write");
    writes.write(&cluster.nodes[2], host, 9, Duration::from_secs(90)).await.expect("the other survivor refused a write");
    // Healed, the old owner may rejoin but cannot publish what the survivors lack.
    let exited = owner.reconnect().await;
    let late = writes.write(owner, host, 10, Duration::from_secs(60)).await.is_some();
    for node in &cluster.nodes[1..] {
        writes.check(node, host).await;
    }
    eprintln!("containers b: the owner cut off from the bucket and its peers acknowledged nothing and its lease expired; the survivors held all {} acknowledged writes and took new ones; after healing (the fenced owner {}), the old owner {} and the survivors held everything acknowledged", writes.acknowledged.len(), if exited { "had exited and was restarted" } else { "was still running" }, if late { "acknowledged one more write" } else { "acknowledged nothing" });
}

/// c: 100 ms +- 20 ms delay and 5% loss on every node-to-node link.
async fn scenario_c(prep: &Prep) {
    let cluster = Cluster::start(prep, 3, "c").await;
    let host = cluster.host.as_str();
    let mut writes = Writes::default();
    healthy(&cluster, &mut writes).await;
    let mut fast = Duration::MAX;
    for n in 7..=9 {
        fast = fast.min(writes.write(&cluster.nodes[1], host, n, Duration::from_secs(60)).await.unwrap());
    }
    for node in &cluster.nodes {
        let peers: Vec<&str> = cluster.nodes.iter().filter(|peer| peer.ip != node.ip).map(|peer| peer.ip.as_str()).collect();
        node.netem(&peers).await;
    }
    let mut slow = Duration::MAX;
    for n in 10..=21 {
        let node = &cluster.nodes[(n % 3) as usize];
        // Delay and loss make requests slower, never wrong or failed.
        let took = writes.write(node, host, n, Duration::from_secs(120)).await.expect("a write failed under delay and loss");
        if node.ip != cluster.nodes[0].ip {
            slow = slow.min(took);
        }
    }
    assert!(slow >= Duration::from_millis(150), "peer writes were not delayed: {slow:?}");
    for node in &cluster.nodes {
        node.unnetem().await;
    }
    assert_eq!(writes.attempted, writes.acknowledged);
    for node in &cluster.nodes {
        writes.check(node, host).await;
    }
    eprintln!("containers c: under 100 ms +- 20 ms delay and 5% loss on peer links all {} writes were acknowledged exactly once with the queue complete; fastest peer write {fast:?} before, {slow:?} during", writes.acknowledged.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires root, KVM, Docker and the approved application guest image/kernel"]
async fn containers_survive_network_faults() {
    celld::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    let image = std::env::var("CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE")
        .expect("CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE is required");
    let image = PathBuf::from(image);
    let runtime_sha256 = std::fs::read_to_string(image.with_extension("sha256")).unwrap().trim().to_owned();
    let kernel_sha256 = std::env::var("CELLD_APPLICATION_JAVASCRIPT_TEST_KERNEL_SHA256").unwrap();
    let logs = std::env::var_os("CELLD_APPLICATION_NODE_ARTIFACTS")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/application-guest")
        });
    std::fs::create_dir_all(&logs).unwrap();
    let directory = tempfile::Builder::new().prefix("containers-t24-").tempdir_in(logs).unwrap().keep();
    eprintln!("container artifacts: {}", directory.display());

    let source = directory.join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("main.mjs"), SOURCE).unwrap();
    let built = Command::new("/usr/bin/python3")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("../applications/guest/build-application-image.py"))
        .arg(&source)
        .arg(directory.join("app.bundle"))
        .env_clear()
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .await
        .unwrap();
    assert!(built.status.success(), "{}", String::from_utf8_lossy(&built.stderr));
    let digest = content_digest(&std::fs::read(directory.join("app.bundle")).unwrap());
    std::fs::write(
        directory.join("vms.json"),
        serde_json::json!({
            "jailer": "/usr/local/bin/jailer",
            "firecracker": "/usr/local/bin/firecracker",
            "kernel": {"path": "/opt/celld/vmlinux", "sha256": kernel_sha256},
            "runtimes": [{"path": "/fixture/runtime.ext4", "sha256": runtime_sha256}],
            "uid": 1000, "gid": 1000,
            "max_vms": 2, "max_memory_mib": 12288, "idle_ttl_secs": 600,
            "startup_pool": std::env::var("CELLD_APPLICATION_STARTUP_POOL").unwrap_or_else(|_| "0".into()).parse::<u32>().unwrap(),
            "platform_domain": PLATFORM_DOMAIN
        })
        .to_string(),
    )
    .unwrap();
    // The node image holds only what the bind-mounted celld, firecracker and
    // kernel do not: `iptables` and `tc`. Cached after the first build.
    let mut build = Command::new("docker")
        .args(["build", "-q", "-t", NODE_IMAGE, "-"])
        .env("DOCKER_HOST", docker_host())
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    tokio::io::AsyncWriteExt::write_all(
        &mut build.stdin.take().unwrap(),
        b"FROM ubuntu:24.04\nRUN apt-get update && apt-get install -y --no-install-recommends iproute2 iptables ca-certificates erofs-utils && rm -rf /var/lib/apt/lists/*\n",
    )
    .await
    .unwrap();
    assert!(build.wait().await.unwrap().success(), "node image build failed");
    let run_id = std::env::var("CELLD_APPLICATION_RUN_ID").unwrap_or_else(|_| format!("t29-{}", std::process::id()));
    assert!(run_id.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'), "invalid run id");
    let prep = Prep { directory, image, runtime_sha256, digest, run_id };

    let wanted = std::env::var("CELLD_CONTAINER_SCENARIOS").unwrap_or_else(|_| "a,d,b,c".to_owned());
    for scenario in wanted.split(',') {
        let began = Instant::now();
        match scenario.trim() {
            "a" => scenario_a(&prep).await,
            "d" => scenario_d(&prep).await,
            "b" => scenario_b(&prep).await,
            "c" => scenario_c(&prep).await,
            other => panic!("unknown scenario {other}"),
        }
        let label = format!("celld-{}-{}", prep.run_id, scenario.trim());
        assert!(docker(&["ps", "-aq", "--filter", &format!("label=celld-t24={label}")]).await.is_empty(), "scenario containers remain");
        assert!(docker(&["network", "ls", "-q", "--filter", &format!("label=celld-t24={label}")]).await.is_empty(), "scenario networks remain");
        let prefix = format!("celld-fixture-{label}-");
        assert!(!std::fs::read_dir("/sys/fs/cgroup").unwrap().flatten().any(|entry| entry.file_name().to_string_lossy().starts_with(&prefix)), "scenario cgroups remain");
        eprintln!("T29_RESULT {}", serde_json::json!({"scenario":format!("faults::containers_{}",scenario.trim()),"status":"passed","duration_ms":began.elapsed().as_millis()}));
    }
}
