//! Real node queue/alarm/workflow wakes after physical retained caching.
use super::*;
use nix::fcntl::{open, openat, OFlag};
use nix::sys::stat::Mode;
use std::fs::File;
use std::io::Read;
use std::os::unix::fs::MetadataExt;

const MIB: u64 = 1024 * 1024;
const HOLD_MS: u64 = 1000;
const DELAY_SECONDS: u64 = 8;
const WRAPPER: &str = r#"
const cacheIdentity = `${Date.now()}:${Math.random()}`;
const cacheEvents = [];
const hold = () => new Promise(resolve => setTimeout(resolve, 1000));
const originalArm = Counter.prototype.arm;
Counter.prototype.arm = async function(delay, failOnce) {
  const result = await originalArm.call(this, delay, failOnce);
  cacheEvents.push({kind:'alarm-armed', at:Date.now(), due:await this.ctx.storage.getAlarm()});
  return result;
};
const originalAlarm = Counter.prototype.alarm;
Counter.prototype.alarm = async function(event) {
  cacheEvents.push({kind:'alarm', identity:cacheIdentity, at:Date.now()});
  await hold();
  return originalAlarm.call(this, event);
};
const originalRun = Flow.prototype.run;
Flow.prototype.run = async function(event, step) {
  cacheEvents.push({kind:'workflow', identity:cacheIdentity, at:Date.now()});
  await hold();
  return originalRun.call(this, event, step);
};
export default {
  async fetch(request, environment, ctx) {
    if (new URL(request.url).pathname === '/cache-wake-proof')
      return Response.json({identity:cacheIdentity, events:cacheEvents});
    return original.fetch(request, environment, ctx);
  },
  async queue(batch, environment, ctx) {
    cacheEvents.push({kind:'queue', identity:cacheIdentity, at:Date.now()});
    await hold();
    return original.queue(batch, environment, ctx);
  }
};
"#;

fn binding(kind: &str, id: &str) -> Value {
    json!({"kind":"resource","resource":{"kind":kind,"id":id}})
}
async fn provision(h: &Harness) -> String {
    let app = "e2e-one";
    h.create(app).await;
    let limits = h.json_file(
        "cache-limits.json",
        &json!({"size_class":"basic","invocation_timeout_ms":10000}),
    );
    h.cli(app, "limits", &["--file", limits.to_str().unwrap()])
        .await;
    for (kind, id) in [
        ("durable_object", "counter"),
        ("d1", "db"),
        ("queue", "jobs"),
        ("workflow", "flow"),
    ] {
        h.resource(app, kind, id, json!({})).await;
    }
    assert_eq!(NODE_SOURCE.matches("\nexport default {").count(), 1);
    assert_eq!(
        NODE_SOURCE
            .matches("await step.sleep('replay', 100);")
            .count(),
        1
    );
    // A discarded streamed response remains live work. Drain the two counter
    // responses before the workflow reaches its durable suspension boundary.
    let drained_calls = [
        (
            "await this.env.COUNTER.getByName(prefix + '-once').fetch('http://counter/');",
            "await (await this.env.COUNTER.getByName(prefix + '-once').fetch('http://counter/')).text();",
        ),
        (
            "await counter.fetch('http://counter/');",
            "await (await counter.fetch('http://counter/')).text();",
        ),
    ];
    for (before, _) in drained_calls {
        assert_eq!(NODE_SOURCE.matches(before).count(), 1);
    }
    let mut source = NODE_SOURCE
        .replacen("\nexport default {", "\nconst original = {", 1)
        .replacen(
            "await step.sleep('replay', 100);",
            "await step.sleep('replay', 8000);",
            1,
        )
        + WRAPPER;
    for (before, after) in drained_calls {
        source = source.replacen(before, after, 1);
    }
    let artifact = h.source(app, &source).await;
    h.stage(app,&json!({"manifest_version":1,"application":{"app":app},"id":"one",
        "handlers":[{"id":"javascript","language":"javascript","entrypoint":"main.mjs",
            "artifact_sha256":artifact,"runtime_image_sha256":h.runtime_sha256,"http":true,
            "environment":{"LABEL":"cache-wake"},"bindings":{
                "COUNTER":binding("durable_object","counter"),"DB":binding("d1","db"),
                "JOBS":binding("queue","jobs"),"FLOW":binding("workflow","flow")},
            "worker_config":{"compatibility_date":"2026-03-01","compatibility_flags":["js_rpc"],
                "do_classes":["Counter"],"durable_objects":{"bindings":[{"name":"COUNTER","class_name":"Counter"}]},
                "queue_consumers":[{"queue":"jobs","max_batch_size":1,"max_batch_timeout":1,"max_retries":0}],
                "workflows":[{"binding":"FLOW","name":"flow","class_name":"Flow"}]}}]})).await;
    h.activate(app, "one").await;
    content_digest(source.as_bytes())
}

struct Record {
    output: File,
    began: Instant,
}
impl Record {
    fn new(h: &Harness) -> Self {
        Self {
            output: std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(h.directory.join("cache-wakes.jsonl"))
                .unwrap(),
            began: Instant::now(),
        }
    }
    fn emit(&mut self, mut value: Value) {
        value["elapsedMs"] = json!(self.began.elapsed().as_millis());
        value["hostWallMs"] = json!(celld::ownership_store::now_ms());
        writeln!(self.output, "{value}").unwrap();
        self.output.flush().unwrap();
    }
}
async fn pool(node: &Node) -> Value {
    let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{}/state", node.internal))
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let state: Value = response.json().await.unwrap();
    state["applicationVms"].clone()
}
fn start_ticks(pid: u32) -> u64 {
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .unwrap()
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .nth(19)
        .unwrap()
        .parse()
        .unwrap()
}
fn descriptor(parent: &File, component: &str) -> File {
    assert!(
        !component.is_empty() && component != "." && component != ".." && !component.contains('/')
    );
    File::from(
        openat(
            parent,
            component,
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
            Mode::empty(),
        )
        .unwrap(),
    )
}
pub(super) struct OwnedVm {
    pid: u32,
    ticks: u64,
    parent: File,
    leaf: File,
    name: String,
    device: u64,
    inode: u64,
    membership: String,
    grandparent: File,
    parent_name: String,
    parent_device: u64,
    parent_inode: u64,
}

pub(super) struct ParentHighOverride {
    parent: File,
    device: u64,
    inode: u64,
    original: u64,
    max: u64,
    active: bool,
}

impl ParentHighOverride {
    fn text(&self, field: &str) -> String {
        assert!(!field.contains('/'));
        let file = File::from(
            openat(
                &self.parent,
                field,
                OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
                Mode::empty(),
            )
            .unwrap(),
        );
        let mut text = String::new();
        file.take(4097).read_to_string(&mut text).unwrap();
        assert!(text.len() <= 4096);
        text
    }

    pub(super) fn high_events(&self) -> u64 {
        self.text("memory.events")
            .lines()
            .find_map(|line| {
                line.strip_prefix("high ")
                    .and_then(|value| value.parse().ok())
            })
            .expect("owned parent high counter missing")
    }

    fn write(&self, value: u64) {
        let metadata = self.parent.metadata().unwrap();
        assert_eq!((metadata.dev(), metadata.ino()), (self.device, self.inode));
        assert!(value > 0 && value <= self.max);
        let mut file = File::from(
            openat(
                &self.parent,
                "memory.high",
                OFlag::O_WRONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC | OFlag::O_NONBLOCK,
                Mode::empty(),
            )
            .unwrap(),
        );
        use std::io::Write;
        file.write_all(value.to_string().as_bytes()).unwrap();
    }

    pub(super) fn restore(&mut self) {
        if self.active {
            self.write(self.original);
            self.active = false;
        }
        assert_eq!(
            self.text("memory.max").trim().parse::<u64>().unwrap(),
            self.max
        );
    }
}

impl Drop for ParentHighOverride {
    fn drop(&mut self) {
        if self.active {
            self.restore();
        }
    }
}

impl OwnedVm {
    pub(super) fn new(node: &Node) -> Self {
        let pids = node.vm_pids();
        assert_eq!(
            pids.len(),
            1,
            "focused node must have one assigned VM and no startup VMs"
        );
        let pid = pids[0];
        let ticks = start_ticks(pid);
        let registry = PathBuf::from(
            std::env::var_os("CELLD_APPLICATION_NODE_ARTIFACTS")
                .expect("trusted node reporter artifacts required"),
        )
        .join("processes.jsonl");
        assert!(
            std::fs::read_to_string(registry)
                .unwrap()
                .lines()
                .any(|line| {
                    let record: Value = serde_json::from_str(line).unwrap();
                    record["pid"] == pid && record["start_ticks"] == ticks
                }),
            "owned VM was not recorded by harness reporter"
        );
        let membership = std::fs::read_to_string(format!("/proc/{pid}/cgroup")).unwrap();
        let path = membership
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .unwrap();
        let components: Vec<_> = path.trim_start_matches('/').split('/').collect();
        assert!(components.len() >= 2);
        assert!(components[components.len() - 2].starts_with("celld-vms-"));
        let mut directory = File::from(
            open(
                "/sys/fs/cgroup",
                OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
                Mode::empty(),
            )
            .unwrap(),
        );
        let mut grandparent = directory.try_clone().unwrap();
        for (index, component) in components[..components.len() - 1].iter().enumerate() {
            if index == components.len() - 2 {
                grandparent = directory.try_clone().unwrap();
            }
            directory = descriptor(&directory, component);
        }
        let name = components.last().unwrap().to_string();
        let parent_metadata = directory.metadata().unwrap();
        let leaf = descriptor(&directory, &name);
        let metadata = leaf.metadata().unwrap();
        let owned = Self {
            pid,
            ticks,
            parent: directory,
            leaf,
            name,
            device: metadata.dev(),
            inode: metadata.ino(),
            membership: path.to_owned(),
            grandparent,
            parent_name: components[components.len() - 2].to_owned(),
            parent_device: parent_metadata.dev(),
            parent_inode: parent_metadata.ino(),
        };
        owned.verify(node);
        owned
    }
    pub(super) fn verify(&self, node: &Node) {
        assert_eq!(node.vm_pids(), vec![self.pid], "assigned VM changed");
        assert_eq!(start_ticks(self.pid), self.ticks, "PID identity changed");
        let current = descriptor(&self.parent, &self.name).metadata().unwrap();
        assert_eq!((current.dev(), current.ino()), (self.device, self.inode));
        let membership = std::fs::read_to_string(format!("/proc/{}/cgroup", self.pid)).unwrap();
        let current = membership
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .unwrap();
        assert_eq!(current, self.membership, "owned membership changed");
        assert!(self
            .text("cgroup.procs")
            .lines()
            .any(|pid| pid == self.pid.to_string()));
    }
    fn text(&self, field: &str) -> String {
        assert!(!field.contains('/'));
        let file = File::from(
            openat(
                &self.leaf,
                field,
                OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
                Mode::empty(),
            )
            .unwrap(),
        );
        let mut text = String::new();
        file.take(4097).read_to_string(&mut text).unwrap();
        assert!(text.len() <= 4096);
        text
    }
    pub(super) fn scalar(&self, field: &str) -> u64 {
        self.text(field).trim().parse().unwrap()
    }

    pub(super) fn lower_parent_high(&self) -> (ParentHighOverride, u64) {
        let parent_metadata = self.parent.metadata().unwrap();
        assert_eq!(
            (parent_metadata.dev(), parent_metadata.ino()),
            (self.parent_device, self.parent_inode)
        );
        let mut override_ = ParentHighOverride {
            parent: self.parent.try_clone().unwrap(),
            device: self.parent_device,
            inode: self.parent_inode,
            original: 0,
            max: 0,
            active: false,
        };
        override_.original = override_.text("memory.high").trim().parse().unwrap();
        override_.max = override_.text("memory.max").trim().parse().unwrap();
        let before = override_.high_events();
        let current: u64 = override_.text("memory.current").trim().parse().unwrap();
        override_.write((current / 2).max(1));
        override_.active = true;
        (override_, before)
    }
    async fn instance(&self, node: &Node) -> Value {
        use http_body_util::{BodyExt, Full, Limited};
        use hyper_util::rt::TokioIo;
        self.verify(node);
        timeout(Duration::from_secs(2), async {
            let stream =
                tokio::net::UnixStream::connect(format!("/proc/{}/root/api.sock", self.pid))
                    .await
                    .unwrap();
            let (mut sender, connection) =
                hyper::client::conn::http1::handshake(TokioIo::new(stream))
                    .await
                    .unwrap();
            let connection = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(connection));
            let request = hyper::Request::builder()
                .uri("/")
                .header("host", "firecracker")
                .body(Full::new(bytes::Bytes::new()))
                .unwrap();
            let response = sender.send_request(request).await.unwrap();
            assert_eq!(response.status(), 200);
            let body = Limited::new(response.into_body(), 4096)
                .collect()
                .await
                .unwrap()
                .to_bytes();
            drop(connection);
            self.verify(node);
            serde_json::from_slice(&body).unwrap()
        })
        .await
        .expect("owned node VM API did not respond")
    }
    fn removed(&self) {
        assert!(
            matches!(
                openat(
                    &self.grandparent,
                    self.parent_name.as_str(),
                    OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
                    Mode::empty()
                ),
                Err(nix::errno::Errno::ENOENT)
            ),
            "stable guard parent outlived node drain"
        );
        assert!(!Path::new(&format!("/proc/{}", self.pid)).exists());
        assert!(matches!(
            openat(
                &self.parent,
                self.name.as_str(),
                OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
                Mode::empty()
            ),
            Err(nix::errno::Errno::ENOENT)
        ));
    }
}
fn charge(state: &Value) {
    assert_eq!(state["vms"], 1);
    assert_eq!(state["memoryMib"], 1024, "nominal RAM changed");
    assert_eq!(state["quarantined"], 0);
    let reservation = &state["memoryReservations"];
    let retained = reservation["retainedLimitBytes"].as_u64().unwrap();
    let cached = reservation["retainedVms"].as_u64().unwrap();
    assert_eq!(reservation["retainedOverheadBytes"], cached * 256 * MIB);
    let full = reservation["fullExecutionMib"].as_u64().unwrap();
    assert_eq!(
        reservation["physicalBytes"],
        full * MIB + retained + 512 * MIB
    );
    assert!(full * MIB + retained + 512 * MIB <= 14 * 1024 * MIB);
    if state["lifecycle"]["running"] == 1 || state["lifecycle"]["resuming"] == 1 {
        assert_eq!(
            (full, retained, cached),
            (1024, 0, 0),
            "native wake bypassed full reservation"
        );
    }
}
async fn cached(node: &Node, vm: &OwnedVm, record: &mut Record, event: &str, scheduled: Instant) {
    timeout(Duration::from_secs(6),async {
        loop {
            let state = pool(node).await;
            charge(&state);
            if state["lifecycle"]["cached"] == 1 && state["memoryReservations"]["retainedVms"] == 1 {
                vm.verify(node);
                let limit = vm.scalar("memory.max");
                let instance = vm.instance(node).await;
                let after = pool(node).await;
                if after["lifecycle"]["cached"] != 1 { continue; }
                assert_eq!(instance["state"],"Paused");
                assert_eq!(after["memoryReservations"]["retainedLimitBytes"],limit);
                assert!(limit < 1024*MIB);
                assert_eq!(vm.scalar("memory.swap.max"),0);
                assert_eq!(vm.scalar("memory.oom.group"),1);
                assert!(scheduled.elapsed() < Duration::from_secs(DELAY_SECONDS),"cached barrier occurred after due time");
                record.emit(json!({"kind":"nativeWakeCachedBarrier","event":event,"pid":vm.pid,"startTicks":vm.ticks,
                    "scheduledElapsedMs":scheduled.elapsed().as_millis(),"ownedLeafLimitBytes":limit,
                    "currentBytes":vm.scalar("memory.current"),"peakBytes":vm.scalar("memory.peak"),
                    "events":vm.text("memory.events.local"),"psi":vm.text("memory.pressure"),"pool":after}));
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap_or_else(|_|panic!("{event} handler did not become physically Cached before due time"));
}
async fn native_wake(
    node: &Node,
    vm: &OwnedVm,
    record: &mut Record,
    event: &str,
    scheduled: Instant,
) {
    timeout(Duration::from_secs(20),async {
        loop {
            let state = pool(node).await;
            charge(&state);
            if state["lifecycle"]["running"] == 1 {
                vm.verify(node);
                let limit = vm.scalar("memory.max");
                let after = pool(node).await;
                if after["lifecycle"]["running"] != 1 { continue; }
                charge(&after);
                assert!(scheduled.elapsed() >= Duration::from_millis(7500), "native wake preceded delayed event: {event}, elapsed={:?}", scheduled.elapsed());
                assert_eq!(limit,1280*MIB,"native event ran without full leaf ceiling");
                record.emit(json!({"kind":"nativeWakeFullAdmission","event":event,"pid":vm.pid,"startTicks":vm.ticks,
                    "ownedLeafLimitBytes":limit,"scheduledElapsedMs":scheduled.elapsed().as_millis(),"pool":after}));
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap_or_else(|_|panic!("real {event} did not wake assigned cached VM"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux node, native queue/alarm/workflow wakers and retained-cache runtime"]
async fn t33_native_cache_wakes() {
    celld::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    let began = Instant::now();
    timeout(Duration::from_secs(600),async {
        let h = Harness::new("cache-wakes",true).await;
        let mut configuration: Value = serde_json::from_slice(&std::fs::read(h.directory.join("vms.json")).unwrap()).unwrap();
        configuration["memory_guard"] = json!({"max_bytes":14*1024*MIB,"high_bytes":13*1024*MIB,
            "overhead_bytes":256*MIB,"shared_cache_bytes":256*MIB,
            "idle_cache":{"max_bytes":4*1024*MIB,"retained_margin_bytes":128*MIB}});
        std::fs::write(h.directory.join("vms.json"),serde_json::to_vec(&configuration).unwrap()).unwrap();
        let source_hash = provision(&h).await;
        let host = h.hostname("e2e-one","javascript").await;
        let node = h.start("cache-wakes").await;
        let mut record = Record::new(&h);
        record.emit(json!({"kind":"provenance","sourceSha256":source_hash,"originalSourceSha256":content_digest(NODE_SOURCE.as_bytes()),
            "runtimeSha256":h.runtime_sha256,"kernelSha256":configuration["kernel"]["sha256"],
            "config":configuration,"callbackHoldMs":HOLD_MS,"delaySeconds":DELAY_SECONDS,
            "scope":"real node native queue/alarm/workflow wake; TTL600 bounded scope, exact original deadline qualified separately",
            "stressOnlyCacheInputs":true}));
        assert_eq!(get(&node,&host,"/q-setup").await,"\"ready\"");
        let original = json(&node,&host,"/cache-wake-proof").await;
        let vm = OwnedVm::new(&node);

        let scheduled = Instant::now();
        assert_eq!(get(&node,&host,"/q-backlog?label=cache-wake&kind=ok&ns=73&delay=8").await,"\"sent\"");
        cached(&node,&vm,&mut record,"queue",scheduled).await;
        native_wake(&node,&vm,&mut record,"queue",scheduled).await;
        tokio::time::sleep(Duration::from_millis(HOLD_MS+1500)).await;
        let rows = json(&node,&host,"/q-log").await;
        assert_eq!(rows.as_array().unwrap().len(),1,"duplicate delayed queue effects");
        let row = &rows[0];
        assert_eq!((&row[0],&row[1],&row[2],&row[6]),(&json!("jobs"),&json!(73),&json!(1),&json!("cache-wake")));
        assert!(row[4].as_u64().unwrap()-row[5].as_u64().unwrap() >= 7500,"queue delivered before native delay");
        let proof = json(&node,&host,"/cache-wake-proof").await;
        assert_eq!(proof["identity"],original["identity"]);
        assert_eq!(proof["events"].as_array().unwrap().iter().filter(|entry|entry["kind"] == "queue").count(),1);
        record.emit(json!({"kind":"queueSettledOnce","rows":rows,"proof":proof}));

        let scheduled = Instant::now();
        record.emit(json!({"kind":"nativeWakeScheduled","event":"alarm"}));
        assert_eq!(get(&node,&host,"/alarm-arm?name=cache-clock&delay=8000").await,"armed");
        cached(&node,&vm,&mut record,"alarm",scheduled).await;
        native_wake(&node,&vm,&mut record,"alarm",scheduled).await;
        tokio::time::sleep(Duration::from_millis(HOLD_MS+1500)).await;
        let alarm = json(&node,&host,"/alarm-state?name=cache-clock").await;
        assert_eq!(alarm,json!({"attempts":1,"hidden":true,"retryCount":0,"isRetry":false,"alarm":null}));
        let proof = json(&node,&host,"/cache-wake-proof").await;
        assert_eq!(proof["identity"],original["identity"]);
        assert_eq!(proof["events"].as_array().unwrap().iter().filter(|entry|entry["kind"] == "alarm").count(),1);
        let events = proof["events"].as_array().unwrap();
        let armed = events.iter().find(|entry| entry["kind"] == "alarm-armed").unwrap();
        let fired = events.iter().find(|entry| entry["kind"] == "alarm").unwrap();
        assert!(armed["due"].as_u64().unwrap() >= armed["at"].as_u64().unwrap() + 7500,
            "guest alarm was not armed with the requested delay: {armed}");
        assert!(fired["at"].as_u64().unwrap() + 1000 >= armed["due"].as_u64().unwrap(),
            "guest callback clock preceded its absolute due by more than the restore clock tolerance");
        record.emit(json!({"kind":"nativeAlarmOnce","alarm":alarm,"proof":proof}));

        let scheduled = Instant::now();
        assert_eq!(json(&node,&host,"/wf-create?id=cache-flow").await,json!({"id":"cache-flow"}));
        // The first run is immediate; qualify caching after its durable sleep, not before that run.
        let first_step = timeout(Duration::from_secs(6), async {
            loop {
                let counts = json(&node,&host,"/wf-counts?id=cache-flow").await;
                if counts["first"] == 1 { break counts; }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }).await.expect("workflow did not reach its first durable step");
        assert_eq!(first_step, json!({"first":1,"retry":0,"after":0}));
        record.emit(json!({"kind":"workflowInitialStep","counts":first_step,
            "scheduledElapsedMs":scheduled.elapsed().as_millis()}));
        // Real workflow suspension persists an eight-second sleep; internal status never invokes the guest.
        cached(&node,&vm,&mut record,"workflow",scheduled).await;
        native_wake(&node,&vm,&mut record,"workflow",scheduled).await;
        tokio::time::sleep(Duration::from_millis(HOLD_MS+2000)).await;
        let flow = timeout(Duration::from_secs(30),async {
            loop {
                let status = json(&node,&host,"/wf-status?id=cache-flow").await;
                assert!(status.get("error").is_none(),"workflow error: {status}");
                assert!(!matches!(status["status"].as_str(),Some("errored"|"terminated")),"workflow failed: {status}");
                if status["status"] == "complete" { break status; }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }).await.expect("cached workflow did not finish");
        assert_eq!(flow["output"],json!({"first":1,"attempts":2,"label":"cache-wake"}));
        let counts = json(&node,&host,"/wf-counts?id=cache-flow").await;
        assert_eq!(counts,json!({"first":1,"retry":2,"after":0}));
        let proof = json(&node,&host,"/cache-wake-proof").await;
        assert_eq!(proof["identity"],original["identity"]);
        assert!(proof["events"].as_array().unwrap().iter().filter(|entry|entry["kind"] == "workflow").count() >= 2,
            "workflow sleep did not replay through native wake");
        record.emit(json!({"kind":"workflowDurableEffects","status":flow,"counts":counts,"proof":proof}));

        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(json(&node,&host,"/q-log").await,rows,"settled queue redelivered");
        assert_eq!(json(&node,&host,"/alarm-state?name=cache-clock").await,alarm,"alarm repeated");
        cached(&node,&vm,&mut record,"shutdown",Instant::now()).await;
        vm.verify(&node);
        node.drain().await;
        vm.removed();
        record.emit(json!({"kind":"cleanup","pid":vm.pid,"startTicks":vm.ticks,"ownedPidLeafAndStableParentRemoved":true,"shutdownFromCached":true,
            "scope":"node drain plus root reporter identity/cgroup cleanup; no universal tenant or TTL-clock claim"}));
    }).await.expect("focused native cache-wake selector exceeded 600 seconds");
    passed("cache_wakes::t33_native_cache_wakes", began);
}
