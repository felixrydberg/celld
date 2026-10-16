//! Actual Fetch/RPC descendant waitUntil ownership and fresh idle TTL after drain.
use super::freeze_baseline::OwnedVmm;
use super::guard_baseline::Group;
use super::lifecycle::{Gate, Phase, Status};
use super::lifecycle_baseline::gate;
use super::memory_baseline::{
    activate_bundle, bridge_check, config, name, no_vms, stage, Recorder,
};
use super::*;
use celld_applications::invocation_wire::HttpRequest;
use futures_util::StreamExt;
use serde_json::{json, Value};

const MIB: u64 = 1024 * 1024;
const BACKGROUND_MS: u64 = 2200;
const SOURCE: &str = r#"
const identity = Math.random().toString(36);
let unrelated = 0, worker;
const childCode = `
import {WorkerEntrypoint} from 'cloudflare:workers';
const identity = Math.random().toString(36);
const retained = new Uint8Array(8*1024*1024);
for(let i=0;i<retained.length;i+=4096) retained[i]=77;
let started=0, completed=0, pending=false;
function status() {
  let checksum=0;
  for(let i=0;i<retained.length;i+=4096) checksum+=retained[i];
  return {identity,started,completed,pending,checksum};
}
function arm(ctx) {
  if(pending) throw new Error('overlapping background wave');
  started++; pending=true;
  ctx.waitUntil(new Promise(resolve=>setTimeout(resolve,2200)).then(()=>{
    completed++; pending=false;
  }));
  return status();
}
// Register detached RPC work after the first await, in the outer native driver.
export class Api extends WorkerEntrypoint { async arm() {await Promise.resolve(); return arm(this.ctx);} }
export default {fetch(request,_env,ctx) {
  return Response.json(new URL(request.url).pathname==='/arm' ? arm(ctx) : status());
}};
`;
function child(env) {
  return worker ??= env.LOADER.get('detached-child',()=>({
    compatibilityDate:'2026-03-01',mainModule:'main.js',modules:{'main.js':childCode}
  }));
}
export default {
  async fetch(request,env) {
    const path = new URL(request.url).pathname;
    if(path==='/unrelated') return Response.json({identity,unrelated:++unrelated});
    const loaded=child(env);
    const result=path==='/rpc'
      ? await loaded.getEntrypoint('Api').arm()
      : await (await loaded.getEntrypoint().fetch('http://child'+(path==='/fetch'?'/arm':'/status'))).json();
    return Response.json({identity,child:result});
  }
};
"#;

fn guard() -> MemoryGuard {
    MemoryGuard {
        max_bytes: 14 * 1024 * MIB,
        high_bytes: 13 * 1024 * MIB,
        overhead_bytes: 256 * MIB,
        shared_cache_bytes: 256 * MIB,
        idle_cache: Some(IdleCache {
            max_bytes: 2 * 1024 * MIB,
            retained_margin_bytes: 128 * MIB,
        }),
    }
}
fn retained(pool: &VmPool, client: &VmClient) -> u64 {
    pool.state
        .lock()
        .unwrap()
        .slots
        .values()
        .find_map(|slot| match slot {
            Slot::Ready(ready) if Arc::ptr_eq(&ready.client.pin, &client.pin) => Some(
                ready
                    .retained_limit
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            _ => None,
        })
        .expect("original loader owner disappeared before stable proof")
}
fn full(pool: &VmPool, client: &VmClient, group: &Group, gate: &Gate, owned: fn() -> HashSet<u32>) {
    assert_eq!(gate.status().phase, Phase::Running);
    group.verify_member(owned).unwrap();
    assert_eq!(group.scalar("memory.max").unwrap(), (1024 + 256) * MIB);
    assert_eq!(retained(pool, client), 0);
    let snapshot = pool.snapshot();
    assert_eq!(
        (snapshot.vms, snapshot.memory_mib, snapshot.quarantined),
        (1, 1024, 0)
    );
    assert!(
        snapshot
            .physical_bytes(guard(), snapshot.memory_mib, snapshot.vms)
            .unwrap()
            <= guard().max_bytes
    );
}
async fn call(
    app: &crate::actor::AppHandle,
    client: &VmClient,
    path: &str,
    recorder: &Recorder,
) -> Value {
    let began = Instant::now();
    let response = timeout(Duration::from_secs(10), async {
        let mut response = client
            .invoke_stateless(
                app,
                Duration::from_secs(10),
                HttpRequest {
                    method: "GET".into(),
                    url: format!("http://loader{path}"),
                    headers: vec![],
                    body: vec![],
                },
                None,
            )
            .await
            .unwrap();
        if let Some(mut stream) = response.stream.take() {
            while let Some(chunk) = stream.next().await {
                response.body.extend(chunk.unwrap());
            }
        }
        assert_eq!(response.status, 200);
        assert!(response.body.len() <= 4096);
        serde_json::from_slice::<Value>(&response.body).unwrap()
    })
    .await
    .expect("original ten-second loader event deadline elapsed");
    recorder.emit(json!({"kind":"request","operation":path,"latencyUs":began.elapsed().as_micros(),"result":response}));
    response
}
#[allow(clippy::too_many_arguments)]
async fn wave(
    app: &crate::actor::AppHandle,
    pool: &VmPool,
    client: &VmClient,
    gate: &Gate,
    group: &Group,
    vm: &OwnedVmm,
    owned: fn() -> HashSet<u32>,
    recorder: &Recorder,
    path: &str,
) -> (Value, Status) {
    timeout(Duration::from_millis(500), async {
        while gate.status().active_events > 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("previous admitted event did not drain before next wave");
    let before = gate.status();
    let began_ms = crate::asyncrt::mono_ms();
    let reply = call(app, client, path, recorder).await;
    let reply_ms = crate::asyncrt::mono_ms();
    assert_eq!(reply["child"]["pending"], true);
    assert_eq!(reply["child"]["checksum"], 77 * (8 * 1024 * 1024 / 4096));
    assert!(
        reply_ms - began_ms < 1000,
        "parent response waited for child completion"
    );
    let unrelated = call(app, client, "/unrelated", recorder).await;
    let unrelated_ms = crate::asyncrt::mono_ms();
    assert_eq!(unrelated["identity"], reply["identity"]);
    assert!(
        unrelated_ms - began_ms < 1000,
        "unrelated event blocked behind background work"
    );
    let old_ttl_bound = unrelated_ms + 1000;
    let background_end_bound = began_ms + BACKGROUND_MS - 25;
    assert!(old_ttl_bound + 100 < background_end_bound);
    let mut minimum_active_events = u32::MAX;
    let mut original_drain_unchanged = true;
    timeout(Duration::from_secs(4), async {
        while crate::asyncrt::mono_ms() < old_ttl_bound + 100 {
            full(pool, client, group, gate, owned);
            let observed = gate.status();
            minimum_active_events = minimum_active_events.min(observed.active_events);
            original_drain_unchanged &= observed.last_drain_ms == before.last_drain_ms;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    full(pool, client, group, gate, owned);
    assert_eq!(vm.read_json("/", owned).await.unwrap()["state"], "Running");
    recorder.emit(json!({"kind":"loaderBackgroundProtected","operation":path,"hostMonoMs":crate::asyncrt::mono_ms(),
        "parentReplyMs":reply_ms,"unrelatedReplyMs":unrelated_ms,"oldTtlBoundMs":old_ttl_bound,
        "backgroundEndLowerBoundMs":background_end_bound,"clockToleranceMs":25,
        "minimumActiveEvents":minimum_active_events,"originalDrainUnchanged":original_drain_unchanged,"ledger":pool.status()}));
    let drain=timeout(Duration::from_secs(5),async {
        loop {
            let status=gate.status();
            assert_ne!(status.phase,Phase::Retiring,
                "descendant retired without a fresh idle window: lastDrain={} expectedAtLeast={} deadline={}",
                status.last_drain_ms,background_end_bound,status.idle_deadline_ms);
            if status.active_events==0 && (status.last_drain_ms>=background_end_bound || gate.known_idle()) {break status;}
            if status.active_events>0 {full(pool,client,group,gate,owned);}
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.expect("descendant driver did not drain");
    assert!(
        drain.last_drain_ms >= background_end_bound,
        "native child became idle without a fresh completion-based drain: {drain:?}"
    );
    assert_eq!(drain.idle_deadline_ms, drain.last_drain_ms + 1000);
    assert!(drain.last_drain_ms > before.last_drain_ms);
    let paused = timeout(Duration::from_secs(2), async {
        loop {
            let status = gate.status();
            assert_ne!(status.phase, Phase::Retiring);
            if status.phase == Phase::Cached && retained(pool, client) > 0 {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("fresh idle window never reached enforced cache");
    assert_eq!(paused.last_drain_ms, drain.last_drain_ms);
    assert_eq!(paused.idle_deadline_ms, drain.idle_deadline_ms);
    group.verify_member(owned).unwrap();
    assert_eq!(vm.read_json("/", owned).await.unwrap()["state"], "Paused");
    let limit = retained(pool, client);
    assert!(limit < 1024 * MIB);
    assert_eq!(group.scalar("memory.max").unwrap(), limit);
    assert!(group.scalar("memory.current").unwrap() <= limit);
    assert_eq!(group.scalar("memory.swap.max").unwrap(), 0);
    assert_eq!(group.scalar("memory.oom.group").unwrap(), 1);
    let now = crate::asyncrt::mono_ms();
    assert!(
        now + 500 <= paused.idle_deadline_ms,
        "fewer than500ms remained in fresh cache window"
    );
    assert_eq!(gate.status(), paused);
    assert!(
        minimum_active_events > 0,
        "parent ownership did not include detached child work"
    );
    assert!(
        original_drain_unchanged,
        "foreground reply refreshed idle time before final child completion"
    );
    recorder.emit(json!({"kind":"loaderBackgroundFreshWindow","operation":path,"hostMonoMs":now,
        "pid":group.member.unwrap().0,"startTicks":group.member.unwrap().1,
        "lastDrainMs":paused.last_drain_ms,"idleDeadlineMs":paused.idle_deadline_ms,
        "remainingWindowMs":paused.idle_deadline_ms-now,"retainedLimitBytes":limit,"ledger":pool.status()}));
    (reply, paused)
}

pub(super) async fn run(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    _directory: &Path,
    owned: fn() -> HashSet<u32>,
) {
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_LIFECYCLE_CHECK_ONLY").is_some());
    assert!(owned().is_empty());
    let output = PathBuf::from(std::env::var_os("CELLD_APPLICATION_MEMORY_OUTPUT").unwrap());
    let recorder = Recorder::new(&output, "lifecycle-loader-background");
    recorder.emit(json!({"kind":"provenance","kernelSha256":options.kernel.sha256,"runtimeSha256":options.runtime.sha256,
        "firecrackerSha256":content_digest(&std::fs::read(&options.firecracker).unwrap()),
        "sourceSha256":content_digest(SOURCE.as_bytes()),"guard":guard(),"startupPool":0,
        "idleTtlSeconds":1,"childWaitUntilMs":BACKGROUND_MS,"operations":["fetch","rpc"],"originalEventDeadlineMs":10000}));
    let mut configuration = config(options, 0, 1);
    configuration.max_vms = 1;
    configuration.memory_guard = Some(guard());
    let pool = Arc::new(VmPool::new(configuration, store.clone()).unwrap());
    let parent = Group::registered_parent().unwrap();
    let (stop, sampler) = recorder.start_sampler(&pool, owned);
    pool.prepare(app).await.unwrap();
    no_vms(&pool, owned).await;
    let application = stage(
        store,
        options,
        "loader-background",
        ApplicationSizeClass::Basic,
    )
    .await;
    let loader = ResourceRef {
        kind: ResourceKind::WorkerLoader,
        id: name("loader"),
    };
    let mut catalog = store.read(&application).await.unwrap();
    catalog
        .register_resource(
            &application,
            crate::applications::Resource {
                resource: loader.clone(),
                configuration: json!({}),
            },
        )
        .unwrap();
    store.commit(catalog).await.unwrap();
    let artifact =
        crate::application_image::build(&[("main.mjs".into(), SOURCE.as_bytes().to_vec())])
            .unwrap();
    activate_bundle(
        store,
        options,
        &application,
        "background",
        artifact,
        (
            "main.mjs",
            json!({"compatibility_date":"2026-03-01","compatibility_flags":["js_rpc"],
            "worker_loaders":[{"binding":"LOADER"}]}),
            json!({"LOADER":{"kind":"resource","resource":loader}}),
        ),
    )
    .await;
    let lease = pool
        .acquire(app, &application, &name("background"), &name("memory"))
        .await
        .unwrap();
    let client = lease.client.clone();
    let gate = gate(&pool, &client);
    let vm = OwnedVmm::identify(owned);
    let pid = *owned().iter().next().unwrap();
    let group = Group::leaf(pid, &parent, owned).unwrap().unwrap();
    // Holding a host lease would conceal incorrect idle retirement in this qualification.
    drop(lease);
    let (fetch_reply, fetch_window) = wave(
        app, &pool, &client, &gate, &group, &vm, owned, &recorder, "/fetch",
    )
    .await;
    assert_eq!(
        (
            fetch_reply["child"]["started"].as_u64(),
            fetch_reply["child"]["completed"].as_u64()
        ),
        (Some(1), Some(0))
    );
    let warm = call(app, &client, "/status", &recorder).await;
    assert_eq!(warm["identity"], fetch_reply["identity"]);
    assert_eq!(warm["child"]["identity"], fetch_reply["child"]["identity"]);
    assert_eq!(warm["child"]["completed"], 1);
    assert_eq!(warm["child"]["pending"], false);
    assert_eq!(warm["child"]["checksum"], fetch_reply["child"]["checksum"]);
    assert!(crate::asyncrt::mono_ms() < fetch_window.idle_deadline_ms);
    let (rpc_reply, final_window) = wave(
        app, &pool, &client, &gate, &group, &vm, owned, &recorder, "/rpc",
    )
    .await;
    assert_eq!(rpc_reply["identity"], fetch_reply["identity"]);
    assert_eq!(
        rpc_reply["child"]["identity"],
        fetch_reply["child"]["identity"]
    );
    assert_eq!(
        (
            rpc_reply["child"]["started"].as_u64(),
            rpc_reply["child"]["completed"].as_u64()
        ),
        (Some(2), Some(1))
    );
    recorder.context(
        "lifecycle-loader-background-idle-expiry",
        Some(ApplicationSizeClass::Basic),
        0,
    );
    timeout(Duration::from_secs(4), async {
        while owned().contains(&pid) {
            let observed = gate.status();
            let now = crate::asyncrt::mono_ms();
            assert_eq!(observed.last_drain_ms, final_window.last_drain_ms);
            assert_eq!(observed.idle_deadline_ms, final_window.idle_deadline_ms);
            if now < final_window.idle_deadline_ms {
                assert_eq!(observed.phase, Phase::Cached);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("loader owner did not expire/reap");
    let reaped_ms = crate::asyncrt::mono_ms();
    assert!(reaped_ms >= final_window.last_drain_ms + 1000);
    no_vms(&pool, owned).await;
    assert!(!group.exists().unwrap());
    let memory = pool.templates.memory_sample().unwrap().unwrap();
    assert_eq!(
        (memory.oom, memory.oom_kill, memory.oom_group_kill),
        (0, 0, 0)
    );
    recorder.emit(json!({"kind":"loaderBackgroundExpiry","pid":pid,"startTicks":group.member.unwrap().1,
        "lastDrainMs":final_window.last_drain_ms,"idleDeadlineMs":final_window.idle_deadline_ms,
        "reapObservedMs":reaped_ms,"expiryToObservedReapMs":reaped_ms-final_window.idle_deadline_ms,"ledger":pool.status()}));
    stop.cancel();
    sampler.await.unwrap();
    pool.shutdown().await.unwrap();
    assert!(!parent.exists().unwrap());
    assert!(recorder
        .groups
        .lock()
        .unwrap()
        .iter()
        .all(|path| !path.exists()));
    assert_eq!(
        (
            pool.snapshot().vms,
            pool.snapshot().memory_mib,
            pool.snapshot().retained_limit_bytes
        ),
        (0, 0, 0)
    );
    recorder.emit(json!({"kind":"cleanup","ownedPids":owned(),"remainingCgroups":[]}));
    eprintln!(
        "T29_RESULT {}",
        json!({"scenario":"t33::loader_background","status":"passed",
        "duration_ms":recorder.began.elapsed().as_millis(),"output":output,
        "scope":"one original Basic owner; Fetch and async-after-await RPC preserve early replies and a fresh TTL after descendant drain; synchronous plain RPC joins waitUntil before reply; nested grandchildren not exercised"})
    );
}
#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires updated descendant-drain guest, registered fresh run, Linux KVM and diagnostic image"]
async fn t33_loader_background() {
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_LOADER_BACKGROUND_CHECK_ONLY").is_some());
    bridge_check("memory-loader-background").await;
}
