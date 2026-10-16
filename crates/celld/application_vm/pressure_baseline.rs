//! Real parent throttling and automatic production pressure eviction/recovery.
use super::guard_baseline::Group;
use super::memory_baseline::{
    activate_bundle, bridge_check, config, name, request, stage, Recorder,
};
use super::*;
use anyhow::ensure;
use celld_applications::invocation_wire::HttpRequest;
use futures_util::StreamExt;
use nix::fcntl::{openat, OFlag};
use nix::sys::stat::Mode;
use serde_json::{json, Value};
use std::fs::File;
use std::io::Write;

const MIB: u64 = 1024 * 1024;
const POLL: Duration = Duration::from_secs(1); // config idle=120 -> clamped production poll
const WORKER: &str = include_str!("../tests/fixtures/t33-memory/worker.mjs");
const WRAPPER: &str = r#"
export default {async fetch(request, env, ctx) {
  if (new URL(request.url).pathname !== '/pressure') return original.fetch(request, env, ctx);
  const began = Date.now();
  const retained = [];
  let pagesTouched = 0;
  while (Date.now() - began < 6000) {
    const bytes = new Uint8Array(8 * 1024 * 1024);
    for (let i = 0; i < bytes.length; i += 4096) { bytes[i] = 17; pagesTouched++; }
    retained.push(bytes);
    if (retained.length > 16) retained.shift();
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  return Response.json({pagesTouched});
}};
"#;

struct HighOverride {
    parent: Arc<Group>,
    original: u64,
    active: bool,
}
impl HighOverride {
    fn write(&self, value: u64) -> Result<()> {
        ensure!(
            self.parent.member.is_none() && self.parent.exists()?,
            "owned parent unavailable"
        );
        ensure!(
            value > 0 && value <= self.parent.scalar("memory.max")?,
            "invalid fixture parent high"
        );
        let mut file = File::from(openat(
            &self.parent.directory,
            "memory.high",
            OFlag::O_WRONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC | OFlag::O_NONBLOCK,
            Mode::empty(),
        )?);
        file.write_all(value.to_string().as_bytes())?;
        Ok(())
    }
    fn restore(&mut self) -> Result<()> {
        if self.active {
            self.write(self.original)?;
            self.active = false;
        }
        Ok(())
    }
}
impl Drop for HighOverride {
    fn drop(&mut self) {
        if let Err(error) = self.restore() {
            eprintln!("pressure fixture high restore failed: {error}");
        }
    }
}

async fn until(mut condition: impl FnMut() -> bool, label: &str) {
    timeout(Duration::from_secs(30), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("pressure fixture timed out: {label}"));
}

fn slot(pool: &VmPool, application: &ApplicationId) -> (bool, bool, bool) {
    let state = pool.state.lock().unwrap();
    match state
        .slots
        .iter()
        .find(|(key, _)| &key.0 == application)
        .map(|(_, slot)| slot)
    {
        Some(Slot::Ready(ready)) => (true, ready.retire.is_cancelled(), ready.idle()),
        Some(Slot::Starting(_)) => panic!("unexpected existing application startup"),
        None => (false, false, false),
    }
}

fn charged(pool: &VmPool) -> (u32, u64) {
    let state = pool.state.lock().unwrap();
    let idle = pool.templates.idle_capacity();
    (
        state.snapshot.vms + idle.0,
        state.snapshot.memory_mib + idle.1,
    )
}

fn signature(sample: crate::application_template::cgroup::MemorySample) -> [u64; 7] {
    [
        sample.high,
        sample.max,
        sample.oom,
        sample.oom_kill,
        sample.oom_group_kill,
        sample.some_total_us,
        sample.full_total_us,
    ]
}

struct Observations {
    stop: CancellationToken,
    task: AbortOnDropHandle<()>,
    samples: Arc<Mutex<Vec<Value>>>,
}
impl Observations {
    fn new(
        pool: Arc<VmPool>,
        parent: Arc<Group>,
        recorder: Arc<Recorder>,
        old: ApplicationId,
        newer: ApplicationId,
        busy: ApplicationId,
        owned: fn() -> HashSet<u32>,
    ) -> Self {
        let stop = CancellationToken::new();
        let stopping = stop.clone();
        let samples = Arc::new(Mutex::new(Vec::<Value>::new()));
        let output = samples.clone();
        let task = AbortOnDropHandle::new(tokio::spawn(async move {
            while !stopping.is_cancelled() {
                let before = charged(&pool);
                let before_pids = owned();
                let mut physical = Vec::new();
                for pid in &before_pids {
                    if let Ok(Some(group)) = Group::leaf(*pid, &parent, owned) {
                        if let Ok(max) = group.scalar("memory.max") {
                            assert!(
                                max >= 256 * MIB && max % MIB == 0,
                                "invalid observed VM max"
                            );
                            physical.push((group, max / MIB - 256));
                        }
                    }
                }
                let after = charged(&pool);
                let after_pids = owned();
                let physical_complete = before == after
                    && before_pids == after_pids
                    && physical.len() == after_pids.len()
                    && physical
                        .iter()
                        .all(|(group, _)| group.verify_member(owned).is_ok());
                if physical_complete {
                    let guest: u64 = physical.iter().map(|(_, guest)| *guest).sum();
                    assert!(
                        after.0 as usize >= physical.len() && after.1 >= guest,
                        "nominal VM charge released before physical reap"
                    );
                }
                let old_state = slot(&pool, &old);
                let new_state = slot(&pool, &newer);
                let busy_state = slot(&pool, &busy);
                assert!(
                    busy_state.0 && !busy_state.1,
                    "busy handler was selected under pressure"
                );
                if old_state.1 || new_state.1 {
                    assert_eq!(
                        pool.templates.winding_down().0,
                        0,
                        "assigned VM selected before startup-pool reap completed"
                    );
                }
                assert!(
                    !(new_state.1 && old_state.0 && !old_state.1),
                    "newer assigned idle VM selected before older LRU"
                );
                let sample = pool.templates.memory_sample().unwrap().unwrap();
                let mut recorded_pids: Vec<_> = after_pids.iter().copied().collect();
                recorded_pids.sort_unstable();
                let value = json!({"kind":"pressureObservation","hostMonoMs":crate::asyncrt::mono_ms(),
                    "blocked":VmPool::under_pressure(&pool.pressure),"kernel":sample,
                    "ledgerVms":after.0,"ledgerGuestMib":after.1,"ownedPids":recorded_pids,"physicalComplete":physical_complete,
                    "pool":pool.templates.status(),"old":old_state,"newer":new_state,"busy":busy_state});
                let changed = {
                    let mut records = output.lock().unwrap();
                    assert!(records.len() < 8192, "unbounded pressure observations");
                    let differs = records.last().is_none_or(|previous| {
                        previous["blocked"] != value["blocked"]
                            || previous["old"] != value["old"]
                            || previous["newer"] != value["newer"]
                            || previous["ownedPids"] != value["ownedPids"]
                            || previous["ledgerVms"] != value["ledgerVms"]
                            || previous["physicalComplete"] != value["physicalComplete"]
                    });
                    if differs {
                        records.push(value.clone());
                    }
                    differs
                };
                if changed {
                    recorder.emit(value);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }));
        Self {
            stop,
            task,
            samples,
        }
    }
    async fn finish(mut self) -> Vec<Value> {
        self.stop.cancel();
        (&mut self.task).await.unwrap();
        Arc::try_unwrap(self.samples).unwrap().into_inner().unwrap()
    }
}

async fn pressure_work(app: crate::actor::AppHandle, client: VmClient) -> Value {
    let mut response = client
        .invoke_stateless(
            &app,
            Duration::from_secs(10),
            HttpRequest {
                method: "GET".into(),
                url: "http://pressure/pressure".into(),
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
    serde_json::from_slice(&response.body).unwrap()
}

pub(super) async fn run(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    _directory: &Path,
    owned: fn() -> HashSet<u32>,
) {
    assert!(owned().is_empty(), "pre-existing pressure fixture VM");
    let output = PathBuf::from(
        std::env::var_os("CELLD_APPLICATION_MEMORY_OUTPUT")
            .expect("pressure fixture requires a fresh memory JSONL output"),
    );
    let recorder = Recorder::new(&output, "pressure-bootstrap");
    let mut config = config(options, 1, 120);
    config.max_vms = 6;
    config.memory_guard = Some(MemoryGuard {
        max_bytes: 14 * 1024 * MIB,
        high_bytes: 13 * 1024 * MIB,
        overhead_bytes: 256 * MIB,
        shared_cache_bytes: 256 * MIB,
        idle_cache: None,
    });
    let pool = Arc::new(VmPool::new(config, store.clone()).unwrap());
    let parent = Arc::new(Group::registered_parent().unwrap());
    assert_eq!(parent.scalar("memory.max").unwrap(), 14 * 1024 * MIB);
    assert_eq!(parent.scalar("memory.high").unwrap(), 13 * 1024 * MIB);
    let (stop, sampler) = recorder.start_sampler(&pool, owned);
    timeout(Duration::from_secs(1500), pool.prepare(app))
        .await
        .unwrap()
        .unwrap();
    until(
        || {
            VmPool::refresh_pressure(&pool.pressure, &pool.templates);
            !VmPool::under_pressure(&pool.pressure)
        },
        "initial admission did not open",
    )
    .await;

    let old = stage(store, options, "pressure-old", ApplicationSizeClass::Basic).await;
    let newer = stage(
        store,
        options,
        "pressure-newer",
        ApplicationSizeClass::Basic,
    )
    .await;
    let busy = stage(store, options, "pressure-busy", ApplicationSizeClass::Basic).await;
    let spare = stage(
        store,
        options,
        "pressure-spare",
        ApplicationSizeClass::Basic,
    )
    .await;
    assert_eq!(WORKER.matches("export default {").count(), 1);
    let source = WORKER.replacen("export default {", "const original = {", 1) + WRAPPER;
    let bundle =
        crate::application_image::build(&[("main.mjs".into(), source.into_bytes())]).unwrap();
    activate_bundle(
        store,
        options,
        &busy,
        "pressure",
        bundle,
        (
            "main.mjs",
            json!({"compatibility_date":"2026-03-01"}),
            json!({}),
        ),
    )
    .await;
    let old_lease = timeout(
        Duration::from_secs(60),
        pool.acquire(app, &old, &name("memory"), &name("memory")),
    )
    .await
    .unwrap()
    .unwrap();
    request(
        app,
        &old_lease,
        "/retain",
        &recorder,
        ApplicationSizeClass::Basic,
    )
    .await;
    drop(old_lease);
    until(|| slot(&pool, &old).2, "older handler did not become idle").await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let new_lease = timeout(
        Duration::from_secs(60),
        pool.acquire(app, &newer, &name("memory"), &name("memory")),
    )
    .await
    .unwrap()
    .unwrap();
    request(
        app,
        &new_lease,
        "/retain",
        &recorder,
        ApplicationSizeClass::Basic,
    )
    .await;
    drop(new_lease);
    until(
        || slot(&pool, &newer).2,
        "newer handler did not become idle",
    )
    .await;
    let busy_lease = timeout(
        Duration::from_secs(60),
        pool.acquire(app, &busy, &name("pressure"), &name("memory")),
    )
    .await
    .unwrap()
    .unwrap();
    let busy_held = request(
        app,
        &busy_lease,
        "/retain",
        &recorder,
        ApplicationSizeClass::Basic,
    )
    .await;
    until(
        || pool.templates.status()["idle"] == 1 && pool.templates.status()["preparing"] == 0,
        "startup pool did not reach one running idle VM",
    )
    .await;
    assert_eq!(charged(&pool), (4, 4096));
    assert_eq!(owned().len(), 4);
    assert!(!slot(&pool, &busy).2, "held busy lease became evictable");
    let observations = Observations::new(
        pool.clone(),
        parent.clone(),
        recorder.clone(),
        old.clone(),
        newer.clone(),
        busy.clone(),
        owned,
    );
    let before = pool.templates.memory_sample().unwrap().unwrap();
    recorder.context("pressure-kernel-high", None, 1);
    recorder.emit(
        json!({"kind":"pressureInputs","parentHighBytes":13*1024*MIB,
        "parentMaxBytes":14*1024*MIB,"temporaryHighBytes":4*MIB,
        "provenance":"explicit fixture stress inputs; no production defaults or global writes",
        "nominalCharge":pool.status(),"kernelBefore":before}),
    );
    let mut high = HighOverride {
        parent: parent.clone(),
        original: parent.scalar("memory.high").unwrap(),
        active: true,
    };
    high.write(4 * MIB).unwrap();
    let work = tokio::spawn(pressure_work(app.clone(), busy_lease.client.clone()));
    timeout(Duration::from_secs(5), async {
        loop {
            let sample = pool.templates.memory_sample().unwrap().unwrap();
            if sample.high > before.high {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("no actual kernel memory.high throttling event");
    until(
        || VmPool::under_pressure(&pool.pressure),
        "automatic pressure polling did not latch the real high event",
    )
    .await;
    assert!(
        VmPool::under_pressure(&pool.pressure),
        "real high event did not latch pressure"
    );
    let acquire_pool = pool.clone();
    let acquire_app = app.clone();
    let acquire_spare = spare.clone();
    let pending = tokio::spawn(async move {
        acquire_pool
            .acquire(
                &acquire_app,
                &acquire_spare,
                &name("memory"),
                &name("memory"),
            )
            .await
    });
    let initial_high = pool.templates.memory_sample().unwrap().unwrap();
    recorder.emit(json!({"kind":"pressureKernelEvidence","kernelAfter":initial_high,
        "configuredHighUnchanged":13*1024*MIB,"temporaryHighReadback":parent.scalar("memory.high").unwrap(),
        "busyPendingEvents":busy_lease.transport.pending()}));

    let policy_began = Instant::now();
    let mut last = signature(initial_high);
    let mut stable_since: Option<Instant> = None;
    let mut saw_blocked_pending = false;
    let mut saw_old_selected = false;
    let mut saw_new_selected = false;
    timeout(Duration::from_secs(8), async {
        loop {
            let sample = pool.templates.memory_sample().unwrap().unwrap();
            if signature(sample) != last {
                stable_since = None;
                last = signature(sample);
            } else {
                stable_since.get_or_insert(Instant::now());
            }
            assert!(
                VmPool::under_pressure(&pool.pressure),
                "kernel pressure recovered before victim qualification"
            );
            assert!(
                !pending.is_finished(),
                "new acquire completed while pressure blocked"
            );
            assert_eq!(
                pool.snapshot().starting,
                0,
                "new startup admitted under pressure"
            );
            assert_eq!(
                pool.templates.status()["preparing"],
                0,
                "refill admitted under pressure"
            );
            saw_blocked_pending = true;
            let old_state = slot(&pool, &old);
            let new_state = slot(&pool, &newer);
            if old_state.1 || !old_state.0 {
                assert_eq!(pool.templates.winding_down().0, 0);
                saw_old_selected = true;
            }
            if new_state.1 || !new_state.0 {
                assert!(
                    saw_old_selected && !old_state.0,
                    "assigned idle LRU order violated"
                );
                saw_new_selected = true;
            }
            if !old_state.0 && !new_state.0 && pool.templates.idle_capacity().0 == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("pressure victims did not reap within bound");
    assert!(saw_blocked_pending && saw_old_selected && saw_new_selected);
    assert_eq!(charged(&pool), (1, 1024));
    assert_eq!(owned().len(), 1);
    assert!(slot(&pool, &busy).0 && !slot(&pool, &busy).1);
    recorder.emit(json!({"kind":"pressurePolicyBoundaries","startupBeforeAssigned":true,
        "assignedOrder":["older","newer"],"busyProtected":true,
        "newAcquirePending":true,"refillPreparing":0,"policyDurationMs":policy_began.elapsed().as_millis(),
        "pollMs":POLL.as_millis(),"fixtureEvictionCalls":0,
        "scope":"automatic production sweeper under real kernel counter evidence"}));
    high.restore().unwrap();
    assert_eq!(parent.scalar("memory.high").unwrap(), 13 * 1024 * MIB);
    assert_eq!(parent.scalar("memory.max").unwrap(), 14 * 1024 * MIB);

    // The same queued acquire resumes; no retry or replay of uncertain work.
    timeout(Duration::from_secs(30), async {
        loop {
            let sample = pool.templates.memory_sample().unwrap().unwrap();
            if signature(sample) != last {
                stable_since = None;
                last = signature(sample);
            } else {
                stable_since.get_or_insert(Instant::now());
            }
            if !VmPool::under_pressure(&pool.pressure) {
                assert!(
                    sample.current_bytes < 12 * 1024 * MIB,
                    "recovered above low watermark"
                );
                assert!(
                    pressure::host_available().unwrap() >= 1024 * MIB,
                    "recovered without host headroom"
                );
                assert!(
                    stable_since.unwrap().elapsed() + Duration::from_millis(30) >= POLL,
                    "admission recovered before a stable pressure poll"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("pressure recovery did not clear its hysteresis latch");
    let work = timeout(Duration::from_secs(15), work)
        .await
        .unwrap()
        .unwrap();
    assert!(
        work["pagesTouched"].as_u64().unwrap() > 0,
        "pressure worker made no physical allocation attempts"
    );
    let spare_lease = timeout(Duration::from_secs(30), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    request(
        app,
        &spare_lease,
        "/status",
        &recorder,
        ApplicationSizeClass::Basic,
    )
    .await;
    assert_eq!(
        request(
            app,
            &busy_lease,
            "/status",
            &recorder,
            ApplicationSizeClass::Basic
        )
        .await,
        busy_held,
        "busy original worker state lost under pressure"
    );
    until(
        || pool.templates.status()["idle"] == 1 && pool.templates.status()["preparing"] == 0,
        "startup refill did not recover",
    )
    .await;
    let after = pool.templates.memory_sample().unwrap().unwrap();
    assert_eq!(
        (after.oom, after.oom_kill, after.oom_group_kill),
        (before.oom, before.oom_kill, before.oom_group_kill),
        "soft-high qualification unexpectedly caused OOM"
    );
    let observations = observations.finish().await;
    assert!(observations.iter().any(|sample| sample["blocked"] == true));
    assert!(
        observations
            .iter()
            .any(|sample| sample["physicalComplete"] == true && sample["blocked"] == true),
        "no complete physical ownership/charge observation while blocked"
    );
    recorder.emit(
        json!({"kind":"pressureRecovery","kernelAfter":after,"sameAcquireCompleted":true,
        "startupRefilled":true,"stablePollMs":POLL.as_millis(),"ledger":pool.status()}),
    );
    drop(spare_lease);
    drop(busy_lease);
    stop.cancel();
    sampler.await.unwrap();
    timeout(Duration::from_secs(30), pool.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(owned().is_empty() && pool.snapshot().vms == 0 && pool.snapshot().quarantined == 0);
    assert!(!parent.exists().unwrap(), "pressure pool parent leaked");
    let leaked: Vec<_> = recorder
        .groups
        .lock()
        .unwrap()
        .iter()
        .filter(|path| path.exists())
        .cloned()
        .collect();
    assert!(leaked.is_empty(), "pressure VM leaf leaked");
    recorder.emit(json!({"kind":"cleanup","ownedPids":owned(),"remainingCgroups":leaked,
        "parentRemoved":true,"pending":["target-host cadence repeats","production thresholds","16GiB density"]}));
    eprintln!(
        "T29_RESULT {}",
        json!({"scenario":"t33::memory_pressure","status":"passed",
        "duration_ms":recorder.began.elapsed().as_millis(),"output":output,
        "scope":"kernel soft-high evidence plus automatic pressure eviction, recovery and proven cleanup"})
    );
}

#[tokio::test]
#[ignore = "requires diagnostic JS image, Linux KVM and explicit memory pressure run"]
async fn t33_memory_pressure() {
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_PRESSURE_CHECK_ONLY").is_some());
    bridge_check("memory-pressure").await;
}
