//! Live M4a qualification: automatic assigned lifecycle, full reservations only.
use super::freeze_baseline::OwnedVmm;
use super::lifecycle::{Gate, Phase, Status};
use super::memory_baseline::{activate_bundle, bridge_check, config, name, no_vms, Recorder};
use super::*;
use celld_applications::invocation_wire::HttpRequest;
use futures_util::StreamExt;
use serde_json::{json, Value};

const SOURCE: &str = include_str!("../tests/fixtures/t33-memory/freeze.mjs");
const WRAPPER: &str = r#"
export default {
  fetch(request, env, ctx) {
    if (new URL(request.url).pathname === '/held-stream') {
      let chunks = 0;
      return new Response(new ReadableStream({
        pull(controller) {
          controller.enqueue(new Uint8Array(1024).fill(13));
          if (++chunks === 64) controller.close();
        }
      }));
    }
    return original.fetch(request, env, ctx);
  }
};
"#;

fn request_for(path: &str) -> HttpRequest {
    HttpRequest {
        method: "GET".into(),
        url: format!("http://lifecycle{path}"),
        headers: vec![],
        body: vec![],
    }
}

async fn fetch(
    app: &crate::actor::AppHandle,
    client: &VmClient,
    path: &str,
    deadline: Duration,
) -> Result<Value> {
    let mut response = client
        .invoke_stateless(app, deadline, request_for(path), None)
        .await?;
    if let Some(mut stream) = response.stream.take() {
        while let Some(chunk) = stream.next().await {
            response
                .body
                .extend(chunk.map_err(|error| anyhow::anyhow!("lifecycle stream: {error}"))?);
        }
    }
    ensure!(
        response.status == 200,
        "lifecycle fixture response {}",
        response.status
    );
    Ok(serde_json::from_slice(&response.body)?)
}

pub(super) fn gate(pool: &VmPool, client: &VmClient) -> Arc<Gate> {
    let state = pool.state.lock().unwrap();
    state
        .slots
        .values()
        .find_map(|slot| match slot {
            Slot::Ready(ready) if Arc::ptr_eq(&ready.client.pin, &client.pin) => ready.gate.clone(),
            _ => None,
        })
        .expect("capable assigned VM has no lifecycle gate")
}

fn full(pool: &VmPool) {
    let snapshot = pool.snapshot();
    assert_eq!(
        (snapshot.vms, snapshot.memory_mib, snapshot.quarantined),
        (1, u64::from(ApplicationSizeClass::Basic.resources().1), 0)
    );
}

struct Watch {
    log: Arc<Mutex<Vec<(u64, Status)>>>,
    stop: CancellationToken,
    task: AbortOnDropHandle<()>,
}

impl Watch {
    fn new(
        pool: Arc<VmPool>,
        gate: Arc<Gate>,
        recorder: Arc<Recorder>,
        owned: fn() -> HashSet<u32>,
    ) -> Self {
        let log = Arc::new(Mutex::new(Vec::<(u64, Status)>::new()));
        let stop = CancellationToken::new();
        let (records, stopping) = (log.clone(), stop.clone());
        let pids = owned();
        assert_eq!(pids.len(), 1);
        let identity = Arc::as_ptr(&gate) as usize;
        let task = AbortOnDropHandle::new(tokio::spawn(async move {
            loop {
                let changed = gate.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                // Read Gate and charge under the pool lock; retirement cannot replace
                // the slot between the phase and reservation observation.
                let (status, charged) = {
                    let state = pool.state.lock().unwrap();
                    (gate.status(), state.snapshot)
                };
                // A zero charge must never precede actual disappearance of our
                // original process, including during Retiring. Read proc outside
                // PoolState so the observation cannot block production admission.
                if status.phase != Phase::Retiring || !pids.is_disjoint(&owned()) {
                    assert_eq!(
                        (charged.vms, charged.memory_mib),
                        (1, u64::from(ApplicationSizeClass::Basic.resources().1))
                    );
                }
                let changed_state = {
                    let mut log = records.lock().unwrap();
                    let differs = log.last().is_none_or(|(_, previous)| *previous != status);
                    if differs {
                        assert!(log.len() < 8192, "unbounded lifecycle observations");
                        log.push((crate::asyncrt::mono_ms(), status));
                    }
                    differs
                };
                if changed_state {
                    recorder.emit(json!({"kind":"lifecycleTransition","hostMonoMs":crate::asyncrt::mono_ms(),
                        "gateIdentity":identity,"pids":pids,"phase":format!("{:?}",status.phase),
                        "activeEvents":status.active_events,"epoch":status.epoch,"lastDrainMs":status.last_drain_ms,
                        "idleDeadlineMs":status.idle_deadline_ms,"chargedVms":charged.vms,"chargedMemoryMib":charged.memory_mib}));
                }
                crate::asyncrt::select! {
                    _ = stopping.cancelled() => break,
                    _ = &mut changed => {},
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {},
                }
            }
        }));
        Self { log, stop, task }
    }

    fn resumes(&self) -> usize {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, status)| status.phase == Phase::Resuming)
            .map(|(_, status)| status.epoch)
            .collect::<BTreeSet<_>>()
            .len()
    }

    async fn finish(self) {
        self.stop.cancel();
        self.task.await.unwrap();
    }
}

async fn cached(
    pool: &VmPool,
    gate: &Gate,
    vm: &OwnedVmm,
    owned: fn() -> HashSet<u32>,
    recorder: &Recorder,
) -> Status {
    let status = timeout(Duration::from_secs(4), async {
        loop {
            let status = gate.status();
            assert_ne!(
                status.phase,
                Phase::Retiring,
                "VM retired before becoming cached"
            );
            if status.phase == Phase::Cached {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("assigned guest did not automatically pause");
    full(pool);
    assert_eq!(status.active_events, 0);
    assert_eq!(vm.read_json("/", owned).await.unwrap()["state"], "Paused");
    recorder.host(
        "lifecycle-idle-cached",
        Some(ApplicationSizeClass::Basic),
        pool,
        owned,
    );
    status
}

async fn running_for(
    pool: &VmPool,
    gate: &Gate,
    vm: &OwnedVmm,
    owned: fn() -> HashSet<u32>,
    duration: Duration,
) {
    assert_eq!(vm.read_json("/", owned).await.unwrap()["state"], "Running");
    let until = Instant::now() + duration;
    while Instant::now() < until {
        let status = gate.status();
        assert_eq!(status.phase, Phase::Running, "active work allowed a pause");
        assert!(
            status.active_events > 0,
            "actual work lost execution ownership"
        );
        full(pool);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn integrity(value: &Value, initial: &Value) {
    assert_eq!(value["identity"], initial["identity"]);
    assert_eq!(value["checksum"], 77 * (8 * 1024 * 1024 / 4096));
}

async fn fresh(
    pool: &VmPool,
    app: &crate::actor::AppHandle,
    application: &ApplicationId,
    owned: fn() -> HashSet<u32>,
) -> (VmClient, Arc<Gate>, OwnedVmm) {
    let lease = pool
        .acquire(app, application, &name("lifecycle"), &name("memory"))
        .await
        .unwrap();
    let client = lease.client.clone();
    let gate = gate(pool, &client);
    drop(lease);
    (client, gate, OwnedVmm::identify(owned))
}

async fn expire_unused(
    pool: &VmPool,
    app: &crate::actor::AppHandle,
    application: &ApplicationId,
    client: &VmClient,
    gate: &Gate,
    owned: fn() -> HashSet<u32>,
    recorder: &Recorder,
) -> VmClient {
    let before = gate.status();
    assert_eq!(before.phase, Phase::Cached);
    let old_pids = owned();
    let lease = pool
        .acquire(app, application, &name("lifecycle"), &name("memory"))
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&lease.client.pin, &client.pin));
    assert_eq!(
        gate.status().idle_deadline_ms,
        before.idle_deadline_ms,
        "unused acquire refreshed TTL"
    );
    full(pool);
    crate::asyncrt::sleep_until(before.idle_deadline_ms.saturating_add(50)).await;
    let refused = fetch(app, client, "/status", Duration::from_millis(500)).await;
    assert!(
        refused.is_err(),
        "expired retained client was revived by a held lease"
    );
    assert_eq!(gate.status().idle_deadline_ms, before.idle_deadline_ms);
    let began = Instant::now();
    let new = pool
        .acquire(app, application, &name("lifecycle"), &name("memory"))
        .await
        .unwrap();
    assert!(
        !Arc::ptr_eq(&new.client.pin, &client.pin),
        "past-TTL acquire reused expired owner"
    );
    assert!(
        old_pids.is_disjoint(&owned()),
        "replacement started before old VMM was reaped"
    );
    let replacement = new.client.clone();
    drop(new);
    drop(lease);
    recorder.emit(json!({"kind":"lifecycleExpiry","ttlSeconds":(before.idle_deadline_ms-before.last_drain_ms)/1000,
        "originalIdleDeadlineMs":before.idle_deadline_ms,"hostMonoMs":crate::asyncrt::mono_ms(),
        "heldLeaseRefused":true,"oldPids":old_pids,"freshPids":owned(),
        "acquireAfterExpiryUs":began.elapsed().as_micros(),"error":format!("{:#}",refused.unwrap_err())}));
    full(pool);
    replacement
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
    let output = PathBuf::from(
        std::env::var_os("CELLD_APPLICATION_MEMORY_OUTPUT").expect("set a fresh JSONL output"),
    );
    let recorder = Recorder::new(&output, "lifecycle-bootstrap");
    recorder.emit(json!({"kind":"provenance","kernel":options.kernel.sha256,"runtime":options.runtime.sha256,
        "firecrackerSha256":content_digest(&std::fs::read(&options.firecracker).unwrap()),
        "fixtureSha256":content_digest(SOURCE.as_bytes()),"streamWrapperSha256":content_digest(WRAPPER.as_bytes()),
        "qualificationTtlSeconds":[5,30],"warmCycles":5,"concurrentWakes":8,
        "policy":"automatic assigned Pause/Resume; full Basic reservation; no startup pool freeze or GC",
        "scope":"stateless fetch, retained clients, response streams and waitUntil only; no object/alarm/socket/queue/facet qualification"}));
    let application = ApplicationId {
        app: name("memory-lifecycle"),
    };
    let mut catalog = store.read(&application).await.unwrap();
    assert!(catalog.application(&application).is_some());
    catalog
        .set_limits(
            &application,
            Some(ApplicationLimits {
                size_class: ApplicationSizeClass::Basic,
                invocation_timeout_ms: 10000,
            }),
        )
        .unwrap();
    store.commit(catalog).await.unwrap();
    assert_eq!(SOURCE.matches("export default {").count(), 1);
    let source = SOURCE.replacen("export default {", "const original = {", 1) + WRAPPER;
    let artifact =
        crate::application_image::build(&[("main.mjs".into(), source.into_bytes())]).unwrap();
    activate_bundle(
        store,
        options,
        &application,
        "lifecycle",
        artifact,
        (
            "main.mjs",
            json!({"compatibility_date":"2026-03-01"}),
            json!({}),
        ),
    )
    .await;

    for ttl in [5, 30] {
        recorder.context(
            &format!("lifecycle-ttl{ttl}"),
            Some(ApplicationSizeClass::Basic),
            0,
        );
        let mut configuration = config(options, 0, ttl);
        configuration.max_vms = 1;
        let pool = Arc::new(VmPool::new(configuration, store.clone()).unwrap());
        let (sampling_stop, sampling) = recorder.start_sampler(&pool, owned);
        pool.prepare(app).await.unwrap();
        recorder.disk(&pool);
        let (client, original_gate, vm) = fresh(&pool, app, &application, owned).await;
        let watch = Watch::new(pool.clone(), original_gate.clone(), recorder.clone(), owned);
        tokio::task::yield_now().await;
        let initial = fetch(app, &client, "/status", Duration::from_secs(10))
            .await
            .unwrap();
        integrity(&initial, &initial);
        cached(&pool, &original_gate, &vm, owned, &recorder).await;

        if ttl == 5 {
            for cycle in 0..5 {
                let resumes = watch.resumes();
                let began = Instant::now();
                tokio::time::sleep(Duration::from_millis(100)).await;
                let after = fetch(app, &client, "/status", Duration::from_secs(10))
                    .await
                    .unwrap();
                integrity(&after, &initial);
                cached(&pool, &original_gate, &vm, owned, &recorder).await;
                let observed = watch.resumes() - resumes;
                assert!(
                    observed <= 1,
                    "one retained wake issued multiple observed Resume transitions"
                );
                recorder.emit(json!({"kind":"lifecycleWarm","cycle":cycle,
                    "latencyUs":began.elapsed().as_micros(),"retainedClient":true,"observedResumes":observed,"result":after}));
            }
            let resumes = watch.resumes();
            let barrier = Arc::new(tokio::sync::Barrier::new(9));
            let mut wakes = Vec::new();
            for _ in 0..8 {
                let (actor, retained, start) = (app.clone(), client.clone(), barrier.clone());
                wakes.push(tokio::spawn(async move {
                    start.wait().await;
                    fetch(&actor, &retained, "/status", Duration::from_secs(10)).await
                }));
            }
            barrier.wait().await;
            for wake in wakes {
                integrity(&wake.await.unwrap().unwrap(), &initial);
            }
            cached(&pool, &original_gate, &vm, owned, &recorder).await;
            let observed = watch.resumes() - resumes;
            assert!(
                observed <= 1,
                "concurrent wakes issued multiple observed Resume transitions"
            );
            recorder.emit(json!({"kind":"lifecycleConcurrentWake","calls":8,"observedResumes":observed,"pids":owned()}));

            let armed_at = crate::asyncrt::mono_ms();
            let armed = fetch(app, &client, "/arm", Duration::from_secs(10))
                .await
                .unwrap();
            assert!(armed["timer"]["completed"].is_null());
            running_for(
                &pool,
                &original_gate,
                &vm,
                owned,
                Duration::from_millis(200),
            )
            .await;
            let drained = cached(&pool, &original_gate, &vm, owned, &recorder).await;
            assert!(
                drained.last_drain_ms >= armed_at + 450,
                "TTL began before waitUntil drain"
            );
            let timer = fetch(app, &client, "/status", Duration::from_secs(10))
                .await
                .unwrap();
            integrity(&timer, &initial);
            assert!(!timer["timer"]["completed"].is_null());
            recorder.emit(json!({"kind":"lifecycleWaitUntil","armedHostMonoMs":armed_at,
                "lastDrainMs":drained.last_drain_ms,"timer":timer["timer"],"clockObservation":"no heartbeat or clock-suspension claim"}));
            cached(&pool, &original_gate, &vm, owned, &recorder).await;

            let (actor, retained) = (app.clone(), client.clone());
            let slow_at = crate::asyncrt::mono_ms();
            let slow = tokio::spawn(async move {
                fetch(&actor, &retained, "/slow", Duration::from_secs(10)).await
            });
            timeout(Duration::from_secs(2), async {
                while original_gate.status().active_events == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            running_for(
                &pool,
                &original_gate,
                &vm,
                owned,
                Duration::from_millis(400),
            )
            .await;
            integrity(&slow.await.unwrap().unwrap(), &initial);
            let drained = cached(&pool, &original_gate, &vm, owned, &recorder).await;
            assert!(
                drained.last_drain_ms >= slow_at + 1900,
                "slow event lost full drain ownership"
            );

            let mut response = client
                .invoke_stateless(
                    app,
                    Duration::from_secs(10),
                    request_for("/held-stream"),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(response.status, 200);
            let mut stream = response
                .stream
                .take()
                .expect("fixture response was not streamed");
            running_for(
                &pool,
                &original_gate,
                &vm,
                owned,
                Duration::from_millis(1200),
            )
            .await;
            let mut bytes = response.body;
            while let Some(chunk) = stream.next().await {
                bytes.extend(chunk.unwrap());
            }
            assert_eq!(bytes.len(), 64 * 1024);
            assert!(bytes.iter().all(|byte| *byte == 13));
            cached(&pool, &original_gate, &vm, owned, &recorder).await;
            recorder.emit(json!({"kind":"lifecycleHeldStream","heldMs":1200,"bytes":bytes.len(),"protectedRunning":true}));
            let phases = watch
                .log
                .lock()
                .unwrap()
                .iter()
                .map(|(_, status)| status.phase)
                .collect::<Vec<_>>();
            for phase in [Phase::Pausing, Phase::Cached, Phase::Resuming] {
                assert!(phases.contains(&phase), "phase never observed: {phase:?}");
            }
        }

        let replacement = expire_unused(
            &pool,
            app,
            &application,
            &client,
            &original_gate,
            owned,
            &recorder,
        )
        .await;
        watch.finish().await;
        let replacement_gate = gate(&pool, &replacement);
        let replacement_vm = OwnedVmm::identify(owned);
        let replacement_watch = Watch::new(
            pool.clone(),
            replacement_gate.clone(),
            recorder.clone(),
            owned,
        );
        let new_identity = fetch(app, &replacement, "/status", Duration::from_secs(10))
            .await
            .unwrap();
        assert_ne!(new_identity["identity"], initial["identity"]);
        cached(&pool, &replacement_gate, &replacement_vm, owned, &recorder).await;
        if ttl == 5 {
            let began = Instant::now();
            timeout(Duration::from_secs(5), async {
                pool.retire_deployment(&application, &name("lifecycle"))
                    .await;
                no_vms(&pool, owned).await;
            })
            .await
            .expect("cached retirement waited for guest grace");
            recorder.emit(json!({"kind":"lifecycleCachedRetirement","reapLatencyUs":began.elapsed().as_micros(),"pids":owned(),"chargedMemoryMib":pool.snapshot().memory_mib}));
            replacement_watch.finish().await;
            pool.readmit_deployment(&application, &name("lifecycle"));
            let (deadline_client, deadline_gate, deadline_vm) =
                fresh(&pool, app, &application, owned).await;
            let deadline_watch =
                Watch::new(pool.clone(), deadline_gate.clone(), recorder.clone(), owned);
            fetch(app, &deadline_client, "/status", Duration::from_secs(10))
                .await
                .unwrap();
            cached(&pool, &deadline_gate, &deadline_vm, owned, &recorder).await;
            let start_ms = crate::asyncrt::mono_ms();
            let began = Instant::now();
            let timed = fetch(app, &deadline_client, "/slow", Duration::from_millis(500)).await;
            let elapsed = began.elapsed();
            assert!(
                timed.is_err(),
                "resumed slow work escaped the original deadline"
            );
            assert!(
                elapsed >= Duration::from_millis(450),
                "request failed before its deadline: {elapsed:?}"
            );
            assert!(
                elapsed < Duration::from_millis(900),
                "timeout renewed after resume: {elapsed:?}"
            );
            let resume_ms = deadline_watch
                .log
                .lock()
                .unwrap()
                .iter()
                .find(|(at, status)| *at >= start_ms && status.phase == Phase::Resuming)
                .map(|(at, _)| *at);
            recorder.emit(json!({"kind":"lifecycleOriginalDeadline","startedHostMonoMs":start_ms,
                "originalDeadlineMs":start_ms + 500,"resumeObservedHostMonoMs":resume_ms,
                "elapsedUs":elapsed.as_micros(),"error":format!("{:#}",timed.unwrap_err()),
                "limit":"normal Resume latency only; no injected slow API proof; transient notify phases may be missed"}));
            pool.shutdown().await.unwrap();
            no_vms(&pool, owned).await;
            deadline_watch.finish().await;
        } else {
            let began = Instant::now();
            timeout(Duration::from_secs(5), pool.shutdown())
                .await
                .expect("cached shutdown waited for guest grace")
                .unwrap();
            no_vms(&pool, owned).await;
            recorder.emit(json!({"kind":"lifecycleCachedShutdown","reapLatencyUs":began.elapsed().as_micros(),"pids":owned(),"chargedMemoryMib":pool.snapshot().memory_mib}));
            replacement_watch.finish().await;
        }
        sampling_stop.cancel();
        sampling.await.unwrap();
        assert_eq!(
            (
                pool.snapshot().vms,
                pool.snapshot().memory_mib,
                pool.snapshot().quarantined
            ),
            (0, 0, 0)
        );
    }
    let leaked = recorder
        .groups
        .lock()
        .unwrap()
        .iter()
        .filter(|path| path.exists())
        .cloned()
        .collect::<Vec<_>>();
    assert!(leaked.is_empty(), "lifecycle fixture cgroups remain");
    recorder.emit(json!({"kind":"cleanup","ownedPids":owned(),"remainingCgroups":leaked}));
    eprintln!(
        "T29_RESULT {}",
        json!({"scenario":"t33::memory_lifecycle","status":"passed",
        "duration_ms":recorder.began.elapsed().as_millis(),"output":output,
        "scope":"M4a automatic assigned stateless lifecycle only; full reservations; no density or other execution-path claim"})
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires explicit diagnostic JavaScript image, Linux KVM and assigned lifecycle gates"]
async fn t33_memory_lifecycle() {
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_LIFECYCLE_CHECK_ONLY").is_some());
    bridge_check("memory-lifecycle").await;
}
