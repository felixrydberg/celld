//! Retained-cache qualification: enforced ceilings and bounded readmission.
use super::guard_baseline::Group;
use super::lifecycle::{Gate, Phase, Status};
use super::memory_baseline::{activate_bundle, bridge_check, config, name, stage, Recorder};
use super::*;
use celld_applications::invocation_wire::HttpRequest;
use futures_util::StreamExt;
use nix::fcntl::{openat, OFlag};
use nix::sys::stat::Mode;
use serde_json::{json, Value};
use std::fs::File;
use std::sync::atomic::Ordering as AtomicOrdering;

const MIB: u64 = 1024 * 1024;
const SOURCE: &str = include_str!("../tests/fixtures/t33-memory/freeze.mjs");
const WRAPPER: &str =
    "\nexport default {fetch(request, env, ctx) {return original.fetch(request, env, ctx);}};\n";
const TARGET: ApplicationSizeClass = ApplicationSizeClass::Standard4;
const ELIGIBLE: ApplicationSizeClass = ApplicationSizeClass::Standard1;
const BUSY: ApplicationSizeClass = ApplicationSizeClass::Basic;
const MARGIN: u64 = 128 * MIB;
const CACHE_MAX: u64 = 4 * 1024 * MIB;

fn guard() -> MemoryGuard {
    MemoryGuard {
        max_bytes: 14 * 1024 * MIB,
        high_bytes: 13 * 1024 * MIB,
        overhead_bytes: 256 * MIB,
        shared_cache_bytes: 256 * MIB,
        idle_cache: Some(IdleCache {
            max_bytes: CACHE_MAX,
            retained_margin_bytes: MARGIN,
        }),
    }
}

fn request_for(path: &str) -> HttpRequest {
    HttpRequest {
        method: "GET".into(),
        url: format!("http://cache{path}"),
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
                .extend(chunk.map_err(|error| anyhow::anyhow!("cache stream: {error}"))?);
        }
    }
    ensure!(
        response.status == 200,
        "cache response status {}",
        response.status
    );
    Ok(serde_json::from_slice(&response.body)?)
}
fn integrity(value: &Value, original: &Value) {
    assert_eq!(value["identity"], original["identity"]);
    assert_eq!(value["checksum"], 77 * (8 * 1024 * 1024 / 4096));
}
async fn until(mut condition: impl FnMut() -> bool, label: &str) {
    timeout(Duration::from_secs(35), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("cache fixture timed out: {label}"));
}

#[derive(Clone, Copy)]
struct Meta {
    status: Status,
    retained: u64,
    snapshot: VmPoolSnapshot,
}
fn meta(pool: &VmPool, client: &VmClient) -> Option<Meta> {
    let state = pool.state.lock().unwrap();
    state.slots.values().find_map(|slot| match slot {
        Slot::Ready(ready) if Arc::ptr_eq(&ready.client.pin, &client.pin) => Some(Meta {
            status: ready
                .gate
                .as_ref()
                .expect("cache guest lacks lifecycle gate")
                .status(),
            retained: ready.retained_limit.load(AtomicOrdering::Relaxed),
            snapshot: state.snapshot,
        }),
        _ => None,
    })
}
fn gate(pool: &VmPool, client: &VmClient) -> Arc<Gate> {
    let state = pool.state.lock().unwrap();
    state
        .slots
        .values()
        .find_map(|slot| match slot {
            Slot::Ready(ready) if Arc::ptr_eq(&ready.client.pin, &client.pin) => ready.gate.clone(),
            _ => None,
        })
        .unwrap()
}
fn same(left: Meta, right: Meta) -> bool {
    left.status == right.status && left.retained == right.retained
}
fn text(group: &Group, control: &str) -> Result<String> {
    let file = File::from(openat(
        &group.directory,
        control,
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )?);
    let mut value = String::new();
    file.take(4097).read_to_string(&mut value)?;
    ensure!(value.len() <= 4096, "oversized owned cache sample");
    Ok(value)
}
fn counter(value: &str, name: &str) -> Result<u64> {
    let mut values = value
        .lines()
        .filter_map(|line| line.split_once(' '))
        .filter(|(key, _)| *key == name)
        .map(|(_, number)| number);
    let number = values.next().context("missing cache event counter")?;
    ensure!(values.next().is_none(), "duplicate cache event counter");
    Ok(number.parse()?)
}
fn psi(value: &str, name: &str) -> Result<u64> {
    let line = value
        .lines()
        .find(|line| line.starts_with(&format!("{name} ")))
        .context("missing cache PSI field")?;
    Ok(line
        .split_whitespace()
        .find_map(|field| field.strip_prefix("total="))
        .context("missing cache PSI total")?
        .parse()?)
}
fn sample(group: &Group) -> Result<crate::application_template::cgroup::MemorySample> {
    let events = text(group, "memory.events.local")?;
    let pressure = text(group, "memory.pressure")?;
    Ok(crate::application_template::cgroup::MemorySample {
        current_bytes: group.scalar("memory.current")?,
        peak_bytes: group.scalar("memory.peak")?,
        high: counter(&events, "high")?,
        max: counter(&events, "max")?,
        oom: counter(&events, "oom")?,
        oom_kill: counter(&events, "oom_kill")?,
        oom_group_kill: counter(&events, "oom_group_kill")?,
        some_total_us: psi(&pressure, "some")?,
        full_total_us: psi(&pressure, "full")?,
    })
}
async fn instance(group: &Group, owned: fn() -> HashSet<u32>) -> Result<Value> {
    use http_body_util::{BodyExt, Full, Limited};
    use hyper_util::rt::TokioIo;
    group.verify_member(owned)?;
    let pid = group.member.unwrap().0;
    timeout(Duration::from_secs(2), async {
        let stream = tokio::net::UnixStream::connect(format!("/proc/{pid}/root/api.sock")).await?;
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
        let _connection = AbortOnDropHandle::new(tokio::spawn(connection));
        let request = hyper::Request::builder()
            .method("GET")
            .uri("/")
            .header("host", "firecracker")
            .body(Full::new(Bytes::new()))?;
        let response = sender.send_request(request).await?;
        ensure!(response.status() == 200, "owned VMM state refused");
        let body = Limited::new(response.into_body(), 4096)
            .collect()
            .await
            .map_err(|error| anyhow::anyhow!("owned VMM state body: {error}"))?
            .to_bytes();
        group.verify_member(owned)?;
        Ok(serde_json::from_slice(&body)?)
    })
    .await
    .context("cache VMM state query timed out")?
}
async fn cached(
    pool: &VmPool,
    client: &VmClient,
    group: &Group,
    class: ApplicationSizeClass,
    owned: fn() -> HashSet<u32>,
    recorder: &Recorder,
) -> Meta {
    until(
        || {
            meta(pool, client)
                .is_some_and(|meta| meta.status.phase == Phase::Cached && meta.retained > 0)
        },
        "no enforced retained cache credit",
    )
    .await;
    let before = meta(pool, client).unwrap();
    group.verify_member(owned).unwrap();
    assert_eq!(instance(group, owned).await.unwrap()["state"], "Paused");
    assert_eq!(group.scalar("memory.max").unwrap(), before.retained);
    assert_eq!(group.scalar("memory.swap.max").unwrap(), 0);
    assert_eq!(group.scalar("memory.oom.group").unwrap(), 1);
    assert!(before.retained < u64::from(class.resources().1) * MIB);
    assert!(
        same(before, meta(pool, client).unwrap()),
        "cache changed during stable check"
    );
    assert_reservations(pool);
    let physical = sample(group).unwrap();
    recorder.emit(json!({"kind":"cachePaused","class":class,"pid":group.member.unwrap().0,
        "startTicks":group.member.unwrap().1,"hostMonoMs":crate::asyncrt::mono_ms(),
        "retainedLimitBytes":before.retained,"retainedMarginBytes":MARGIN,"overheadBytes":256*MIB,
        "currentHeadroomBytes":before.retained.saturating_sub(physical.current_bytes),
        "currentOvershootBytes":physical.current_bytes.saturating_sub(before.retained),
        "kernel":physical,"lifetimePeakIncludesRunning":true,"idleDeadlineMs":before.status.idle_deadline_ms,
        "ledger":pool.status()}));
    before
}

fn assert_reservations(pool: &VmPool) {
    let status = pool.status();
    let reservation = &status["memoryReservations"];
    let nominal = status["memoryMib"].as_u64().unwrap();
    let retained_nominal = reservation["cachedNominalMib"].as_u64().unwrap();
    let retained = reservation["retainedLimitBytes"].as_u64().unwrap();
    let cached_vms = reservation["retainedVms"].as_u64().unwrap();
    let vms = status["vms"].as_u64().unwrap();
    assert_eq!(reservation["fullExecutionMib"], nominal - retained_nominal);
    assert_eq!(
        reservation["retainedOverheadBytes"],
        cached_vms * guard().overhead_bytes
    );
    assert!(retained + cached_vms * guard().overhead_bytes <= CACHE_MAX);
    let physical = (nominal - retained_nominal) * MIB
        + retained
        + vms * guard().overhead_bytes
        + guard().shared_cache_bytes;
    assert_eq!(reservation["physicalBytes"], physical);
    assert!(physical <= guard().max_bytes);
}

#[derive(Default)]
struct Observed {
    frames: u32,
    pause_proofs: u32,
    full_proofs: u32,
    maximum_cached_current: u64,
    maximum_current_overshoot: u64,
    epochs: BTreeSet<u64>,
}
struct Watch {
    stop: CancellationToken,
    task: AbortOnDropHandle<Observed>,
}
impl Watch {
    fn new(
        pool: Arc<VmPool>,
        client: VmClient,
        group: Arc<Group>,
        recorder: Arc<Recorder>,
        class: ApplicationSizeClass,
        owned: fn() -> HashSet<u32>,
    ) -> Self {
        let stop = CancellationToken::new();
        let stopping = stop.clone();
        let gate = gate(&pool, &client);
        let task = AbortOnDropHandle::new(tokio::spawn(async move {
            let mut observed = Observed::default();
            let mut last: Option<(Phase, u64, u64)> = None;
            let mut verified_pause = None;
            while !stopping.is_cancelled() {
                let changed = gate.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if let Some(before) = meta(&pool, &client) {
                    assert!(before.snapshot.memory_mib >= u64::from(class.resources().1));
                    if before.status.phase == Phase::Resuming
                        || before.status.phase == Phase::Running
                    {
                        assert_eq!(before.retained, 0, "execution phase kept retained credit");
                        assert!(
                            before
                                .snapshot
                                .physical_bytes(
                                    guard(),
                                    before.snapshot.memory_mib,
                                    before.snapshot.vms
                                )
                                .unwrap()
                                <= guard().max_bytes,
                            "Resume before full-budget admission"
                        );
                        observed.full_proofs += 1;
                    }
                    if before.status.phase != Phase::Retiring {
                        group.verify_member(owned).unwrap();
                        let limit = group.scalar("memory.max").unwrap();
                        let physical = sample(&group).unwrap();
                        assert_eq!(
                            (physical.oom, physical.oom_kill, physical.oom_group_kill),
                            (0, 0, 0),
                            "warm retained cache caused an owned leaf OOM"
                        );
                        let after = meta(&pool, &client).unwrap();
                        if same(before, after) && before.status.phase == Phase::Running {
                            assert_eq!(
                                limit,
                                (u64::from(class.resources().1) + 256) * MIB,
                                "Running guest lacks full owned leaf ceiling"
                            );
                        }
                        if same(before, after) && before.retained > 0 {
                            assert_eq!(
                                before.status.phase,
                                Phase::Cached,
                                "credit before Pause commit"
                            );
                            assert_eq!(
                                limit, before.retained,
                                "credit before owned leaf enforcement"
                            );
                            observed.maximum_cached_current =
                                observed.maximum_cached_current.max(physical.current_bytes);
                            observed.maximum_current_overshoot = observed
                                .maximum_current_overshoot
                                .max(physical.current_bytes.saturating_sub(limit));
                            if verified_pause != Some(before.status.epoch) {
                                let state = instance(&group, owned).await.unwrap();
                                if meta(&pool, &client).is_some_and(|after| same(before, after)) {
                                    assert_eq!(
                                        state["state"], "Paused",
                                        "cache credit before Pause acknowledgement"
                                    );
                                    verified_pause = Some(before.status.epoch);
                                    observed.pause_proofs += 1;
                                }
                            }
                        }
                        let identity = (before.status.phase, before.status.epoch, before.retained);
                        if last != Some(identity) {
                            assert!(observed.frames < 4096, "unbounded cache transition records");
                            observed.frames += 1;
                            observed.epochs.insert(before.status.epoch);
                            recorder.emit(json!({"kind":"cacheTransition","class":class,
                                "hostMonoMs":crate::asyncrt::mono_ms(),"phase":format!("{:?}",before.status.phase),
                                "epoch":before.status.epoch,"activeEvents":before.status.active_events,
                                "retainedLimitBytes":before.retained,"ownedLeafLimitBytes":limit,
                                "fullExecutionMib":before.snapshot.memory_mib-before.snapshot.cached_nominal_mib,
                                "nominalMib":before.snapshot.memory_mib,"retainedVms":before.snapshot.cached_vms,
                                "cachedNominalMib":before.snapshot.cached_nominal_mib,"kernel":physical}));
                            last = Some(identity);
                        }
                    }
                }
                crate::asyncrt::select! { _ = stopping.cancelled() => break, _ = &mut changed => {},
                _ = tokio::time::sleep(Duration::from_millis(5)) => {} }
            }
            observed
        }));
        Self { stop, task }
    }
    async fn finish(self) -> Observed {
        self.stop.cancel();
        self.task.await.unwrap()
    }
}

async fn launch(
    pool: &VmPool,
    app: &crate::actor::AppHandle,
    application: &ApplicationId,
    parent: &Group,
    owned: fn() -> HashSet<u32>,
) -> (VmLease, Arc<Group>) {
    let before = owned();
    let lease = timeout(
        Duration::from_secs(60),
        pool.acquire(app, application, &name("cache"), &name("memory")),
    )
    .await
    .unwrap()
    .unwrap();
    let added: Vec<_> = owned().difference(&before).copied().collect();
    assert_eq!(
        added.len(),
        1,
        "new identity must add one VM; startup pool disabled"
    );
    let group = Arc::new(Group::leaf(added[0], parent, owned).unwrap().unwrap());
    (lease, group)
}

pub(super) async fn run(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    _directory: &Path,
    owned: fn() -> HashSet<u32>,
) {
    assert!(owned().is_empty());
    let output = PathBuf::from(
        std::env::var_os("CELLD_APPLICATION_MEMORY_OUTPUT").expect("fresh cache JSONL required"),
    );
    let recorder = Recorder::new(&output, "cache-bootstrap");
    recorder.emit(json!({"kind":"cacheInputs","guard":guard(),"classes":[TARGET,ELIGIBLE,BUSY],
        "kernelSha256":options.kernel.sha256,"runtimeSha256":options.runtime.sha256,
        "firecrackerSha256":content_digest(&std::fs::read(&options.firecracker).unwrap()),
        "sourceSha256":content_digest(SOURCE.as_bytes()),"wrapperSha256":content_digest(WRAPPER.as_bytes()),"cycles":5,"concurrentWakes":8,"ttlSeconds":30,
        "provenance":"explicit stress-only cache budget/margin; no universal calibration or production defaults"}));
    let mut configuration = config(options, 0, 30);
    configuration.max_vms = 3;
    configuration.memory_guard = Some(guard());
    let pool = Arc::new(VmPool::new(configuration, store.clone()).unwrap());
    let parent = Arc::new(Group::registered_parent().unwrap());
    let (sampling_stop, sampling) = recorder.start_sampler(&pool, owned);
    timeout(Duration::from_secs(1500), pool.prepare(app))
        .await
        .unwrap()
        .unwrap();
    until(
        || {
            VmPool::refresh_pressure(&pool.pressure, &pool.templates);
            !VmPool::under_pressure(&pool.pressure)
        },
        "initial pressure latch did not open",
    )
    .await;
    assert_eq!(SOURCE.matches("export default {").count(), 1);
    let source = SOURCE.replacen("export default {", "const original = {", 1) + WRAPPER;
    let artifact =
        crate::application_image::build(&[("main.mjs".into(), source.into_bytes())]).unwrap();
    let target = stage(store, options, "cache-target", TARGET).await;
    let eligible = stage(store, options, "cache-eligible", ELIGIBLE).await;
    let busy = stage(store, options, "cache-busy", BUSY).await;
    for application in [&target, &eligible, &busy] {
        activate_bundle(
            store,
            options,
            application,
            "cache",
            artifact.clone(),
            (
                "main.mjs",
                json!({"compatibility_date":"2026-03-01"}),
                json!({}),
            ),
        )
        .await;
    }
    recorder.context("cache-repeated-cycles", Some(TARGET), 0);
    let (lease, target_group) = launch(&pool, app, &target, &parent, owned).await;
    let client = lease.client.clone();
    let original = fetch(app, &client, "/status", Duration::from_secs(10))
        .await
        .unwrap();
    let watch = Watch::new(
        pool.clone(),
        client.clone(),
        target_group.clone(),
        recorder.clone(),
        TARGET,
        owned,
    );
    drop(lease);
    cached(&pool, &client, &target_group, TARGET, owned, &recorder).await;
    let parent_before = pool.templates.memory_sample().unwrap().unwrap();
    let mut last_calls = original["calls"].as_u64().unwrap();
    for cycle in 0..5 {
        let began = Instant::now();
        let result = fetch(app, &client, "/status", Duration::from_secs(10))
            .await
            .unwrap();
        integrity(&result, &original);
        assert_eq!(result["calls"].as_u64().unwrap(), last_calls + 1);
        last_calls += 1;
        target_group.verify_member(owned).unwrap();
        let paused = cached(&pool, &client, &target_group, TARGET, owned, &recorder).await;
        assert_eq!(
            (
                paused.snapshot.vms,
                paused.snapshot.memory_mib,
                paused.snapshot.cached_vms,
                paused.snapshot.cached_nominal_mib
            ),
            (1, 12288, 1, 12288)
        );
        recorder.emit(json!({"kind":"cacheWarmCycle","cycle":cycle,"class":TARGET,
            "latencyUs":began.elapsed().as_micros(),"pid":target_group.member.unwrap().0,"result":result}));
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(9));
    let mut wakes = Vec::new();
    for _ in 0..8 {
        let (actor, retained, barrier) = (app.clone(), client.clone(), barrier.clone());
        wakes.push(AbortOnDropHandle::new(tokio::spawn(async move {
            barrier.wait().await;
            fetch(&actor, &retained, "/status", Duration::from_secs(10)).await
        })));
    }
    barrier.wait().await;
    let mut concurrent_calls = BTreeSet::new();
    for wake in wakes {
        let result = timeout(Duration::from_secs(15), wake)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        integrity(&result, &original);
        concurrent_calls.insert(result["calls"].as_u64().unwrap());
    }
    assert_eq!(
        concurrent_calls,
        (last_calls + 1..=last_calls + 8).collect()
    );
    last_calls += 8;
    cached(&pool, &client, &target_group, TARGET, owned, &recorder).await;

    recorder.context("cache-bounded-density", None, 0);
    let (eligible_lease, eligible_group) = launch(&pool, app, &eligible, &parent, owned).await;
    let eligible_client = eligible_lease.client.clone();
    fetch(app, &eligible_client, "/status", Duration::from_secs(10))
        .await
        .unwrap();
    drop(eligible_lease);
    let eligible_cache = cached(
        &pool,
        &eligible_client,
        &eligible_group,
        ELIGIBLE,
        owned,
        &recorder,
    )
    .await;
    // Holding this already paused VM's unused lease protects it without undoing credit.
    let protected = pool
        .acquire(app, &eligible, &name("cache"), &name("memory"))
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&protected.client.pin, &eligible_client.pin));
    assert_eq!(
        meta(&pool, &eligible_client)
            .unwrap()
            .status
            .idle_deadline_ms,
        eligible_cache.status.idle_deadline_ms,
        "unused held lease refreshed cache TTL"
    );
    assert_eq!(
        meta(&pool, &eligible_client).unwrap().retained,
        eligible_cache.retained
    );
    assert_eq!(
        instance(&eligible_group, owned).await.unwrap()["state"],
        "Paused"
    );
    let (busy_lease, busy_group) = launch(&pool, app, &busy, &parent, owned).await;
    let busy_original = fetch(app, &busy_lease, "/status", Duration::from_secs(10))
        .await
        .unwrap();
    until(
        || {
            VmPool::refresh_pressure(&pool.pressure, &pool.templates);
            !VmPool::under_pressure(&pool.pressure)
        },
        "density admission conflated with pressure",
    )
    .await;
    let busy_gate = gate(&pool, &busy_lease);
    let (actor, retained) = (app.clone(), busy_lease.client.clone());
    let slow = AbortOnDropHandle::new(tokio::spawn(async move {
        fetch(&actor, &retained, "/slow", Duration::from_secs(10)).await
    }));
    until(
        || busy_gate.status().active_events > 0,
        "busy handler never accepted actual work",
    )
    .await;
    let waiting = meta(&pool, &client).unwrap();
    assert_eq!(
        (
            waiting.snapshot.vms,
            waiting.snapshot.memory_mib,
            waiting.snapshot.cached_vms
        ),
        (3, 17408, 2)
    );
    let physical = waiting
        .snapshot
        .physical_bytes(guard(), waiting.snapshot.memory_mib, waiting.snapshot.vms)
        .unwrap();
    assert!(waiting.snapshot.memory_mib * MIB > guard().max_bytes && physical <= guard().max_bytes);
    let mut promoted = waiting.snapshot;
    promoted.full(12288, waiting.retained).unwrap();
    assert!(
        promoted
            .physical_bytes(guard(), promoted.memory_mib, promoted.vms)
            .unwrap()
            > guard().max_bytes
    );
    assert!(
        busy_gate.status().active_events > 0,
        "busy work ended before blocked wake"
    );
    let refused = fetch(app, &client, "/status", Duration::from_millis(500)).await;
    assert!(
        refused.is_err(),
        "wake entered without full budget while all victims protected"
    );
    let after_timeout = meta(&pool, &client).unwrap();
    assert_eq!(after_timeout.status.phase, Phase::Cached);
    assert_eq!(after_timeout.retained, waiting.retained);
    assert_eq!(
        after_timeout.status.idle_deadline_ms, waiting.status.idle_deadline_ms,
        "blocked timeout refreshed original TTL"
    );
    assert_eq!(target_group.scalar("memory.max").unwrap(), waiting.retained);
    assert_eq!(
        instance(&target_group, owned).await.unwrap()["state"],
        "Paused"
    );
    eligible_group.verify_member(owned).unwrap();
    busy_group.verify_member(owned).unwrap();
    assert_eq!(busy_gate.status().phase, Phase::Running);
    recorder.emit(json!({"kind":"cacheBlockedReadmission","nominalGuestMib":17408,
        "physicalReservationBytes":physical,"fullCandidateBytes":promoted.physical_bytes(guard(),17408,3),
        "timeoutMs":500,"originalIdleDeadlineMs":waiting.status.idle_deadline_ms,
        "protectedCachedVictim":true,"busyClass":BUSY,"busyActiveEvents":busy_gate.status().active_events,
        "retainedLimitUnchanged":true,"pausedUnchanged":true,"ledger":pool.status()}));
    assert!(eligible_group.exists().unwrap());
    assert_eq!(pool.snapshot().vms, 3);
    let (observed_pool, observed_leaf) = (pool.clone(), eligible_group.clone());
    let reap_observer = AbortOnDropHandle::new(tokio::spawn(async move {
        timeout(Duration::from_secs(15), async {
            let mut frames = 1_u32; // The stable, charged owned leaf above.
            loop {
                let before = observed_pool.snapshot();
                let exists = observed_leaf.exists().unwrap();
                let after = observed_pool.snapshot();
                if before == after && exists {
                    assert_eq!(before.vms, 3, "capacity released before owned leaf removal");
                    frames += 1;
                }
                if !exists {
                    return frames;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("eligible cache leaf did not reap before observation deadline")
    }));
    drop(protected);
    let admitted = fetch(app, &client, "/status", Duration::from_secs(10))
        .await
        .unwrap();
    integrity(&admitted, &original);
    assert_eq!(
        admitted["calls"].as_u64().unwrap(),
        last_calls + 1,
        "timed-out blocked request executed or replayed"
    );
    assert!(!owned().contains(&eligible_group.member.unwrap().0));
    assert!(
        !eligible_group.exists().unwrap(),
        "eligible cache credit reused before leaf reap"
    );
    let reap_frames = reap_observer.await.unwrap();
    busy_group.verify_member(owned).unwrap();
    let slow_result = timeout(Duration::from_secs(15), slow)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    integrity(&slow_result, &busy_original);
    integrity(
        &fetch(app, &busy_lease, "/status", Duration::from_secs(10))
            .await
            .unwrap(),
        &busy_original,
    );
    cached(&pool, &client, &target_group, TARGET, owned, &recorder).await;
    recorder.emit(json!({"kind":"cacheReadmissionAfterReap","class":TARGET,
        "samePid":target_group.member.unwrap().0,"eligibleClass":ELIGIBLE,"eligibleLeafRemoved":true,
        "chargedLeafObservationFrames":reap_frames,"busyOriginalStatePreserved":true,"nominalGuestMib":pool.snapshot().memory_mib,"ledger":pool.status()}));
    drop(busy_lease);
    pool.retire_deployment(&busy, &name("cache")).await;
    until(
        || pool.snapshot().vms == 1 && owned() == HashSet::from([target_group.member.unwrap().0]),
        "busy cleanup not proved",
    )
    .await;
    let observed = watch.finish().await;
    assert!(observed.pause_proofs >= 5 && observed.full_proofs > 0);
    recorder.emit(json!({"kind":"cacheMarginObservations","class":TARGET,"cycles":5,
        "pauseProofs":observed.pause_proofs,"fullProofs":observed.full_proofs,
        "maximumObservedPausedCurrentBytes":observed.maximum_cached_current,
        "maximumObservedCurrentOvershootBytes":observed.maximum_current_overshoot,
        "retainedMarginBytes":MARGIN,"scope":"one-host observed envelope; lifetime peak also includes running phases"}));

    recorder.context("cache-original-ttl", Some(TARGET), 0);
    let expiration = cached(&pool, &client, &target_group, TARGET, owned, &recorder).await;
    let unused = pool
        .acquire(app, &target, &name("cache"), &name("memory"))
        .await
        .unwrap();
    assert_eq!(
        meta(&pool, &client).unwrap().status.idle_deadline_ms,
        expiration.status.idle_deadline_ms
    );
    crate::asyncrt::sleep_until(expiration.status.idle_deadline_ms.saturating_add(50)).await;
    assert!(
        fetch(app, &client, "/status", Duration::from_millis(500))
            .await
            .is_err(),
        "expired cache revived by unused held lease"
    );
    drop(unused);
    until(
        || owned().is_empty() && pool.snapshot().vms == 0,
        "expired cache did not reap",
    )
    .await;
    assert_eq!(
        (
            pool.snapshot().cached_vms,
            pool.snapshot().cached_nominal_mib,
            pool.snapshot().retained_limit_bytes
        ),
        (0, 0, 0)
    );
    let (replacement, replacement_group) = launch(&pool, app, &target, &parent, owned).await;
    assert!(!Arc::ptr_eq(&replacement.client.pin, &client.pin));
    let fresh = fetch(app, &replacement, "/status", Duration::from_secs(10))
        .await
        .unwrap();
    assert_ne!(fresh["identity"], original["identity"]);
    let replacement_client = replacement.client.clone();
    drop(replacement);
    cached(
        &pool,
        &replacement_client,
        &replacement_group,
        TARGET,
        owned,
        &recorder,
    )
    .await;
    let parent_after = pool.templates.memory_sample().unwrap().unwrap();
    assert_eq!(
        (
            parent_before.oom,
            parent_before.oom_kill,
            parent_before.oom_group_kill
        ),
        (
            parent_after.oom,
            parent_after.oom_kill,
            parent_after.oom_group_kill
        ),
        "retained cache unexpectedly caused OOM"
    );
    sampling_stop.cancel();
    sampling.await.unwrap();
    timeout(Duration::from_secs(30), pool.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(owned().is_empty() && !parent.exists().unwrap());
    let final_state = pool.snapshot();
    assert_eq!(
        (
            final_state.vms,
            final_state.memory_mib,
            final_state.quarantined,
            final_state.cached_vms,
            final_state.cached_nominal_mib,
            final_state.retained_limit_bytes
        ),
        (0, 0, 0, 0, 0, 0)
    );
    let leaked: Vec<_> = recorder
        .groups
        .lock()
        .unwrap()
        .iter()
        .filter(|path| path.exists())
        .cloned()
        .collect();
    assert!(leaked.is_empty());
    recorder.emit(json!({"kind":"cleanup","ownedPids":owned(),"remainingCgroups":leaked,
        "cachedShutdownProved":true,"pending":["multiple-host margin calibration","repeat density trials","production density"]}));
    eprintln!(
        "T29_RESULT {}",
        json!({"scenario":"t33::memory_cache","status":"passed",
        "duration_ms":recorder.began.elapsed().as_millis(),"output":output,
        "scope":"enforced retained cache, retained-client admission, bounded nominal density and cleanup; no universal margin claim"})
    );
}

#[tokio::test]
#[ignore = "requires diagnostic JS image, Linux KVM and explicit retained-cache run"]
async fn t33_memory_cache() {
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_CACHE_CHECK_ONLY").is_some());
    bridge_check("memory-cache").await;
}
#[test]
fn quarantine_preserves_enforced_cache_claim_without_releasing_nominal_ram() {
    let mut original = VmPoolSnapshot {
        vms: 1,
        memory_mib: 12288,
        quarantined: 1,
        ..VmPoolSnapshot::default()
    };
    original.retain(12288, 256 * MIB).unwrap();
    assert_eq!(
        (
            original.vms,
            original.memory_mib,
            original.quarantined,
            original.cached_vms
        ),
        (1, 12288, 1, 1)
    );
    assert_eq!(
        original.physical_bytes(guard(), original.memory_mib, original.vms),
        Some(768 * MIB)
    );
    let mut unguarded = guard();
    unguarded.idle_cache = None;
    assert!(original
        .physical_bytes(unguarded, original.memory_mib, original.vms)
        .is_none());
    let mut invalid = original;
    invalid.cached_nominal_mib = 12289;
    assert!(invalid
        .physical_bytes(guard(), invalid.memory_mib, invalid.vms)
        .is_none());
    assert_eq!(original.retained_limit_bytes, 256 * MIB);
}
