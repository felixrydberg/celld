//! Actual owner death and test-only withheld completion after a real successful Resume.
use super::freeze_baseline::OwnedVmm;
use super::guard_baseline::Group;
use super::lifecycle::{Gate, Phase};
use super::lifecycle_baseline::gate;
use super::memory_baseline::{activate_bundle, bridge_check, config, name, stage, Recorder};
use super::*;
use celld_applications::invocation_wire::HttpRequest;
use futures_util::StreamExt;
use serde_json::{json, Value};

const MIB: u64 = 1024 * 1024;
const SOURCE: &str = include_str!("../tests/fixtures/t33-memory/freeze.mjs");
const WRAPPER: &str =
    "\nexport default {fetch(request, env, ctx) {return original.fetch(request, env, ctx);}};\n";
pub(super) fn guard() -> MemoryGuard {
    MemoryGuard {
        max_bytes: 14 * 1024 * MIB,
        high_bytes: 13 * 1024 * MIB,
        overhead_bytes: 256 * MIB,
        shared_cache_bytes: 256 * MIB,
        idle_cache: Some(IdleCache {
            max_bytes: 4 * 1024 * MIB,
            retained_margin_bytes: 128 * MIB,
        }),
    }
}
pub(super) async fn fetch(app: &crate::actor::AppHandle, client: &VmClient) -> Result<Value> {
    let mut response = client
        .invoke_stateless(
            app,
            Duration::from_secs(5),
            HttpRequest {
                method: "GET".into(),
                url: "http://cache-failure/status".into(),
                headers: vec![],
                body: vec![],
            },
            None,
        )
        .await?;
    if let Some(mut stream) = response.stream.take() {
        while let Some(chunk) = stream.next().await {
            response
                .body
                .extend(chunk.map_err(|error| anyhow::anyhow!("failure fixture stream: {error}"))?);
        }
    }
    ensure!(
        response.status == 200,
        "failure fixture answered {}",
        response.status
    );
    Ok(serde_json::from_slice(&response.body)?)
}
async fn until(duration: Duration, mut ready: impl FnMut() -> bool, label: &str) {
    timeout(duration, async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("failure fixture timed out: {label}"));
}
fn retained(pool: &VmPool, client: &VmClient) -> u64 {
    let state = pool.state.lock().unwrap();
    state
        .slots
        .values()
        .find_map(|slot| match slot {
            Slot::Ready(ready) if Arc::ptr_eq(&ready.client.pin, &client.pin) => {
                Some(ready.retained_limit.load(Ordering::Relaxed))
            }
            _ => None,
        })
        .expect("owned ready slot disappeared before failure")
}
fn safe_claim(snapshot: VmPoolSnapshot, limit: u64) {
    assert_eq!(
        (snapshot.vms, snapshot.memory_mib, snapshot.quarantined),
        (1, 1024, 0),
        "unreaped owner lost nominal charge"
    );
    match snapshot.cached_vms {
        1 => assert_eq!(
            (snapshot.cached_nominal_mib, snapshot.retained_limit_bytes),
            (1024, limit),
            "unreaped owner lost enforced retained claim"
        ),
        0 => assert_eq!(
            (snapshot.cached_nominal_mib, snapshot.retained_limit_bytes),
            (0, 0),
            "partial full claim"
        ),
        _ => panic!("invalid one-owner retained count"),
    }
    let bytes = snapshot
        .physical_bytes(guard(), snapshot.memory_mib, snapshot.vms)
        .unwrap();
    assert!(bytes >= limit + 512 * MIB && bytes <= guard().max_bytes);
}
fn no_claim(snapshot: VmPoolSnapshot) {
    assert_eq!(
        (
            snapshot.vms,
            snapshot.memory_mib,
            snapshot.starting,
            snapshot.ready,
            snapshot.quarantined,
            snapshot.cached_vms,
            snapshot.cached_nominal_mib,
            snapshot.retained_limit_bytes
        ),
        (0, 0, 0, 0, 0, 0, 0, 0)
    );
}
pub(super) fn signal(group: &Group, owned: fn() -> HashSet<u32>, signal: i32) {
    group.verify_member(owned).unwrap();
    let pid: i32 = group.member.unwrap().0.try_into().unwrap();
    assert!(pid > 0);
    // Direct child, start ticks, cgroup membership and leaf identity are proved immediately before this signal.
    assert_eq!(
        unsafe { libc::kill(pid, signal) },
        0,
        "owned signal failed: {}",
        std::io::Error::last_os_error()
    );
}
fn stopped(group: &Group, owned: fn() -> HashSet<u32>) -> bool {
    group.verify_member(owned).unwrap();
    let stat = std::fs::read_to_string(format!("/proc/{}/stat", group.member.unwrap().0)).unwrap();
    matches!(
        stat.rsplit_once(')').unwrap().1.split_whitespace().next(),
        Some("T" | "t")
    )
}
struct Stopped<'a> {
    group: &'a Group,
    owned: fn() -> HashSet<u32>,
    killed: bool,
}
impl Stopped<'_> {
    fn kill(&mut self) {
        signal(self.group, self.owned, libc::SIGKILL);
        self.killed = true;
    }
}
impl Drop for Stopped<'_> {
    fn drop(&mut self) {
        // Unwind releases this fixture-only stop if the still-owned process has not been killed.
        if !self.killed && self.group.verify_member(self.owned).is_ok() {
            let pid: i32 = self.group.member.unwrap().0.try_into().unwrap();
            unsafe {
                libc::kill(pid, libc::SIGCONT);
            }
        }
    }
}
pub(super) async fn cached(
    pool: &VmPool,
    client: &VmClient,
    group: &Group,
    owned: fn() -> HashSet<u32>,
    recorder: &Recorder,
    label: &str,
) -> (Arc<Gate>, u64) {
    let gate = gate(pool, client);
    until(
        Duration::from_secs(8),
        || gate.status().phase == Phase::Cached && retained(pool, client) > 0,
        "assigned owner did not physically cache",
    )
    .await;
    let status = gate.status();
    let limit = retained(pool, client);
    assert_eq!(
        OwnedVmm::identify(owned)
            .read_json("/", owned)
            .await
            .unwrap()["state"],
        "Paused"
    );
    group.verify_member(owned).unwrap();
    assert_eq!(group.scalar("memory.max").unwrap(), limit);
    assert!(limit < 1024 * MIB);
    assert_eq!(group.scalar("memory.swap.max").unwrap(), 0);
    assert_eq!(group.scalar("memory.oom.group").unwrap(), 1);
    assert_eq!(gate.status(), status, "cached barrier changed");
    safe_claim(pool.snapshot(), limit);
    assert_eq!(
        (
            pool.snapshot().cached_vms,
            pool.snapshot().cached_nominal_mib
        ),
        (1, 1024)
    );
    recorder.emit(
        json!({"kind":"failureCachedBarrier","arm":label,"pid":group.member.unwrap().0,
        "startTicks":group.member.unwrap().1,"idleDeadlineMs":status.idle_deadline_ms,
        "retainedLimitBytes":limit,"currentBytes":group.scalar("memory.current").unwrap(),
        "peakBytes":group.scalar("memory.peak").unwrap(),"ledger":pool.status()}),
    );
    (gate, limit)
}
fn observe_reap(
    pool: Arc<VmPool>,
    group: Arc<Group>,
    limit: u64,
    owned: fn() -> HashSet<u32>,
    recorder: Arc<Recorder>,
    arm: &'static str,
) -> AbortOnDropHandle<u32> {
    AbortOnDropHandle::new(tokio::spawn(async move {
        timeout(Duration::from_secs(25),async {
            let mut charged_frames = 0_u32;
            let mut last = None;
            loop {
                let before = pool.snapshot();
                let exists = group.exists().unwrap();
                let present = owned().contains(&group.member.unwrap().0);
                let after = pool.snapshot();
                if before == after {
                    if exists || present { safe_claim(before,limit); charged_frames += 1; }
                    if last != Some(before) {
                        recorder.emit(json!({"kind":"failureReapLedger","arm":arm,"pid":group.member.unwrap().0,
                            "startTicks":group.member.unwrap().1,"pidPresent":present,"ownedLeafPresent":exists,
                            "chargedVms":before.vms,"nominalMib":before.memory_mib,"retainedVms":before.cached_vms,
                            "retainedLimitBytes":before.retained_limit_bytes,"quarantined":before.quarantined}));
                        last = Some(before);
                    }
                    if before.vms == 0 {
                        assert!(!exists && !present,"released capacity preceded actual owned PID/leaf reap");
                        no_claim(before);
                        return charged_frames;
                    }
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await.expect("owned death did not prove reap within 25 seconds")
    }))
}
pub(super) async fn acquire(
    pool: &VmPool,
    app: &crate::actor::AppHandle,
    application: &ApplicationId,
    parent: &Group,
    owned: fn() -> HashSet<u32>,
) -> (VmLease, Arc<Group>, Value) {
    assert!(owned().is_empty());
    no_claim(pool.snapshot());
    let lease = timeout(
        Duration::from_secs(60),
        pool.acquire(app, application, &name("failure"), &name("memory")),
    )
    .await
    .unwrap()
    .unwrap();
    let original = fetch(app, &lease).await.unwrap();
    assert_eq!(
        original["calls"], 1,
        "new owner executed an old failed invocation"
    );
    assert_eq!(original["checksum"], 77 * (8 * 1024 * 1024 / 4096));
    let pids = owned();
    assert_eq!(pids.len(), 1);
    let group = Arc::new(
        Group::leaf(*pids.iter().next().unwrap(), parent, owned)
            .unwrap()
            .unwrap(),
    );
    (lease, group, original)
}
async fn old_fails(
    app: &crate::actor::AppHandle,
    client: &VmClient,
    pool: &VmPool,
    owned: fn() -> HashSet<u32>,
) {
    assert!(
        timeout(Duration::from_secs(7), fetch(app, client))
            .await
            .unwrap()
            .is_err(),
        "original retained client survived owner death"
    );
    assert!(
        owned().is_empty(),
        "failed retained client automatically started a new owner"
    );
    no_claim(pool.snapshot());
    timeout(Duration::from_secs(3), app.snapshot())
        .await
        .expect("host actor failed after owned VM death");
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
        std::env::var_os("CELLD_APPLICATION_MEMORY_OUTPUT").expect("fresh failure JSONL required"),
    );
    let recorder = Recorder::new(&output, "cache-failure-bootstrap");
    recorder.emit(json!({"kind":"failureInputs","class":ApplicationSizeClass::Basic,"guard":guard(),
        "kernelSha256":options.kernel.sha256,"runtimeSha256":options.runtime.sha256,
        "firecrackerSha256":content_digest(&std::fs::read(&options.firecracker).unwrap()),
        "sourceSha256":content_digest(SOURCE.as_bytes()),"wrapperSha256":content_digest(WRAPPER.as_bytes()),
        "ttlSeconds":30,"scope":"actual Cached and Resuming process death plus test-only real Resume response loss and post-API completion withholding; conservative accounting and no automatic replay",
        "stressOnlyGuardAndMargin":true}));
    let mut configuration = config(options, 0, 30);
    configuration.max_vms = 1;
    configuration.memory_guard = Some(guard());
    let pool = Arc::new(VmPool::new(configuration, store.clone()).unwrap());
    let parent = Arc::new(Group::registered_parent().unwrap());
    let (sampling_stop, sampling) = recorder.start_sampler(&pool, owned);
    timeout(Duration::from_secs(1500), pool.prepare(app))
        .await
        .unwrap()
        .unwrap();
    until(
        Duration::from_secs(8),
        || {
            VmPool::refresh_pressure(&pool.pressure, &pool.templates);
            !VmPool::under_pressure(&pool.pressure)
        },
        "initial pressure did not recover",
    )
    .await;
    let application = stage(
        store,
        options,
        "memory-cache-failure",
        ApplicationSizeClass::Basic,
    )
    .await;
    assert_eq!(SOURCE.matches("export default {").count(), 1);
    let source = SOURCE.replacen("export default {", "const original = {", 1) + WRAPPER;
    let bundle =
        crate::application_image::build(&[("main.mjs".into(), source.into_bytes())]).unwrap();
    activate_bundle(
        store,
        options,
        &application,
        "failure",
        bundle,
        (
            "main.mjs",
            json!({"compatibility_date":"2026-03-01"}),
            json!({}),
        ),
    )
    .await;

    recorder.context(
        "failure-cached-owner-death",
        Some(ApplicationSizeClass::Basic),
        0,
    );
    let (lease, first_group, first) = acquire(&pool, app, &application, &parent, owned).await;
    let first_client = lease.client.clone();
    drop(lease);
    let (_, limit) = cached(
        &pool,
        &first_client,
        &first_group,
        owned,
        &recorder,
        "Cached",
    )
    .await;
    let proof = observe_reap(
        pool.clone(),
        first_group.clone(),
        limit,
        owned,
        recorder.clone(),
        "Cached",
    );
    signal(&first_group, owned, libc::SIGKILL);
    let cached_frames = proof.await.unwrap();
    assert!(!first_group.exists().unwrap() && parent.exists().unwrap());
    old_fails(app, &first_client, &pool, owned).await;
    recorder.emit(json!({"kind":"cachedOwnerDeath","pid":first_group.member.unwrap().0,
        "startTicks":first_group.member.unwrap().1,"chargedSampleFrames":cached_frames,
        "originalClientFailed":true,"replacementRequiresExplicitAcquire":true,"ledger":pool.status()}));

    recorder.context(
        "failure-resuming-owner-death",
        Some(ApplicationSizeClass::Basic),
        0,
    );
    let (lease, second_group, second) = acquire(&pool, app, &application, &parent, owned).await;
    assert_ne!(second["identity"], first["identity"]);
    assert!(!Arc::ptr_eq(&first_client.pin, &lease.client.pin));
    let second_client = lease.client.clone();
    drop(lease);
    let (gate, limit) = cached(
        &pool,
        &second_client,
        &second_group,
        owned,
        &recorder,
        "Resuming",
    )
    .await;
    let original_deadline = gate.status().idle_deadline_ms;
    signal(&second_group, owned, libc::SIGSTOP);
    let mut stopped_owner = Stopped {
        group: &second_group,
        owned,
        killed: false,
    };
    until(
        Duration::from_millis(500),
        || stopped(&second_group, owned),
        "SIGSTOP was not observed",
    )
    .await;
    let (actor, retained_client) = (app.clone(), second_client.clone());
    let pending = AbortOnDropHandle::new(tokio::spawn(async move {
        fetch(&actor, &retained_client).await
    }));
    until(
        Duration::from_secs(1),
        || {
            gate.status().phase == Phase::Resuming
                && pool.snapshot().cached_vms == 0
                && second_group.scalar("memory.max").unwrap() == 1280 * MIB
        },
        "owned stopped process did not enter real full-budget Resume",
    )
    .await;
    assert_eq!(
        gate.status().idle_deadline_ms,
        original_deadline,
        "Resume attempt refreshed idle TTL"
    );
    assert!(
        stopped(&second_group, owned),
        "stopped original owner unexpectedly ran"
    );
    safe_claim(pool.snapshot(), limit);
    assert_eq!(
        (
            pool.snapshot().cached_vms,
            pool.snapshot().retained_limit_bytes
        ),
        (0, 0)
    );
    recorder.emit(json!({"kind":"resumeWithoutAcknowledgement","pid":second_group.member.unwrap().0,
        "startTicks":second_group.member.unwrap().1,"phase":"Resuming","processState":"stopped",
        "originalIdleDeadlineMs":original_deadline,"fullLeafLimitBytes":second_group.scalar("memory.max").unwrap(),
        "ledger":pool.status(),"scope":"process stopped before API execution, then killed; post-execution response loss unqualified"}));
    let proof = observe_reap(
        pool.clone(),
        second_group.clone(),
        limit,
        owned,
        recorder.clone(),
        "Resuming",
    );
    stopped_owner.kill();
    assert!(
        timeout(Duration::from_secs(7), pending)
            .await
            .unwrap()
            .unwrap()
            .is_err(),
        "original pending wake survived Resume owner death"
    );
    let resume_frames = proof.await.unwrap();
    assert!(!second_group.exists().unwrap() && parent.exists().unwrap());
    old_fails(app, &second_client, &pool, owned).await;
    recorder.emit(json!({"kind":"resumingOwnerDeath","pid":second_group.member.unwrap().0,
        "startTicks":second_group.member.unwrap().1,"chargedSampleFrames":resume_frames,
        "originalPendingClientFailed":true,"replacementRequiresExplicitAcquire":true,"ledger":pool.status()}));

    recorder.context(
        "failure-resume-completion-withheld",
        Some(ApplicationSizeClass::Basic),
        0,
    );
    let (lease, third_group, third) = acquire(&pool, app, &application, &parent, owned).await;
    assert_ne!(third["identity"], first["identity"]);
    assert_ne!(third["identity"], second["identity"]);
    let third_client = lease.client.clone();
    drop(lease);
    let (third_gate, limit) = cached(
        &pool,
        &third_client,
        &third_group,
        owned,
        &recorder,
        "ResumeCompletionWithheld",
    )
    .await;
    let original_deadline = third_gate.status().idle_deadline_ms;
    third_group.verify_member(owned).unwrap();
    let (pid, start_ticks) = third_group.member.unwrap();
    let mut injection =
        super::assigned_supervisor::resume_ack_test::Withheld::arm(pid, start_ticks).unwrap();
    let proof = observe_reap(
        pool.clone(),
        third_group.clone(),
        limit,
        owned,
        recorder.clone(),
        "ResumeCompletionWithheld",
    );
    let (actor, retained_client) = (app.clone(), third_client.clone());
    let pending = AbortOnDropHandle::new(tokio::spawn(async move {
        fetch(&actor, &retained_client).await
    }));
    let (executed_ms, resume_deadline_ms) =
        timeout(Duration::from_secs(3), &mut injection.executed)
            .await
            .expect("real Resume did not complete")
            .unwrap();
    assert!(
        executed_ms < resume_deadline_ms,
        "Resume completed after original timeout"
    );
    let instance = OwnedVmm::identify_pid(pid, owned)
        .read_json("/", owned)
        .await
        .unwrap();
    assert_eq!(
        instance["state"], "Running",
        "real Resume did not run the guest"
    );
    third_group.verify_member(owned).unwrap();
    let status = third_gate.status();
    assert_eq!((status.phase, status.active_events), (Phase::Resuming, 0));
    assert_eq!(
        status.idle_deadline_ms, original_deadline,
        "withheld completion refreshed TTL"
    );
    let ledger = pool.snapshot();
    assert_eq!((ledger.cached_vms, ledger.retained_limit_bytes), (0, 0));
    let full_limit = third_group.scalar("memory.max").unwrap();
    assert_eq!(full_limit, 1280 * MIB);
    safe_claim(ledger, limit);
    assert_eq!(
        ledger
            .physical_bytes(guard(), ledger.memory_mib, ledger.vms)
            .unwrap(),
        1536 * MIB
    );
    assert_eq!(third_gate.status(), status, "Running barrier changed gate");
    assert_eq!(
        pool.snapshot(),
        ledger,
        "Running barrier changed reservation"
    );
    assert!(
        crate::asyncrt::mono_ms() < resume_deadline_ms,
        "Running barrier missed original timeout"
    );
    recorder.emit(json!({"kind":"resumeCompletionWithheldBarrier","pid":pid,"startTicks":start_ticks,
        "realApiCompleted":true,"instance":instance,"phase":"Resuming","activeEvents":status.active_events,
        "originalIdleDeadlineMs":original_deadline,"resumeDeadlineMs":resume_deadline_ms,
        "executedMs":executed_ms,"fullLeafLimitBytes":full_limit,
        "chargedVms":ledger.vms,"nominalMib":ledger.memory_mib,"retainedVms":ledger.cached_vms,
        "retainedLimitBytes":ledger.retained_limit_bytes,"physicalBytes":1536 * MIB,
        "scope":"test-only supervisor acknowledgement completion withheld after real successful API completion; not actual network ACK loss"}));
    assert!(
        timeout(Duration::from_secs(7), pending)
            .await
            .unwrap()
            .unwrap()
            .is_err(),
        "pending request survived withheld Resume completion"
    );
    assert!(
        crate::asyncrt::mono_ms() >= resume_deadline_ms,
        "failure preceded original Resume timeout"
    );
    assert_eq!(third_gate.status().phase, Phase::Retiring);
    let withheld_frames = proof.await.unwrap();
    assert!(
        withheld_frames > 0,
        "withheld completion lacked charged reap observations"
    );
    drop(injection);
    assert!(!third_group.exists().unwrap() && parent.exists().unwrap());
    old_fails(app, &third_client, &pool, owned).await;
    recorder.emit(json!({"kind":"resumeCompletionWithheldFailure","pid":pid,"startTicks":start_ticks,
        "chargedSampleFrames":withheld_frames,"originalPendingClientFailed":true,
        "replacementRequiresExplicitAcquire":true,"ledger":pool.status(),
        "scope":"original transition timeout retired and reaped the actually Running owner; no signal injection"}));

    recorder.context(
        "failure-resume-response-loss",
        Some(ApplicationSizeClass::Basic),
        0,
    );
    let (lease, fourth_group, fourth) = acquire(&pool, app, &application, &parent, owned).await;
    assert_ne!(fourth["identity"], first["identity"]);
    assert_ne!(fourth["identity"], second["identity"]);
    assert_ne!(fourth["identity"], third["identity"]);
    let fourth_client = lease.client.clone();
    drop(lease);
    let (fourth_gate, limit) = cached(
        &pool,
        &fourth_client,
        &fourth_group,
        owned,
        &recorder,
        "ResumeResponseLost",
    )
    .await;
    let started = std::time::Instant::now();
    let original_idle_deadline = fourth_gate.status().idle_deadline_ms;
    let resume_deadline_ms =
        original_idle_deadline.min(crate::asyncrt::mono_ms().saturating_add(2000));
    fourth_group.verify_member(owned).unwrap();
    let (pid, start_ticks) = fourth_group.member.unwrap();
    let mut response_loss =
        super::assigned_supervisor::resume_ack_test::ResponseLost::arm(pid, start_ticks).unwrap();
    let proof = observe_reap(
        pool.clone(),
        fourth_group.clone(),
        limit,
        owned,
        recorder.clone(),
        "ResumeResponseLost",
    );
    let (actor, retained_client) = (app.clone(), fourth_client.clone());
    let pending = AbortOnDropHandle::new(tokio::spawn(async move {
        fetch(&actor, &retained_client).await
    }));
    let executed_ms = timeout(Duration::from_secs(3), &mut response_loss.executed)
        .await
        .expect("actual Resume response was not consumed by the proxy")
        .unwrap();
    assert!(
        executed_ms < resume_deadline_ms && started.elapsed() < Duration::from_secs(2),
        "actual Resume response loss did not occur within the original 2s transition window"
    );
    let instance = OwnedVmm::identify_pid(pid, owned)
        .read_json("/", owned)
        .await
        .unwrap();
    assert_eq!(
        instance["state"], "Running",
        "the original Firecracker processed Resume before its response was suppressed"
    );
    fourth_group.verify_member(owned).unwrap();
    let status = fourth_gate.status();
    assert!(
        matches!(status.phase, Phase::Resuming | Phase::Retiring),
        "response loss left an unexpected lifecycle phase {:?}",
        status.phase
    );
    let ledger = pool.snapshot();
    assert_eq!((ledger.cached_vms, ledger.retained_limit_bytes), (0, 0));
    let full_limit = fourth_group.scalar("memory.max").unwrap();
    assert_eq!(full_limit, 1280 * MIB);
    safe_claim(ledger, limit);
    assert_eq!(
        ledger
            .physical_bytes(guard(), ledger.memory_mib, ledger.vms)
            .unwrap(),
        1536 * MIB
    );
    recorder.emit(json!({"kind":"resumeResponseLostBarrier","pid":pid,"startTicks":start_ticks,
        "realApiCompleted":true,"responseForwarded":false,"instance":instance,
        "phase":format!("{:?}",status.phase),"activeEvents":status.active_events,
        "originalIdleDeadlineMs":original_idle_deadline,"resumeDeadlineMs":resume_deadline_ms,
        "executedMs":executed_ms,"fullLeafLimitBytes":full_limit,
        "chargedVms":ledger.vms,"nominalMib":ledger.memory_mib,"retainedVms":ledger.cached_vms,
        "retainedLimitBytes":ledger.retained_limit_bytes,"physicalBytes":1536 * MIB,
        "scope":"test-only Unix proxy forwarded Resume to the exact original Firecracker API socket, consumed the real response, and suppressed it on the host connection"}));
    assert!(
        timeout(Duration::from_secs(7), pending)
            .await
            .unwrap()
            .unwrap()
            .is_err(),
        "pending request survived actual Resume response loss"
    );
    assert_eq!(fourth_gate.status().phase, Phase::Retiring);
    let response_loss_frames = proof.await.unwrap();
    assert!(
        response_loss_frames > 0,
        "actual response loss lacked charged reap observations"
    );
    drop(response_loss);
    assert!(!fourth_group.exists().unwrap() && parent.exists().unwrap());
    old_fails(app, &fourth_client, &pool, owned).await;
    recorder.emit(json!({"kind":"resumeResponseLostFailure","pid":pid,"startTicks":start_ticks,
        "chargedSampleFrames":response_loss_frames,"originalPendingClientFailed":true,
        "replacementRequiresExplicitAcquire":true,"ledger":pool.status(),
        "scope":"actual Firecracker Resume succeeded while its Unix HTTP response was lost; original owner retired and reaped"}));

    recorder.context(
        "failure-fresh-replacement",
        Some(ApplicationSizeClass::Basic),
        0,
    );
    // Corrupt only this fixture's private snapshot state after every original owner was reaped.
    assert!(owned().is_empty());
    let sample = super::pressure::Sample {
        memory: pool.templates.memory_sample().unwrap().unwrap(),
        available_bytes: super::pressure::host_available().unwrap(),
    };
    pool.pressure
        .lock()
        .unwrap()
        .observe(Instant::now() - Duration::from_secs(3), Some(sample));
    assert!(VmPool::under_pressure(&pool.pressure));
    let stale_started = Instant::now();
    assert!(
        timeout(
            Duration::from_millis(300),
            pool.acquire(app, &application, &name("failure"), &name("memory"))
        )
        .await
        .is_err(),
        "stale metrics admitted a new original owner"
    );
    no_claim(pool.snapshot());
    assert!(owned().is_empty());
    recorder.emit(
        json!({"kind":"staleMetricsRefused","injectedSampleAgeMs":3000,
        "originalCallerDeadlineMs":300,"elapsedUs":stale_started.elapsed().as_micros(),
        "ownedPids":owned(),"ledger":pool.status()}),
    );
    until(
        Duration::from_secs(8),
        || {
            VmPool::refresh_pressure(&pool.pressure, &pool.templates);
            !VmPool::under_pressure(&pool.pressure)
        },
        "stale pressure metrics did not recover through fresh stable samples",
    )
    .await;
    let states: Vec<_> = pool
        .templates
        .state_files()
        .into_iter()
        .map(|path| {
            let metadata = std::fs::symlink_metadata(&path).unwrap();
            assert!(metadata.is_file() && metadata.len() <= 4 * MIB);
            let bytes = std::fs::read(&path).unwrap();
            std::fs::write(&path, b"not a snapshot").unwrap();
            (path, bytes)
        })
        .collect();
    assert_eq!(states.len(), ApplicationSizeClass::ALL.len());
    let restore_started = Instant::now();
    let failed_restore = timeout(
        Duration::from_secs(15),
        pool.acquire(app, &application, &name("failure"), &name("memory")),
    )
    .await
    .expect("broken restore did not fail within its fixture bound");
    let restore_error = match failed_restore {
        Ok(_) => panic!("production startup silently accepted a broken snapshot"),
        Err(error) => format!("{error:#}"),
    };
    assert!(
        restore_error.contains("restore failed"),
        "wrong broken-restore failure: {restore_error}"
    );
    no_claim(pool.snapshot());
    assert!(owned().is_empty());
    recorder.emit(json!({"kind":"brokenRestoreRefused","elapsedUs":restore_started.elapsed().as_micros(),
        "error":restore_error,"ownedPids":owned(),"ledger":pool.status(),"automaticColdFallback":false}));
    for (path, bytes) in states {
        std::fs::write(path, bytes).unwrap();
    }
    let (lease, replacement_group, fresh) = acquire(&pool, app, &application, &parent, owned).await;
    assert_ne!(fresh["identity"], third["identity"]);
    assert_ne!(fresh["identity"], second["identity"]);
    assert_ne!(fresh["identity"], first["identity"]);
    assert!(!Arc::ptr_eq(&lease.client.pin, &second_client.pin));
    let replacement_client = lease.client.clone();
    assert!(timeout(Duration::from_secs(7), fetch(app, &first_client))
        .await
        .unwrap()
        .is_err());
    assert!(timeout(Duration::from_secs(7), fetch(app, &second_client))
        .await
        .unwrap()
        .is_err());
    assert!(timeout(Duration::from_secs(7), fetch(app, &third_client))
        .await
        .unwrap()
        .is_err());
    assert!(timeout(Duration::from_secs(7), fetch(app, &fourth_client))
        .await
        .unwrap()
        .is_err());
    assert!(!Arc::ptr_eq(&lease.client.pin, &third_client.pin));
    replacement_group.verify_member(owned).unwrap();
    let after_old_failures = fetch(app, &replacement_client).await.unwrap();
    assert_eq!(after_old_failures["identity"], fresh["identity"]);
    assert_eq!(
        after_old_failures["calls"], 2,
        "failed old request was replayed into explicit replacement"
    );
    drop(lease);
    cached(
        &pool,
        &replacement_client,
        &replacement_group,
        owned,
        &recorder,
        "replacement-shutdown",
    )
    .await;
    sampling_stop.cancel();
    sampling.await.unwrap();
    timeout(Duration::from_secs(30), pool.shutdown())
        .await
        .unwrap()
        .unwrap();
    no_claim(pool.snapshot());
    assert!(
        owned().is_empty() && !replacement_group.exists().unwrap() && !parent.exists().unwrap()
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
    recorder.emit(json!({"kind":"cleanup","ownedPids":owned(),"remainingCgroups":leaked,"cachedShutdownProved":true,
        "unqualified":["forced unproved reap/quarantine","durable external side-effect failure matrix"]}));
    eprintln!(
        "T29_RESULT {}",
        json!({"scenario":"t33::cache_failure","status":"passed",
        "duration_ms":recorder.began.elapsed().as_millis(),"output":output,
        "scope":"real cached/resuming owner death, actual Resume response loss, and test-only withheld completion after real Resume; original clients fail, safe claims, fresh explicit replacements and cleanup"})
    );
}
#[tokio::test]
#[ignore = "requires Linux KVM, lifecycle-capable diagnostic runtime and explicit cache failure selector"]
async fn t33_cache_failure() {
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_CACHE_FAILURE_CHECK_ONLY").is_some());
    bridge_check("cache-failure").await;
}
