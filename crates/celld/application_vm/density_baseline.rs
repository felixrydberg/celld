//! Separate-process conservative/full-charge versus retained-cache working-set pair.
use super::freeze_baseline::OwnedVmm;
use super::guard_baseline::Group;
use super::lifecycle::{Gate, Phase, Status};
use super::lifecycle_baseline::gate;
use super::memory_baseline::{
    activate_bundle, bridge_check, config, name, no_vms, request, stage, Recorder,
};
use super::*;
use serde_json::{json, Value};

const MIB: u64 = 1024 * 1024;
const SOURCE: &str = include_str!("../tests/fixtures/t33-memory/freeze.mjs");
const CLASSES: [ApplicationSizeClass; 3] = [
    ApplicationSizeClass::Standard4,
    ApplicationSizeClass::Standard1,
    ApplicationSizeClass::Basic,
];

fn guard(cache: bool) -> MemoryGuard {
    MemoryGuard {
        max_bytes: 14 * 1024 * MIB,
        high_bytes: 13 * 1024 * MIB,
        overhead_bytes: 256 * MIB,
        shared_cache_bytes: 256 * MIB,
        idle_cache: cache.then_some(IdleCache {
            max_bytes: 6 * 1024 * MIB,
            retained_margin_bytes: 128 * MIB,
        }),
    }
}

struct Owner {
    client: VmClient,
    gate: Arc<Gate>,
    vm: OwnedVmm,
    group: Group,
    initial: Value,
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
        .expect("density owner removed before stable barrier")
}
fn ledger(pool: &VmPool, cache: bool) {
    let snapshot = pool.snapshot();
    assert!(
        snapshot
            .physical_bytes(guard(cache), snapshot.memory_mib, snapshot.vms)
            .unwrap()
            <= guard(cache).max_bytes
    );
    assert_eq!(snapshot.quarantined, 0);
    if !cache {
        assert_eq!(
            (
                snapshot.cached_vms,
                snapshot.cached_nominal_mib,
                snapshot.retained_limit_bytes
            ),
            (0, 0, 0)
        );
    }
}
fn integrity(value: &Value, original: Option<&Value>) {
    assert_eq!(value["checksum"], 77 * (8 * 1024 * 1024 / 4096));
    if let Some(original) = original {
        assert_eq!(value["identity"], original["identity"]);
    }
}
async fn cached(
    pool: &VmPool,
    owner: &Owner,
    class: ApplicationSizeClass,
    cache: bool,
    owned: fn() -> HashSet<u32>,
    recorder: &Recorder,
) -> Status {
    let before = timeout(Duration::from_secs(10), async {
        loop {
            let status = owner.gate.status();
            assert_ne!(
                status.phase,
                Phase::Retiring,
                "owner retired before idle sample"
            );
            if status.phase == Phase::Cached && (!cache || retained(pool, &owner.client) > 0) {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("density owner never reached actual cached barrier");
    owner.group.verify_member(owned).unwrap();
    assert_eq!(
        owner.vm.read_json("/", owned).await.unwrap()["state"],
        "Paused"
    );
    let limit = retained(pool, &owner.client);
    assert_eq!(
        owner.group.scalar("memory.max").unwrap(),
        if cache {
            limit
        } else {
            (u64::from(class.resources().1) + 256) * MIB
        }
    );
    assert_eq!(owner.group.scalar("memory.swap.max").unwrap(), 0);
    assert_eq!(owner.group.scalar("memory.oom.group").unwrap(), 1);
    if cache {
        assert!(limit < u64::from(class.resources().1) * MIB);
    }
    assert_eq!(
        owner.gate.status(),
        before,
        "cache changed during owned Pause proof"
    );
    owner.group.verify_member(owned).unwrap();
    ledger(pool, cache);
    recorder.emit(json!({"kind":"densityPaused","class":class,"pid":owner.group.member.unwrap().0,
        "startTicks":owner.group.member.unwrap().1,"hostMonoMs":crate::asyncrt::mono_ms(),
        "epoch":before.epoch,"idleDeadlineMs":before.idle_deadline_ms,"retainedLimitBytes":limit,"ledger":pool.status()}));
    before
}

#[allow(clippy::too_many_arguments)]
async fn visit(
    app: &crate::actor::AppHandle,
    pool: &VmPool,
    application: &ApplicationId,
    class: ApplicationSizeClass,
    previous: Option<Owner>,
    parent: &Group,
    cache: bool,
    owned: fn() -> HashSet<u32>,
    recorder: &Recorder,
    cycle: usize,
    disabled_between_bursts: bool,
) -> (Owner, bool, VmPoolSnapshot) {
    let before = owned();
    let previous_identity = previous
        .as_ref()
        .map(|owner| owner.initial["identity"].clone());
    let started = Instant::now();
    let lease = timeout(
        Duration::from_secs(120),
        pool.acquire(app, application, &name("density"), &name("memory")),
    )
    .await
    .unwrap()
    .unwrap();
    let acquire_us = started.elapsed().as_micros();
    // Owned PID membership can include an expired owner while acquire waits for its reap.
    let live = previous
        .as_ref()
        .is_some_and(|owner| Arc::ptr_eq(&owner.client.pin, &lease.client.pin));
    recorder.emit(json!({"kind":"acquire","class":class,"cycle":cycle,
        "mode":if live {"paused-existing-acquire"} else {"fresh-assigned-acquire"},"latencyUs":acquire_us,
        "includesOnDemandTemplateBuild":false,"application":application}));
    let owner = if live {
        let owner = previous.unwrap();
        assert!(
            Arc::ptr_eq(&owner.client.pin, &lease.client.pin),
            "live cache reference replaced at acquire"
        );
        owner
    } else {
        let added: Vec<_> = owned().difference(&before).copied().collect();
        assert_eq!(
            added.len(),
            1,
            "startup pool is zero; fresh identity must add one owner"
        );
        let group = Group::leaf(added[0], parent, owned).unwrap().unwrap();
        let client = lease.client.clone();
        let gate = gate(pool, &client);
        let vm = OwnedVmm::identify_pid(added[0], owned);
        Owner {
            client,
            gate,
            vm,
            group,
            initial: Value::Null,
        }
    };
    let started = Instant::now();
    let response = async {
        let result = request(app, &lease, "/status", recorder, class).await;
        (result, started.elapsed().as_micros())
    };
    let resumed = async {
        if !live {
            return None;
        }
        timeout(Duration::from_secs(15), async {
            while owner.gate.status().phase != Phase::Running {
                assert_ne!(owner.gate.status().phase, Phase::Retiring);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("cached wake never committed Running");
        Some(started.elapsed().as_micros())
    };
    let ((first, response_us), resume_observed_us) = tokio::join!(response, resumed);
    integrity(&first, live.then_some(&owner.initial));
    if !live {
        if let Some(previous_identity) = previous_identity {
            assert_ne!(first["identity"], previous_identity);
        }
    }
    owner.group.verify_member(owned).unwrap();
    assert_eq!(
        owner.vm.read_json("/", owned).await.unwrap()["state"],
        "Running"
    );
    recorder.emit(json!({"kind":"request","class":class,"cycle":cycle,
        "operation":if live {"cached-resume-response"} else {"fresh-first-response"},"latencyUs":response_us,
        "acquirePlusResponseUs":acquire_us + response_us,"resumeToRunningObservedUs":resume_observed_us,"resumePollIntervalMs":1,"cacheHit":live,"pid":owner.group.member.unwrap().0,
        "startTicks":owner.group.member.unwrap().1,"application":application}));
    // An already held lease keeps this second event running; it is not a thaw trial.
    let started = Instant::now();
    let warm = request(app, &lease, "/status", recorder, class).await;
    integrity(&warm, Some(&first));
    recorder.emit(json!({"kind":"request","class":class,"cycle":cycle,
        "operation":"unpaused-warm-response","latencyUs":started.elapsed().as_micros(),"pid":owner.group.member.unwrap().0}));
    let owner = Owner {
        initial: first,
        ..owner
    };
    let burst_snapshot = pool.snapshot();
    recorder.emit(
        json!({"kind":"densityBurstComplete","application":application,"cycle":cycle,
        "hostMonoMs":crate::asyncrt::mono_ms(),"ledger":pool.status()}),
    );
    drop(lease);
    if disabled_between_bursts {
        retire_burst(pool, &owner, cache, owned, recorder).await;
    } else {
        cached(pool, &owner, class, cache, owned, recorder).await;
    }
    (owner, live, burst_snapshot)
}

async fn retire_burst(
    pool: &VmPool,
    owner: &Owner,
    cache: bool,
    owned: fn() -> HashSet<u32>,
    recorder: &Recorder,
) {
    let before = owner.gate.status();
    let began = Instant::now();
    owner.group.verify_member(owned).unwrap();
    pool.retire_deployment(&owner.client.pin.application, &name("density"))
        .await;
    no_vms(pool, owned).await;
    assert!(!owner.group.exists().unwrap());
    assert_eq!(
        (
            pool.snapshot().vms,
            pool.snapshot().memory_mib,
            pool.snapshot().retained_limit_bytes
        ),
        (0, 0, 0)
    );
    ledger(pool, cache);
    // Re-enable the same catalog deployment only after its old owner was proved reaped.
    pool.readmit_deployment(&owner.client.pin.application, &name("density"));
    recorder.emit(
        json!({"kind":"densityDisabledRetire","policy":"disabled-between-bursts",
        "pid":owner.group.member.unwrap().0,"startTicks":owner.group.member.unwrap().1,
        "originalIdleDeadlineMs":before.idle_deadline_ms,"hostMonoMs":crate::asyncrt::mono_ms(),
        "elapsedUs":began.elapsed().as_micros(),"ledger":pool.status()}),
    );
}

async fn full_refusal(
    app: &crate::actor::AppHandle,
    pool: &Arc<VmPool>,
    target: &Owner,
    candidate: &ApplicationId,
    cache: bool,
    owned: fn() -> HashSet<u32>,
    recorder: &Arc<Recorder>,
) {
    let held = pool
        .acquire(
            app,
            &target.client.pin.application,
            &name("density"),
            &name("memory"),
        )
        .await
        .unwrap();
    let idle_deadline = target.gate.status().idle_deadline_ms;
    let recovery_started = Instant::now();
    timeout(Duration::from_secs(5), async {
        loop {
            assert_ne!(target.gate.status().phase, Phase::Retiring);
            VmPool::refresh_pressure(&pool.pressure, &pool.templates);
            if !VmPool::under_pressure(&pool.pressure) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("pressure latch did not recover before active-envelope refusal");
    assert_eq!(target.gate.status().idle_deadline_ms, idle_deadline);
    recorder.emit(
        json!({"kind":"densityPressureRecovery","elapsedUs":recovery_started.elapsed().as_micros(),
        "idleDeadlineMs":idle_deadline,"heldUnusedLease":true,"workNotStarted":true}),
    );
    let (actor, client, output) = (app.clone(), target.client.clone(), recorder.clone());
    let work = AbortOnDropHandle::new(tokio::spawn(async move {
        request(&actor, &client, "/slow", &output, CLASSES[0]).await
    }));
    timeout(Duration::from_secs(5), async {
        while target.gate.status().active_events == 0
            || target.gate.status().phase != Phase::Running
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(target.gate.status().phase, Phase::Running);
    assert_eq!(retained(pool, &target.client), 0);
    assert_eq!(
        target.group.scalar("memory.max").unwrap(),
        (12288 + 256) * MIB
    );
    ledger(pool, cache);
    let before = owned();
    let snapshot = pool.snapshot();
    assert_eq!((snapshot.memory_mib, snapshot.vms), (12288, 1));
    VmPool::refresh_pressure(&pool.pressure, &pool.templates);
    assert!(
        !VmPool::under_pressure(&pool.pressure),
        "full-envelope refusal conflated with pressure latch"
    );
    let rejected_bytes = guard(cache).reservation_bytes(12288 + 4096, 2).unwrap();
    assert!(rejected_bytes > guard(cache).max_bytes);
    let began = Instant::now();
    let refused = timeout(
        Duration::from_millis(500),
        pool.acquire(app, candidate, &name("density"), &name("memory")),
    )
    .await;
    let frontend_cancelled_wait = match refused {
        Err(_) => true,
        Ok(Err(error)) => {
            assert!(
                error.is::<CapacityExhausted>(),
                "unexpected admission failure: {error:#}"
            );
            false
        }
        Ok(Ok(_)) => panic!("full active envelopes admitted another class"),
    };
    assert_eq!(owned(), before, "blocked acquire started an additional VMM");
    assert!(
        target.gate.status().active_events > 0,
        "actual work ended before refusal proof"
    );
    assert_eq!(
        target.vm.read_json("/", owned).await.unwrap()["state"],
        "Running"
    );
    recorder.emit(json!({"kind":"densityFullRefusal","elapsedUs":began.elapsed().as_micros(),
        "waitingDeadlineMs":500,"existingFullMib":12288,"candidateFullMib":4096,"candidateReservationBytes":rejected_bytes,
        "actualWorkActive":true,"noNewOwner":true,"frontendCancelledWait":frontend_cancelled_wait,"ledger":pool.status()}));
    integrity(&work.await.unwrap(), Some(&target.initial));
    drop(held);
    cached(pool, target, CLASSES[0], cache, owned, recorder).await;
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
    let arm = std::env::var("CELLD_APPLICATION_MEMORY_DENSITY_ARM")
        .expect("set full or cache in a fresh process/run ID");
    assert!(arm == "full" || arm == "cache", "unknown density arm");
    let cache = arm == "cache";
    let retention =
        std::env::var("CELLD_APPLICATION_MEMORY_DENSITY_RETENTION").unwrap_or_else(|_| "30".into());
    let (ttl, disabled_between_bursts) = match retention.as_str() {
        "disabled" => (30_u64, true),
        "5" | "30" | "60" | "120" => (retention.parse::<u64>().unwrap(), false),
        _ => panic!("density retention must be disabled, 5, 30, 60 or 120"),
    };
    let reporting = std::env::var_os("CELLD_APPLICATION_MEMORY_REPORTING_DISABLED").is_none();
    let output = PathBuf::from(
        std::env::var_os("CELLD_APPLICATION_MEMORY_OUTPUT").expect("fresh arm JSONL required"),
    );
    let recorder = Recorder::new(&output, "cache-density-bootstrap");
    recorder.emit(json!({"kind":"provenance","arm":arm,"reporting":reporting,"guard":guard(cache),
        "kernelSha256":options.kernel.sha256,"runtimeSha256":options.runtime.sha256,
        "firecrackerSha256":content_digest(&std::fs::read(&options.firecracker).unwrap()),"sourceSha256":content_digest(SOURCE.as_bytes()),
        "classes":CLASSES,"identitySchedule":[0,0,1,2,0],"cycles":5,"startupPool":0,"nominalBudgetMib":24576,
        "idleTtlSeconds":ttl,"retentionPolicy":if disabled_between_bursts {"disabled-between-bursts"} else {"idle-ttl"},
        "retentionWindowSeconds":if disabled_between_bursts {None} else {Some(ttl)},"comparisonScope":"identical two-event bursts; shared bootstrap control precedes retention comparison",
        "touchedRetainedBytesPerIdentity":8*MIB,"maximumRetainedFixtureBufferBytes":24*MIB,
        "limits":"stress allowances only; full active reservation refusal, not concurrent physical near-ceiling allocations; no forced GC",
        "freshAcquireScope":"includes catalog/image/pin/restore/claim/readiness after three class templates are built; pure restore stages remain vm_startup INFO logs"}));
    let mut configuration = config(options, 0, ttl);
    configuration.max_vms = 3;
    configuration.memory_guard = Some(guard(cache));
    let pool = Arc::new(VmPool::new(configuration, store.clone()).unwrap());
    let parent = Group::registered_parent().unwrap();
    let (stop, sampler) = recorder.start_sampler(&pool, owned);
    pool.templates.begin_startup();
    for class in CLASSES {
        let mut launch = pool
            .config
            .options(&options.runtime.sha256, pool.templates.clone())
            .unwrap();
        (launch.vcpus, launch.memory_mib, launch.cpu_quota) = class.resources();
        let charge = pool.charge_startup(launch.memory_mib).await.unwrap();
        let templates = pool.templates.clone();
        let began = Instant::now();
        tokio::spawn(async move {
            let built = templates.build_template(&launch).await;
            charge.finish(
                !built
                    .as_ref()
                    .is_err_and(|error| error.is::<ReapUnproved>()),
            );
            built.map(drop)
        })
        .await
        .unwrap()
        .unwrap();
        recorder.emit(json!({"kind":"densityTemplateStartup","class":class,"durationUs":began.elapsed().as_micros()}));
        no_vms(&pool, owned).await;
    }
    pool.templates.finish_startup();
    let artifact =
        crate::application_image::build(&[("main.mjs".into(), SOURCE.as_bytes().to_vec())])
            .unwrap();
    let mut applications = Vec::new();
    for (index, class) in CLASSES.into_iter().enumerate() {
        let application = stage(store, options, &format!("density-{index}"), class).await;
        activate_bundle(
            store,
            options,
            &application,
            "density",
            artifact.clone(),
            (
                "main.mjs",
                json!({"compatibility_date":"2026-03-01"}),
                json!({}),
            ),
        )
        .await;
        applications.push(application);
    }
    recorder.context("cache-density-bootstrap-control", Some(CLASSES[0]), 0);
    let (hot, _, _) = visit(
        app,
        &pool,
        &applications[0],
        CLASSES[0],
        None,
        &parent,
        cache,
        owned,
        &recorder,
        0,
        false,
    )
    .await;
    full_refusal(app, &pool, &hot, &applications[1], cache, owned, &recorder).await;
    let balloon = hot.vm.read_json("/balloon", owned).await.unwrap();
    assert_eq!(balloon["amount_mib"], 0);
    assert_eq!(balloon["free_page_reporting"], reporting);
    assert_eq!(balloon["free_page_hinting"], false);
    recorder.emit(
        json!({"kind":"balloonConfiguration","arm":arm,"reporting":reporting,"actual":balloon}),
    );
    if disabled_between_bursts {
        retire_burst(&pool, &hot, cache, owned, &recorder).await;
    }
    let mut owners = [Some(hot), None, None];
    let mut hits = 0_u32;
    let mut replacements = 0_u32;
    let mut maximum_nominal_mib = 0_u64;
    let mut maximum_cached_nominal_mib = 0_u64;
    for cycle in 0..5 {
        for index in [0, 0, 1, 2, 0] {
            recorder.context(
                if index == 0 {
                    "cache-density-hot"
                } else {
                    "cache-density-sparse"
                },
                Some(CLASSES[index]),
                0,
            );
            let previous = owners[index].take();
            let had_previous = previous.is_some();
            let (owner, hit, burst_snapshot) = visit(
                app,
                &pool,
                &applications[index],
                CLASSES[index],
                previous,
                &parent,
                cache,
                owned,
                &recorder,
                cycle,
                disabled_between_bursts,
            )
            .await;
            if disabled_between_bursts {
                assert!(!hit, "disabled retention reused a retired owner");
            }
            if hit {
                hits += 1;
            } else if had_previous {
                replacements += 1;
            }
            owners[index] = Some(owner);
            maximum_nominal_mib = maximum_nominal_mib
                .max(burst_snapshot.memory_mib)
                .max(pool.snapshot().memory_mib);
            maximum_cached_nominal_mib =
                maximum_cached_nominal_mib.max(pool.snapshot().cached_nominal_mib);
            if index == 2 {
                recorder.context("cache-density-idle-sparse-working-set", None, 0);
                ledger(&pool, cache);
                recorder.host("cache-density-idle-sparse-working-set", None, &pool, owned);
                recorder.emit(json!({"kind":"densityWorkingSet","arm":arm,"cycle":cycle,"position":"before-hot-wake",
                    "hostMonoMs":crate::asyncrt::mono_ms(),"ledger":pool.status()}));
                tokio::time::sleep(Duration::from_secs(2)).await;
                recorder.host("cache-density-idle-sparse-working-set", None, &pool, owned);
            }
        }
        recorder.context("cache-density-idle-working-set", None, 0);
        ledger(&pool, cache);
        // A five-second owner may expire normally while later visits finish.
        if cache && !disabled_between_bursts && ttl >= 30 {
            for (index, owner) in owners.iter().enumerate() {
                let owner = owner.as_ref().unwrap();
                if owned().contains(&owner.group.member.unwrap().0) {
                    cached(&pool, owner, CLASSES[index], cache, owned, &recorder).await;
                }
            }
        }
        recorder.host("cache-density-idle-working-set", None, &pool, owned);
        recorder.emit(json!({"kind":"densityWorkingSet","arm":arm,"cycle":cycle,"hits":hits,
            "replacementAcquires":replacements,"hostMonoMs":crate::asyncrt::mono_ms(),"ledger":pool.status()}));
        tokio::time::sleep(Duration::from_secs(2)).await;
        recorder.host("cache-density-idle-working-set", None, &pool, owned);
    }
    if cache && !disabled_between_bursts && ttl >= 30 {
        assert!(
            maximum_cached_nominal_mib * MIB > guard(cache).max_bytes,
            "no measured cache density beyond full nominal budget"
        );
    }
    if disabled_between_bursts {
        assert_eq!((hits, replacements), (0, 23));
        assert_eq!(maximum_cached_nominal_mib, 0);
    }
    recorder.emit(json!({"kind":"densityObservedTotals","arm":arm,"retention":retention,
        "disabledBetweenBursts":disabled_between_bursts,"idleTtlSeconds":ttl,
        "maximumNominalMib":maximum_nominal_mib,
        "maximumCachedNominalMib":maximum_cached_nominal_mib,"cacheHits":hits,"replacementAcquires":replacements,"cycles":5,"physicalBudgetBytes":guard(cache).max_bytes}));
    recorder.disk(&pool);
    recorder.context("cache-density-idle-expiry", None, 0);
    let mut deadlines: Vec<_> = owners
        .iter()
        .filter_map(|owner| owner.as_ref())
        .filter_map(|owner| {
            let (pid, ticks) = owner.group.member.unwrap();
            owned()
                .contains(&pid)
                .then_some((pid, ticks, owner.gate.status().idle_deadline_ms))
        })
        .collect();
    timeout(Duration::from_secs(ttl + 10), async {
        while !deadlines.is_empty() {
            let pids = owned();
            deadlines.retain(|(pid, ticks, deadline)| {
                if pids.contains(pid) { return true; }
                let now = crate::asyncrt::mono_ms();
                assert!(now >= *deadline, "idle owner evicted before qualification expiry");
                recorder.emit(json!({"kind":"densityExpiryReap","pid":pid,"startTicks":ticks,
                    "idleDeadlineMs":deadline,"reapObservedHostMonoMs":now,"expiryToObservedReapMs":now-*deadline,
                    "pollIntervalMs":20,"ledger":pool.status()}));
                false
            });
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("density idle owners did not expire/reap");
    no_vms(&pool, owned).await;
    let parent_memory = pool.templates.memory_sample().unwrap().unwrap();
    assert_eq!(
        (
            parent_memory.oom,
            parent_memory.oom_kill,
            parent_memory.oom_group_kill
        ),
        (0, 0, 0)
    );
    stop.cancel();
    sampler.await.unwrap();
    pool.shutdown().await.unwrap();
    assert!(!parent.exists().unwrap());
    let leaked: Vec<_> = recorder
        .groups
        .lock()
        .unwrap()
        .iter()
        .filter(|path| path.exists())
        .cloned()
        .collect();
    assert!(leaked.is_empty());
    assert_eq!(
        (
            pool.snapshot().vms,
            pool.snapshot().memory_mib,
            pool.snapshot().retained_limit_bytes
        ),
        (0, 0, 0)
    );
    recorder
        .emit(json!({"kind":"cleanup","ownedPids":owned(),"remainingCgroups":leaked,"arm":arm,"retention":retention}));
    eprintln!(
        "T29_RESULT {}",
        json!({"scenario":"t33::density_pair","status":"passed","arm":arm,"retention":retention,
        "duration_ms":recorder.began.elapsed().as_millis(),"output":output,
        "scope":"three bounded touched identities, active-envelope refusal and separate sampled RAM-seconds; no production density calibration"})
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires separate full/cache processes, fresh registered run IDs, Linux KVM and diagnostic image"]
async fn t33_density_pair() {
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_DENSITY_CHECK_ONLY").is_some());
    bridge_check("memory-density").await;
}
