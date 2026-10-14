//! Bounded many-identity and larger-working-set qualification; stress limits only.
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
const SPARSE: usize = 7;
const LARGE_MIB: u64 = 2560;
const LARGE: ApplicationSizeClass = ApplicationSizeClass::Standard1;
const SOURCE: &str = r#"
const identity = `${Date.now()}:${Math.random()}`;
const chunks = [];
let retainedBytes = 0;
function status() {
  let checksum = 0;
  for (const chunk of chunks)
    for (let i = 0; i < chunk.length; i += 4096) checksum += chunk[i];
  return {identity, retainedBytes, checksum};
}
export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === '/allocate') {
      const mib = Number(url.searchParams.get('mib'));
      if (![8, 256].includes(mib) || retainedBytes + mib * 1024 * 1024 > 2560 * 1024 * 1024)
        return new Response('invalid allocation', {status: 400});
      for (let remaining = mib; remaining > 0; remaining -= Math.min(64, remaining)) {
        const chunk = new Uint8Array(Math.min(64, remaining) * 1024 * 1024);
        for (let i = 0; i < chunk.length; i += 4096) chunk[i] = 77;
        chunks.push(chunk);
        retainedBytes += chunk.length;
      }
    } else if (url.pathname === '/hold') {
      await new Promise(resolve => setTimeout(resolve, 6000));
    } else if (url.pathname !== '/status') {
      return new Response('unknown operation', {status: 404});
    }
    return Response.json(status());
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
            max_bytes: 12 * 1024 * MIB,
            retained_margin_bytes: 128 * MIB,
        }),
    }
}
struct Owner {
    application: ApplicationId,
    class: ApplicationSizeClass,
    client: VmClient,
    gate: Arc<Gate>,
    group: Group,
    vm: OwnedVmm,
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
        .expect("working-set owner retired before stable proof")
}
fn ledger(pool: &VmPool) {
    let snapshot = pool.snapshot();
    assert_eq!(snapshot.quarantined, 0);
    assert!(
        snapshot
            .physical_bytes(guard(), snapshot.memory_mib, snapshot.vms)
            .unwrap()
            <= guard().max_bytes
    );
    assert!(
        snapshot.retained_limit_bytes + u64::from(snapshot.cached_vms) * guard().overhead_bytes
            <= guard().idle_cache.unwrap().max_bytes
    );
}
fn integrity(value: &Value, owner: &Owner) {
    assert_eq!(value["identity"], owner.initial["identity"]);
    assert_eq!(value["retainedBytes"], owner.initial["retainedBytes"]);
    assert_eq!(value["checksum"], owner.initial["checksum"]);
}
async fn paused(
    pool: &VmPool,
    owner: &Owner,
    owned: fn() -> HashSet<u32>,
    recorder: &Recorder,
) -> Status {
    let before = timeout(Duration::from_secs(15), async {
        loop {
            let status = owner.gate.status();
            assert_ne!(status.phase, Phase::Retiring);
            if status.phase == Phase::Cached
                && (owner.class == LARGE || retained(pool, &owner.client) > 0)
            {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("working-set owner did not enter enforced cache");
    owner.group.verify_member(owned).unwrap();
    assert_eq!(
        owner.vm.read_json("/", owned).await.unwrap()["state"],
        "Paused"
    );
    let limit = retained(pool, &owner.client);
    let current = owner.group.scalar("memory.current").unwrap();
    let installed = owner.group.scalar("memory.max").unwrap();
    let nominal = u64::from(owner.class.resources().1) * MIB;
    assert_eq!(
        installed,
        if limit == 0 {
            nominal + guard().overhead_bytes
        } else {
            limit
        }
    );
    assert!(current <= installed);
    assert!(limit < nominal);
    assert!(
        current >= owner.initial["retainedBytes"].as_u64().unwrap(),
        "touched payload was not retained in the owned leaf"
    );
    assert_eq!(owner.group.scalar("memory.swap.max").unwrap(), 0);
    assert_eq!(owner.group.scalar("memory.oom.group").unwrap(), 1);
    assert_eq!(owner.gate.status(), before);
    ledger(pool);
    recorder.emit(json!({"kind":"workingSetPaused","application":owner.application,
        "class":owner.class,"pid":owner.group.member.unwrap().0,"startTicks":owner.group.member.unwrap().1,
        "retainedPayloadBytes":owner.initial["retainedBytes"],"retainedLimitBytes":limit,
        "installedLimitBytes":installed,"fullChargeFallback":limit == 0,
        "currentBytes":current,"idleDeadlineMs":before.idle_deadline_ms,"epoch":before.epoch,
        "hostMonoMs":crate::asyncrt::mono_ms(),"ledger":pool.status()}));
    before
}
async fn recover(pool: &VmPool) {
    timeout(Duration::from_secs(10), async {
        loop {
            VmPool::refresh_pressure(&pool.pressure, &pool.templates);
            if !VmPool::under_pressure(&pool.pressure) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("pressure latch did not recover before controlled phase");
}
async fn launch(
    app: &crate::actor::AppHandle,
    pool: &VmPool,
    application: ApplicationId,
    class: ApplicationSizeClass,
    parent: &Group,
    owned: fn() -> HashSet<u32>,
    recorder: &Recorder,
) -> (Owner, VmLease) {
    recover(pool).await;
    let before = owned();
    let lease = timeout(
        Duration::from_secs(60),
        pool.acquire(app, &application, &name("working-set"), &name("memory")),
    )
    .await
    .unwrap()
    .unwrap();
    let added: Vec<_> = owned().difference(&before).copied().collect();
    assert_eq!(added.len(), 1);
    let group = Group::leaf(added[0], parent, owned).unwrap().unwrap();
    let vm = OwnedVmm::identify_pid(added[0], owned);
    let payload_mib = if class == LARGE { LARGE_MIB } else { 8 };
    // Each bounded allocation is a separate event with its original deadline.
    let step = payload_mib.min(256);
    let mut initial = Value::Null;
    for total in (step..=payload_mib).step_by(step as usize) {
        let next = request(
            app,
            &lease,
            &format!("/allocate?mib={step}"),
            recorder,
            class,
        )
        .await;
        if !initial.is_null() {
            assert_eq!(next["identity"], initial["identity"]);
        }
        assert_eq!(next["retainedBytes"], total * MIB);
        assert_eq!(next["checksum"], total * MIB / 4096 * 77);
        initial = next;
    }
    assert_eq!(initial["retainedBytes"], payload_mib * MIB);
    assert_eq!(initial["checksum"], payload_mib * MIB / 4096 * 77);
    let client = lease.client.clone();
    let owner = Owner {
        application,
        class,
        gate: gate(pool, &client),
        client,
        group,
        vm,
        initial,
    };
    (owner, lease)
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
    let recorder = Recorder::new(&output, "cache-larger-working-set");
    recorder.emit(json!({"kind":"provenance","guard":guard(),"startupPool":0,"idleTtlSeconds":120,
        "sparseIdentities":SPARSE,"largeIdentities":2,"nominalBudgetMib":40960,"maximumVms":10,"largePayloadBytesEach":LARGE_MIB*MIB,
        "maximumTouchedPayloadBytes":(2*LARGE_MIB+SPARSE as u64*8)*MIB,
        "kernelSha256":options.kernel.sha256,"runtimeSha256":options.runtime.sha256,
        "firecrackerSha256":content_digest(&std::fs::read(&options.firecracker).unwrap()),
        "sourceSha256":content_digest(SOURCE.as_bytes()),
        "scope":"nine identities and 5 GiB page-touched payload on one host; explicit stress margins, no forced GC, target hardware calibration remains"}));
    let mut configuration = config(options, 0, 120);
    configuration.max_vms = (SPARSE + 3) as u32;
    configuration.max_memory_mib = 40 * 1024;
    configuration.memory_guard = Some(guard());
    let pool = Arc::new(VmPool::new(configuration, store.clone(), "working-set".into()).unwrap());
    let parent = Group::registered_parent().unwrap();
    let (stop, sampler) = recorder.start_sampler(&pool, owned);
    pool.templates.begin_startup();
    for class in [ApplicationSizeClass::Basic, LARGE] {
        let mut launch = pool
            .config
            .options(&options.runtime.sha256, pool.templates.clone())
            .unwrap();
        (launch.vcpus, launch.memory_mib, launch.cpu_quota) = class.resources();
        let charge = pool.charge_startup(launch.memory_mib).await.unwrap();
        let templates = pool.templates.clone();
        tokio::spawn(async move {
            let result = templates.build_template(&launch).await;
            charge.finish(
                !result
                    .as_ref()
                    .is_err_and(|error| error.is::<ReapUnproved>()),
            );
            result.map(drop)
        })
        .await
        .unwrap()
        .unwrap();
        no_vms(&pool, owned).await;
    }
    pool.templates.finish_startup();
    let artifact =
        crate::application_image::build(&[("main.mjs".into(), SOURCE.as_bytes().to_vec())])
            .unwrap();
    let mut owners = Vec::new();
    let mut large_owners = Vec::new();
    let mut large_leases = Vec::new();
    // Keep large owners leased through allocation pressure, then build sparse identities.
    for index in (SPARSE..SPARSE + 2).chain(0..SPARSE) {
        let class = if index < SPARSE {
            ApplicationSizeClass::Basic
        } else {
            LARGE
        };
        let application = stage(store, options, &format!("working-set-{index}"), class).await;
        activate_bundle(
            store,
            options,
            &application,
            "working-set",
            artifact.clone(),
            (
                "main.mjs",
                json!({"compatibility_date":"2026-03-01"}),
                json!({}),
            ),
        )
        .await;
        let (owner, lease) =
            launch(app, &pool, application, class, &parent, owned, &recorder).await;
        if class == LARGE {
            large_owners.push(owner);
            large_leases.push(lease);
            if large_owners.len() == 2 {
                recover(&pool).await;
                large_leases.clear();
                for owner in &large_owners {
                    paused(&pool, owner, owned, &recorder).await;
                }
            }
        } else {
            drop(lease);
            paused(&pool, &owner, owned, &recorder).await;
            owners.push(owner);
        }
    }
    owners.extend(large_owners);
    assert_eq!(pool.snapshot().vms, (SPARSE + 2) as u32);
    assert_eq!(pool.snapshot().memory_mib, SPARSE as u64 * 1024 + 2 * 4096);
    assert!(pool.snapshot().memory_mib * MIB > guard().max_bytes);
    for owner in &owners {
        paused(&pool, owner, owned, &recorder).await;
    }
    recorder.host("cache-larger-working-set-all-paused", None, &pool, owned);
    tokio::time::sleep(Duration::from_secs(2)).await;
    recorder.host("cache-larger-working-set-all-paused", None, &pool, owned);

    // Check every sparse identity before the concurrent full-envelope phase.
    for owner in &owners[..SPARSE] {
        let lease = pool
            .acquire(
                app,
                &owner.application,
                &name("working-set"),
                &name("memory"),
            )
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&owner.client.pin, &lease.client.pin));
        integrity(
            &request(app, &lease, "/status", &recorder, owner.class).await,
            owner,
        );
        assert_eq!(retained(&pool, &owner.client), 0);
        assert_eq!(
            owner.group.scalar("memory.max").unwrap(),
            (1024 + 256) * MIB
        );
        owner.group.verify_member(owned).unwrap();
        drop(lease);
        paused(&pool, owner, owned, &recorder).await;
    }
    let large = &owners[SPARSE..];
    recover(&pool).await;
    let initial_pids = owned();
    let cached_sparse: u64 = owners[..SPARSE]
        .iter()
        .map(|owner| retained(&pool, &owner.client))
        .sum();
    let concurrent_full_bytes = 2 * 4096 * MIB
        + cached_sparse
        + (SPARSE as u64 + 2) * guard().overhead_bytes
        + guard().shared_cache_bytes;
    assert!(
        concurrent_full_bytes <= guard().max_bytes,
        "measured sparse cache leaves no room for both full envelopes"
    );
    let left = pool
        .acquire(
            app,
            &large[0].application,
            &name("working-set"),
            &name("memory"),
        )
        .await
        .unwrap();
    let right = pool
        .acquire(
            app,
            &large[1].application,
            &name("working-set"),
            &name("memory"),
        )
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&left.client.pin, &large[0].client.pin));
    assert!(Arc::ptr_eq(&right.client.pin, &large[1].client.pin));
    // Wait for transient startup PSI to settle before beginning deadline-bound work.
    recover(&pool).await;
    let candidate = stage(
        store,
        options,
        "working-set-refused",
        ApplicationSizeClass::Standard4,
    )
    .await;
    recover(&pool).await;
    let first = {
        let actor = app.clone();
        let client = large[0].client.clone();
        let output = recorder.clone();
        AbortOnDropHandle::new(tokio::spawn(async move {
            request(&actor, &client, "/hold", &output, LARGE).await
        }))
    };
    let second = {
        let actor = app.clone();
        let client = large[1].client.clone();
        let output = recorder.clone();
        AbortOnDropHandle::new(tokio::spawn(async move {
            request(&actor, &client, "/hold", &output, LARGE).await
        }))
    };
    timeout(Duration::from_secs(10), async {
        while large.iter().any(|owner| {
            owner.gate.status().phase != Phase::Running || owner.gate.status().active_events == 0
        }) {
            assert!(large
                .iter()
                .all(|owner| owner.gate.status().phase != Phase::Retiring));
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    for owner in large {
        owner.group.verify_member(owned).unwrap();
        assert_eq!(retained(&pool, &owner.client), 0);
        assert_eq!(
            owner.group.scalar("memory.max").unwrap(),
            (4096 + 256) * MIB
        );
        assert_eq!(
            owner.vm.read_json("/", owned).await.unwrap()["state"],
            "Running"
        );
    }
    ledger(&pool);
    recover(&pool).await;
    let full_pids = owned();
    assert!(
        full_pids.is_subset(&initial_pids),
        "full readmission created a replacement owner"
    );
    assert!(large
        .iter()
        .all(|owner| full_pids.contains(&owner.group.member.unwrap().0)));
    for owner in &owners[..SPARSE] {
        if !full_pids.contains(&owner.group.member.unwrap().0) {
            assert!(
                !owner.group.exists().unwrap(),
                "eviction left an unreaped leaf"
            );
        }
    }
    recorder.emit(json!({"kind":"workingSetReadmissionEvictions",
        "originalPids":initial_pids,"remainingPids":full_pids,
        "scope":"production pressure may evict idle sparse owners; original large owners remain protected"}));
    assert!(
        !VmPool::under_pressure(&pool.pressure),
        "refusal conflated with pressure latch"
    );
    let snapshot = pool.snapshot();
    assert!(snapshot.vms < pool.config.max_vms);
    assert!(snapshot.memory_mib + 12288 <= pool.config.max_memory_mib);
    assert!(
        snapshot
            .physical_bytes(guard(), snapshot.memory_mib + 12288, snapshot.vms + 1)
            .unwrap()
            > guard().max_bytes
    );
    let began = Instant::now();
    let before = owned();
    let refused = timeout(
        Duration::from_millis(500),
        pool.acquire(app, &candidate, &name("memory"), &name("memory")),
    )
    .await;
    let cancelled = match refused {
        Err(_) => true,
        Ok(Err(error)) => {
            assert!(error.is::<CapacityExhausted>(), "{error:#}");
            false
        }
        Ok(Ok(_)) => panic!("two full 4 GiB envelopes admitted a 12 GiB candidate"),
    };
    let after_refusal = owned();
    assert!(
        after_refusal.is_subset(&before),
        "capacity refusal launched another owner"
    );
    assert!(large
        .iter()
        .all(|owner| after_refusal.contains(&owner.group.member.unwrap().0)));
    assert!(large
        .iter()
        .all(|owner| owner.gate.status().active_events > 0));
    VmPool::refresh_pressure(&pool.pressure, &pool.templates);
    assert!(
        !VmPool::under_pressure(&pool.pressure),
        "pressure changed during capacity refusal"
    );
    recorder.emit(
        json!({"kind":"workingSetConcurrentFull","hostMonoMs":crate::asyncrt::mono_ms(),
        "concurrentFullMib":8192,"candidateFullMib":12288,"candidateDeadlineMs":500,
        "candidateElapsedUs":began.elapsed().as_micros(),"frontendCancelledWait":cancelled,
        "candidatePidsBefore":before,"candidatePidsAfter":after_refusal,
        "largePayloadBytes":2*LARGE_MIB*MIB,"sameOwners":true,"ledger":pool.status()}),
    );
    integrity(&first.await.unwrap(), &large[0]);
    integrity(&second.await.unwrap(), &large[1]);
    drop(left);
    drop(right);
    for owner in large {
        paused(&pool, owner, owned, &recorder).await;
    }
    ledger(&pool);
    recorder.host("cache-larger-working-set-after-resume", None, &pool, owned);
    tokio::time::sleep(Duration::from_secs(2)).await;
    recorder.host("cache-larger-working-set-after-resume", None, &pool, owned);
    recorder.disk(&pool);
    let parent_memory = pool.templates.memory_sample().unwrap().unwrap();
    assert_eq!(
        (
            parent_memory.oom,
            parent_memory.oom_kill,
            parent_memory.oom_group_kill
        ),
        (0, 0, 0)
    );
    recorder.emit(json!({"kind":"workingSetObserved","identityCount":owners.len(),
        "nominalMib":pool.snapshot().memory_mib,"parentMemory":parent_memory,"ledger":pool.status()}));
    stop.cancel();
    sampler.await.unwrap();
    pool.shutdown().await.unwrap();
    no_vms(&pool, owned).await;
    assert!(!parent.exists().unwrap());
    assert!(owners.iter().all(|owner| !owner.group.exists().unwrap()));
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
        json!({"scenario":"t33::working_set","status":"passed",
        "duration_ms":recorder.began.elapsed().as_millis(),"output":output,
        "scope":"nine identities, two 2.5 GiB retained payloads, supported simultaneous full envelopes and bounded near-ceiling refusal; single host only"})
    );
}
#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires fresh registered run, diagnostic image, Linux KVM and explicit 5 GiB payload allowance"]
async fn t33_working_set() {
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_WORKING_SET_CHECK_ONLY").is_some());
    bridge_check("memory-working-set").await;
}
