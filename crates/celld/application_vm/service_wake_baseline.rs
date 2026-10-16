//! Focused M5: assigned caller/backend service RPC and named fetch after cache wakes.
use super::freeze_baseline::OwnedVmm;
use super::lifecycle::{Gate, Phase};
use super::lifecycle_baseline::gate;
use super::memory_baseline::{bridge_check, config, name, no_vms, Recorder};
use super::*;
use celld_applications::invocation_wire::HttpRequest;
use futures_util::StreamExt;
use serde_json::{json, Value};

// The same WorkerEntrypoint service path as check_service_bindings.
const SOURCE: &str = r#"
import {WorkerEntrypoint} from 'cloudflare:workers';
const identity = Math.random().toString(36);
export class Named extends WorkerEntrypoint {
  get value() { return 'named:' + this.env.LABEL; }
  fetch() { return Response.json({label: 'named:' + this.env.LABEL, identity}); }
}
export default class Main extends WorkerEntrypoint {
  get value() { return this.env.LABEL; }
  get identity() { return identity; }
  get nested() { return {sum(a, b) { return a + b; }}; }
  async fetch(request) {
    if (new URL(request.url).pathname === '/named') return this.env.NAMED.fetch(request);
    return Response.json({value: await this.env.SVC.value, sum: await this.env.SVC.nested.sum(2, 3),
      named: await this.env.NAMED.value, callerIdentity: identity, backendIdentity: await this.env.SVC.identity});
  }
}
"#;

async fn call(
    app: &crate::actor::AppHandle,
    dispatcher: &RouteDispatcher,
    host: &str,
    path: &str,
) -> Value {
    let mut response = dispatcher
        .dispatch(
            app,
            Duration::from_secs(10),
            HttpRequest {
                method: "GET".into(),
                url: format!("http://{host}{path}"),
                headers: vec![],
                body: vec![],
            },
            None,
        )
        .await
        .unwrap()
        .expect("assigned caller route disappeared");
    assert_eq!(response.status, 200);
    if let Some(mut stream) = response.stream.take() {
        while let Some(chunk) = stream.next().await {
            response.body.extend(chunk.unwrap());
        }
    }
    serde_json::from_slice(&response.body).unwrap()
}

async fn cached(
    pool: &VmPool,
    owners: &[(Arc<Gate>, OwnedVmm)],
    owned: fn() -> HashSet<u32>,
    recorder: &Recorder,
) -> Vec<u64> {
    timeout(Duration::from_secs(4), async {
        loop {
            let statuses: Vec<_> = owners.iter().map(|(gate, _)| gate.status()).collect();
            assert!(
                statuses
                    .iter()
                    .all(|status| status.phase != Phase::Retiring),
                "service owner retired before caching"
            );
            if statuses.iter().all(|status| status.phase == Phase::Cached) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("caller/backend did not both automatically pause");
    assert_eq!(owned().len(), 2);
    let snapshot = pool.snapshot();
    assert_eq!(
        (snapshot.vms, snapshot.memory_mib, snapshot.quarantined),
        (
            2,
            2 * u64::from(ApplicationSizeClass::Basic.resources().1),
            0
        )
    );
    let mut epochs = Vec::new();
    for (gate, vm) in owners {
        let status = gate.status();
        assert_eq!(status.phase, Phase::Cached);
        assert_eq!(status.active_events, 0);
        assert_eq!(vm.read_json("/", owned).await.unwrap()["state"], "Paused");
        epochs.push(status.epoch);
    }
    recorder.host(
        "lifecycle-service-idle-cached",
        Some(ApplicationSizeClass::Basic),
        pool,
        owned,
    );
    recorder.emit(json!({"kind":"serviceWakeBarrier", "epochs":epochs, "pids":owned()}));
    epochs
}

pub(super) async fn run(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    _directory: &Path,
    owned: fn() -> HashSet<u32>,
) {
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_SERVICE_WAKE_CHECK_ONLY").is_some());
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_LIFECYCLE_CHECK_ONLY").is_some());
    assert!(owned().is_empty());
    let output = PathBuf::from(
        std::env::var_os("CELLD_APPLICATION_MEMORY_OUTPUT")
            .expect("set a fresh service-wake JSONL output"),
    );
    let recorder = Recorder::new(&output, "lifecycle-service-wake");
    recorder.emit(json!({"kind":"provenance", "kernel":options.kernel.sha256, "runtime":options.runtime.sha256,
        "fixtureSha256":content_digest(SOURCE.as_bytes()), "cycles":3, "ttlSeconds":30,
        "scope":"assigned caller/backend service RPC and named fetch; full Basic reservations; no startup-pool freeze"}));
    let application = ApplicationId {
        app: name("memory-service-wake"),
    };
    let mut catalog = store.read(&application).await.unwrap();
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
    let artifact =
        crate::application_image::build(&[("main.mjs".into(), SOURCE.as_bytes().to_vec())])
            .unwrap();
    let digest = content_digest(&artifact);
    store
        .upload_artifact(&application, &digest, artifact.into())
        .await
        .unwrap();
    let manifest: Deployment = serde_json::from_value(json!({"manifest_version":1,"application":application,"id":"service-wake",
        "handlers":[
            {"id":"caller","language":"javascript","entrypoint":"main.mjs","artifact_sha256":digest,
             "runtime_image_sha256":options.runtime.sha256,"http":true,"environment":{"LABEL":"caller"},
             "bindings":{"SVC":{"kind":"handler","handler":"backend"},"NAMED":{"kind":"handler","handler":"backend"}},
             "worker_config":{"compatibility_date":"2026-03-01","compatibility_flags":["js_rpc"],
                "services":[{"binding":"NAMED","service":"backend","entrypoint":"Named"}]}},
            {"id":"backend","language":"javascript","entrypoint":"main.mjs","artifact_sha256":digest,
             "runtime_image_sha256":options.runtime.sha256,"environment":{"LABEL":"backend"},
             "worker_config":{"compatibility_date":"2026-03-01","compatibility_flags":["js_rpc"]}}
        ]})).unwrap();
    store.stage(&manifest).await.unwrap();
    let mut catalog = store.read(&application).await.unwrap();
    store.activate(&mut catalog, manifest).await.unwrap();
    store.commit(catalog).await.unwrap();
    let mut configuration = config(options, 0, 30);
    configuration.max_vms = 2;
    configuration.max_memory_mib = u64::from(ApplicationSizeClass::Standard4.resources().1);
    let dispatcher = RouteDispatcher::new(configuration, store.clone())
        .await
        .unwrap();
    let pool = dispatcher.routes.pool.as_ref().unwrap();
    let host = super::tests::assigned_host(store, &application, "caller").await;
    let caller = pool
        .acquire(app, &application, &name("service-wake"), &name("caller"))
        .await
        .unwrap();
    let caller_gate = gate(pool, &caller.client);
    let caller_pid = *owned().iter().next().unwrap();
    let caller_vm = OwnedVmm::identify(owned);
    drop(caller);
    let initial = call(app, &dispatcher, &host, "/rpc").await;
    assert_eq!(initial["value"], "backend");
    assert_eq!(initial["sum"], 5);
    assert_eq!(initial["named"], "named:backend");
    for key in ["callerIdentity", "backendIdentity"] {
        assert!(!initial[key].as_str().unwrap().is_empty());
    }
    let backend = pool
        .acquire(app, &application, &name("service-wake"), &name("backend"))
        .await
        .unwrap();
    let backend_gate = gate(pool, &backend.client);
    let original_pids = owned();
    assert_eq!(original_pids.len(), 2);
    let backend_pid = *original_pids
        .iter()
        .find(|pid| **pid != caller_pid)
        .unwrap();
    let backend_vm = OwnedVmm::identify_pid(backend_pid, owned);
    drop(backend);
    let owners = [(caller_gate, caller_vm), (backend_gate, backend_vm)];
    recorder.emit(json!({"kind":"serviceWakeOwners", "callerPid":caller_pid, "backendPid":backend_pid, "initial":initial}));
    let mut before = cached(pool, &owners, owned, &recorder).await;
    for cycle in 0..3 {
        for path in ["/rpc", "/named"] {
            let began = Instant::now();
            let response = call(app, &dispatcher, &host, path).await;
            let latency = began.elapsed().as_micros();
            if path == "/rpc" {
                assert_eq!(response, initial);
            } else {
                assert_eq!(
                    response,
                    json!({"label":"named:backend", "identity":initial["backendIdentity"]})
                );
            }
            assert_eq!(owned(), original_pids);
            let after = cached(pool, &owners, owned, &recorder).await;
            assert!(
                after
                    .iter()
                    .zip(&before)
                    .all(|(after, before)| after > before),
                "both service owners must transition after each cached event"
            );
            recorder.emit(json!({"kind":"serviceWake", "cycle":cycle, "path":path, "latencyUs":latency,
                "beforeEpochs":before, "afterEpochs":after, "response":response, "pids":original_pids}));
            before = after;
        }
    }
    dispatcher.shutdown().await.unwrap();
    no_vms(pool, owned).await;
    let leaked: Vec<_> = recorder
        .groups
        .lock()
        .unwrap()
        .iter()
        .filter(|path| path.exists())
        .cloned()
        .collect();
    assert!(leaked.is_empty(), "service wake fixture cgroups remain");
    recorder.emit(json!({"kind":"cleanup", "ownedPids":owned(), "remainingCgroups":leaked}));
    eprintln!(
        "T29_RESULT {}",
        json!({"scenario":"t33::service_wake", "status":"passed",
        "duration_ms":recorder.began.elapsed().as_millis(), "output":output,
        "scope":"same original assigned caller/backend PID and start ticks across RPC/named-fetch cache wakes; full reservations"})
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires idle-capable JavaScript image, Linux KVM and assigned service lifecycle gates"]
async fn t33_service_wake() {
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_SERVICE_WAKE_CHECK_ONLY").is_some());
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_LIFECYCLE_CHECK_ONLY").is_some());
    bridge_check("memory-service-wake").await;
}
