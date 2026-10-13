//! Paired free-page-reporting observations; only the test device reporting bit differs.
use super::freeze_baseline::OwnedVmm;
use super::memory_baseline::{
    bridge_check, complete_class_sample, config, name, no_vms, request, stage, Recorder,
};
use super::*;
use serde_json::json;

const SOURCE: &str = include_str!("../tests/fixtures/t33-memory/worker.mjs");

pub(super) async fn run(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    _directory: &Path,
    owned: fn() -> HashSet<u32>,
) {
    assert!(owned().is_empty());
    let reporting = std::env::var_os("CELLD_APPLICATION_MEMORY_REPORTING_DISABLED").is_none();
    let output = PathBuf::from(
        std::env::var_os("CELLD_APPLICATION_MEMORY_OUTPUT").expect("set a new JSONL output"),
    );
    let recorder = Recorder::new(&output, "reporting-bootstrap");
    let expected = json!({"retained":true,"objects":32768,"bufferBytes":8*1024*1024,
        "wasmBytes":4*1024*1024,"checksum":(0u64..32768).map(|i|i^0x5a5a).sum::<u64>()+77*2048+91*1024});
    let empty = json!({"retained":false});
    let balloon = json!({"amount_mib":0,"deflate_on_oom":false,"stats_polling_interval_s":0,
        "free_page_reporting":reporting,"free_page_hinting":false});
    recorder.emit(json!({"kind":"provenance","reporting":reporting,"kernel":options.kernel.sha256,
        "runtime":options.runtime.sha256,"firecrackerSha256":content_digest(&std::fs::read(&options.firecracker).unwrap()),
        "hostKernel":std::fs::read_to_string("/proc/version").unwrap(),"fixtureSha256":content_digest(SOURCE.as_bytes()),
        "profile":if reporting{"virtio-balloon-reporting-v1"}else{"virtio-balloon-reporting-off-v1"},
        "expectedBalloon":balloon,"warmTrialsPerClass":5,"initialTrialsPerClass":1,"temporaryRequestsPerClass":20,
        "idleObservationSeconds":[2,5,30],"startupPool":0,"nominalBudgetMib":24576,
        "policy":"same kernel and zero-balloon device in both arms; no forced GC or RSS admission",
        "scope":"JS/native/Wasm reporting pair; no Python or independent-host qualification"}));
    let pool = Arc::new(
        VmPool::new(
            config(options, 0, 120),
            store.clone(),
            "memory-reporting".into(),
        )
        .unwrap(),
    );
    let (stop, sampler) = recorder.start_sampler(&pool, owned);
    pool.prepare(app).await.unwrap();
    recorder.disk(&pool);
    for (index, class) in ApplicationSizeClass::ALL.into_iter().enumerate() {
        recorder.context("reporting-active", Some(class), 0);
        let application = stage(store, options, &format!("reporting-{index}"), class).await;
        let began = Instant::now();
        let mut lease = pool
            .acquire(app, &application, &name("memory"), &name("memory"))
            .await
            .unwrap();
        recorder.emit(
            json!({"kind":"acquire","class":class,"mode":"initial","reporting":reporting,
            "latencyUs":began.elapsed().as_micros(),"trial":0,"trials":1}),
        );
        let client = lease.client.clone();
        let pin = client.pin.clone();
        let vmm = OwnedVmm::identify(owned);
        let before = vmm.read_json("/balloon", owned).await.unwrap();
        for (key, value) in balloon.as_object().unwrap() {
            assert_eq!(&before[key], value, "balloon config {key}");
        }
        recorder.emit(json!({"kind":"balloonConfiguration","class":class,"phase":"before","reporting":reporting,"actual":before}));
        assert_eq!(pool.snapshot().memory_mib, u64::from(class.resources().1));
        assert_eq!(
            request(app, &client, "/status", &recorder, class).await,
            empty
        );
        complete_class_sample(
            &recorder,
            "reporting-required-ready",
            class,
            &pool,
            &client,
            owned,
        )
        .await;
        assert_eq!(
            request(app, &client, "/retain", &recorder, class).await,
            expected
        );
        complete_class_sample(
            &recorder,
            "reporting-required-retained",
            class,
            &pool,
            &client,
            owned,
        )
        .await;
        for trial in 0..5 {
            drop(lease);
            let began = Instant::now();
            lease = pool
                .acquire(app, &application, &name("memory"), &name("memory"))
                .await
                .unwrap();
            recorder.emit(json!({"kind":"acquire","class":class,"mode":"retained-reuse","trial":trial,"trials":5,
                "reporting":reporting,"latencyUs":began.elapsed().as_micros()}));
            assert!(
                Arc::ptr_eq(&pin, &lease.pin),
                "warm reuse replaced the assigned VM"
            );
            assert_eq!(
                request(app, &lease, "/status", &recorder, class).await,
                expected
            );
            let actual = vmm.read_json("/balloon", owned).await.unwrap();
            assert_eq!(actual, before, "warm reuse changed balloon configuration");
            recorder.emit(
                json!({"kind":"balloonConfiguration","class":class,"phase":"warm-reuse",
                "trial":trial,"reporting":reporting,"actual":actual}),
            );
        }
        recorder.context("reporting-churn", Some(class), 0);
        for _ in 0..20 {
            assert_eq!(
                request(app, &lease, "/temporary", &recorder, class).await,
                expected
            );
        }
        assert_eq!(
            request(app, &lease, "/status", &recorder, class).await,
            expected
        );
        complete_class_sample(
            &recorder,
            "reporting-required-churn",
            class,
            &pool,
            &client,
            owned,
        )
        .await;
        assert_eq!(
            request(app, &lease, "/release", &recorder, class).await,
            empty
        );
        drop(lease);
        let idle = Instant::now();
        recorder.context("reporting-released-idle", Some(class), 0);
        complete_class_sample(
            &recorder,
            "reporting-required-released",
            class,
            &pool,
            &client,
            owned,
        )
        .await;
        for seconds in [2, 5, 30] {
            tokio::time::sleep_until(idle + Duration::from_secs(seconds)).await;
            complete_class_sample(
                &recorder,
                &format!("reporting-required-released-idle-{seconds}"),
                class,
                &pool,
                &client,
                owned,
            )
            .await;
            assert_eq!(
                pool.snapshot().memory_mib,
                u64::from(class.resources().1),
                "idle changed full reservation"
            );
        }
        recorder.context("reporting-reallocate", Some(class), 0);
        let reused = pool
            .acquire(app, &application, &name("memory"), &name("memory"))
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&pin, &reused.pin));
        assert_eq!(
            request(app, &reused, "/status", &recorder, class).await,
            empty
        );
        assert_eq!(
            request(app, &reused, "/retain", &recorder, class).await,
            expected
        );
        assert_eq!(
            request(app, &reused, "/status", &recorder, class).await,
            expected
        );
        complete_class_sample(
            &recorder,
            "reporting-required-reallocated",
            class,
            &pool,
            &client,
            owned,
        )
        .await;
        let after = vmm.read_json("/balloon", owned).await.unwrap();
        assert_eq!(after, before);
        recorder.emit(json!({"kind":"balloonConfiguration","class":class,"phase":"after","reporting":reporting,"actual":after}));
        drop(reused);
        drop(client);
        pool.retire_deployment(&application, &name("memory")).await;
        no_vms(&pool, owned).await;
    }
    pool.shutdown().await.unwrap();
    no_vms(&pool, owned).await;
    stop.cancel();
    sampler.await.unwrap();
    let leaked: Vec<_> = recorder
        .groups
        .lock()
        .unwrap()
        .iter()
        .filter(|path| path.exists())
        .cloned()
        .collect();
    assert!(leaked.is_empty(), "reporting fixture cgroups remain");
    recorder.emit(json!({"kind":"cleanup","ownedPids":owned(),"remainingCgroups":leaked}));
    eprintln!(
        "T29_RESULT {}",
        json!({"scenario":"t33::memory_reporting","status":"passed","reporting":reporting,
        "duration_ms":recorder.began.elapsed().as_millis(),"output":output,
        "scope":"six classes, five warm reuses each, touched JS/native/Wasm and complete host/guest accounting"})
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires reporting kernel, explicit diagnostic JavaScript image, Linux KVM and jailer"]
async fn t33_memory_reporting() {
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_REPORTING_CHECK_ONLY").is_some());
    bridge_check("memory-reporting").await;
}
