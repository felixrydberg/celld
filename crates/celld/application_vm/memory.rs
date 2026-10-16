//! Measure the current conservative lifecycle. No freeze, GC or admission changes.
use super::*;
use celld_applications::invocation_wire::{GuestMemorySample, HttpRequest};
use serde_json::{json, Value};
use std::fs::File;

const SOURCE: &str = include_str!("../tests/fixtures/t33-memory/worker.mjs");
const LOADER_SOURCE: &str = include_str!("../tests/fixtures/t33-memory/loader-facets.mjs");
const PYTHON_SOURCE: &str = include_str!("../tests/fixtures/t33-memory/python/src/entry.py");

pub(super) struct Recorder {
    output: Mutex<File>,
    pub(super) began: Instant,
    pub(super) groups: Mutex<BTreeSet<PathBuf>>,
    context: Mutex<(String, Option<ApplicationSizeClass>, u32)>,
    context_changed: tokio::sync::Notify,
    guest_phases: Mutex<BTreeSet<(u32, String, String)>>,
}

impl Recorder {
    pub(super) fn new(output: &Path, phase: &str) -> Arc<Self> {
        Arc::new(Self {
            output: Mutex::new(
                std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(output)
                    .unwrap(),
            ),
            began: Instant::now(),
            groups: Mutex::new(BTreeSet::new()),
            context: Mutex::new((phase.into(), Some(ApplicationSizeClass::Basic), 0)),
            context_changed: tokio::sync::Notify::new(),
            guest_phases: Mutex::new(BTreeSet::new()),
        })
    }
    pub(super) fn context(&self, phase: &str, class: Option<ApplicationSizeClass>, startup: u32) {
        *self.context.lock().unwrap() = (phase.into(), class, startup);
        self.context_changed.notify_one();
    }

    pub(super) fn start_sampler(
        self: &Arc<Self>,
        pool: &Arc<VmPool>,
        owned: fn() -> HashSet<u32>,
    ) -> (CancellationToken, JoinHandle<()>) {
        let stop = CancellationToken::new();
        let (recorder, pool, stopping) = (self.clone(), pool.clone(), stop.clone());
        let task = tokio::spawn(async move {
            loop {
                let changed = recorder.context_changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let interval = if recorder.context.lock().unwrap().0.contains("idle") {
                    Duration::from_secs(1)
                } else {
                    Duration::from_millis(100)
                };
                recorder.host("periodic", None, &pool, owned);
                crate::asyncrt::select! {
                    _=stopping.cancelled()=>break,
                    _=changed=>{},
                    _=tokio::time::sleep(interval)=>{}
                }
            }
        });
        (stop, task)
    }

    pub(super) fn emit(&self, mut value: Value) {
        value["schema"] = json!("t33-memory-v1");
        let context = self.context.lock().unwrap().clone();
        value["startupPool"] = json!(context.2);
        value["workload"] = json!(if context.0.starts_with("followon-loader") {
            "loader-facets"
        } else if context.0.starts_with("followon-python") {
            "python"
        } else if context.0.starts_with("reporting") {
            "page-reporting-pair"
        } else if context.0.starts_with("freeze") {
            "plain-freeze"
        } else if context.0.starts_with("lifecycle") {
            "assigned-lifecycle"
        } else if context.0.starts_with("guard") {
            "kernel-memory-guard"
        } else if context.0.starts_with("pressure") {
            "kernel-memory-pressure"
        } else if context.0.starts_with("cpu") {
            "cpu-quota"
        } else if context.0.starts_with("failure") {
            "cache-owner-death"
        } else if context.0.starts_with("cache-density") {
            "cache-density-pair"
        } else if context.0.starts_with("cache") {
            "enforced-retained-cache"
        } else if context.0.starts_with("burst") || context.0.starts_with("ttl30") {
            "distinct-vm-burst-and-retirement"
        } else {
            "class-baseline"
        });
        value["elapsedMs"] = json!(self.began.elapsed().as_millis());
        let mut output = self.output.lock().unwrap();
        serde_json::to_writer(&mut *output, &value).unwrap();
        writeln!(output).unwrap();
        output.flush().unwrap();
    }

    pub(super) fn host(
        &self,
        phase: &str,
        class: Option<ApplicationSizeClass>,
        pool: &VmPool,
        owned: fn() -> HashSet<u32>,
    ) {
        let (phase, class) = if phase == "periodic" {
            let (phase, selected, _) = self.context.lock().unwrap().clone();
            let charged = pool.snapshot();
            let inferred = if charged.starting == 1 {
                ApplicationSizeClass::ALL
                    .into_iter()
                    .find(|class| u64::from(class.resources().1) == charged.memory_mib)
            } else {
                None
            };
            (phase, selected.or(inferred))
        } else {
            (phase.to_owned(), class)
        };
        let mut pids: Vec<_> = owned().into_iter().collect();
        pids.sort_unstable();
        let mut groups = BTreeSet::new();
        let mut memberships_complete = true;
        let processes: Vec<_> = pids
            .into_iter()
            .map(|pid| {
                let proc = PathBuf::from(format!("/proc/{pid}"));
                let stat = text(&proc.join("stat"));
                // Parenthesized comm may contain spaces; starttime is field 22.
                let start_ticks = stat
                    .as_deref()
                    .and_then(|s| s.rsplit_once(')'))
                    .and_then(|(_, tail)| tail.split_whitespace().nth(19))
                    .and_then(|s| s.parse::<u64>().ok());
                let membership = text(&proc.join("cgroup"));
                let mut owned_group = false;
                if let Some(relative) = membership
                    .as_deref()
                    .and_then(|s| s.lines().find_map(|line| line.strip_prefix("0::")))
                {
                    if relative.contains("/celld-vms-") {
                        owned_group = true;
                        groups.insert(
                            Path::new("/sys/fs/cgroup").join(relative.trim_start_matches('/')),
                        );
                    }
                }
                memberships_complete &= owned_group && stat.is_some();
                json!({"pid":pid,"startTicks":start_ticks,"stat":stat,
                "smapsRollup":text(&proc.join("smaps_rollup")),
                "snapshotMappings": snapshot_mappings(&proc.join("smaps"))})
            })
            .collect();
        self.groups.lock().unwrap().extend(groups.iter().cloned());
        let mut current = 0u64;
        let mut aggregate_complete = memberships_complete;
        let cgroups: Vec<_> = groups.into_iter().map(|path| {
            let memory = text(&path.join("memory.current"));
            if let Some(bytes) = memory.as_deref().and_then(|s| s.trim().parse::<u64>().ok()) {
                current += bytes;
            } else { aggregate_complete = false; }
            json!({"path":path,"memoryCurrent":memory,"memoryStat":text(&path.join("memory.stat")),
                "memoryPeak":text(&path.join("memory.peak")),"memoryEvents":text(&path.join("memory.events")),
                "memoryEventsLocal":text(&path.join("memory.events.local")),
                "memoryPressure":text(&path.join("memory.pressure")),"cpuStat":text(&path.join("cpu.stat")),
                "cpuMax":text(&path.join("cpu.max")),"cpuPressure":text(&path.join("cpu.pressure"))})
        }).collect();
        if phase == "application-ready"
            || phase == "loader-ready"
            || phase == "python-adapter-ready"
            || phase.starts_with("reporting-required-")
        {
            assert!(
                !processes.is_empty() && aggregate_complete,
                "memory controller was not enabled before fixture VM attachment"
            );
        }
        self.emit(json!({"kind":"host","phase":phase,"class":class,"pool":pool.status(),
            "samplingIntervalMs":if phase.contains("idle"){1000}else{100},
            "processes":processes,"cgroups":cgroups,"aggregateVmMemoryCurrent":current,
            "aggregateComplete":aggregate_complete,"hostMeminfo":text(Path::new("/proc/meminfo")),
            "hostMemoryPressure":text(Path::new("/proc/pressure/memory")),
            "hostCpuPressure":text(Path::new("/proc/pressure/cpu")),
            "hostStat":text(Path::new("/proc/stat")),
            "cacheOwnership":"snapshot file mappings may share cache; PSS, cgroup charges and RSS are separate observations; file cache outside VM groups remains in host counters"}));
    }

    async fn sample(
        &self,
        phase: &str,
        class: ApplicationSizeClass,
        pool: &VmPool,
        client: &VmClient,
        owned: fn() -> HashSet<u32>,
    ) {
        self.host(phase, Some(class), pool, owned);
        match client.transport.memory_sample().await {
            Ok(Some(stats)) => {
                if phase == "application-ready" {
                    assert!(stats.complete, "initial guest memory sample incomplete");
                }
                self.guest_phases.lock().unwrap().insert((
                    self.context.lock().unwrap().2,
                    serde_json::to_value(class)
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into(),
                    phase.into(),
                ));
                self.emit(json!({"kind":"guest","phase":phase,"class":class,"stats":stats}));
            }
            Ok(None) => {
                self.emit(json!({"kind":"guest","phase":phase,"class":class,"available":false}));
                assert_ne!(
                    phase, "application-ready",
                    "diagnostic runtime did not return an initial guest sample"
                );
            }
            Err(error) => {
                self.emit(json!({"kind":"guest","phase":phase,"class":class,
                    "available":false,"error":format!("{error:#}")}));
                assert_ne!(
                    phase, "application-ready",
                    "initial guest memory sample failed: {error:#}"
                );
            }
        }
    }

    pub(super) fn disk(&self, pool: &VmPool) {
        let files: Vec<_> = pool.templates.state_files().into_iter().flat_map(|state| {
            let directory = state.parent().unwrap();
            ["state", "memory", "runtime.ext4"].into_iter().map(|name| {
                let path = directory.join(name);
                let metadata = std::fs::metadata(&path).unwrap();
                json!({"path":path,"logicalBytes":metadata.len(),"allocatedBytes":metadata.blocks()*512,
                    "device":metadata.dev(),"inode":metadata.ino(),"links":metadata.nlink()})
            }).collect::<Vec<_>>()
        }).collect();
        self.emit(json!({"kind":"snapshotDisk","files":files,
            "ownership":"pool owns immutable templates; device+inode identify hard links; allocated disk bytes are not resident memory"}));
    }
}

fn text(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

fn snapshot_mappings(path: &Path) -> Value {
    // Preserve only mapping counters, never mapped contents or handler data.
    let Some(smaps) = text(path) else {
        return Value::Null;
    };
    let mut selected = false;
    let mut totals = BTreeMap::<String, u64>::new();
    for line in smaps.lines() {
        if line.split_whitespace().next().is_some_and(|token| {
            token.split_once('-').is_some_and(|(a, b)| {
                u64::from_str_radix(a, 16).is_ok() && u64::from_str_radix(b, 16).is_ok()
            })
        }) {
            selected = line
                .split_whitespace()
                .last()
                .is_some_and(|p| p.ends_with("/memory"));
        } else if selected {
            let mut fields = line.split_whitespace();
            if let (Some(key), Some(value), Some("kB")) =
                (fields.next(), fields.next(), fields.next())
            {
                if let Ok(value) = value.parse::<u64>() {
                    *totals.entry(key.into()).or_default() += value * 1024;
                }
            }
        }
    }
    json!(totals)
}

pub(super) fn config(options: &LaunchOptions, startup_pool: u32, idle: u64) -> VmHostConfig {
    VmHostConfig {
        jailer: options.jailer.clone(),
        firecracker: options.firecracker.clone(),
        kernel: options.kernel.clone(),
        runtimes: vec![options.runtime.clone()],
        uid: options.uid,
        gid: options.gid,
        max_vms: 3,
        max_memory_mib: 24576,
        memory_guard: None,
        idle_ttl_secs: idle,
        platform_domain: super::tests::TEST_DOMAIN.into(),
        container_fence_image: None,
        artifact_cache_ttl_secs: None,
        metadata_cache_entries: None,
        metadata_cache_bytes: None,
        startup_pool,
    }
}

pub(super) async fn stage(
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    id: &str,
    class: ApplicationSizeClass,
) -> ApplicationId {
    let application = ApplicationId {
        app: id.to_owned().try_into().unwrap(),
    };
    let mut catalog = store.read(&application).await.unwrap();
    catalog.create(application.clone()).unwrap();
    catalog
        .set_limits(
            &application,
            Some(ApplicationLimits {
                size_class: class,
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
    let deployment: Deployment = serde_json::from_value(json!({
        "manifest_version":1,"application":application,"id":"memory",
        "handlers":[{"id":"memory","language":"javascript","entrypoint":"main.mjs",
            "artifact_sha256":digest,"runtime_image_sha256":options.runtime.sha256,
            "worker_config":{"compatibility_date":"2026-03-01"}}]}))
    .unwrap();
    store.stage(&deployment).await.unwrap();
    let mut catalog = store.read(&application).await.unwrap();
    store.activate(&mut catalog, deployment).await.unwrap();
    store.commit(catalog).await.unwrap();
    application
}

pub(super) fn name(value: &str) -> Name {
    value.to_owned().try_into().unwrap()
}

pub(super) async fn request(
    app: &crate::actor::AppHandle,
    lease: &VmClient,
    path: &str,
    recorder: &Recorder,
    class: ApplicationSizeClass,
) -> Value {
    let began = Instant::now();
    let mut response = lease
        .invoke_stateless(
            app,
            Duration::from_secs(
                if std::env::var_os("CELLD_APPLICATION_MEMORY_FOLLOWONS_ONLY").is_some() {
                    120
                } else {
                    15
                },
            ),
            HttpRequest {
                method: "GET".into(),
                url: format!("http://memory{path}"),
                headers: vec![],
                body: vec![],
            },
            None,
        )
        .await
        .unwrap();
    if let Some(mut stream) = response.stream.take() {
        use futures_util::StreamExt;
        while let Some(chunk) = stream.next().await {
            response.body.extend(chunk.unwrap());
        }
    }
    assert_eq!(response.status, 200);
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    recorder.emit(json!({"kind":"request","class":class,"operation":path,
        "latencyUs":began.elapsed().as_micros(),"result":body}));
    body
}

/// The host `vm_startup` record and the guest's initialization timings, read
/// from the `key=value` fields its tracing writes to the console.
fn launch(client: &VmClient, class: ApplicationSizeClass, mode: &str) -> Value {
    let console = client.console();
    let phases = |event: &str| {
        let line = console.lines().find(|line| line.contains(event))?;
        let fields: serde_json::Map<_, _> = line
            .split_whitespace()
            .filter_map(|field| field.split_once('='))
            .filter_map(|(key, value)| Some((key.to_owned(), json!(value.parse::<f64>().ok()?))))
            .collect();
        Some(fields)
    };
    json!({"kind":"launch","class":class,"mode":mode,"host":client.startup,
        "application":phases("guest_application_initialization"),
        "worker":phases("guest_worker_initialization")})
}

pub(super) async fn no_vms(pool: &VmPool, owned: fn() -> HashSet<u32>) {
    timeout(Duration::from_secs(30), async {
        while !owned().is_empty() || pool.snapshot().vms != 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("fixture VMM not reaped");
    assert_eq!(pool.snapshot().quarantined, 0);
}

pub(super) async fn run(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    _directory: &Path,
    owned: fn() -> HashSet<u32>,
) {
    if std::env::var_os("CELLD_APPLICATION_MEMORY_FOLLOWONS_ONLY").is_some() {
        run_followons(app, store, options, _directory, owned).await;
        return;
    }
    assert!(owned().is_empty(), "pre-existing fixture VMM");
    let output = PathBuf::from(
        std::env::var_os("CELLD_APPLICATION_MEMORY_OUTPUT")
            .expect("set CELLD_APPLICATION_MEMORY_OUTPUT to a new JSONL path"),
    );
    let recorder = Arc::new(Recorder {
        output: Mutex::new(
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&output)
                .unwrap(),
        ),
        began: Instant::now(),
        groups: Mutex::new(BTreeSet::new()),
        context: Mutex::new(("template-bootstrap".into(), None, 0)),
        context_changed: tokio::sync::Notify::new(),
        guest_phases: Mutex::new(BTreeSet::new()),
    });
    let version = Command::new(&options.firecracker)
        .arg("--version")
        .output()
        .await
        .unwrap();
    assert!(version.status.success());
    recorder.emit(json!({"kind":"provenance","kernel":options.kernel.sha256,
        "runtime":options.runtime.sha256,"firecrackerSha256":content_digest(&std::fs::read(&options.firecracker).unwrap()),
        "firecrackerVersion":String::from_utf8_lossy(&version.stdout),
        "hostKernel":text(Path::new("/proc/version")),"hostCpu":text(Path::new("/proc/cpuinfo")),
        "fixtureSha256":content_digest(SOURCE.as_bytes()),"revision":option_env!("CELLD_BUILD_REVISION"),
        "activeSamplingIntervalMs":100,"idleSamplingIntervalMs":1000,"guestTimeoutMs":1000,"idleObservationSeconds":[2,5,30,60],
        "idleWorkloads":["retained globals","released globals"],"baselineIdleTtlSeconds":120,"retirementIdleTtlSeconds":30,"nominalBudgetMib":24576,
        "densityTarget":null,"policy":"current full reservations; no freeze, GC or reporting",
        "remaining":["Pyodide","loader/facet","pause/thaw and retained-host-client wakes","threshold and 16GiB density selection"]}));

    let deployment = name("memory");
    let handler = name("memory");
    for startup in [0, 1] {
        recorder.context("template-bootstrap", None, startup);
        let pool = Arc::new(
            VmPool::new(
                config(options, startup, 120),
                store.clone(),
                format!("memory-pool-{startup}"),
            )
            .unwrap(),
        );
        let (stop, sampler) = recorder.start_sampler(&pool, owned);
        recorder.host("before-template", None, &pool, owned);
        let began = Instant::now();
        pool.prepare(app).await.unwrap();
        recorder.emit(json!({"kind":"startup","startupPool":startup,
            "durationUs":began.elapsed().as_micros(),"templates":pool.templates.status()}));
        assert_eq!(pool.templates.state_files().len(), 6);
        recorder.host("templates-ready", None, &pool, owned);
        recorder.disk(&pool);
        for (index, class) in ApplicationSizeClass::ALL.into_iter().enumerate() {
            recorder.context("application-workload", Some(class), startup);
            let application =
                stage(store, options, &format!("memory-{startup}-{index}"), class).await;
            let began = Instant::now();
            let lease = pool
                .acquire(app, &application, &deployment, &handler)
                .await
                .unwrap();
            recorder.emit(
                json!({"kind":"acquire","class":class,"startupPool":startup,"mode":"initial",
                "latencyUs":began.elapsed().as_micros()}),
            );
            let client = lease.client.clone();
            assert_eq!(
                request(app, &client, "/status", &recorder, class).await["retained"],
                false
            );
            recorder.emit(launch(&client, class, "initial"));
            recorder
                .sample("application-ready", class, &pool, &client, owned)
                .await;
            let temporary = request(app, &client, "/temporary", &recorder, class).await;
            assert_eq!(temporary["bufferBytes"], 8 * 1024 * 1024);
            recorder
                .sample("temporary-released", class, &pool, &client, owned)
                .await;
            let retained = request(app, &client, "/retain", &recorder, class).await;
            recorder
                .sample("retained", class, &pool, &client, owned)
                .await;
            assert_eq!(
                request(app, &client, "/status", &recorder, class).await,
                retained
            );
            let old_pin = client.pin.clone();
            drop(lease);
            let idle = Instant::now();
            recorder.context("retained-idle", Some(class), startup);
            recorder
                .sample("request-complete", class, &pool, &client, owned)
                .await;
            for seconds in [2, 5, 30, 60] {
                tokio::time::sleep_until(idle + Duration::from_secs(seconds)).await;
                recorder
                    .sample(&format!("idle-{seconds}"), class, &pool, &client, owned)
                    .await;
            }
            recorder.context("retained-reuse", Some(class), startup);
            let began = Instant::now();
            let lease = pool
                .acquire(app, &application, &deployment, &handler)
                .await
                .unwrap();
            recorder.emit(
                json!({"kind":"acquire","class":class,"mode":"retained-reuse",
                "latencyUs":began.elapsed().as_micros()}),
            );
            assert!(
                Arc::ptr_eq(&old_pin, &lease.pin),
                "idle baseline did not reuse assigned VM"
            );
            assert_eq!(
                request(app, &lease, "/status", &recorder, class).await,
                retained
            );
            recorder.sample("reused", class, &pool, &lease, owned).await;
            assert_eq!(
                request(app, &lease, "/release", &recorder, class).await["retained"],
                false
            );
            recorder
                .sample("retained-released", class, &pool, &lease, owned)
                .await;
            drop(lease);
            let released_idle = Instant::now();
            recorder.context("released-idle", Some(class), startup);
            for seconds in [2, 5, 30, 60] {
                tokio::time::sleep_until(released_idle + Duration::from_secs(seconds)).await;
                recorder
                    .sample(
                        &format!("released-idle-{seconds}"),
                        class,
                        &pool,
                        &client,
                        owned,
                    )
                    .await;
            }
            recorder.context("released-reuse", Some(class), startup);
            let began = Instant::now();
            let reused = pool
                .acquire(app, &application, &deployment, &handler)
                .await
                .unwrap();
            recorder.emit(
                json!({"kind":"acquire","class":class,"mode":"released-reuse",
                "latencyUs":began.elapsed().as_micros()}),
            );
            assert!(Arc::ptr_eq(&old_pin, &reused.pin));
            assert_eq!(
                request(app, &reused, "/status", &recorder, class).await["retained"],
                false
            );
            drop(reused);
            pool.retire_deployment(&application, &deployment).await;
            // A startup pool may retain or refill unassigned VMs; physical reap
            // of every VMM is proved at pool shutdown, not inferred from RSS.
            recorder.host("assigned-retirement", Some(class), &pool, owned);

            // The initial launch above was a pool miss: a class is refilled only
            // once needed. A second application claims that refill (startup 1)
            // or restores again (startup 0).
            recorder.context("fresh-acquire", Some(class), startup);
            if startup > 0 {
                // Reaping every waiting VM times one clean refill. Other classes'
                // refills would take the slots it needs (refill never takes the
                // last one), so only this class stays needed while it waits.
                for idle in pool.templates.evict_all() {
                    pool.reap(idle);
                }
                let mib = class.resources().1;
                let refill = Instant::now();
                timeout(Duration::from_secs(60), async {
                    while !pool.templates.status()["templates"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|t| t["memoryMib"] == mib && t["idle"].as_u64() >= Some(1))
                    {
                        for idle in pool.templates.focus(mib) {
                            pool.reap(idle);
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                })
                .await
                .expect("startup pool did not refill");
                // Wall clock: includes the reaps it waited for and the 200 ms warmer tick.
                recorder.emit(json!({"kind":"refill","class":class,
                    "latencyUs":refill.elapsed().as_micros()}));
                // Let the guest settle before sampling its idle memory.
                tokio::time::sleep(Duration::from_secs(2)).await;
                recorder.host("pool-waiting", Some(class), &pool, owned);
            }
            let application = stage(
                store,
                options,
                &format!("memory-{startup}-{index}-fresh"),
                class,
            )
            .await;
            let began = Instant::now();
            let lease = pool
                .acquire(app, &application, &deployment, &handler)
                .await
                .unwrap();
            recorder.emit(json!({"kind":"acquire","class":class,"mode":"fresh",
                "latencyUs":began.elapsed().as_micros()}));
            request(app, &lease, "/status", &recorder, class).await;
            let fresh = launch(&lease, class, "fresh");
            assert_eq!(
                fresh["host"]["path"],
                if startup > 0 { "pool" } else { "restore" }
            );
            recorder.emit(fresh);
            drop(lease);
            pool.retire_deployment(&application, &deployment).await;
        }
        stop.cancel();
        sampler.await.unwrap();
        pool.shutdown().await.unwrap();
        no_vms(&pool, owned).await;
        recorder.host("pool-reaped", None, &pool, owned);
    }

    // Bounded sustained requests and simultaneous work in two distinct VMs.
    recorder.context("burst-template-bootstrap", None, 0);
    let pool = Arc::new(
        VmPool::new(config(options, 0, 30), store.clone(), "memory-burst".into()).unwrap(),
    );
    let (stop, sampler) = recorder.start_sampler(&pool, owned);
    pool.prepare(app).await.unwrap();
    recorder.context("burst-workload", Some(ApplicationSizeClass::Lite), 0);
    let a = stage(store, options, "memory-burst-a", ApplicationSizeClass::Lite).await;
    let b = stage(store, options, "memory-burst-b", ApplicationSizeClass::Lite).await;
    let (a, b) = tokio::join!(
        pool.acquire(app, &a, &deployment, &handler),
        pool.acquire(app, &b, &deployment, &handler)
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert!(!Arc::ptr_eq(&a.pin, &b.pin));
    assert_eq!(owned().len(), 2);
    for _ in 0..20 {
        request(app, &a, "/temporary", &recorder, ApplicationSizeClass::Lite).await;
    }
    recorder
        .sample("sustained", ApplicationSizeClass::Lite, &pool, &a, owned)
        .await;
    let (first, second) = tokio::join!(
        request(app, &a, "/slow", &recorder, ApplicationSizeClass::Lite),
        request(app, &b, "/slow", &recorder, ApplicationSizeClass::Lite)
    );
    assert_eq!(first["retained"], false);
    assert_eq!(second["retained"], false);
    recorder
        .sample(
            "distinct-vm-burst",
            ApplicationSizeClass::Lite,
            &pool,
            &a,
            owned,
        )
        .await;
    drop(a);
    drop(b);
    let idle = Instant::now();
    recorder.context("ttl30-idle", Some(ApplicationSizeClass::Lite), 0);
    for seconds in [2, 5, 30, 60] {
        tokio::time::sleep_until(idle + Duration::from_secs(seconds)).await;
        recorder.host(
            &format!("ttl30-idle-{seconds}"),
            Some(ApplicationSizeClass::Lite),
            &pool,
            owned,
        );
    }
    no_vms(&pool, owned).await;
    pool.shutdown().await.unwrap();
    stop.cancel();
    sampler.await.unwrap();
    {
        let phases = recorder.guest_phases.lock().unwrap();
        for startup in [0, 1] {
            for class in ApplicationSizeClass::ALL {
                let class = serde_json::to_value(class)
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_owned();
                for phase in [
                    "application-ready",
                    "temporary-released",
                    "retained",
                    "request-complete",
                    "idle-2",
                    "idle-5",
                    "idle-30",
                    "idle-60",
                    "reused",
                    "retained-released",
                    "released-idle-2",
                    "released-idle-5",
                    "released-idle-30",
                    "released-idle-60",
                ] {
                    assert!(
                        phases.contains(&(startup, class.clone(), phase.into())),
                        "missing guest phase: startup_pool={startup} class={class} phase={phase}"
                    );
                }
            }
        }
    }
    recorder.emit(json!({"kind":"coverage","classes":6,"startupPools":[0,1],
        "guestMatrixComplete":true}));
    let leaked: Vec<_> = recorder
        .groups
        .lock()
        .unwrap()
        .iter()
        .filter(|path| path.exists())
        .cloned()
        .collect();
    recorder.emit(json!({"kind":"cleanup","ownedPids":owned(),"remainingCgroups":leaked}));
    assert!(owned().is_empty());
    assert!(leaked.is_empty(), "fixture cgroup not removed");
    eprintln!(
        "T29_RESULT {}",
        json!({"scenario":"t33::memory_baseline","status":"passed","duration_ms":recorder.began.elapsed().as_millis(),
        "output":output,"scope":"M1 JS/native/Wasm baseline; pending follow-ons remain"})
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires explicit diagnostic JavaScript image, Linux KVM and jailer"]
async fn t33_memory_baseline() {
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_FOLLOWONS_ONLY").is_none());
    bridge_check("memory-baseline").await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires prepared Python memory fixture, diagnostic JavaScript image and Linux KVM"]
async fn t33_memory_followons() {
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_FOLLOWONS_ONLY").is_some());
    bridge_check("memory-followons").await;
}

pub(super) async fn bridge_check(application: &str) {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();
    crate::asyncrt::bind_test_runtime();
    assert!(std::env::var_os("CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE").is_some());
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_CHECK_ONLY").is_some());
    let directory = tempfile::tempdir().unwrap();
    let address = ResourceAddress {
        application: ApplicationId {
            app: name(application),
        },
        resource: ResourceRef {
            kind: ResourceKind::DurableObject,
            id: name("counter"),
        },
    };
    let (app, runtime, actor) =
        crate::application_storage::tests::actor_bridge(directory.path(), &address).await;
    let cell = address.cell_scope("Counter", "named-object").unwrap();
    super::tests::check_supervised_handler_bridge(&app, &runtime, &address, &cell).await;
    actor.abort();
    assert!(actor.await.unwrap_err().is_cancelled());
}

pub(super) async fn complete_sample(
    recorder: &Recorder,
    phase: &str,
    pool: &VmPool,
    client: &VmClient,
    owned: fn() -> HashSet<u32>,
) -> GuestMemorySample {
    complete_class_sample(
        recorder,
        phase,
        ApplicationSizeClass::Basic,
        pool,
        client,
        owned,
    )
    .await
}

pub(super) async fn complete_class_sample(
    recorder: &Recorder,
    phase: &str,
    class: ApplicationSizeClass,
    pool: &VmPool,
    client: &VmClient,
    owned: fn() -> HashSet<u32>,
) -> GuestMemorySample {
    // Child disposal may finish after the response. Retry observations only;
    // sampling never forces a turn or collection.
    for attempt in 0..5 {
        recorder.host(phase, Some(class), pool, owned);
        let sample = client
            .transport
            .memory_sample()
            .await
            .unwrap()
            .expect("diagnostic image did not produce a guest sample");
        recorder.emit(json!({"kind":"guest","phase":phase,"class":class,
            "attempt":attempt,"stats":sample}));
        if sample.complete {
            return sample;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("guest sample remained incomplete at {phase}");
}

async fn idle_samples(
    recorder: &Recorder,
    prefix: &str,
    pool: &VmPool,
    client: &VmClient,
    owned: fn() -> HashSet<u32>,
    expected_isolates: Option<u32>,
) {
    let began = Instant::now();
    recorder.context(
        &format!("followon-{prefix}-idle"),
        Some(ApplicationSizeClass::Basic),
        0,
    );
    for seconds in [2, 5, 30, 60] {
        tokio::time::sleep_until(began + Duration::from_secs(seconds)).await;
        let sample = complete_sample(
            recorder,
            &format!("{prefix}-idle-{seconds}"),
            pool,
            client,
            owned,
        )
        .await;
        if let Some(expected) = expected_isolates {
            assert_eq!(sample.isolates, expected);
        }
    }
}

pub(super) async fn activate_bundle(
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    application: &ApplicationId,
    deployment_id: &str,
    bundle: Vec<u8>,
    metadata: (&str, Value, Value),
) {
    let (entrypoint, worker_config, bindings) = metadata;
    let digest = content_digest(&bundle);
    store
        .upload_artifact(application, &digest, bundle.into())
        .await
        .unwrap();
    let deployment: Deployment = serde_json::from_value(json!({
        "manifest_version":1,"application":application,"id":deployment_id,
        "handlers":[{"id":"memory","language":"javascript","entrypoint":entrypoint,
            "artifact_sha256":digest,"runtime_image_sha256":options.runtime.sha256,
            "worker_config":worker_config,"bindings":bindings}]}))
    .unwrap();
    store.stage(&deployment).await.unwrap();
    let mut catalog = store.read(application).await.unwrap();
    store.activate(&mut catalog, deployment).await.unwrap();
    store.commit(catalog).await.unwrap();
}

async fn followon_acquire(
    app: &crate::actor::AppHandle,
    pool: &VmPool,
    application: &ApplicationId,
    deployment: &Name,
    handler: &Name,
    recorder: &Recorder,
    mode: &str,
) -> VmLease {
    let began = Instant::now();
    let lease = pool
        .acquire(app, application, deployment, handler)
        .await
        .unwrap();
    recorder.emit(json!({"kind":"acquire","class":ApplicationSizeClass::Basic,
        "mode":mode,"latencyUs":began.elapsed().as_micros()}));
    lease
}

async fn run_followons(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    _directory: &Path,
    owned: fn() -> HashSet<u32>,
) {
    assert!(owned().is_empty());
    let output = PathBuf::from(
        std::env::var_os("CELLD_APPLICATION_MEMORY_OUTPUT")
            .expect("set a new follow-on JSONL output path"),
    );
    let recorder = Arc::new(Recorder {
        output: Mutex::new(
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&output)
                .unwrap(),
        ),
        began: Instant::now(),
        groups: Mutex::new(BTreeSet::new()),
        context: Mutex::new(("followon-prepare".into(), None, 0)),
        context_changed: tokio::sync::Notify::new(),
        guest_phases: Mutex::new(BTreeSet::new()),
    });
    let version = Command::new(&options.firecracker)
        .arg("--version")
        .output()
        .await
        .unwrap();
    assert!(version.status.success());
    recorder.emit(json!({"kind":"provenance","kernel":options.kernel.sha256,
        "runtime":options.runtime.sha256,"firecrackerSha256":content_digest(&std::fs::read(&options.firecracker).unwrap()),
        "firecrackerVersion":String::from_utf8_lossy(&version.stdout),
        "hostKernel":text(Path::new("/proc/version")),"hostCpu":text(Path::new("/proc/cpuinfo")),
        "loaderFixtureSha256":content_digest(LOADER_SOURCE.as_bytes()),
        "pythonFixtureSha256":content_digest(PYTHON_SOURCE.as_bytes()),
        "activeSamplingIntervalMs":100,"idleSamplingIntervalMs":1000,
        "idleObservationSeconds":[2,5,30,60],"idleTtlSeconds":120,
        "nominalBudgetMib":24576,"policy":"current full reservations; no freeze or forced GC",
        "remaining":["pause/thaw timer/clock/heartbeat/retained-client wakes",
            "repeat trials","regression/pressure thresholds","16GiB density target selection"]}));

    let pool = Arc::new(
        VmPool::new(
            config(options, 0, 120),
            store.clone(),
            "memory-followons".into(),
        )
        .unwrap(),
    );
    let (stop, sampler) = recorder.start_sampler(&pool, owned);
    pool.prepare(app).await.unwrap();
    recorder.disk(&pool);
    let application = ApplicationId {
        app: name("memory-followons"),
    };
    let loader = ResourceRef {
        kind: ResourceKind::WorkerLoader,
        id: name("loader"),
    };
    let counter = ResourceRef {
        kind: ResourceKind::DurableObject,
        id: name("counter"),
    };
    let mut catalog = store.read(&application).await.unwrap();
    assert!(
        catalog.application(&application).is_some(),
        "follow-on actor application was not initialized"
    );
    catalog
        .set_limits(
            &application,
            Some(ApplicationLimits {
                size_class: ApplicationSizeClass::Basic,
                invocation_timeout_ms: 120000,
            }),
        )
        .unwrap();
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
        crate::application_image::build(&[("main.mjs".into(), LOADER_SOURCE.as_bytes().to_vec())])
            .unwrap();
    activate_bundle(
        store,
        options,
        &application,
        "loader",
        artifact,
        (
            "main.mjs",
            json!({
                "compatibility_date":"2026-03-01","compatibility_flags":["js_rpc"],
                "worker_loaders":[{"binding":"LOADER"}],"do_classes":["Counter"],
                "durable_objects":{"bindings":[{"name":"COUNTER","class_name":"Counter"}]}
            }),
            json!({"LOADER":{"kind":"resource","resource":loader},
        "COUNTER":{"kind":"resource","resource":counter}}),
        ),
    )
    .await;

    // Same original host facet stream wiring as check_facets; one dedicated
    // selector owns this process and its single Actor.
    let (facet_tx, mut requests) = tokio::sync::mpsc::unbounded_channel();
    crate::js::set_facet_tx(facet_tx);
    let host = app.clone();
    let facet_task = tokio::spawn(async move {
        use crate::js::FacetReq;
        while let Some(request) = requests.recv().await {
            let runtime = host.runtime.as_ref().expect("cell runtime");
            match request {
                FacetReq::Open {
                    root,
                    epoch,
                    names,
                    reply,
                } => drop(reply.send(runtime.open_facet(&root, epoch, &names).await)),
                FacetReq::Delete {
                    root,
                    epoch,
                    names,
                    reply,
                } => drop(reply.send(runtime.delete_facet(&root, epoch, &names).await)),
                FacetReq::Prove {
                    stream,
                    epoch,
                    reply,
                } => drop(reply.send(runtime.prove_facet(&stream, epoch).await)),
            }
        }
    });

    let handler = name("memory");
    let deployment = name("loader");
    recorder.context(
        "followon-loader-active",
        Some(ApplicationSizeClass::Basic),
        0,
    );
    let lease = followon_acquire(
        app,
        &pool,
        &application,
        &deployment,
        &handler,
        &recorder,
        "loader-ready",
    )
    .await;
    let client = lease.client.clone();
    let pin = client.pin.clone();
    complete_sample(&recorder, "loader-ready", &pool, &client, owned).await;
    let held = request(
        app,
        &client,
        "/hold",
        &recorder,
        ApplicationSizeClass::Basic,
    )
    .await;
    assert_eq!(held["child"]["bytes"], 8 * 1024 * 1024);
    assert_eq!(held["first"]["bytes"], 4 * 1024 * 1024);
    assert_eq!(held["second"]["bytes"], 4 * 1024 * 1024);
    assert_eq!(held["child"]["checksum"], 7 * 2048);
    assert_eq!(held["first"]["checksum"], 7 * 1024);
    assert_eq!(held["second"]["checksum"], 7 * 1024);
    let loaded = complete_sample(&recorder, "loader-held", &pool, &client, owned).await;
    assert_eq!(
        loaded.isolates, 2,
        "two facets must share the loader child slot"
    );
    assert!(
        loaded
            .total
            .external_bytes
            .saturating_sub(loaded.root.external_bytes)
            >= 16 * 1024 * 1024,
        "loader sample omitted touched child/facet ArrayBuffer backing stores"
    );
    drop(lease);
    idle_samples(&recorder, "loader-held", &pool, &client, owned, Some(2)).await;
    recorder.context(
        "followon-loader-reuse",
        Some(ApplicationSizeClass::Basic),
        0,
    );
    let lease = followon_acquire(
        app,
        &pool,
        &application,
        &deployment,
        &handler,
        &recorder,
        "loader-retained-reuse",
    )
    .await;
    assert!(Arc::ptr_eq(&pin, &lease.pin));
    assert_eq!(
        request(
            app,
            &lease,
            "/status",
            &recorder,
            ApplicationSizeClass::Basic
        )
        .await,
        held
    );
    assert_eq!(
        request(
            app,
            &lease,
            "/release",
            &recorder,
            ApplicationSizeClass::Basic
        )
        .await["released"],
        true
    );
    complete_sample(&recorder, "loader-released", &pool, &lease, owned).await;
    drop(lease);
    idle_samples(&recorder, "loader-released", &pool, &client, owned, Some(2)).await;
    recorder.context(
        "followon-loader-teardown",
        Some(ApplicationSizeClass::Basic),
        0,
    );
    let lease = followon_acquire(
        app,
        &pool,
        &application,
        &deployment,
        &handler,
        &recorder,
        "loader-released-reuse",
    )
    .await;
    assert!(Arc::ptr_eq(&pin, &lease.pin));
    let released = request(
        app,
        &lease,
        "/status",
        &recorder,
        ApplicationSizeClass::Basic,
    )
    .await;
    for key in ["child", "first", "second"] {
        assert_eq!(released[key]["bytes"], 0);
    }
    assert_eq!(
        request(
            app,
            &lease,
            "/teardown",
            &recorder,
            ApplicationSizeClass::Basic
        )
        .await["loaded"],
        false
    );
    timeout(Duration::from_secs(10), async {
        loop {
            let sample =
                complete_sample(&recorder, "loader-child-torn-down", &pool, &lease, owned).await;
            if sample.isolates == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("disposed child remained in diagnostic registry");
    drop(lease);
    pool.retire_deployment(&application, &deployment).await;
    no_vms(&pool, owned).await;
    recorder.host(
        "loader-reaped",
        Some(ApplicationSizeClass::Basic),
        &pool,
        owned,
    );
    facet_task.abort();
    assert!(facet_task.await.unwrap_err().is_cancelled());

    let project = PathBuf::from(
        std::env::var_os("CELLD_APPLICATION_MEMORY_PYTHON_PROJECT")
            .expect("prepare the dedicated Python memory project"),
    );
    assert_eq!(
        content_digest(&std::fs::read(project.join("src/entry.py")).unwrap()),
        content_digest(PYTHON_SOURCE.as_bytes()),
        "prepared Python source differs from this fixture"
    );
    recorder.context(
        "followon-python-artifact",
        Some(ApplicationSizeClass::Basic),
        0,
    );
    let python_config = project.join("wrangler.jsonc");
    let began = Instant::now();
    let (bundle, worker_config) =
        crate::asyncrt::blocking(move || crate::application_cli::python_artifact(python_config))
            .await
            .unwrap()
            .unwrap();
    recorder.emit(
        json!({"kind":"pythonArtifact","durationUs":began.elapsed().as_micros(),
        "artifactSha256":content_digest(&bundle),"logicalBytes":bundle.len(),
        "workerConfigSha256":content_digest(&serde_json::to_vec(&worker_config).unwrap())}),
    );
    let lookup = LaunchOptions {
        templates: pool.templates.clone(),
        ..options.clone()
    };
    let shared = pool.templates.pyodide(&lookup, app).await.unwrap();
    let metadata = std::fs::metadata(&shared).unwrap();
    recorder.emit(json!({"kind":"pyodideSnapshotDisk","path":shared,"logicalBytes":metadata.len(),
        "allocatedBytes":metadata.blocks()*512,"device":metadata.dev(),"inode":metadata.ino(),
        "ownership":"pool-owned read-only shared Python runtime; disk bytes are not resident memory"}));
    // Preserve the exact generated metadata. The production acquire path adds
    // the already prepared shared drive when its pyodide field requires it.
    activate_bundle(
        store,
        options,
        &application,
        "python",
        bundle,
        ("index.js", worker_config, json!({})),
    )
    .await;
    let deployment = name("python");
    recorder.context(
        "followon-python-active",
        Some(ApplicationSizeClass::Basic),
        0,
    );
    let lease = followon_acquire(
        app,
        &pool,
        &application,
        &deployment,
        &handler,
        &recorder,
        "python-adapter-ready",
    )
    .await;
    let client = lease.client.clone();
    let pin = client.pin.clone();
    complete_sample(&recorder, "python-adapter-ready", &pool, &client, owned).await;
    let first = request(
        app,
        &lease,
        "/hello",
        &recorder,
        ApplicationSizeClass::Basic,
    )
    .await;
    assert_eq!(first["served"], 1);
    complete_sample(&recorder, "python-lazy-first-fetch", &pool, &lease, owned).await;
    let temporary = request(
        app,
        &lease,
        "/temporary",
        &recorder,
        ApplicationSizeClass::Basic,
    )
    .await;
    assert_eq!(temporary["bytes"], 8 * 1024 * 1024);
    assert_eq!(temporary["checksum"], 11 * 2048);
    complete_sample(&recorder, "python-temporary-released", &pool, &lease, owned).await;
    let held = request(app, &lease, "/hold", &recorder, ApplicationSizeClass::Basic).await;
    assert_eq!(held["bytes"], 8 * 1024 * 1024);
    let served = held["served"].as_u64().unwrap();
    complete_sample(&recorder, "python-held", &pool, &lease, owned).await;
    drop(lease);
    idle_samples(&recorder, "python-held", &pool, &client, owned, None).await;
    recorder.context(
        "followon-python-reuse",
        Some(ApplicationSizeClass::Basic),
        0,
    );
    let lease = followon_acquire(
        app,
        &pool,
        &application,
        &deployment,
        &handler,
        &recorder,
        "python-retained-reuse",
    )
    .await;
    assert!(Arc::ptr_eq(&pin, &lease.pin));
    let reused = request(
        app,
        &lease,
        "/status",
        &recorder,
        ApplicationSizeClass::Basic,
    )
    .await;
    assert_eq!(reused["served"], served + 1);
    assert_eq!(reused["bytes"], held["bytes"]);
    assert_eq!(reused["checksum"], held["checksum"]);
    assert_eq!(
        request(
            app,
            &lease,
            "/release",
            &recorder,
            ApplicationSizeClass::Basic
        )
        .await["bytes"],
        0
    );
    complete_sample(&recorder, "python-released", &pool, &lease, owned).await;
    drop(lease);
    idle_samples(&recorder, "python-released", &pool, &client, owned, None).await;
    recorder.context(
        "followon-python-final-reuse",
        Some(ApplicationSizeClass::Basic),
        0,
    );
    let lease = followon_acquire(
        app,
        &pool,
        &application,
        &deployment,
        &handler,
        &recorder,
        "python-released-reuse",
    )
    .await;
    assert!(Arc::ptr_eq(&pin, &lease.pin));
    assert_eq!(
        request(
            app,
            &lease,
            "/status",
            &recorder,
            ApplicationSizeClass::Basic
        )
        .await["bytes"],
        0
    );
    drop(lease);
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
    recorder.emit(json!({"kind":"cleanup","ownedPids":owned(),"remainingCgroups":leaked}));
    assert!(owned().is_empty());
    assert!(leaked.is_empty());
    recorder.emit(
        json!({"kind":"coverage","loaderFacets":true,"pythonPyodide":true,
        "requiredFollowonPhasesComplete":true,
        "pending":["pause/thaw and retained-client wake observations","repeat trials",
            "regression/pressure thresholds","16GiB density target"]}),
    );
    eprintln!(
        "T29_RESULT {}",
        json!({"scenario":"t33::memory_followons","status":"passed",
        "duration_ms":recorder.began.elapsed().as_millis(),"output":output,
        "scope":"M1 required workload follow-ons; lifecycle and threshold qualification pending"})
    );
}
