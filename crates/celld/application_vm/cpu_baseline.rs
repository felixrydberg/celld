//! Scoped CPU quota qualification; this fixture does not isolate host CPU affinity.
use super::guard_baseline::Group;
use super::memory_baseline::{activate_bundle, bridge_check, config, name, stage, Recorder};
use super::*;
use anyhow::{ensure, Context};
use celld_applications::invocation_wire::HttpRequest;
use futures_util::StreamExt;
use nix::fcntl::{openat, OFlag};
use nix::sys::stat::Mode;
use serde_json::{json, Value};
use std::fs::File;

const MIB: u64 = 1024 * 1024;
const CLASSES: [ApplicationSizeClass; 2] =
    [ApplicationSizeClass::Lite, ApplicationSizeClass::Basic];
const WORKER: &str = r#"
const identity = `${Date.now()}:${Math.random()}`;
const retained = new Uint8Array(1024 * 1024);
for (let i = 0; i < retained.length; i += 4096) retained[i] = 73;
let calls = 0, result = 0;
export default {async fetch(request) {
  calls++;
  const url = new URL(request.url);
  if (url.pathname === '/delay') await new Promise(resolve => setTimeout(resolve, 500));
  const batches = url.pathname === '/saturated' ? 1024 : url.pathname === '/burst' ? 16 : 0;
  const began = Date.now();
  let rounds = 0;
  // The isolate clock advances at I/O boundaries; use fixed work for CPU saturation.
  while (rounds < batches) {
    for (let i = 0; i < 65536; i++) result = (Math.imul(result ^ i, 1664525) + 1013904223) | 0;
    rounds++;
  }
  let checksum = 0;
  for (let i = 0; i < retained.length; i += 4096) checksum += retained[i];
  return Response.json({identity, checksum, calls, rounds, result, wallMs: Date.now() - began});
}};
"#;

fn text(group: &Group, control: &str) -> Result<String> {
    let file = File::from(openat(
        &group.directory,
        control,
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )?);
    let mut value = String::new();
    file.take(4097).read_to_string(&mut value)?;
    ensure!(value.len() <= 4096, "oversized owned CPU sample");
    Ok(value)
}
fn counter(value: &str, key: &str) -> Result<u64> {
    let values: Vec<_> = value
        .lines()
        .filter_map(|line| line.split_once(' '))
        .filter(|(name, _)| *name == key)
        .collect();
    ensure!(values.len() == 1, "missing or duplicate CPU counter {key}");
    Ok(values[0].1.parse()?)
}
fn psi(value: &str, key: &str) -> Result<u64> {
    let line = value
        .lines()
        .find(|line| line.starts_with(&format!("{key} ")))
        .context("missing CPU pressure line")?;
    Ok(line
        .split_whitespace()
        .find_map(|field| field.strip_prefix("total="))
        .context("missing CPU pressure total")?
        .parse()?)
}
fn allowed(pid: u32) -> Result<String> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
    Ok(status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
        .context("process has no allowed CPU list")?
        .trim()
        .into())
}
fn sample(group: &Group, owned: fn() -> HashSet<u32>) -> Value {
    group.verify_member(owned).unwrap();
    let stat = text(group, "cpu.stat").unwrap();
    let pressure = text(group, "cpu.pressure").unwrap();
    let value = json!({"pid":group.member.unwrap().0,"startTicks":group.member.unwrap().1,
        "cpuMax":text(group,"cpu.max").unwrap().trim(),"cpuStat":stat,
        "usageUs":counter(&stat,"usage_usec").unwrap(),
        "periods":counter(&stat,"nr_periods").unwrap(),
        "throttledPeriods":counter(&stat,"nr_throttled").unwrap(),
        "throttledUs":counter(&stat,"throttled_usec").unwrap(),
        "cpuPressure":pressure,"someUs":psi(&pressure,"some").unwrap(),"fullUs":psi(&pressure,"full").unwrap(),
        "allowedCpuList":allowed(group.member.unwrap().0).unwrap()});
    group.verify_member(owned).unwrap();
    value
}
fn quota(group: &Group, class: ApplicationSizeClass, owned: fn() -> HashSet<u32>) {
    group.verify_member(owned).unwrap();
    assert_eq!(
        text(group, "cpu.max").unwrap().trim(),
        format!("{} 100000", class.resources().2)
    );
}
async fn state(group: &Group, owned: fn() -> HashSet<u32>) -> Result<Value> {
    use http_body_util::{BodyExt, Full, Limited};
    use hyper_util::rt::TokioIo;
    group.verify_member(owned)?;
    let value = timeout(Duration::from_secs(2), async {
        let stream = tokio::net::UnixStream::connect(format!(
            "/proc/{}/root/api.sock",
            group.member.unwrap().0
        ))
        .await?;
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
        let _connection = AbortOnDropHandle::new(tokio::spawn(connection));
        let response = sender
            .send_request(
                hyper::Request::builder()
                    .method("GET")
                    .uri("/")
                    .header("host", "firecracker")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        ensure!(response.status() == 200, "owned VMM state query refused");
        let body = Limited::new(response.into_body(), 4096)
            .collect()
            .await
            .map_err(|error| anyhow::anyhow!("CPU VMM state body: {error}"))?
            .to_bytes();
        Ok::<Value, anyhow::Error>(serde_json::from_slice(&body)?)
    })
    .await
    .context("CPU VMM state query timed out")??;
    group.verify_member(owned)?;
    Ok(value)
}
async fn fetch(
    app: &crate::actor::AppHandle,
    client: &VmClient,
    path: &str,
    deadline: Duration,
) -> Result<Value> {
    let mut response = client
        .invoke_stateless(
            app,
            deadline,
            HttpRequest {
                method: "GET".into(),
                url: format!("http://cpu{path}"),
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
                .extend(chunk.map_err(|error| anyhow::anyhow!("CPU stream: {error}"))?);
        }
    }
    ensure!(
        response.status == 200,
        "CPU request status {}",
        response.status
    );
    Ok(serde_json::from_slice(&response.body)?)
}
fn integrity(value: &Value, original: &Value) {
    assert_eq!(value["identity"], original["identity"]);
    assert_eq!(value["checksum"], 73 * 256);
}
async fn until(mut check: impl FnMut() -> bool, label: &str) {
    timeout(Duration::from_secs(30), async {
        while !check() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("CPU fixture timed out: {label}"));
}
async fn control(
    app: crate::actor::AppHandle,
    stop: CancellationToken,
    recorder: Arc<Recorder>,
) -> u32 {
    let mut count = 0;
    let mut next = tokio::time::Instant::now();
    while !stop.is_cancelled() {
        tokio::time::sleep_until(next).await;
        let lateness = next.elapsed().as_micros();
        let began = Instant::now();
        let status = timeout(Duration::from_secs(2), app.snapshot())
            .await
            .expect("Actor control unresponsive during CPU work");
        let status: Value = serde_json::from_str(&status).unwrap();
        assert!(
            status.get("error").is_none(),
            "Actor control stopped during CPU work"
        );
        recorder.emit(
            json!({"kind":"cpuControl","actorLatencyUs":began.elapsed().as_micros(),
            "heartbeatLatenessUs":lateness,"periodMs":100}),
        );
        count += 1;
        assert!(
            count <= 1000,
            "CPU control recorder exceeded bounded sample count"
        );
        next = tokio::time::Instant::now() + Duration::from_millis(100);
    }
    count
}

pub(super) async fn run(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    _directory: &Path,
    owned: fn() -> HashSet<u32>,
) {
    assert!(owned().is_empty(), "pre-existing CPU fixture VM");
    let output = PathBuf::from(
        std::env::var_os("CELLD_APPLICATION_MEMORY_OUTPUT")
            .expect("CPU fixture requires new JSONL output"),
    );
    let startup: u32 = std::env::var("CELLD_APPLICATION_CPU_STARTUP_POOL")
        .expect("CPU fixture requires startup arm 0 or 1")
        .parse()
        .unwrap();
    assert!(startup <= 1, "CPU fixture startup arm must be 0 or 1");
    let recorder = Recorder::new(&output, "cpu-bootstrap");
    recorder.emit(json!({"kind":"cpuInputs","classes":CLASSES,"startupPoolArm":startup,
        "hostAllowedCpuList":allowed(std::process::id()).unwrap(),"isolatedCpuSet":false,
        "affinityWrites":false,"fairMinimumClaim":false,
        "scope":"owned leaf exact quota and observed throttling/control latency; shared CPU placement"}));
    let bundle =
        crate::application_image::build(&[("main.mjs".into(), WORKER.as_bytes().to_vec())])
            .unwrap();
    {
        recorder.context("cpu-bootstrap", None, startup);
        let mut settings = config(options, startup, 120);
        settings.max_vms = 5;
        settings.memory_guard = Some(MemoryGuard {
            max_bytes: 14 * 1024 * MIB,
            high_bytes: 13 * 1024 * MIB,
            overhead_bytes: 256 * MIB,
            shared_cache_bytes: 256 * MIB,
            idle_cache: None,
        });
        let pool =
            Arc::new(VmPool::new(settings, store.clone(), format!("cpu-{startup}")).unwrap());
        let parent = Group::registered_parent().unwrap();
        pool.prepare(app).await.unwrap();
        assert_eq!(pool.templates.state_files().len(), 6);
        let mut targets = Vec::new();
        for (index, class) in CLASSES.into_iter().enumerate() {
            recorder.context("cpu-acquire", Some(class), startup);
            let application = stage(store, options, &format!("cpu-{startup}-{index}"), class).await;
            activate_bundle(
                store,
                options,
                &application,
                "cpu",
                bundle.clone(),
                (
                    "main.mjs",
                    json!({"compatibility_date":"2026-03-01"}),
                    json!({}),
                ),
            )
            .await;
            let claimed = if startup == 1 {
                // Seed this class, then use the production refill as the next claim.
                let seed = pool
                    .acquire(app, &application, &name("cpu"), &name("memory"))
                    .await
                    .unwrap();
                fetch(app, &seed, "/status", Duration::from_secs(10))
                    .await
                    .unwrap();
                drop(seed);
                pool.retire_deployment(&application, &name("cpu")).await;
                until(
                    || {
                        pool.snapshot().vms == index as u32
                            && pool.templates.status()["templates"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .any(|entry| {
                                    entry["memoryMib"] == class.resources().1
                                        && entry["idle"] == 1
                                        && entry["preparing"] == 0
                                })
                    },
                    "class startup refill missing",
                )
                .await;
                let candidates: Vec<_> = owned()
                    .into_iter()
                    .filter_map(|pid| Group::leaf(pid, &parent, owned).unwrap())
                    .filter(|group| {
                        text(group, "cpu.max").unwrap().trim()
                            == format!("{} 100000", class.resources().2)
                    })
                    .collect();
                assert_eq!(
                    candidates.len(),
                    1,
                    "expected exactly one idle VM of selected class"
                );
                let group = candidates.into_iter().next().unwrap();
                quota(&group, class, owned);
                assert_eq!(
                    state(&group, owned).await.unwrap()["state"],
                    "Running",
                    "startup-pool VM was paused"
                );
                Some(group)
            } else {
                None
            };
            // A new application has the same template key without reviving a retiring generation.
            let target = if startup == 1 {
                let target = stage(store, options, &format!("cpu-claim-{index}"), class).await;
                activate_bundle(
                    store,
                    options,
                    &target,
                    "cpu",
                    bundle.clone(),
                    (
                        "main.mjs",
                        json!({"compatibility_date":"2026-03-01"}),
                        json!({}),
                    ),
                )
                .await;
                target
            } else {
                application
            };
            let before = owned();
            let hits = pool.templates.status()["hits"].as_u64().unwrap();
            let began = Instant::now();
            let lease = pool
                .acquire(app, &target, &name("cpu"), &name("memory"))
                .await
                .unwrap();
            let group = if let Some(group) = claimed {
                assert_eq!(
                    pool.templates.status()["hits"],
                    hits + 1,
                    "nonzero startup arm did not claim pooled VM"
                );
                group.verify_member(owned).unwrap();
                group
            } else {
                let after = owned();
                let added: Vec<_> = after.difference(&before).copied().collect();
                assert_eq!(added.len(), 1);
                Group::leaf(added[0], &parent, owned).unwrap().unwrap()
            };
            quota(&group, class, owned);
            let original = fetch(app, &lease, "/status", Duration::from_secs(10))
                .await
                .unwrap();
            recorder.emit(json!({"kind":"cpuAcquire","class":class,"mode":if startup==0 {"on-demand-prebuilt"} else {"startup-pool-claimed"},
                "latencyUs":began.elapsed().as_micros(),"quota":sample(&group,owned),"templates":pool.templates.status()}));
            targets.push((class, lease, group, original));
        }
        let stop = CancellationToken::new();
        let monitor = AbortOnDropHandle::new(tokio::spawn(control(
            app.clone(),
            stop.clone(),
            recorder.clone(),
        )));
        let before: Vec<_> = targets
            .iter()
            .map(|(_, _, group, _)| sample(group, owned))
            .collect();
        for phase in ["burst", "saturated", "mixed"] {
            recorder.context(&format!("cpu-{phase}"), None, startup);
            let mut work = Vec::new();
            for (class, lease, _, original) in &targets {
                let repeats = if phase == "saturated" { 1 } else { 4 };
                for index in 0..repeats {
                    let (actor, client, expected, recorder, class) = (
                        app.clone(),
                        lease.client.clone(),
                        original.clone(),
                        recorder.clone(),
                        *class,
                    );
                    let path = if phase == "saturated" || (phase == "mixed" && index == 0) {
                        "/saturated"
                    } else {
                        "/burst"
                    };
                    let began = Instant::now();
                    work.push(AbortOnDropHandle::new(tokio::spawn(async move {
                        let remaining = Duration::from_secs(10).checked_sub(began.elapsed()).expect("CPU request exhausted original deadline before dispatch");
                        let value = fetch(&actor,&client,path,remaining).await.unwrap();
                        integrity(&value,&expected);
                        assert!(value["rounds"].as_u64().unwrap()>0);
                        recorder.emit(json!({"kind":"cpuRequest","class":class,"phase":phase,
                            "operation":path,"originalDeadlineMs":10000,"latencyUs":began.elapsed().as_micros(),"result":value}));
                    })));
                }
            }
            timeout(Duration::from_secs(15), async {
                for task in work {
                    task.await.unwrap();
                }
            })
            .await
            .unwrap();
        }
        for (index, (class, lease, group, original)) in targets.iter().enumerate() {
            let after = sample(group, owned);
            let delta = |field: &str| {
                after[field]
                    .as_u64()
                    .unwrap()
                    .checked_sub(before[index][field].as_u64().unwrap())
                    .unwrap()
            };
            assert!(delta("usageUs") > 0);
            assert!(
                delta("throttledPeriods") > 0 && delta("throttledUs") > 0,
                "saturated JS did not exercise CPU quota"
            );
            quota(group, *class, owned);
            integrity(
                &fetch(app, lease, "/status", Duration::from_secs(10))
                    .await
                    .unwrap(),
                original,
            );
            recorder.emit(
                json!({"kind":"cpuDelta","class":class,"before":before[index],"after":after,
                "usageUs":delta("usageUs"),"periods":delta("periods"),
                "throttledPeriods":delta("throttledPeriods"),"throttledUs":delta("throttledUs"),
                "someUs":delta("someUs"),"fullUs":delta("fullUs")}),
            );
            let began = Instant::now();
            assert!(
                timeout(
                    Duration::from_secs(2),
                    fetch(app, lease, "/delay", Duration::from_millis(50))
                )
                .await
                .unwrap()
                .is_err(),
                "original request deadline was not enforced"
            );
            recorder.emit(json!({"kind":"cpuDeadline","class":class,"originalDeadlineMs":50,"wallUs":began.elapsed().as_micros()}));
            integrity(
                &fetch(app, lease, "/status", Duration::from_secs(10))
                    .await
                    .unwrap(),
                original,
            );
        }
        stop.cancel();
        assert!(monitor.await.unwrap() > 0);
        recorder.host("cpu-workload-complete", None, &pool, owned);
        drop(targets);
        timeout(Duration::from_secs(30), pool.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(owned().is_empty() && !parent.exists().unwrap());
        assert_eq!(
            (
                pool.snapshot().vms,
                pool.snapshot().memory_mib,
                pool.snapshot().quarantined
            ),
            (0, 0, 0)
        );
    }
    recorder.emit(json!({"kind":"cleanup","ownedPids":owned(),"quotaCoverageClasses":2,"startupPoolArm":startup,
        "pending":["owned-task CPU-set isolation","isolated competing-share qualification","multi-host repeat"]}));
    eprintln!(
        "T29_RESULT {}",
        json!({"scenario":"t33::cpu_quota","status":"passed",
        "duration_ms":recorder.began.elapsed().as_millis(),"output":output,
        "startupPoolArm":startup,"scope":"exact owned quotas, observed throttling, mixed request deadlines, retained state and Actor response; CPU affinity shared"})
    );
}
#[tokio::test]
#[ignore = "requires diagnostic JS image, Linux KVM and explicit CPU qualification run"]
async fn t33_cpu_quota() {
    assert!(std::env::var_os("CELLD_APPLICATION_CPU_CHECK_ONLY").is_some());
    bridge_check("cpu-quota").await;
}

#[test]
fn cpu_counter_samples_are_bounded_and_unambiguous() {
    assert_eq!(
        counter("usage_usec 5\nnr_throttled 2\n", "usage_usec").unwrap(),
        5
    );
    assert!(counter("usage_usec 5\nusage_usec 6\n", "usage_usec").is_err());
    assert!(counter("other 1\n", "usage_usec").is_err());
    assert_eq!(
        psi("some avg10=0.00 avg60=0.00 avg300=0.00 total=7\n", "some").unwrap(),
        7
    );
    assert!(psi("some avg10=0.00\n", "some").is_err());
}
