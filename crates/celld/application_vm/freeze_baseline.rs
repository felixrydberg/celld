//! Plain pause/resume observations on a warm assigned VM; no production freeze policy.
use super::memory_baseline::{
    activate_bundle, bridge_check, complete_sample, config, name, no_vms, Recorder,
};
use super::*;
use celld_applications::invocation_wire::HttpRequest;
use serde_json::{json, Value};
use std::os::unix::fs::FileTypeExt;

const SOURCE: &str = include_str!("../tests/fixtures/t33-memory/freeze.mjs");

pub(super) struct OwnedVmm {
    pid: u32,
    start_ticks: u64,
    cgroup: String,
    api: PathBuf,
}

impl OwnedVmm {
    pub(super) fn identify(owned: fn() -> HashSet<u32>) -> Self {
        let pids = owned();
        assert_eq!(pids.len(), 1, "benchmark must own exactly one assigned VMM");
        Self::identify_pid(*pids.iter().next().unwrap(), owned)
    }

    pub(super) fn identify_pid(pid: u32, owned: fn() -> HashSet<u32>) -> Self {
        assert!(owned().contains(&pid), "VMM is not owned by this fixture");
        let (parent, start_ticks) = process_identity(pid).unwrap();
        assert_eq!(parent, std::process::id());
        let cgroup = std::fs::read_to_string(format!("/proc/{pid}/cgroup")).unwrap();
        assert!(cgroup
            .lines()
            .any(|line| line.starts_with("0::") && line.contains("/celld-vms-")));
        let api = PathBuf::from(format!("/proc/{pid}/root/api.sock"));
        let result = Self {
            pid,
            start_ticks,
            cgroup,
            api,
        };
        result.verify(owned).unwrap();
        result
    }

    fn verify(&self, owned: fn() -> HashSet<u32>) -> Result<()> {
        ensure!(owned().contains(&self.pid), "owned VMM set changed");
        let (parent, start_ticks) = process_identity(self.pid)?;
        ensure!(
            parent == std::process::id() && start_ticks == self.start_ticks,
            "VMM identity changed"
        );
        let comm = std::fs::read_to_string(format!("/proc/{}/comm", self.pid))?;
        ensure!(
            matches!(comm.trim(), "firecracker" | "jailer"),
            "unexpected VMM executable"
        );
        ensure!(
            std::fs::read_to_string(format!("/proc/{}/cgroup", self.pid))? == self.cgroup,
            "VMM cgroup changed"
        );
        ensure!(
            std::fs::metadata(&self.api)?.file_type().is_socket(),
            "VMM API is not a socket"
        );
        Ok(())
    }

    async fn state(&self, owned: fn() -> HashSet<u32>) -> Result<Value> {
        self.read_json("/", owned).await
    }

    pub(super) async fn read_json(&self, path: &str, owned: fn() -> HashSet<u32>) -> Result<Value> {
        use http_body_util::{BodyExt, Full, Limited};
        use hyper_util::rt::TokioIo;
        self.verify(owned)?;
        timeout(Duration::from_secs(2), async {
            let stream = tokio::net::UnixStream::connect(&self.api).await?;
            let (mut sender, connection) =
                hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
            let _connection = AbortOnDropHandle::new(tokio::spawn(connection));
            let request = hyper::Request::builder()
                .method("GET")
                .uri(path)
                .header("host", "firecracker")
                .body(Full::new(Bytes::new()))?;
            let response = sender.send_request(request).await?;
            ensure!(
                response.status() == 200,
                "Firecracker instance state refused"
            );
            let body = Limited::new(response.into_body(), 4096)
                .collect()
                .await
                .map_err(|error| anyhow::anyhow!("instance state body: {error}"))?
                .to_bytes();
            Ok(serde_json::from_slice(&body)?)
        })
        .await
        .context("instance state query timed out")?
    }

    async fn transition(
        &self,
        state: &str,
        owned: fn() -> HashSet<u32>,
        recorder: &Recorder,
    ) -> Result<()> {
        self.verify(owned)?;
        let began = Instant::now();
        timeout(
            Duration::from_secs(2),
            super::request(&self.api, "PATCH", "/vm", json!({"state":state})),
        )
        .await
        .context("plain VM transition timed out")??;
        let transition_latency = began.elapsed();
        let instance = self.state(owned).await?;
        let expected = if state == "Resumed" { "Running" } else { state };
        ensure!(
            instance["state"] == expected,
            "unexpected instance state: {instance}"
        );
        recorder.emit(json!({"kind":"freezeTransition", "requested":state,"instance":instance,
            "latencyUs":transition_latency.as_micros(),"stateVerifiedAfterUs":began.elapsed().as_micros(),"pid":self.pid,"startTicks":self.start_ticks}));
        Ok(())
    }
}

fn process_identity(pid: u32) -> Result<(u32, u64)> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let (_, tail) = stat.rsplit_once(')').context("malformed VMM proc stat")?;
    let fields: Vec<_> = tail.split_whitespace().collect();
    Ok((
        fields.get(1).context("missing parent PID")?.parse()?,
        fields
            .get(19)
            .context("missing process start ticks")?
            .parse()?,
    ))
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
                url: format!("http://freeze{path}"),
                headers: vec![],
                body: vec![],
            },
            None,
        )
        .await?;
    if let Some(mut stream) = response.stream.take() {
        use futures_util::StreamExt;
        while let Some(chunk) = stream.next().await {
            response
                .body
                .extend(chunk.map_err(|error| anyhow::anyhow!("guest stream: {error}"))?);
        }
    }
    ensure!(
        response.status == 200,
        "freeze fixture status {}",
        response.status
    );
    Ok(serde_json::from_slice(&response.body)?)
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
        std::env::var_os("CELLD_APPLICATION_MEMORY_OUTPUT").expect("set a fresh JSONL output"),
    );
    let recorder = Recorder::new(&output, "freeze-bootstrap");
    recorder.emit(json!({"kind":"provenance","kernel":options.kernel.sha256,"runtime":options.runtime.sha256,
        "firecrackerSha256":content_digest(&std::fs::read(&options.firecracker).unwrap()),
        "fixtureSha256":content_digest(SOURCE.as_bytes()),"pauseSeconds":[2,5,30,2,5],
        "invocationLimitMs":60000,"policy":"plain PATCH /vm on the original assigned VM; full reservations; no GC",
        "scope":"control and retained-client survival only; no guest heartbeat claim"}));
    let pool = Arc::new(VmPool::new(config(options, 0, 120), store.clone()).unwrap());
    let (stop, sampler) = recorder.start_sampler(&pool, owned);
    pool.prepare(app).await.unwrap();
    recorder.disk(&pool);
    let application = ApplicationId {
        app: name("memory-freeze"),
    };
    let mut catalog = store.read(&application).await.unwrap();
    assert!(
        catalog.application(&application).is_some(),
        "actor bridge did not initialize freeze application"
    );
    catalog
        .set_limits(
            &application,
            Some(ApplicationLimits {
                size_class: ApplicationSizeClass::Basic,
                invocation_timeout_ms: 60000,
            }),
        )
        .unwrap();
    store.commit(catalog).await.unwrap();
    let artifact =
        crate::application_image::build(&[("main.mjs".into(), SOURCE.as_bytes().to_vec())])
            .unwrap();
    activate_bundle(
        store,
        options,
        &application,
        "freeze",
        artifact,
        (
            "main.mjs",
            json!({"compatibility_date":"2026-03-01"}),
            json!({}),
        ),
    )
    .await;
    let lease = pool
        .acquire(app, &application, &name("freeze"), &name("memory"))
        .await
        .unwrap();
    let client = lease.client.clone();
    let owned_vm = OwnedVmm::identify(owned);
    assert_eq!(owned_vm.state(owned).await.unwrap()["state"], "Running");
    let initial = fetch(app, &client, "/status", Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(initial["checksum"], 77 * (8 * 1024 * 1024 / 4096));
    complete_sample(&recorder, "application-ready", &pool, &client, owned).await;
    for (trial, seconds) in [2, 5, 30, 2, 5].into_iter().enumerate() {
        recorder.context(
            &format!("freeze-trial-{trial}"),
            Some(ApplicationSizeClass::Basic),
            0,
        );
        let charged = pool.snapshot().memory_mib;
        assert_eq!(
            charged,
            u64::from(ApplicationSizeClass::Basic.resources().1)
        );
        let armed = fetch(app, &client, "/arm", Duration::from_secs(60))
            .await
            .unwrap();
        assert!(
            armed["timer"]["completed"].is_null(),
            "timer completed before pause"
        );
        let began = Instant::now();
        let host_wall_before = crate::ownership_store::now_ms();
        owned_vm
            .transition("Paused", owned, &recorder)
            .await
            .unwrap();
        recorder.host(
            "freeze-paused",
            Some(ApplicationSizeClass::Basic),
            &pool,
            owned,
        );
        let retained = client.clone();
        let actor = app.clone();
        let queued = Instant::now();
        let waiting = tokio::spawn(async move {
            fetch(&actor, &retained, "/status", Duration::from_secs(60)).await
        });
        tokio::time::sleep(Duration::from_secs(seconds)).await;
        assert!(
            !waiting.is_finished(),
            "retained client executed while VMM paused"
        );
        assert_eq!(
            pool.snapshot().memory_mib,
            charged,
            "plain pause changed nominal reservation"
        );
        recorder.host(
            "freeze-paused-end",
            Some(ApplicationSizeClass::Basic),
            &pool,
            owned,
        );
        owned_vm
            .transition("Resumed", owned, &recorder)
            .await
            .unwrap();
        let after = waiting.await.unwrap().unwrap();
        let host_wall_after = crate::ownership_store::now_ms();
        let skew = after["clocks"]["wallMs"].as_i64().unwrap() - host_wall_after as i64;
        assert!(skew.abs() < 1000, "thawed guest clock skew is {skew} ms");
        let retained_latency = queued.elapsed();
        assert!(after["calls"].as_u64().unwrap() > armed["calls"].as_u64().unwrap());
        assert_eq!(after["identity"], initial["identity"]);
        assert_eq!(after["checksum"], initial["checksum"]);
        let completed = timeout(Duration::from_secs(3), async {
            loop {
                let status = fetch(app, &client, "/status", Duration::from_secs(60))
                    .await
                    .unwrap();
                if !status["timer"]["completed"].is_null() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("refed timer did not finish after resume");
        owned_vm.verify(owned).unwrap();
        assert_eq!(completed["identity"], initial["identity"]);
        recorder.emit(json!({"kind":"freezeTrial","trial":trial,"pauseSeconds":seconds,
            "hostElapsedUs":began.elapsed().as_micros(),"retainedClientLatencyUs":retained_latency.as_micros(),
            "hostWallBeforePauseMs":host_wall_before,"hostWallAfterResumeMs":host_wall_after,"guestWallSkewMs":skew,
            "originalRequestDeadlineMs":60000,"nominalChargedMib":charged,
            "before":armed,"afterRetainedClient":after,"timerCompleted":completed,
            "pid":owned_vm.pid,"startTicks":owned_vm.start_ticks,
            "observation":"guest wall/performance and overdue timer behavior are recorded, not assumed"}));
        complete_sample(&recorder, "freeze-resumed", &pool, &client, owned).await;
    }
    // Keep the original caller deadline while the guest cannot run its cancel.
    recorder.context(
        "freeze-original-deadline",
        Some(ApplicationSizeClass::Basic),
        0,
    );
    owned_vm
        .transition("Paused", owned, &recorder)
        .await
        .unwrap();
    let actor = app.clone();
    let retained = client.clone();
    let timed = tokio::spawn(async move {
        let began = Instant::now();
        let result = fetch(&actor, &retained, "/slow", Duration::from_millis(500)).await;
        (began.elapsed(), result)
    });
    tokio::time::sleep(Duration::from_millis(950)).await;
    let resume = owned_vm.transition("Resumed", owned, &recorder).await;
    let (elapsed, result) = timed.await.unwrap();
    assert!(
        result.is_err(),
        "paused request escaped its original 500 ms deadline"
    );
    assert!(
        elapsed >= Duration::from_millis(450),
        "request rejected before its host deadline: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(900),
        "host deadline waited for resume: {elapsed:?}"
    );
    let probe = if resume.is_ok() {
        fetch(app, &client, "/status", Duration::from_secs(2)).await
    } else {
        Err(anyhow::anyhow!(
            "VM already retired before grace-case resume"
        ))
    };
    recorder.emit(json!({"kind":"freezeDeadline","originalDeadlineMs":500,"pauseMs":950,
        "elapsedUs":elapsed.as_micros(),"requestError":format!("{:#}",result.unwrap_err()),
        "resumeError":resume.err().map(|error|format!("{error:#}")),
        "retainedClientAlive":probe.is_ok(),"probeError":probe.err().map(|error|format!("{error:#}")),
        "policy":"deadline must reject; retirement after cancellation is an acceptable baseline outcome"}));
    drop(client);
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
    assert!(leaked.is_empty(), "freeze fixture cgroups remain");
    recorder.emit(json!({"kind":"cleanup","ownedPids":owned(),"remainingCgroups":leaked}));
    eprintln!(
        "T29_RESULT {}",
        json!({"scenario":"t33::memory_freeze","status":"passed",
        "duration_ms":recorder.began.elapsed().as_millis(),"output":output,
        "scope":"five plain freeze trials and original deadline; no production gates or heartbeat qualification"})
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires explicit diagnostic JavaScript image, Linux KVM and jailer"]
async fn t33_memory_freeze() {
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_FREEZE_ONLY").is_some());
    bridge_check("memory-freeze").await;
}
