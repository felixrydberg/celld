//! The assigned-VM monitor owns all pause/resume I/O; frontend tasks only wait.
use super::lifecycle::{Gate, Phase, Status};
use super::*;
use std::sync::Weak;
use tokio::sync::Notify;

#[cfg(test)]
pub(super) mod resume_ack_test {
    use super::*;

    struct Injection {
        pid: u32,
        start_ticks: u64,
        token: Arc<()>,
        executed: oneshot::Sender<(u64, u64)>,
    }
    static NEXT: Mutex<Option<Injection>> = Mutex::new(None);

    pub(in crate::application_vm) struct Withheld {
        token: Arc<()>,
        pub(in crate::application_vm) executed: oneshot::Receiver<(u64, u64)>,
    }

    fn live_start_ticks(pid: u32) -> Result<u64> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
        let tail = stat.rsplit_once(')').context("invalid owned VMM stat")?.1;
        let fields: Vec<_> = tail.split_whitespace().collect();
        ensure!(fields.len() > 19, "short owned VMM stat");
        ensure!(
            fields[1].parse::<u32>()? == std::process::id(),
            "VMM is not a direct child"
        );
        ensure!(!matches!(fields[0], "Z" | "X" | "x"), "VMM is not live");
        Ok(fields[19].parse()?)
    }

    impl Withheld {
        pub(in crate::application_vm) fn arm(pid: u32, start_ticks: u64) -> Result<Self> {
            ensure!(
                live_start_ticks(pid)? == start_ticks,
                "original VMM identity changed"
            );
            let mut next = NEXT.lock().expect("Resume injection poisoned");
            ensure!(next.is_none(), "Resume injection already armed");
            let token = Arc::new(());
            let (send, executed) = oneshot::channel();
            *next = Some(Injection {
                pid,
                start_ticks,
                token: token.clone(),
                executed: send,
            });
            Ok(Self { token, executed })
        }
    }

    impl Drop for Withheld {
        fn drop(&mut self) {
            let mut next = NEXT.lock().expect("Resume injection poisoned");
            if next
                .as_ref()
                .is_some_and(|entry| Arc::ptr_eq(&entry.token, &self.token))
            {
                *next = None;
            }
        }
    }

    #[derive(Clone, Copy)]
    struct SocketIdentity {
        dev: u64,
        ino: u64,
    }

    struct ResponseLossInjection {
        pid: u32,
        start_ticks: u64,
        expected_api: SocketIdentity,
        token: Arc<()>,
        executed: Option<oneshot::Sender<u64>>,
        proxy_path: Option<std::path::PathBuf>,
        task: Option<tokio::task::JoinHandle<()>>,
    }

    static RESPONSE_LOSS: Mutex<Option<ResponseLossInjection>> = Mutex::new(None);

    pub(in crate::application_vm) struct ResponseLost {
        token: Arc<()>,
        pub(in crate::application_vm) executed: oneshot::Receiver<u64>,
    }

    fn socket_identity(path: &std::path::Path) -> Result<SocketIdentity> {
        let metadata = std::fs::metadata(path)?;
        ensure!(
            std::os::unix::fs::FileTypeExt::is_socket(&metadata.file_type()),
            "Firecracker API path is not a Unix socket"
        );
        Ok(SocketIdentity {
            dev: metadata.dev(),
            ino: metadata.ino(),
        })
    }

    fn owned_api_socket(pid: u32) -> Result<std::path::PathBuf> {
        // /proc's root is a namespace-aware handle; its symlink text is not a host path.
        let api = std::path::PathBuf::from(format!("/proc/{pid}/root/api.sock"));
        socket_identity(&api)?;
        Ok(api)
    }

    async fn forward_resume(
        listener: tokio::net::UnixListener,
        api: std::path::PathBuf,
        executed: oneshot::Sender<u64>,
    ) -> Result<()> {
        use http_body_util::{BodyExt, Full, Limited};
        use hyper::body::Incoming;
        use hyper::service::service_fn;
        use hyper_util::rt::TokioIo;

        let (client, _) = listener.accept().await?;
        let upstream = api;
        let executed = Arc::new(Mutex::new(Some(executed)));
        let service = service_fn(move |request: hyper::Request<Incoming>| {
            let upstream = upstream.clone();
            let executed = executed.clone();
            async move {
                let (parts, body) = request.into_parts();
                let body = Limited::new(body, 4096)
                    .collect()
                    .await
                    .map_err(|error| anyhow::anyhow!("Resume proxy request body: {error}"))?
                    .to_bytes();
                let stream = tokio::net::UnixStream::connect(upstream).await?;
                let (mut sender, connection) =
                    hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
                let _connection = AbortOnDropHandle::new(tokio::spawn(connection));
                let request = hyper::Request::from_parts(parts, Full::new(body));
                let response = sender.send_request(request).await?;
                let status = response.status();
                Limited::new(response.into_body(), 4096)
                    .collect()
                    .await
                    .map_err(|error| anyhow::anyhow!("Resume proxy response body: {error}"))?;
                ensure!(
                    status == hyper::StatusCode::NO_CONTENT || status == hyper::StatusCode::OK,
                    "Firecracker Resume response was {status}"
                );
                let executed = executed
                    .lock()
                    .unwrap()
                    .take()
                    .context("Resume proxy already consumed")?;
                let _ = executed.send(crate::asyncrt::mono_ms());
                // Keep the host connection blackholed until its original monitor timeout
                // drops it, or ResponseLost RAII aborts this helper.
                std::future::pending::<Result<hyper::Response<Full<bytes::Bytes>>, anyhow::Error>>()
                    .await
            }
        });
        hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(client), service)
            .await
            .map_err(Into::into)
    }

    impl ResponseLost {
        pub(in crate::application_vm) fn arm(pid: u32, start_ticks: u64) -> Result<Self> {
            ensure!(
                live_start_ticks(pid)? == start_ticks,
                "original VMM identity changed"
            );
            let expected_api = socket_identity(&owned_api_socket(pid)?)?;
            let token = Arc::new(());
            let (send, executed) = oneshot::channel();
            let mut next = RESPONSE_LOSS.lock().expect("Resume response-loss poisoned");
            ensure!(next.is_none(), "Resume response loss already armed");
            *next = Some(ResponseLossInjection {
                pid,
                start_ticks,
                expected_api,
                token: token.clone(),
                executed: Some(send),
                proxy_path: None,
                task: None,
            });
            Ok(Self { token, executed })
        }
    }

    impl Drop for ResponseLost {
        fn drop(&mut self) {
            let mut next = RESPONSE_LOSS.lock().expect("Resume response-loss poisoned");
            if next
                .as_ref()
                .is_some_and(|entry| Arc::ptr_eq(&entry.token, &self.token))
            {
                next.take();
            }
        }
    }

    impl Drop for ResponseLossInjection {
        fn drop(&mut self) {
            if let Some(task) = self.task.take() {
                task.abort();
            }
            if let Some(path) = self.proxy_path.take() {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    pub(in crate::application_vm) fn route(
        api: &std::path::Path,
        method: &str,
        path: &str,
        value: &serde_json::Value,
    ) -> Result<std::path::PathBuf> {
        if method != "PATCH"
            || path != "/vm"
            || value.get("state").and_then(serde_json::Value::as_str) != Some("Resumed")
        {
            return Ok(api.to_owned());
        }
        let mut next = RESPONSE_LOSS.lock().expect("Resume response-loss poisoned");
        let Some(injection) = next.as_mut() else {
            return Ok(api.to_owned());
        };
        ensure!(
            live_start_ticks(injection.pid)? == injection.start_ticks,
            "original Resume VMM identity changed"
        );
        let actual = socket_identity(api)?;
        ensure!(
            injection.expected_api.dev == actual.dev && injection.expected_api.ino == actual.ino,
            "Resume API socket identity changed"
        );
        if let Some(proxy) = &injection.proxy_path {
            return Ok(proxy.clone());
        }
        let proxy = api
            .parent()
            .context("Firecracker API socket has no parent")?
            .join(".rl");
        let listener = tokio::net::UnixListener::bind(&proxy)?;
        std::fs::set_permissions(&proxy, std::fs::Permissions::from_mode(0o600))?;
        let executed = injection
            .executed
            .take()
            .context("Resume response loss already consumed")?;
        let upstream = api.to_owned();
        injection.proxy_path = Some(proxy.clone());
        injection.task = Some(tokio::spawn(async move {
            if let Err(error) = forward_resume(listener, upstream, executed).await {
                tracing::warn!(error = %format!("{error:#}"), "Resume response-loss proxy failed");
            }
        }));
        Ok(proxy)
    }

    pub(super) async fn after_resume(pid: u32, deadline_ms: u64) -> Result<()> {
        let injection = {
            let mut next = NEXT.lock().expect("Resume injection poisoned");
            if next.as_ref().is_some_and(|entry| entry.pid == pid) {
                ensure!(
                    live_start_ticks(pid)? == next.as_ref().unwrap().start_ticks,
                    "original Resume VMM identity changed"
                );
                next.take()
            } else {
                None
            }
        };
        if let Some(injection) = injection {
            let _ = injection
                .executed
                .send((crate::asyncrt::mono_ms(), deadline_ms));
            // Real API completion already happened; only this timeout's completion is withheld.
            std::future::pending::<()>().await;
        }
        Ok(())
    }
}

pub(super) struct Assignment {
    pub gate: Arc<Gate>,
    pub leases: Weak<()>,
    pub changed: Arc<Notify>,
    pub cache: Option<Box<CacheAdmission>>,
}

pub(super) struct Control {
    assignment: Mutex<Option<oneshot::Sender<Assignment>>>,
    leases_changed: Notify,
}

impl Control {
    pub(super) fn new() -> (Arc<Self>, oneshot::Receiver<Assignment>) {
        let (send, receive) = oneshot::channel();
        (
            Arc::new(Self {
                assignment: Mutex::new(Some(send)),
                leases_changed: Notify::new(),
            }),
            receive,
        )
    }

    pub(super) fn install(&self, assignment: Assignment) -> Result<()> {
        self.assignment
            .lock()
            .expect("VM supervisor poisoned")
            .take()
            .context("VM lifecycle already assigned")?
            .send(assignment)
            .map_err(|_| anyhow::anyhow!("VM supervisor stopped before assignment"))
    }

    pub(super) fn lease_dropped(&self) {
        self.leases_changed.notify_waiters();
    }
}

pub(super) fn enabled(client: &Client) -> bool {
    #[cfg(test)]
    if std::env::var_os("CELLD_APPLICATION_MEMORY_CHECK_ONLY").is_some()
        && std::env::var_os("CELLD_APPLICATION_MEMORY_LIFECYCLE_CHECK_ONLY").is_none()
        && std::env::var_os("CELLD_APPLICATION_MEMORY_PRESSURE_CHECK_ONLY").is_none()
        && std::env::var_os("CELLD_APPLICATION_MEMORY_CACHE_CHECK_ONLY").is_none()
        && std::env::var_os("CELLD_APPLICATION_MEMORY_CACHE_FAILURE_CHECK_ONLY").is_none()
    {
        return false;
    }
    client.supports_idle_lifecycle()
}

fn should_probe(status: Status, leases: usize, pending: usize, now_ms: u64) -> bool {
    status.phase == Phase::Running
        && status.active_events == 0
        && pending == 0
        && (leases <= 1 || now_ms >= status.idle_deadline_ms || status.resume_requested)
}

fn transition_deadline(status: Status, now_ms: u64) -> u64 {
    status.idle_deadline_ms.min(now_ms.saturating_add(2000))
}

enum Wake {
    Stop,
    Retired,
    Exited(std::io::Result<ExitStatus>),
    Assigned(Result<Assignment, oneshot::error::RecvError>),
    Changed,
}

pub(super) async fn wait(
    jail: &mut Jail,
    dispatcher: Dispatcher,
    transport: &Client,
    stop: &CancellationToken,
    retired: &CancellationToken,
    control: &Control,
    mut assigned: oneshot::Receiver<Assignment>,
) -> Result<(&'static str, Option<ExitStatus>)> {
    let mut assignment: Option<Assignment> = None;
    let mut paused = false;
    let mut next_probe_ms = 0;
    loop {
        let wake = if let Some(current) = &assignment {
            let gate_changed = current.gate.changed.notified();
            let leases_changed = control.leases_changed.notified();
            let pool_changed = current.changed.notified();
            tokio::pin!(gate_changed, leases_changed, pool_changed);
            gate_changed.as_mut().enable();
            leases_changed.as_mut().enable();
            pool_changed.as_mut().enable();
            let status = current.gate.status();
            if status.phase == Phase::Retiring {
                return Ok(("idle retired", None));
            }
            if stop.is_cancelled() {
                Wake::Stop
            } else if retired.is_cancelled() {
                Wake::Retired
            } else {
                let progress = progress(
                    current,
                    transport,
                    jail,
                    stop,
                    retired,
                    &mut paused,
                    &mut next_probe_ms,
                )
                .await;
                match progress {
                    Err(error) => {
                        current.gate.retire();
                        current.changed.notify_waiters();
                        tracing::warn!(error=%format!("{error:#}"), pid=jail.pid, "assigned VM lifecycle retired");
                        return Ok(("lifecycle retired", None));
                    }
                    Ok(true) => {
                        current.changed.notify_waiters();
                        continue;
                    }
                    Ok(false) => {}
                }
                let status = current.gate.status();
                let now = crate::asyncrt::mono_ms();
                let wait_ms = if status.phase == Phase::Cached {
                    status.idle_deadline_ms.saturating_sub(now)
                } else if status.active_events == 0 && now < status.idle_deadline_ms {
                    (status.idle_deadline_ms - now).min(250)
                } else {
                    250
                };
                crate::asyncrt::select! {
                    _ = stop.cancelled() => Wake::Stop,
                    _ = retired.cancelled() => Wake::Retired,
                    status = jail.child.wait() => Wake::Exited(status),
                    _ = &mut gate_changed => Wake::Changed,
                    _ = &mut leases_changed => Wake::Changed,
                    _ = &mut pool_changed => Wake::Changed,
                    _ = crate::asyncrt::sleep(Duration::from_millis(wait_ms)) => Wake::Changed,
                }
            }
        } else {
            crate::asyncrt::select! {
                _ = stop.cancelled() => Wake::Stop,
                _ = retired.cancelled() => Wake::Retired,
                status = jail.child.wait() => Wake::Exited(status),
                result = &mut assigned => Wake::Assigned(result),
            }
        };
        match wake {
            Wake::Stop => {
                let uncertain = assignment
                    .as_ref()
                    .is_some_and(|current| current.gate.status().phase != Phase::Running);
                if paused || uncertain {
                    // A frozen or uncertain guest cannot acknowledge Shutdown.
                    return Ok(("retired", None));
                }
                let reason = if dispatcher.shutdown().await.is_ok() {
                    "shutdown"
                } else {
                    "retired"
                };
                return Ok((reason, None));
            }
            Wake::Retired => return Ok(("retired", None)),
            Wake::Exited(status) => return Ok(("exited", Some(status?))),
            Wake::Assigned(Ok(current)) => assignment = Some(current),
            Wake::Assigned(Err(_)) => return Ok(("supervisor abandoned", None)),
            Wake::Changed => {}
        }
    }
}

async fn progress(
    current: &Assignment,
    transport: &Client,
    jail: &mut Jail,
    stop: &CancellationToken,
    retired: &CancellationToken,
    paused: &mut bool,
    next_probe_ms: &mut u64,
) -> Result<bool> {
    let api = jail.root.join("api.sock");
    let gate = &current.gate;
    let status = gate.status();
    let now = crate::asyncrt::mono_ms();
    match status.phase {
        Phase::Running
            if should_probe(
                status,
                current.leases.strong_count(),
                transport.pending(),
                now,
            ) && (now >= *next_probe_ms || status.resume_requested) =>
        {
            let Some(mut transition) = gate.begin_pause() else {
                return Ok(false);
            };
            // Unknown guest work may be busy after the host-only drain deadline.
            let probe_deadline = now.saturating_add(2000);
            transport.writer_fence(probe_deadline).await?;
            if !transport.idle_probe(probe_deadline).await? {
                transition.cancel_pause()?;
                *next_probe_ms = crate::asyncrt::mono_ms().saturating_add(250);
                return Ok(true);
            }
            transition.confirm_idle()?;
            if current.leases.strong_count() > 1 || gate.status().resume_requested {
                transition.cancel_pause_idle()?;
                *next_probe_ms = crate::asyncrt::mono_ms().saturating_add(250);
                return Ok(true);
            }
            transition.check()?;
            let deadline = transition_deadline(gate.status(), crate::asyncrt::mono_ms());
            // From this point an API error leaves the physical state uncertain.
            *paused = true;
            crate::asyncrt::timeout_at(
                deadline,
                request(&api, "PATCH", "/vm", serde_json::json!({"state":"Paused"})),
            )
            .await
            .context("VM Pause acknowledgement timed out")??;
            // Full charge survives until the write/readback and pool commit.
            let retained_limit = if let Some(cache) = &current.cache {
                let policy = cache
                    .config
                    .memory_guard
                    .and_then(|guard| guard.idle_cache)
                    .context("assigned cache policy absent")?;
                let nominal = cache
                    .nominal_mib
                    .checked_mul(1024 * 1024)
                    .context("VM nominal bytes overflow")?;
                match jail.retained_target(nominal, policy.retained_margin_bytes)? {
                    Some(limit) => {
                        let deadline =
                            transition_deadline(gate.status(), crate::asyncrt::mono_ms());
                        jail.set_memory_limit(Some(limit), deadline, stop, retired)
                            .await?;
                        cache.cached(gate, transition, limit)?;
                        limit
                    }
                    None => {
                        gate.confirm_pause(transition)?;
                        0
                    }
                }
            } else {
                gate.confirm_pause(transition)?;
                0
            };
            tracing::info!(
                event = "vm_cached",
                retained_limit_bytes = retained_limit,
                "assigned VM paused with confirmed reservation"
            );
            Ok(true)
        }
        Phase::Cached => {
            if now >= status.idle_deadline_ms {
                gate.retire();
                return Ok(true);
            }
            if !status.resume_requested {
                return Ok(false);
            };
            let had_retained_limit = current
                .cache
                .as_ref()
                .is_some_and(|cache| cache.retained_limit.load(Ordering::Relaxed) > 0);
            let transition = if let Some(cache) = &current.cache {
                match cache.resume(gate)? {
                    Some(transition) => transition,
                    None => {
                        cache.evict_for_resume(gate);
                        return Ok(false);
                    }
                }
            } else {
                let Some(transition) = gate.begin_resume() else {
                    return Ok(false); // canceled demand does not retire a cached owner
                };
                transition
            };
            // Full capacity is reserved BEFORE raising the owned leaf or Resume.
            transition.check()?;
            let deadline = transition_deadline(gate.status(), crate::asyncrt::mono_ms());
            if had_retained_limit {
                jail.set_memory_limit(None, deadline, stop, retired).await?;
                transition.check()?;
            }
            let resume = request(&api, "PATCH", "/vm", serde_json::json!({"state":"Resumed"}));
            #[cfg(test)]
            let resume = async {
                resume.await?;
                resume_ack_test::after_resume(jail.pid, deadline).await
            };
            crate::asyncrt::timeout_at(deadline, resume)
                .await
                .context("VM Resume acknowledgement timed out")??;
            gate.confirm_resume(transition)?;
            *paused = false;
            *next_probe_ms = crate::asyncrt::mono_ms().saturating_add(250);
            tracing::info!(
                event = "vm_resumed",
                "assigned VM resumed with original idle deadline"
            );
            Ok(true)
        }
        Phase::Pausing | Phase::Resuming => {
            anyhow::bail!("VM transition escaped its owning supervisor")
        }
        _ => Ok(false),
    }
}

#[cfg(test)]
#[path = "../unit_tests/application_vm/assigned_supervisor/tests.rs"]
mod tests;
