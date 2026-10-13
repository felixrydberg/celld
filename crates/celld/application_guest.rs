//! Owned host lifetime for a guest storage event. Transport adapters supply
//! the handler future; the original Actor and runtime retain all authority.

use crate::actor::{ActivityGuard, AppHandle, PublicRequestGuard};
use crate::application_storage::{StorageEvent, StorageSession};
use crate::application_vm::lifecycle::EventHold;
use crate::runtime::{RuntimeManager, StorageAdmission};
use anyhow::{ensure, Context, Result};
use celld_applications::invocation_wire::ObjectTarget;
use celld_applications::storage_wire::{Reply, Request};
use celld_applications::{ApplicationId, ResourceAddress, ResourceKind};
use futures_util::FutureExt;
use std::future::Future;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::{oneshot, watch, Mutex};

/// Same framing as the synchronous guest SDK, without blocking the Actor.
/// Keep a pending read future alive when multiplexing another channel: a
/// partially consumed prefix/body cannot be cancelled and restarted safely.
pub(crate) async fn read_frame(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<Option<Vec<u8>>> {
    read_frame_limited(reader, celld_applications::storage_wire::MAX_MESSAGE_BYTES).await
}

pub(crate) async fn read_frame_limited(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
    limit: usize,
) -> Result<Option<Vec<u8>>> {
    use tokio::io::AsyncReadExt;
    let mut length = [0; 4];
    if reader.read(&mut length[..1]).await? == 0 {
        return Ok(None);
    }
    // Idle control streams may wait indefinitely. Once a frame starts, a
    // guest cannot retain its allocation or block dispatch with a partial body.
    crate::asyncrt::timeout(Duration::from_secs(5), async {
        reader.read_exact(&mut length[1..]).await?;
        let length = u32::from_be_bytes(length) as usize;
        ensure!(length > 0 && length <= limit, "guest frame exceeds limit");
        let mut bytes = vec![0; length];
        reader.read_exact(&mut bytes).await?;
        Ok(Some(bytes))
    })
    .await
    .context("guest partial frame timed out")?
}

pub(crate) async fn write_frame(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    bytes: &[u8],
) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    ensure!(
        !bytes.is_empty() && bytes.len() <= celld_applications::storage_wire::MAX_MESSAGE_BYTES,
        "guest frame exceeds limit"
    );
    writer.write_u32(bytes.len() as u32).await?;
    writer.write_all(bytes).await?;
    Ok(())
}

struct State {
    admission: StorageAdmission,
    session: StorageSession,
    event: StorageEvent,
}

/// Host-only lifetime evidence. A stopped transport does not prove that guest
/// code has stopped. Only a joined guest event or the owning process's reap
/// can release the event's existing input hold for native settlement.
pub(crate) struct GuestSettlement {
    pub(crate) event: tokio_util::sync::CancellationToken,
    pub(crate) process: tokio_util::sync::CancellationToken,
}

impl GuestSettlement {
    async fn wait(&self) {
        crate::asyncrt::select! {
            _ = self.event.cancelled() => {},
            _ = self.process.cancelled() => {},
        }
    }
}

/// Host transport access to one event. Escaped clones retire when the driver
/// takes its state, so they cannot keep event grants or runtime permits alive.
/// Saved read cursors instead belong to the independently owned VM table.
#[derive(Clone)]
pub(crate) struct GuestStorage {
    runtime: RuntimeManager,
    state: Arc<Mutex<Option<State>>>,
    accepting: Arc<AtomicBool>,
    cancellation: watch::Receiver<bool>,
    grant: String,
    application: Option<ApplicationId>,
    object: Option<ObjectTarget>,
    settlement: Arc<std::sync::Mutex<Option<GuestSettlement>>>,
    cleanup_only: bool,
    alarm: Option<celld_applications::invocation_wire::AlarmInvocation>,
    finished: tokio_util::sync::CancellationToken,
    output: std::sync::Weak<GuestOutput>,
    /// Native container mutations this event owns; settlement joins them.
    effects: crate::application_container::Effects,
    /// Sockets this event opened to its container; settlement frees them.
    sockets: crate::application_container::Sockets,
    execution: Arc<std::sync::Mutex<Option<EventHold>>>,
    execution_deadline_ms: u64,
}

/// Trusted request identity and delivery lifetime. Storage/replay witnesses
/// keep only a weak reference, so they cannot pin a completed public request.
pub(crate) struct GuestOutput {
    pub(crate) app: AppHandle,
    cell: String,
    request: u64,
    activity: Arc<ActivityGuard>,
    _public: Option<Arc<PublicRequestGuard>>,
    proof: Mutex<()>,
    execution: Arc<std::sync::Mutex<Option<EventHold>>>,
}

impl GuestOutput {
    pub(crate) async fn prove(self: &Arc<Self>, access: &GuestStorage) -> Result<()> {
        self.activity.set_phase_only("guest_response_gate");
        let output = access.prepare_output().await?;
        self.gate(output).await
    }

    async fn gate(
        self: &Arc<Self>,
        output: crate::application_storage::PreparedOutput,
    ) -> Result<()> {
        let owner = self.clone();
        let execution = self
            .execution
            .lock()
            .expect("guest execution poisoned")
            .clone();
        let (reply, receive) = oneshot::channel();
        // The Actor accepts only one pending response output per request.
        // A reply, stream, egress or terminal can race another proof. Once
        // submitted, own that wait even if its frontend future is cancelled.
        crate::asyncrt::spawn(async move {
            let _execution = execution;
            let _proof = owner.proof.lock().await;
            if let Some(position) = output.position() {
                owner.activity.gate_started(position);
            }
            let result = output.gate_at(&owner.app, &owner.cell, owner.request).await;
            owner.activity.gate_finished(result.is_ok());
            let _ = reply.send(result);
        });
        receive.await.context("guest output proof task stopped")?
    }
}

struct Finished {
    finished: tokio_util::sync::CancellationToken,
    execution: Arc<std::sync::Mutex<Option<EventHold>>>,
}
impl Drop for Finished {
    fn drop(&mut self) {
        self.finished.cancel();
        self.execution
            .lock()
            .expect("guest execution poisoned")
            .take();
    }
}

pub(crate) fn execution_deadline_ms(duration: Duration) -> Result<u64> {
    deadline_after(crate::asyncrt::mono_ms(), duration)
}

fn deadline_after(now_ms: u64, duration: Duration) -> Result<u64> {
    let millis = u64::try_from(duration.as_millis())?;
    ensure!(millis > 0, "guest execution deadline must be positive");
    now_ms
        .checked_add(millis)
        .context("guest execution deadline overflow")
}

impl GuestStorage {
    pub(crate) fn retain_execution(&self, hold: EventHold) -> Result<()> {
        let mut execution = self.execution.lock().expect("guest execution poisoned");
        ensure!(
            !self.is_finished() && execution.is_none(),
            "guest execution already retained or finished"
        );
        *execution = Some(hold);
        Ok(())
    }

    pub(crate) fn execution_hold(&self) -> Option<EventHold> {
        self.execution
            .lock()
            .expect("guest execution poisoned")
            .clone()
    }

    pub(crate) fn execution_deadline_ms(&self) -> u64 {
        self.execution_deadline_ms
    }

    pub(crate) fn output(&self) -> Result<Arc<GuestOutput>> {
        self.output
            .upgrade()
            .context("guest output lifetime retired")
    }
    pub(crate) async fn wait_finished(&self) {
        self.finished.cancelled().await;
    }
    pub(crate) fn is_finished(&self) -> bool {
        self.finished.is_cancelled()
    }

    pub(crate) async fn retirement(
        runtime: RuntimeManager,
        admission: StorageAdmission,
        application: ApplicationId,
        object: ObjectTarget,
        deadline_ms: u64,
        execution: Option<EventHold>,
    ) -> Result<Self> {
        let (session, event) = runtime
            .begin_guest_storage_event(&admission, None, None)
            .await?;
        if let Some(hold) = &execution {
            hold.mark_admitted();
        }
        let grant = session.grant().into();
        let (_, cancellation) = watch::channel(false);
        let access = Self {
            runtime,
            state: Arc::new(Mutex::new(Some(State {
                admission,
                session,
                event,
            }))),
            accepting: Arc::new(AtomicBool::new(true)),
            cancellation,
            grant,
            application: Some(application),
            object: Some(object),
            settlement: Arc::new(std::sync::Mutex::new(None)),
            cleanup_only: true,
            alarm: None,
            finished: tokio_util::sync::CancellationToken::new(),
            output: std::sync::Weak::new(),
            effects: Default::default(),
            sockets: Default::default(),
            execution: Arc::new(std::sync::Mutex::new(None)),
            execution_deadline_ms: deadline_ms,
        };
        if let Some(hold) = execution {
            access.retain_execution(hold)?;
        }
        Ok(access)
    }

    pub(crate) async fn finish_retirement(&self) -> Result<()> {
        let _finished = Finished {
            finished: self.finished.clone(),
            execution: self.execution.clone(),
        };
        self.accepting.store(false, Ordering::Release);
        let settlement = self
            .settlement
            .lock()
            .expect("guest settlement poisoned")
            .take();
        if let Some(settlement) = settlement {
            settlement.wait().await;
        }
        self.effects.join().await;
        self.sockets.close();
        let State {
            admission,
            mut session,
            ..
        } = self
            .state
            .lock()
            .await
            .take()
            .context("guest retirement already settled")?;
        let result = self
            .runtime
            .admitted_storage_turn(&admission, |cells| session.revoke(cells))
            .await?;
        drop(admission);
        self.finished.cancel();
        result
    }

    #[cfg(unix)]
    pub(crate) async fn register_retirement(
        &self,
        guest: crate::application_guest_transport::GuestObject,
    ) -> Result<()> {
        let state = self.state.lock().await;
        let state = state.as_ref().context("guest event retired")?;
        self.runtime
            .admitted_storage_turn(&state.admission, |_| {
                self.runtime.register_guest_object(&state.admission, guest)
            })
            .await?
    }
    pub(crate) fn register_settlement(&self, settlement: GuestSettlement) -> Result<()> {
        ensure!(
            self.accepting.load(Ordering::Acquire),
            "guest event retired"
        );
        let mut current = self.settlement.lock().expect("guest settlement poisoned");
        ensure!(current.is_none(), "guest settlement already registered");
        *current = Some(settlement);
        Ok(())
    }

    /// Select the VM's cursor table under this event's current admission.
    /// The returned identity is an opaque correlation token, never a grant.
    pub(crate) async fn retain_read_cursors(
        &self,
        owner: &crate::application_storage::CursorResidencies,
    ) -> Result<String> {
        let mut state = self.state.lock().await;
        let State {
            admission, session, ..
        } = state.as_mut().context("guest event retired")?;
        self.runtime
            .admitted_storage_turn(admission, |_| {
                ensure!(
                    self.accepting.load(Ordering::Acquire) && !*self.cancellation.borrow(),
                    "guest event retired"
                );
                owner.attach(session, &self.runtime)
            })
            .await?
    }

    pub(crate) fn effects(&self) -> &crate::application_container::Effects {
        &self.effects
    }

    pub(crate) fn sockets(&self) -> &crate::application_container::Sockets {
        &self.sockets
    }

    /// The container bound to this live event's object.
    pub(crate) async fn container(&self) -> Result<Arc<crate::container::CellContainer>> {
        self.container_turn(|session| session.container()).await
    }

    /// As `container`, consuming the start this event opened.
    pub(crate) async fn take_container_run(
        &self,
        run: u64,
    ) -> Result<Arc<crate::container::CellContainer>> {
        self.container_turn(|session| session.take_container_run(run))
            .await
    }

    /// The native container handle of this event's object.
    pub(crate) async fn bind_container(
        &self,
        cell: Arc<crate::container::CellContainer>,
    ) -> Result<()> {
        self.container_turn(|session| session.bind_container(cell))
            .await
    }

    /// The session's residency check reads native storage, so container
    /// bookkeeping runs inside this event's admitted storage turn.
    async fn container_turn<T>(
        &self,
        f: impl FnOnce(&mut crate::application_storage::StorageSession) -> Result<T>,
    ) -> Result<T> {
        let mut state = self.state.lock().await;
        let State {
            admission, session, ..
        } = state.as_mut().context("guest event retired")?;
        self.runtime
            .admitted_storage_turn(admission, |_| {
                ensure!(
                    self.accepting.load(Ordering::Acquire) && !*self.cancellation.borrow(),
                    "guest event retired"
                );
                f(session)
            })
            .await?
    }

    pub(crate) fn alarm(&self) -> Option<celld_applications::invocation_wire::AlarmInvocation> {
        self.alarm.clone()
    }

    pub(crate) fn grant(&self) -> &str {
        &self.grant
    }

    pub(crate) fn application(&self) -> Result<&ApplicationId> {
        self.application
            .as_ref()
            .context("guest admission is not application scoped")
    }

    pub(crate) fn object(&self) -> Result<&ObjectTarget> {
        self.object
            .as_ref()
            .context("guest admission is not object scoped")
    }

    /// Sample an early reply without retiring this event's effect authority.
    /// Drop the state lock before waiting: the same event must remain able to
    /// finish a turn/transaction which is currently holding its reply closed.
    pub(crate) async fn prepare_output(
        &self,
    ) -> Result<crate::application_storage::PreparedOutput> {
        loop {
            let (output, waiting) = {
                let state = self.state.lock().await;
                let State {
                    admission,
                    session,
                    event,
                } = state.as_ref().context("guest event retired")?;
                self.runtime
                    .admitted_storage_turn(admission, |cells| {
                        ensure!(
                            self.accepting.load(Ordering::Acquire) && !*self.cancellation.borrow(),
                            "guest event retired"
                        );
                        if let Some(waiting) = session.wait_output(cells)? {
                            return Ok((None, Some(waiting)));
                        }
                        Ok::<_, anyhow::Error>((
                            Some(session.prepare_output(
                                cells,
                                event,
                                celld_logic::Channel::Response,
                            )?),
                            None,
                        ))
                    })
                    .await??
            };
            if let Some(output) = output {
                return Ok(output);
            }
            waiting
                .expect("closed output input gate has a waiter")
                .await
                .context("cell stopped before guest output")?
                .map_err(anyhow::Error::msg)?;
        }
    }

    #[cfg(test)]
    pub(crate) async fn execute(&self, request: Request) -> Result<Reply> {
        self.execute_while(request, || true).await
    }

    /// Transport retirement is checked inside the native turn, alongside the
    /// original activity verdict, rather than before awaiting a queued turn.
    pub(crate) async fn execute_while(
        &self,
        request: Request,
        accepting: impl Fn() -> bool,
    ) -> Result<Reply> {
        if matches!(
            request.operation,
            celld_applications::storage_wire::Operation::AbortObject { .. }
        ) {
            self.application()?;
            self.object()?;
        }
        if request.grant != self.grant {
            return self.execute_facet(request, accepting).await;
        }
        let mut state = self.state.lock().await;
        let State {
            admission, session, ..
        } = state.as_mut().context("guest event retired")?;
        self.runtime
            .guest_storage_operation(
                admission,
                session,
                request,
                || {
                    accepting()
                        && self.accepting.load(Ordering::Acquire)
                        && !*self.cancellation.borrow()
                },
                self.cleanup_only,
            )
            .await
    }

    /// A facet event's operation, routed by the live root session. Unlike the
    /// root's own, it waits for a facet input gate without holding the state:
    /// the holder's next operation needs it.
    async fn execute_facet(
        &self,
        mut request: Request,
        accepting: impl Fn() -> bool,
    ) -> Result<Reply> {
        use crate::application_storage::{StorageAttempt, StorageSession};
        {
            let mut state = self.state.lock().await;
            let State { session, .. } = state.as_mut().context("guest event retired")?;
            if let Err(error) = session.open_facet_stream(&request).await {
                return Ok(StorageSession::error_reply(&error));
            }
        }
        let followup = StorageSession::facet_followup(&request);
        loop {
            let attempt = {
                let mut state = self.state.lock().await;
                let State {
                    admission, session, ..
                } = state.as_mut().context("guest event retired")?;
                self.runtime
                    .admitted_storage_turn(admission, |cells| {
                        session.try_execute(
                            cells,
                            accepting()
                                && self.accepting.load(Ordering::Acquire)
                                && !*self.cancellation.borrow(),
                            request,
                        )
                    })
                    .await?
            };
            match attempt {
                StorageAttempt::Reply(reply) => {
                    if followup.is_none() {
                        return Ok(reply);
                    }
                    let mut state = self.state.lock().await;
                    let State { session, .. } = state.as_mut().context("guest event retired")?;
                    return Ok(session.finish_facet_request(followup.as_ref(), reply).await);
                }
                StorageAttempt::Wait {
                    gate,
                    request: pending,
                } => {
                    request = pending;
                    gate.await
                        .context("cell stopped before guest facet storage")?
                        .map_err(anyhow::Error::msg)?;
                }
            }
        }
    }

    /// Whether `grant` names a live facet event session of this event.
    pub(crate) async fn has_facet(&self, grant: &str) -> bool {
        match self.state.lock().await.as_ref() {
            Some(State { session, .. }) => session.has_facet(grant),
            None => false,
        }
    }

    /// Cancelled guest code may acquire/release an execution turn to unwind,
    /// but this channel cannot authorize storage effects or renew admission.
    pub(crate) async fn cleanup_turn(
        &self,
        request: Request,
        live: impl Fn() -> bool,
    ) -> Result<Reply> {
        let mut state = self.state.lock().await;
        let State {
            admission, session, ..
        } = state.as_mut().context("guest event retired")?;
        self.runtime
            .guest_storage_operation(admission, session, request, live, true)
            .await
    }
}

/// The admitted activity follows output delivery. A response stream retains
/// the same guards through GuestOutput even when this wrapper is dropped.
pub(crate) struct GuestCompletion<T> {
    pub(crate) result: Result<T>,
    pub(crate) write_position: Option<u64>,
    // The completion owns the activity guard until it drops; only tests read it.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) activity: Arc<ActivityGuard>,
    _public: Option<Arc<PublicRequestGuard>>,
}

async fn cancelled(mut cancellation: watch::Receiver<bool>) {
    while !*cancellation.borrow() {
        if cancellation.changed().await.is_err() {
            break;
        }
    }
}

async fn alarm_cancelled(alarm: &Option<(i64, tokio_util::sync::CancellationToken)>) {
    match alarm {
        Some((_, cancelled)) => cancelled.cancelled().await,
        None => std::future::pending().await,
    }
}

async fn frontend_closed<T>(reply: &mut Option<oneshot::Sender<T>>) {
    if let Some(reply) = reply {
        reply.closed().await;
    } else {
        std::future::pending::<()>().await;
    }
}

/// Dropping this frontend closes its reply receiver. The owned driver stops
/// accepting effects, drops transport/handler work, cleans up and proves the
/// resulting position before releasing its Actor/runtime admission.
#[cfg(test)]
#[path = "unit_tests/application_guest/tests.rs"]
pub(crate) mod tests;

/// Named Durable Object entry for a host-authorized application resource.
/// Transport clients cannot attach an unscoped or another application's event.
/// Other service, peer and lifecycle entry points still require their own
/// original admission paths; this is not a stateless handler dispatcher.
/// Sending a reply ends handler-budget charging, not event authority. The
/// future still owns event drain; Some returns the legacy buffered response.
///
/// The same entry for an object already addressed by name or unique id; its
/// id must authenticate under this resource's namespace.
pub(crate) async fn invoke_object<
    T: Send + 'static,
    F: Future<Output = Result<Option<T>>> + Send + 'static,
>(
    app: AppHandle,
    resource: ResourceAddress,
    object: ObjectTarget,
    request_id: Option<crate::js::RequestId>,
    deadline_ms: u64,
    execution: Option<EventHold>,
    handler: impl FnOnce(GuestStorage, oneshot::Sender<Result<T>>) -> F + Send + 'static,
) -> Result<GuestCompletion<T>> {
    ensure!(
        match resource.resource.kind {
            ResourceKind::D1 => object.class == crate::deploy::D1_CLASS,
            ResourceKind::Workflow =>
                object.class == crate::deploy::workflow_class(resource.resource.id.as_str())
                    && crate::application_javascript_guest::namespace_owns_object(
                        &resource,
                        &object.class,
                        &object
                    ),
            ResourceKind::DurableObject => !crate::deploy::is_reserved_class(&object.class),
            _ => false,
        },
        "guest object class does not match its resource kind"
    );
    object.validate()?;
    ensure!(
        resource.owns(&object),
        "object is outside the resource namespace"
    );
    invoke_object_event(
        app,
        resource,
        object,
        request_id,
        deadline_ms,
        execution,
        None,
        None,
        handler,
    )
    .await
}

// Each parameter names one independent admission input of a guest object event.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn invoke_object_event<
    T: Send + 'static,
    F: Future<Output = Result<Option<T>>> + Send + 'static,
>(
    app: AppHandle,
    resource: ResourceAddress,
    object: ObjectTarget,
    request_id: Option<crate::js::RequestId>,
    deadline_ms: u64,
    execution: Option<EventHold>,
    alarm: Option<(i64, tokio_util::sync::CancellationToken)>,
    websocket: Option<u64>,
    handler: impl FnOnce(GuestStorage, oneshot::Sender<Result<T>>) -> F + Send + 'static,
) -> Result<GuestCompletion<T>> {
    object.validate()?;
    ensure!(
        resource.owns(&object),
        "object is outside the resource namespace"
    );
    ensure!(
        alarm.is_none()
            || matches!(
                resource.resource.kind,
                ResourceKind::DurableObject | ResourceKind::Workflow
            ),
        "only guest object resources dispatch alarms"
    );
    #[cfg(target_os = "linux")]
    if let Some(host) = crate::application_vm::hosted() {
        host.register_cell(&resource, &object).await?;
    }
    invoke_target(
        app,
        format!("{}:{}", object.class, object.id),
        Some((resource.application, object)),
        request_id,
        deadline_ms,
        execution,
        alarm,
        websocket,
        handler,
    )
    .await
}

/// A joined guest handler rejection counts against the original alarm retry
/// limit; admission, cancellation and transport failures do not.
#[derive(Debug)]
pub(crate) struct AlarmHandlerFailure(pub(crate) String);
impl std::fmt::Display for AlarmHandlerFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for AlarmHandlerFailure {}

/// Another node owns the object; nothing was admitted or run here.
#[derive(Debug)]
pub(crate) struct RemoteOwner;

impl std::fmt::Display for RemoteOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("guest invocation requires local routing")
    }
}

impl std::error::Error for RemoteOwner {}

// Each parameter names one independent admission input of a guest event.
#[allow(clippy::too_many_arguments)]
async fn invoke_target<
    T: Send + 'static,
    F: Future<Output = Result<Option<T>>> + Send + 'static,
>(
    app: AppHandle,
    cell: String,
    target: Option<(ApplicationId, ObjectTarget)>,
    request_id: Option<crate::js::RequestId>,
    deadline_ms: u64,
    execution: Option<EventHold>,
    alarm: Option<(i64, tokio_util::sync::CancellationToken)>,
    websocket: Option<u64>,
    handler: impl FnOnce(GuestStorage, oneshot::Sender<Result<T>>) -> F + Send + 'static,
) -> Result<GuestCompletion<T>> {
    ensure!(
        crate::asyncrt::mono_ms() < deadline_ms,
        "guest execution deadline must be positive"
    );
    let (reply, receive) = oneshot::channel();
    let mut reply = Some(reply);
    crate::asyncrt::spawn(async move {
        let result = drive(
            &app,
            cell,
            target,
            request_id,
            deadline_ms,
            execution,
            alarm,
            websocket,
            &mut reply,
            handler,
        )
        .await;
        if let Some(reply) = reply {
            let _ =
                reply
                    .send(result.and_then(|completion| {
                        completion.context("guest settled without a reply")
                    }));
        }
    });
    receive
        .await
        .context("guest driver stopped before settlement")?
}

// Each parameter names one independent admission input of a guest event.
#[allow(clippy::too_many_arguments)]
async fn drive<T: Send + 'static, F: Future<Output = Result<Option<T>>> + Send + 'static>(
    app: &AppHandle,
    cell: String,
    target: Option<(ApplicationId, ObjectTarget)>,
    request_id: Option<crate::js::RequestId>,
    deadline_ms: u64,
    execution: Option<EventHold>,
    alarm: Option<(i64, tokio_util::sync::CancellationToken)>,
    websocket: Option<u64>,
    reply: &mut Option<oneshot::Sender<Result<GuestCompletion<T>>>>,
    handler: impl FnOnce(GuestStorage, oneshot::Sender<Result<T>>) -> F,
) -> Result<Option<GuestCompletion<T>>> {
    // An already core-admitted alarm retains its firing pin during drain.
    // Public admission belongs only to incoming object calls.
    let public = if alarm.is_some() {
        None
    } else {
        Some(Arc::new(
            app.admit_continuation().context("node is draining")?,
        ))
    };
    let deadline = crate::asyncrt::sleep_until(deadline_ms);
    tokio::pin!(deadline);
    let routed = crate::asyncrt::select! {
        _ = alarm_cancelled(&alarm) => anyhow::bail!("alarm cancelled before routing"),
        // A socket event routes as the registered socket, as the original
        // `websocket_request` does, so it may finish on a leaving cell.
        route = async {
            match websocket {
                Some(websocket) => app.websocket_request(cell.clone(), websocket).await,
                None => app.request(cell.clone()).await,
            }
        } => route.map_err(|error| anyhow::anyhow!("guest route refused: {error:?}"))?,
        _ = frontend_closed(reply) => anyhow::bail!("guest frontend disconnected before admission"),
        _ = &mut deadline => anyhow::bail!("guest deadline exceeded before admission"),
    };
    let activity = Arc::new(app.activity_for(routed.request, cell.clone(), request_id, "guest"));
    if routed.route != celld_logic::Route::Local {
        return Err(RemoteOwner.into());
    }
    let runtime = app
        .runtime
        .as_ref()
        .context("guest storage runtime is unavailable")?;
    let epoch = app
        .resident_epoch(cell.clone())
        .await
        .context("guest residency disappeared")?;
    #[cfg(target_os = "linux")]
    crate::asyncrt::select! {
        refreshed = runtime.refresh_application_scope(&cell) => refreshed?,
        _ = alarm_cancelled(&alarm) => anyhow::bail!("alarm cancelled before storage admission"),
        _ = frontend_closed(reply) => anyhow::bail!("guest frontend disconnected before storage admission"),
        _ = cancelled(activity.cancellation()) => anyhow::bail!("guest activity cancelled before storage admission"),
        _ = &mut deadline => anyhow::bail!("guest deadline exceeded before storage admission"),
    }
    let admission = runtime.admit_storage_request(&cell, epoch)?;
    let finished = tokio_util::sync::CancellationToken::new();
    let execution = Arc::new(std::sync::Mutex::new(execution));
    let _finished = Finished {
        finished: finished.clone(),
        execution: execution.clone(),
    };
    let actor_name = target
        .as_ref()
        .and_then(|(_, object)| object.name.as_deref());
    let (session, event) = if let Some((_, token)) = &alarm {
        // Once queued, the native claiming turn must join even if cancellation
        // arrives. Otherwise it could acquire a claim after its owner vanished.
        // Cancellation is checked in that turn and during its input-gate wait.
        let mut begin =
            Box::pin(runtime.begin_guest_storage_event(&admission, actor_name, alarm.clone()));
        crate::asyncrt::select! {
            begun = &mut begin => begun?,
            _ = &mut deadline => { token.cancel(); begin.await? },
            _ = cancelled(activity.cancellation()) => { token.cancel(); begin.await? },
            _ = frontend_closed(reply) => { token.cancel(); begin.await? },
        }
    } else {
        crate::asyncrt::select! {
            begun = runtime.begin_guest_storage_event(&admission, actor_name, None) => begun?,
            _ = frontend_closed(reply) => anyhow::bail!("guest frontend disconnected before input admission"),
            _ = cancelled(activity.cancellation()) => anyhow::bail!("guest activity cancelled before input admission"),
            _ = &mut deadline => anyhow::bail!("guest deadline exceeded before input admission"),
        }
    };
    if let Some(hold) = execution.lock().expect("guest execution poisoned").as_ref() {
        hold.mark_admitted();
    }
    let alarm_metadata = session.alarm();
    let grant = session.grant().to_owned();
    let cancellation = activity.cancellation();
    let state = Arc::new(Mutex::new(Some(State {
        admission,
        session,
        event,
    })));
    let accepting = Arc::new(AtomicBool::new(true));
    let settlement = Arc::new(std::sync::Mutex::new(None));
    let (application, object) = target.map_or((None, None), |(application, object)| {
        (Some(application), Some(object))
    });
    let output_lifetime = Arc::new(GuestOutput {
        app: app.clone(),
        cell: cell.clone(),
        request: routed.request,
        activity: activity.clone(),
        _public: public.clone(),
        proof: Mutex::new(()),
        execution: execution.clone(),
    });
    let effects = crate::application_container::Effects::default();
    let sockets = crate::application_container::Sockets::default();
    let access = GuestStorage {
        effects: effects.clone(),
        sockets: sockets.clone(),
        execution,
        execution_deadline_ms: deadline_ms,
        runtime: runtime.clone(),
        state: state.clone(),
        accepting: accepting.clone(),
        cancellation: cancellation.clone(),
        cleanup_only: false,
        alarm: alarm_metadata,
        finished,
        grant,
        application,
        object,
        settlement: settlement.clone(),
        output: Arc::downgrade(&output_lifetime),
    };
    activity.set_phase("guest_handler", false, false);
    let (response, mut response_ready) = oneshot::channel();
    let handler_settled = AtomicBool::new(false);
    let mut reply_body = None;
    let mut delivered = false;
    let mut result = if alarm
        .as_ref()
        .is_some_and(|(_, token)| token.is_cancelled())
    {
        // A claim can win just before the cancellation signal. It still owns
        // native cleanup/proof, but no application event may be delivered.
        Err(anyhow::anyhow!("alarm cancelled during input admission"))
    } else {
        // The future is dropped before cleanup on cancellation. It owns its
        // transports. A retired grant refuses effects; gated cleanup can still
        // resume until joined terminal or owning VM reaping permits settlement.
        let proof_access = access.clone();
        let early = std::panic::AssertUnwindSafe(async {
            let Ok(body) = (&mut response_ready).await else {
                return Ok(None);
            };
            reply_body = Some(body);
            handler_settled.store(true, Ordering::Release);
            activity.set_phase("guest_reply_gate", false, false);
            output_lifetime.prove(&proof_access).await?;
            Ok::<_, anyhow::Error>(reply_body.take())
        })
        .catch_unwind();
        tokio::pin!(early);
        let handler = std::panic::AssertUnwindSafe(async move { handler(access, response).await })
            .catch_unwind();
        tokio::pin!(handler);
        let mut early_open = true;
        loop {
            crate::asyncrt::select_biased! {
                "a completed handler wins a tie with every cancellation or early-reply branch so its result is never discarded";
                result = &mut handler => break result.unwrap_or_else(|_| Err(anyhow::anyhow!("guest transport driver panicked"))),
                result = &mut early, if early_open => {
                    early_open = false;
                    match result.unwrap_or_else(|_| Err(anyhow::anyhow!("guest reply proof panicked"))) {
                        Ok(Some(result)) => {
                            let sender = reply.take().expect("one guest reply");
                            if sender.send(Ok(GuestCompletion { result, write_position: None, activity: activity.clone(), _public: public.clone() })).is_err() {
                                break Err(anyhow::anyhow!("guest frontend disconnected before delivery"));
                            }
                            delivered = true;
                            activity.set_phase_only("guest_background");
                        }
                        Ok(None) => {},
                        Err(error) => break Err(error),
                    }
                }
                _ = frontend_closed(reply) => break Err(anyhow::anyhow!("guest frontend disconnected")),
                _ = alarm_cancelled(&alarm) => break Err(anyhow::anyhow!("alarm cancelled")),
                _ = cancelled(cancellation.clone()) => break Err(anyhow::anyhow!("guest activity cancelled")),
                _ = async {
                    (&mut deadline).await;
                    if handler_settled.load(Ordering::Acquire) { std::future::pending::<()>().await; }
                } => break Err(anyhow::anyhow!("guest execution deadline exceeded")),
            }
        }
    };
    // Joined completion can win the same poll as response-ready. Preserve its
    // buffered response for final settlement instead of losing the reply.
    if reply_body.is_none() {
        if let Ok(body) = response_ready.try_recv() {
            reply_body = Some(body);
        }
    }
    if matches!(result, Ok(Some(_)))
        && (handler_settled.load(Ordering::Acquire) || reply_body.is_some())
    {
        result = Err(anyhow::anyhow!(
            "guest supplied both early and buffered responses"
        ));
    }
    accepting.store(false, Ordering::Release);
    let settled = settlement.lock().expect("guest settlement poisoned").take();
    if let Some(settled) = settled {
        activity.set_phase_only("guest_settlement");
        settled.wait().await;
    }
    // Container effects begun by this event finish before its storage settles.
    effects.join().await;
    sockets.close();
    activity.set_phase_only("guest_cleanup");
    let State {
        admission,
        mut session,
        event,
    } = state.lock().await.take().expect("owned guest state");
    let finished = runtime
        .finish_guest_storage_event(
            &admission,
            &mut session,
            &event,
            alarm.as_ref().map(|_| {
                (
                    result.is_ok(),
                    result
                        .as_ref()
                        .err()
                        .is_some_and(|error| error.is::<AlarmHandlerFailure>()),
                )
            }),
        )
        .await;
    let output = match finished {
        Ok((output, refused)) => {
            if let Some(error) = refused {
                result = Err(error);
            }
            output
        }
        Err(error) => {
            runtime.quarantine_storage_request(&admission);
            // Keep failed cleanup quarantined until the original lifecycle
            // closes it. An unconditional eviction here could race a same-
            // epoch generation replacement and retire the successor instead.
            tracing::error!(%cell, epoch, %error, "guest storage settlement failed");
            drop(admission);
            drop(activity);
            anyhow::bail!("guest storage could not settle");
        }
    };
    let write_position = output.position();
    activity.set_phase_only("output_gate");
    let gated = output_lifetime.gate(output).await;
    if let Err(error) = gated {
        result = Err(error);
    }
    drop(admission);
    if delivered {
        if let Err(error) = result {
            tracing::warn!(%cell, %error, "guest work failed after reply delivery");
        }
        return Ok(None);
    }
    let result = match result {
        Ok(Some(value)) => Ok(value),
        Ok(None) => {
            reply_body.unwrap_or_else(|| Err(anyhow::anyhow!("guest settled without a response")))
        }
        Err(error) => Err(error),
    };
    Ok(Some(GuestCompletion {
        result,
        write_position,
        activity,
        _public: public,
    }))
}

/// The queue consumer wire. The host sends one JSON batch (message bodies in
/// base64, as in the queue cell's own dispatch envelope) and the guest answers
/// the same settlement record `dispatch_queue_batch` posts to the queue cell.
mod queue_wire {
    use crate::js::{
        QueueBatch, QueueContentType, QueueDispatchResult, QueueMessage, QueueMetrics,
        QueueOutcome, QueueRetryBatch, QueueRetryMessage,
    };
    use anyhow::{anyhow, Result};
    use base64::Engine;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Batch {
        queue: String,
        messages: Vec<Message>,
        backlog_count: f64,
        backlog_bytes: f64,
        oldest_message_timestamp_ms: Option<i64>,
    }

    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Message {
        id: String,
        timestamp_ms: i64,
        body_base64: String,
        content_type: String,
        attempts: u16,
    }

    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Settlement {
        outcome: String,
        error: Option<String>,
        ack_all: bool,
        retry_batch: Retry,
        explicit_acks: Vec<String>,
        retry_messages: Vec<MessageRetry>,
    }

    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Retry {
        retry: bool,
        delay_seconds: Option<i32>,
    }

    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct MessageRetry {
        msg_id: String,
        delay_seconds: Option<i32>,
    }

    const TYPES: [(&str, QueueContentType); 4] = [
        ("text", QueueContentType::Text),
        ("bytes", QueueContentType::Bytes),
        ("json", QueueContentType::Json),
        ("v8", QueueContentType::V8),
    ];

    pub(crate) fn encode_queue_batch(batch: &QueueBatch) -> Result<Vec<u8>> {
        let base64 = base64::engine::general_purpose::STANDARD;
        Ok(serde_json::to_vec(&Batch {
            queue: batch.queue.clone(),
            messages: batch
                .messages
                .iter()
                .map(|message| Message {
                    id: message.id.clone(),
                    timestamp_ms: message.timestamp_ms,
                    body_base64: base64.encode(&message.body),
                    content_type: TYPES
                        .iter()
                        .find(|(_, kind)| *kind == message.content_type)
                        .expect("every content type is listed")
                        .0
                        .into(),
                    attempts: message.attempts,
                })
                .collect(),
            backlog_count: batch.metrics.backlog_count,
            backlog_bytes: batch.metrics.backlog_bytes,
            oldest_message_timestamp_ms: batch.metrics.oldest_message_timestamp_ms,
        })?)
    }

    pub(crate) fn decode_queue_batch(bytes: &[u8]) -> Result<QueueBatch> {
        let base64 = base64::engine::general_purpose::STANDARD;
        let wire: Batch = serde_json::from_slice(bytes)?;
        Ok(QueueBatch {
            queue: wire.queue,
            messages: wire
                .messages
                .into_iter()
                .map(|message| {
                    Ok(QueueMessage {
                        content_type: TYPES
                            .iter()
                            .find(|(name, _)| *name == message.content_type)
                            .ok_or_else(|| anyhow!("invalid queue content type"))?
                            .1,
                        body: base64.decode(message.body_base64)?,
                        id: message.id,
                        timestamp_ms: message.timestamp_ms,
                        attempts: message.attempts,
                    })
                })
                .collect::<Result<_>>()?,
            metrics: QueueMetrics {
                backlog_count: wire.backlog_count,
                backlog_bytes: wire.backlog_bytes,
                oldest_message_timestamp_ms: wire.oldest_message_timestamp_ms,
            },
        })
    }

    pub(crate) fn encode_queue_result(result: &QueueDispatchResult) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&Settlement {
            outcome: match result.outcome {
                QueueOutcome::Ok => "ok",
                QueueOutcome::Exception => "exception",
            }
            .into(),
            error: result.error.clone(),
            ack_all: result.ack_all,
            retry_batch: Retry {
                retry: result.retry_batch.retry,
                delay_seconds: result.retry_batch.delay_seconds,
            },
            explicit_acks: result.explicit_acks.clone(),
            retry_messages: result
                .retry_messages
                .iter()
                .map(|retry| MessageRetry {
                    msg_id: retry.msg_id.clone(),
                    delay_seconds: retry.delay_seconds,
                })
                .collect(),
        })?)
    }

    pub(crate) fn decode_queue_result(bytes: &[u8]) -> Result<QueueDispatchResult> {
        let wire: Settlement = serde_json::from_slice(bytes)?;
        Ok(QueueDispatchResult {
            outcome: match wire.outcome.as_str() {
                "ok" => QueueOutcome::Ok,
                "exception" => QueueOutcome::Exception,
                _ => anyhow::bail!("invalid queue outcome"),
            },
            error: wire.error,
            ack_all: wire.ack_all,
            retry_batch: QueueRetryBatch {
                retry: wire.retry_batch.retry,
                delay_seconds: wire.retry_batch.delay_seconds,
            },
            explicit_acks: wire.explicit_acks,
            retry_messages: wire
                .retry_messages
                .into_iter()
                .map(|retry| QueueRetryMessage {
                    msg_id: retry.msg_id,
                    delay_seconds: retry.delay_seconds,
                })
                .collect(),
        })
    }
}
pub(crate) use queue_wire::{
    decode_queue_batch, decode_queue_result, encode_queue_batch, encode_queue_result,
};

#[cfg(test)]
#[path = "unit_tests/application_guest/execution_tests.rs"]
mod execution_tests;
