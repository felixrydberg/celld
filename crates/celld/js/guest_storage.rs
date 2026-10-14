//! Event-owned synchronous guest storage. The host selects the grant/cell;
//! neither JavaScript nor this channel selects a host path or writer epoch.

use super::*;
use celld_applications::storage_wire::{
    self as wire, CloneEntry, Operation, Outcome, Reply, Request, Value,
};
use celld_applications::invocation_wire::GuestControl;
#[cfg(test)]
use std::os::unix::net::UnixStream;

pub(crate) trait Stream: std::io::Read + std::io::Write + Send {}
impl<T: std::io::Read + std::io::Write + Send> Stream for T {}

struct State {
    stream: Option<Box<dyn Stream>>,
    retained_effect_stream: Option<Box<dyn Stream>>,
    turn: Option<String>,
    pending: Vec<CloneEntry>,
    pending_bytes: usize,
    /// Local label, opaque token and, for a savepoint this driver holds for a
    /// foreign continuation, that origin's grant.
    transactions: Vec<(String, String, Option<String>)>,
    /// Local label, opaque host capability and, for a block driven for a
    /// foreign origin, the original cross-entry claim retaining that origin.
    critical_sections: Vec<(u64, String, Option<CrossEntryGateClaim>)>,
    /// Bookkeeping was discarded or a failure latched while the host can
    /// still hold this event's native gate until trusted settlement.
    native_hold_retained: bool,
    cleanup_only: bool,
    cleanup_admitted: bool,
}

pub(crate) struct Channel {
    cell: String,
    grant: String,
    /// The root event's invocation: a facet event connects its own stream
    /// under it, and the host accepts only a live facet grant of that event.
    invocation: OnceLock<String>,
    owner: OnceLock<u64>,
    effects_closed: AtomicBool,
    /// A stateless event's placeholder: no storage, gate or host turn.
    detached: bool,
    state: Mutex<State>,
}

impl Channel {
    #[cfg(test)]
    pub(crate) fn new(cell: String, grant: String, stream: UnixStream) -> Result<Arc<Self>> {
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        Self::from_stream(cell, grant, stream)
    }

    /// The guest vsock connector installs the same deadlines before handing
    /// over its owned descriptor. Read/write failures retain no-replay rules.
    pub(crate) fn from_stream(
        cell: String,
        grant: String,
        stream: impl Stream + 'static,
    ) -> Result<Arc<Self>> {
        anyhow::ensure!(
            celld_logic::cell::valid_cell_scope(&cell) && wire::valid_handle(&grant),
            "invalid guest storage identity"
        );
        Ok(Arc::new(Self {
            cell,
            grant,
            invocation: OnceLock::new(),
            owner: OnceLock::new(),
            effects_closed: AtomicBool::new(false),
            detached: false,
            state: Mutex::new(State {
                stream: Some(Box::new(stream)),
                retained_effect_stream: None,
                turn: None,
                pending: Vec::new(),
                pending_bytes: 0,
                transactions: Vec::new(),
                critical_sections: Vec::new(),
                native_hold_retained: false,
                cleanup_only: false,
                cleanup_admitted: false,
            }),
        }))
    }

    /// A stateless handler has no object, storage grant or native gate. The
    /// scheduler drives it through the same phases; its turns are no-ops and it
    /// is never attached to an event as storage.
    pub(crate) fn detached() -> Arc<Self> {
        Arc::new(Self {
            cell: String::new(),
            grant: String::new(),
            invocation: OnceLock::new(),
            owner: OnceLock::new(),
            effects_closed: AtomicBool::new(false),
            detached: true,
            state: Mutex::new(State {
                stream: None,
                retained_effect_stream: None,
                turn: None,
                pending: Vec::new(),
                pending_bytes: 0,
                transactions: Vec::new(),
                critical_sections: Vec::new(),
                native_hold_retained: false,
                cleanup_only: false,
                cleanup_admitted: false,
            }),
        })
    }

    pub(crate) fn set_invocation(&self, invocation: String) {
        let _ = self.invocation.set(invocation);
    }

    // Acquire outside the V8 locker. A closed native input gate must never
    // park the isolate that another event needs in order to release that gate.
    pub(crate) fn begin_turn(&self) -> Result<()> {
        if self.detached {
            return Ok(());
        }
        let mut state = self.state.lock().unwrap();
        anyhow::ensure!(state.turn.is_none(), "guest execution turn already active");
        match self.exchange(&mut state, Operation::TurnStart {})? {
            Value::Turn { turn } if wire::valid_handle(&turn) => {
                state.turn = Some(turn);
                Ok(())
            }
            _ => {
                state.stream = None;
                anyhow::bail!("invalid guest execution-turn reply")
            }
        }
    }

    pub(crate) fn finish_turn(&self) -> Result<()> {
        if self.detached {
            return Ok(());
        }
        let mut state = self.state.lock().unwrap();
        anyhow::ensure!(
            state.pending.is_empty(),
            "guest pending puts were not flushed"
        );
        let turn = state
            .turn
            .clone()
            .context("guest execution turn is not active")?;
        match self.exchange(&mut state, Operation::TurnFinish { turn })? {
            Value::Unit {} => {
                state.turn = None;
                Ok(())
            }
            _ => {
                state.stream = None;
                anyhow::bail!("invalid guest execution-turn reply")
            }
        }
    }

    fn require_turn(&self, cell: &str) -> Result<()> {
        anyhow::ensure!(self.cell == cell, "guest storage belongs to another object");
        let state = self.state.lock().unwrap();
        anyhow::ensure!(state.stream.is_some(), "guest storage channel retired");
        anyhow::ensure!(
            state.turn.is_some(),
            "guest storage requires its active execution turn"
        );
        Ok(())
    }

    pub(super) fn require_response_turn(&self, cell: &str, owner: u64) -> Result<()> {
        self.require_turn(cell)?;
        anyhow::ensure!(
            self.owner.get() == Some(&owner),
            "guest response belongs to another event"
        );
        let state = self.state.lock().unwrap();
        anyhow::ensure!(
            !state.cleanup_only && !self.effects_closed.load(Ordering::Acquire),
            "guest response requires ordinary admission, not cleanup"
        );
        Ok(())
    }

    /// Ask the host to end this facet event's session. It needs no turn: the
    /// facet's own grant is the only authority it names.
    fn end_facet(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        self.exchange(
            &mut state,
            Operation::FacetClose {
                facet: self.grant.clone(),
            },
        )
        .map(drop)
    }

    pub(crate) fn is_detached(&self) -> bool {
        self.detached
    }

    pub(super) fn holds_critical_section(&self) -> bool {
        !self.state.lock().unwrap().critical_sections.is_empty()
    }

    /// Whether this event's native gate can outlive its current turn. Call
    /// only while no admission of this channel is in flight.
    pub(crate) fn may_hold_native_gate(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.native_hold_retained
            || !state.transactions.is_empty()
            || !state.critical_sections.is_empty()
    }

    /// Select a lifecycle-only channel before entering guest code. Even if
    /// trusted host cleanup admission is live, it never authorizes effects.
    pub(crate) fn restrict_to_cleanup(&self) {
        self.request_cleanup();
        self.state.lock().unwrap().cleanup_only = true;
    }

    /// Refuse foreign/queued effects immediately without waiting behind a
    /// blocking native admission that another event's executor must release.
    pub(crate) fn request_cleanup(&self) {
        self.effects_closed.store(true, Ordering::Release);
    }

    /// Called on a blocking worker after ordinary admission has joined. Keep
    /// the original transport alive until joined retirement; cleanup can reuse
    /// this event's existing native turn but cannot restore storage authority.
    pub(crate) fn begin_cleanup(
        &self,
        invocation: String,
        mut stream: impl Stream + 'static,
    ) -> Result<()> {
        self.request_cleanup();
        if self.detached {
            return Ok(());
        }
        wire::write_frame(
            &mut stream,
            &celld_applications::invocation_wire::StorageConnect {
                version: wire::VERSION,
                invocation,
                grant: self.grant.clone(),
                cleanup: true,
                facet: false,
            }
            .encode()?,
        )?;
        let mut state = self.state.lock().unwrap();
        anyhow::ensure!(!state.cleanup_only, "guest cleanup already selected");
        state.cleanup_only = true;
        state.pending.clear();
        state.pending_bytes = 0;
        state.retained_effect_stream = state.stream.replace(Box::new(stream));
        let previous = state.turn.take();
        let admitted = match self.exchange(&mut state, Operation::TurnStart {}) {
            Ok(value) => value,
            Err(error) => {
                state.stream = None;
                return Err(error);
            }
        };
        match admitted {
            Value::Turn { turn } if wire::valid_handle(&turn) => {
                if previous.is_some() && previous.as_ref() != Some(&turn) {
                    state.stream = None;
                    anyhow::bail!("cleanup changed the event's existing native turn");
                }
                state.turn = Some(turn);
                state.cleanup_admitted = true;
                Ok(())
            }
            _ => {
                state.stream = None;
                anyhow::bail!("invalid guest cleanup-turn reply")
            }
        }
    }

    fn exchange(&self, state: &mut State, operation: Operation) -> Result<Value> {
        anyhow::ensure!(
            !state.cleanup_only
                || matches!(
                    operation,
                    Operation::TurnStart {} | Operation::TurnFinish { .. }
                ),
            "guest lifecycle channel refuses storage effects"
        );
        let bytes = Request {
            version: wire::VERSION,
            grant: self.grant.clone(),
            operation,
        }
        .encode()?;
        let stream = state
            .stream
            .as_mut()
            .context("guest storage channel retired")?;
        let result = (|| {
            wire::write_frame(stream, &bytes)?;
            Reply::decode(
                &wire::read_frame(stream)?.context("guest storage reply ended; outcome unknown")?,
            )
        })();
        match result {
            Ok(Reply {
                outcome: Outcome::Ok { value },
                ..
            }) => Ok(value),
            Ok(Reply {
                outcome: Outcome::Error { message },
                ..
            }) => Err(anyhow!(message)),
            // Keep transaction/critical labels: host cancellation can drop this
            // stream mid-turn, and the cleanup turn must still unwind them in
            // nesting order. The native holds stay with trusted settlement.
            Err(error) => {
                state.stream = None;
                state.pending.clear();
                state.pending_bytes = 0;
                state.native_hold_retained = true;
                Err(error)
            }
        }
    }

    fn call(&self, operation: Operation, borrowed: Option<String>) -> Result<Value> {
        let mut state = self.state.lock().unwrap();
        anyhow::ensure!(
            state.turn.is_some() || borrowed.is_some(),
            "guest storage requires its active execution turn"
        );
        let operation = if state.turn.is_some() {
            operation
        } else {
            in_turn(operation, borrowed)
        };
        self.exchange(&mut state, operation)
    }

    pub(crate) fn retire(&self) {
        let mut state = self.state.lock().unwrap();
        state.stream = None;
        state.retained_effect_stream = None;
        state.pending.clear();
        state.pending_bytes = 0;
        state.native_hold_retained |=
            !state.transactions.is_empty() || !state.critical_sections.is_empty();
        state.transactions.clear();
        state.critical_sections.clear();
    }
}

impl Worker {
    /// Private bootstrap hook. Guest mode is selected before user modules
    /// evaluate; direct egress remains denied until its host bridge exists.
    pub(crate) fn load_guest_config(config: Arc<WorkerConfig>) -> Result<Self> {
        Self::load_config_inner(config, true)
    }

    /// Reuse the original cell event, promise settlement and operation
    /// adoption. The caller acquires native admission outside the V8 locker
    /// and must drive/release each later turn, including background work.
    /// Replies remain guest-local; host publication and proof stay outside
    /// this hook.
    pub(crate) fn turn_begin_guest_cell(
        &mut self,
        job: CellJob,
        trace: Option<crate::telemetry::TraceContext>,
        channel: Arc<Channel>,
    ) -> Result<(Option<InFlight>, Vec<Op>)> {
        let prepared = (|| {
            let inner = self.inner.as_ref().context("worker is closed")?;
            anyhow::ensure!(
                inner
                    .runtime_state
                    .guest_storage_enabled
                    .load(Ordering::Acquire),
                "worker was not loaded in guest mode"
            );
            // Only host-claimed guest alarms carry native admission metadata.
            // Native alarm entry variants cannot acquire guest authority here.
            anyhow::ensure!(
                matches!(
                    &job,
                    CellJob::Fetch {
                        body: RequestBody::Bytes(_),
                        ..
                    } | CellJob::Rpc { .. }
                        | CellJob::Alarm {
                            claim: AlarmDispatch::Guest { .. },
                            ..
                        }
                        | CellJob::WsMessage { .. }
                        | CellJob::WsClosed { .. }
                ),
                "guest cell entry requires buffered HTTP, RPC, a host-claimed guest alarm \
                 or a socket message/close"
            );
            channel.require_turn(job.scope())?;
            let context = IoContext::tracked(&inner.runtime_state);
            context.attach_guest_storage(channel)?;
            Ok(context)
        })();
        match prepared {
            Ok(context) => Ok(self.turn_begin_cell_with_context(job, trace, Some(context))),
            Err(error) => {
                job.fail(anyhow!("guest cell entry refused: {error}"));
                Err(error)
            }
        }
    }

    fn require_guest_turn(&self, entry: &InFlight) -> Result<()> {
        let inner = self.inner.as_ref().context("worker is closed")?;
        anyhow::ensure!(
            Arc::ptr_eq(&inner.runtime_state, &entry.runtime_state),
            "guest entry belongs to another worker"
        );
        let channel = entry
            .context
            .guest_storage
            .get()
            .context("entry has no guest storage channel")?;
        channel.require_turn(entry.scope.as_deref().context("guest entry has no cell")?)
    }

    pub(crate) fn turn_deliver_guest_cell(
        &mut self,
        entry: &mut InFlight,
        op: u64,
        result: Result<asyncrt::OpOut, String>,
    ) -> Result<Vec<Op>> {
        self.require_guest_turn(entry)?;
        let _driver = CurrentGuard::enter(entry.context.clone());
        Ok(self.turn_deliver(entry, op, result))
    }

    pub(crate) fn turn_poll_guest_cell(&mut self, entry: &mut InFlight) -> Result<Vec<Op>> {
        self.require_guest_turn(entry)?;
        let _driver = CurrentGuard::enter(entry.context.clone());
        Ok(self.turn_poll(entry))
    }

    /// Reuse original AbortSignal/background/input-gate cancellation inside
    /// the same event's host cleanup turn. The caller must join/drop native
    /// operations before closing both streams and publishing its terminal.
    pub(crate) fn turn_cancel_guest_cell(&mut self, entry: &mut InFlight) -> Result<Vec<Op>> {
        self.require_guest_turn(entry)?;
        let channel = entry.context.guest_storage.get().unwrap().clone();
        anyhow::ensure!(
            {
                let state = channel.state.lock().unwrap();
                state.cleanup_only && state.cleanup_admitted
            },
            "guest cancellation requires cleanup-only admission"
        );
        let _driver = CurrentGuard::enter(entry.context.clone());
        let ops = self.turn_cancel_for_guest_cleanup(entry);
        // These are guest bookkeeping only. The native transaction/critical
        // claims remain held until trusted host settlement or physical reaping.
        {
            let mut state = channel.state.lock().unwrap();
            state.native_hold_retained |= !state.critical_sections.is_empty();
            state.critical_sections.clear();
        }
        entry.finish_cross_entry_gates();
        Ok(ops)
    }

    /// Drop an idle object's instance after the host retired its residency,
    /// as the original reset after a failed critical section or abort does.
    /// No user code runs and no storage is reached; the next event constructs.
    pub(crate) fn reset_guest_cell(&mut self, cell: &str) -> Result<()> {
        let inner = self.inner.as_mut().context("worker is closed")?;
        anyhow::ensure!(
            inner
                .runtime_state
                .guest_storage_enabled
                .load(Ordering::Acquire),
            "worker was not loaded in guest mode"
        );
        let (mut locker, _cells) = inner.lock();
        v8::scope!(let hs, &mut *locker);
        let realm = inner.realm(hs);
        let cs = &mut v8::ContextScope::new(hs, realm.context);
        let source = format!(
            "const instances = __celld.__cell.instances; const instance = instances[{cell}]; \
             if (instance !== undefined) {{ instance.__celldState._aborted = true; \
             try {{ instance.ctx?.facets?._release()?.catch?.(() => {{}}); }} catch (_) {{}} \
             delete instances[{cell}]; }} \
             return true;",
            cell = serde_json::to_string(cell)?
        );
        run_internal_snippet(cs, &source).context("guest object reset failed")?;
        // The release leaves the loaded facets' instance drops outside any
        // event; run them here rather than on whichever event drains next.
        for (_, release, ..) in asyncrt::drain_spawns() {
            tokio::spawn(super::guest_activity::track(release));
        }
        Ok(())
    }

    /// Release an idle object's original instance/name/facet/stub registries
    /// under the host-selected lifecycle turn. Native storage is untouched.
    pub(crate) fn turn_release_guest_cell(
        &mut self,
        cell: &str,
        channel: Arc<Channel>,
        reply: tokio::sync::oneshot::Sender<Result<Option<u64>>>,
    ) -> Result<(Option<InFlight>, Vec<Op>)> {
        channel.require_turn(cell)?;
        anyhow::ensure!(
            channel.state.lock().unwrap().cleanup_only,
            "guest object release requires cleanup-only admission"
        );
        let inner = self.inner.as_mut().context("worker is closed")?;
        anyhow::ensure!(
            inner
                .runtime_state
                .guest_storage_enabled
                .load(Ordering::Acquire),
            "worker was not loaded in guest mode"
        );
        let context = IoContext::tracked(&inner.runtime_state);
        context.attach_guest_storage(channel)?;
        let (mut locker, _cells) = inner.lock();
        inner.recover_heap(&mut locker);
        v8::scope!(let hs, &mut *locker);
        let realm = inner.realm(hs);
        let cs = &mut v8::ContextScope::new(hs, realm.context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        let tc = &mut tc.init();
        let _driver = CurrentGuard::enter(context.clone());
        let previous = install_io_context(tc, &context);
        let cpu_watchdog = inner.cpu_watchdog(
            inner.initial_cpu_budget(),
            inner
                .runtime_state
                .resource_limits
                .and_then(|limits| limits.cpu_ms),
        );
        let begun = start_cell_event(
            tc,
            cell,
            Answer::Ack(reply),
            None,
            None,
            false,
            Some(context.clone()),
            |tc| super::bootstrap::release_cell_instance_value(tc, cell),
        );
        drop(cpu_watchdog);
        let result = match begun {
            Begun::Running(mut entry) => {
                // The raw release promise and callbacks/late waitUntil must
                // remain owned even if a user-replaced aggregate resolves early.
                entry.cleanup = Some(vec![entry.promise.clone()]);
                context.events.lock().unwrap().wait_until_active = true;
                let ops = finish_turn(tc, &mut entry);
                Ok((Some(*entry), ops))
            }
            Begun::Nothing => Ok((None, Vec::new())),
            Begun::Threw(_) => Err(anyhow!("guest object release could not start")),
        };
        restore_io_context(tc, previous);
        result
    }
}

impl IoContext {
    pub(crate) fn attach_guest_storage(&self, channel: Arc<Channel>) -> Result<()> {
        let (id, owner) = self
            .continuation
            .as_ref()
            .context("guest storage requires a tracked execution context")?;
        let owner = owner.upgrade().context("guest execution context retired")?;
        anyhow::ensure!(
            self.guest_storage.get().is_none(),
            "guest storage identity already selected"
        );
        anyhow::ensure!(
            channel.owner.set(*id).is_ok(),
            "guest storage channel already belongs to an event"
        );
        anyhow::ensure!(
            self.guest_storage.set(channel).is_ok(),
            "guest storage identity already selected"
        );
        owner.guest_storage_enabled.store(true, Ordering::Release);
        Ok(())
    }
}

/// One outbound fetch handed to the guest scheduler, which asks the trusted
/// host to perform it. The guest itself has no network device or client.
pub(crate) struct GuestFetch {
    /// The issuing event's context. A weak reference keeps its allocation, so
    /// the identity cannot be reused by a later event while this is queued.
    context: std::sync::Weak<IoContext>,
    pub(crate) request: celld_applications::invocation_wire::HttpRequest,
    pub(crate) redirect: String,
    /// A Durable Object call the host performs instead of a network request.
    pub(crate) target: Option<celld_applications::invocation_wire::ObjectCall>,
    pub(crate) service: Option<celld_applications::invocation_wire::ServiceCall>,
    pub(crate) resource: Option<celld_applications::resource_wire::ResourceCall>,
    /// An outbound WebSocket the host dials, and the guest's local id for it.
    pub(crate) dial: Option<(celld_applications::invocation_wire::SocketDial, u64)>,
    pub(crate) upload: Option<HttpChunkStream>,
    pub(crate) reply: tokio::sync::oneshot::Sender<Result<HttpResponse, String>>,
}

static EGRESS: OnceLock<tokio::sync::mpsc::UnboundedSender<GuestFetch>> = OnceLock::new();

/// Installed once by the guest scheduler before user code can run.
pub(crate) fn install_egress(sender: tokio::sync::mpsc::UnboundedSender<GuestFetch>) -> Result<()> {
    EGRESS
        .set(sender)
        .map_err(|_| anyhow!("guest egress broker already installed"))
}

static CONDEMN: Mutex<Option<GuestControl>> = Mutex::new(None);

/// What `condemn` left for the guest scheduler, the only control writer. It
/// writes this ahead of its next frame, so the host reads the condemnation
/// before the reply of the event that caused it.
pub(crate) fn take_condemn() -> Option<GuestControl> {
    CONDEMN.lock().unwrap().take()
}

/// `__isolate_condemn` in a guest: ask the host to retire this VM, the guest's
/// own isolate pool. Whether the request was recorded; the VM drains first.
pub(crate) fn condemn(reason: &str) -> bool {
    CONDEMN.lock().unwrap().get_or_insert_with(|| GuestControl::Condemn {
        version: wire::VERSION,
        reason: if reason.is_empty() { "condemned" } else { reason }.into(),
    });
    true
}

type FacetConnector = Arc<dyn Fn(&str, &str, &str) -> Result<Arc<Channel>> + Send + Sync>;
static FACET_CONNECTOR: Mutex<Option<FacetConnector>> = Mutex::new(None);

/// Installed by the guest scheduler (a test host in checks): opens one facet
/// event's own storage stream for `(cell, invocation, grant)`.
pub(crate) fn install_facet_connector(
    connector: impl Fn(&str, &str, &str) -> Result<Arc<Channel>> + Send + Sync + 'static,
) {
    *FACET_CONNECTOR.lock().unwrap() = Some(Arc::new(connector));
}

/// What the host granted for one facet event, from the live root session.
pub(super) struct FacetGrant {
    pub(super) scope: String,
    pub(super) depth: usize,
    invocation: String,
    grant: String,
}

/// Ask the host to open one event of a facet of `parent` (the running object
/// or facet). The host derives the facet's scope; this names no storage.
pub(super) fn facet_open(
    scope: &mut v8::PinScope,
    parent: &str,
    class: &str,
    owner: &str,
    name: &str,
) -> Result<FacetGrant, String> {
    let channel = channel(scope, parent)?
        .ok_or("facets are available only inside a Durable Object event")?;
    let invocation = channel
        .invocation
        .get()
        .cloned()
        .ok_or("guest event has no invocation")?;
    let operation = Operation::FacetOpen {
        owner: owner.into(),
        name: name.into(),
        class: class.into(),
    };
    match call(scope, &channel, operation)? {
        Value::Facet {
            scope,
            grant,
            depth,
        } => Ok(FacetGrant {
            scope,
            depth: depth as usize,
            invocation,
            grant,
        }),
        _ => Err(unexpected(scope, &channel, "facet open")),
    }
}

/// Best effort: a released object's lifecycle channel refuses storage effects,
/// and the host already retired that event's facet sessions.
pub(super) fn facet_abort(scope: &mut v8::PinScope, parent: &str, class: &str, owner: &str, name: &str) {
    if let Ok(Some(channel)) = channel(scope, parent) {
        let operation = Operation::FacetAbort {
            owner: owner.into(),
            name: name.into(),
            class: class.into(),
        };
        let _ = call(scope, &channel, operation);
    }
}

pub(super) fn facet_delete(scope: &mut v8::PinScope, parent: &str, name: &str) -> Result<(), String> {
    let channel = channel(scope, parent)?
        .ok_or("facets are available only inside a Durable Object event")?;
    unit(scope, &channel, Operation::FacetDelete { name: name.into() })
}

/// Aborts a facet event's driver when the root operation awaiting it is dropped.
pub(super) struct FacetDriver(Option<tokio::task::JoinHandle<()>>);

impl FacetDriver {
    /// The reply arrived; the event's background work still runs to the end.
    pub(super) fn detach(&mut self) {
        self.0 = None;
    }
}

impl Drop for FacetDriver {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

/// Drive one facet event on its own storage stream to completion. The reply
/// reaches the caller only after the host flushed the turn that produced it,
/// so a failed flush refuses it. Every V8 turn is bracketed by the host's
/// execution turn, as for any guest event.
pub(super) fn spawn_facet<R: Send + 'static>(
    facet: FacetGrant,
    slot: Arc<crate::pool::Slot>,
    job: CellJob,
    inner: tokio::sync::oneshot::Receiver<Result<R>>,
) -> (tokio::sync::oneshot::Receiver<Result<R>>, FacetDriver) {
    let (reply, receive) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(super::guest_activity::track(async move {
        let mut reply = Some(reply);
        let mut inner = Some(inner);
        let result = run_facet(facet, slot, job, &mut inner, &mut reply).await;
        if let (Err(error), Some(reply)) = (result, reply.take()) {
            let _ = reply.send(Err(error));
        }
    }));
    (receive, FacetDriver(Some(task)))
}

async fn finished_op((id, op): Op) -> (u64, Result<asyncrt::OpOut, String>) {
    (id, op.await)
}

async fn run_facet<R: Send + 'static>(
    facet: FacetGrant,
    slot: Arc<crate::pool::Slot>,
    job: CellJob,
    inner: &mut Option<tokio::sync::oneshot::Receiver<Result<R>>>,
    reply: &mut Option<tokio::sync::oneshot::Sender<Result<R>>>,
) -> Result<()> {
    use futures_util::StreamExt;
    async fn edge(channel: &Arc<Channel>, begin: bool) -> Result<()> {
        let channel = channel.clone();
        tokio::task::spawn_blocking(super::guest_activity::blocking(move || {
            if begin {
                channel.begin_turn()
            } else {
                channel.finish_turn()
            }
        }))
        .await
        .context("guest facet turn task panicked")?
    }
    // Forward the event's reply once its turn is closed and flushed.
    fn forward<R>(
        inner: &mut Option<tokio::sync::oneshot::Receiver<Result<R>>>,
        reply: &mut Option<tokio::sync::oneshot::Sender<Result<R>>>,
    ) {
        if let Some(receive) = inner.as_mut() {
            match receive.try_recv() {
                Ok(result) => {
                    if let Some(reply) = reply.take() {
                        let _ = reply.send(result);
                    }
                    *inner = None;
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => *inner = None,
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
            }
        }
    }
    let connector = FACET_CONNECTOR
        .lock()
        .unwrap()
        .clone()
        .context("guest facet connector is not installed")?;
    let (cell, invocation, grant) = (facet.scope.clone(), facet.invocation, facet.grant);
    let channel = {
        let grant = grant.clone();
        tokio::task::spawn_blocking(super::guest_activity::blocking(move || {
            connector(&cell, &invocation, &grant)
        }))
        .await
        .context("guest facet connector panicked")??
    };
    let result = async {
        edge(&channel, true).await?;
        let begun = slot
            .turn(|worker| worker.turn_begin_guest_cell(job, None, channel.clone()))
            .await;
        edge(&channel, false).await?;
        let (entry, ops) = begun?;
        forward(inner, reply);
        let Some(mut entry) = entry else {
            return Ok(());
        };
        let mut ops: futures_util::stream::FuturesUnordered<_> =
            ops.into_iter().map(finished_op).collect();
        let deadline = tokio::time::Instant::now() + handler_budget();
        let outcome = async {
            while !entry.finished() {
                let delivered = if ops.is_empty() {
                    edge(&channel, true).await?;
                    let polled = slot.turn(|worker| worker.turn_poll_guest_cell(&mut entry)).await;
                    edge(&channel, false).await?;
                    polled?
                } else {
                    let (id, result) = tokio::time::timeout_at(deadline, ops.next())
                        .await
                        .context("guest facet event deadline exceeded")?
                        .context("guest facet event lost its operations")?;
                    edge(&channel, true).await?;
                    let delivered = slot
                        .turn(|worker| worker.turn_deliver_guest_cell(&mut entry, id, result))
                        .await;
                    edge(&channel, false).await?;
                    delivered?
                };
                forward(inner, reply);
                anyhow::ensure!(
                    !delivered.is_empty() || !ops.is_empty() || entry.finished(),
                    "guest facet event is idle without work"
                );
                ops.extend(delivered.into_iter().map(finished_op));
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        entry.finish_tail_report();
        entry.abandon();
        outcome
    }
    .await;
    // End this event's host session: it flushes the image and revokes the grant.
    let closing = channel.clone();
    let closed =
        tokio::task::spawn_blocking(super::guest_activity::blocking(move || closing.end_facet()))
            .await;
    channel.retire();
    if let Ok(Err(error)) = closed {
        tracing::debug!(%error, "guest facet session close");
    }
    result
}

/// One frame a guest object sent on a hibernatable socket, handed to the guest
/// scheduler, which attributes it to its event and asks the host to release it.
pub(crate) struct GuestSocketFrame {
    context: std::sync::Weak<IoContext>,
    pub(crate) ws: u64,
    /// For a socket this guest dialed (`dial`), whose `ws` is its local id.
    pub(crate) dial: bool,
    pub(crate) frame: celld_applications::invocation_wire::SocketFrame,
}

static SOCKETS: OnceLock<tokio::sync::mpsc::UnboundedSender<GuestSocketFrame>> = OnceLock::new();

/// Installed once by the guest scheduler before user code can run.
pub(crate) fn install_sockets(
    sender: tokio::sync::mpsc::UnboundedSender<GuestSocketFrame>,
) -> Result<()> {
    SOCKETS
        .set(sender)
        .map_err(|_| anyhow!("guest socket broker already installed"))
}

impl InFlight {
    /// A frame this event sent, as a handler's `ws.send` would have queued it.
    #[cfg(test)]
    pub(crate) fn frame_for_test(
        &self,
        ws: u64,
        frame: celld_applications::invocation_wire::SocketFrame,
    ) -> GuestSocketFrame {
        GuestSocketFrame {
            context: Arc::downgrade(&self.context),
            ws,
            dial: false,
            frame,
        }
    }

    /// Whether this event sent `frame`.
    pub(crate) fn sent(&self, frame: &GuestSocketFrame) -> bool {
        std::ptr::eq(frame.context.as_ptr(), Arc::as_ptr(&self.context))
    }
}

/// `ws.send`/`ws.close` of a hibernatable socket in guest mode. The host owns
/// the socket: the frame is attributed to the sending object event, proved
/// under that event's output gate and released in order, never written here.
pub(super) fn socket_frame(
    scope: &mut v8::PinScope,
    ws: u64,
    dial: bool,
    frame: celld_applications::invocation_wire::SocketFrame,
) -> Result<(), String> {
    let context = event_context(scope);
    // A hibernatable socket is its object's; a dialed one is any event's own.
    if !dial && !context.has_guest_storage() {
        return Err("WebSocket frames can only be sent from a Durable Object event".into());
    }
    send_frame(&context, ws, dial, frame)
}

fn send_frame(
    context: &Arc<IoContext>,
    ws: u64,
    dial: bool,
    frame: celld_applications::invocation_wire::SocketFrame,
) -> Result<(), String> {
    let sender = SOCKETS.get().ok_or("guest socket broker is not installed")?;
    sender
        .send(GuestSocketFrame {
            context: Arc::downgrade(context),
            ws,
            dial,
            frame,
        })
        .map_err(|_| "guest socket broker stopped".into())
}

/// The request ended with a dialed socket open: close it, as the original
/// closes an isolate-polled socket with its request. The host's echo of the
/// close ends the pump that is reading it.
pub(super) fn close_dialed(context: &Arc<IoContext>, ws: u64) {
    let _ = send_frame(
        context,
        ws,
        true,
        celld_applications::invocation_wire::SocketFrame::Close {
            code: 1001,
            reason: "request ended".into(),
        },
    );
}

/// An outbound WebSocket in guest mode: the handshake is a brokered fetch the
/// trusted host dials through its own connector (`FetchStart.dial`), so the
/// guest opens no connection. `url` is the handshake's `http(s)` twin.
pub(super) fn dial(
    scope: &mut v8::PinScope,
    ws: u64,
    url: String,
    protocols: Vec<String>,
    headers: Vec<(String, String)>,
    response: bool,
) -> Result<Brokered, String> {
    let request = celld_applications::invocation_wire::HttpRequest {
        method: "GET".into(),
        url,
        headers,
        body: Vec::new(),
    };
    request.validate().map_err(|error| error.to_string())?;
    let sender = EGRESS.get().ok_or(FETCH_OUTSIDE_EVENT)?;
    let (reply, brokered) = tokio::sync::oneshot::channel();
    sender
        .send(GuestFetch {
            context: Arc::downgrade(&event_context(scope)),
            request,
            redirect: "manual".into(),
            target: None,
            service: None,
            resource: None,
            upload: None,
            dial: Some((
                celld_applications::invocation_wire::SocketDial {
                    protocols,
                    response,
                },
                ws,
            )),
            reply,
        })
        .map_err(|_| "guest egress broker stopped")?;
    Ok(brokered)
}

/// The original refusal for egress no live event can own.
pub(crate) const FETCH_OUTSIDE_EVENT: &str = "This worker is not permitted to access the \
    internet via global functions like fetch() outside an event.";

impl InFlight {
    /// Whether this event issued `fetch`, stateless or object alike.
    pub(crate) fn issued(&self, fetch: &GuestFetch) -> bool {
        std::ptr::eq(fetch.context.as_ptr(), Arc::as_ptr(&self.context))
    }
}

/// Original `fetch()` in guest mode: the request goes to the trusted host,
/// which applies egress policy and the output gate before sending it. The
/// reply body arrives as a credited stream.
pub(super) fn fetch(
    scope: &mut v8::PinScope,
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    body: Option<RequestBody>,
    redirect: String,
) -> Result<u64, String> {
    let body = match body {
        None => Vec::new(),
        Some(RequestBody::Bytes(bytes)) => bytes.to_vec(),
        Some(RequestBody::Stream(_)) => {
            return Err("guest fetch request bodies must be buffered".into());
        }
    };
    let request = celld_applications::invocation_wire::HttpRequest {
        method,
        url,
        headers,
        body,
    };
    let response = broker(scope, request, redirect, None, None, None)?;
    let stream_service = http_stream_service();
    Ok(asyncrt::enqueue(async move {
        encode_http_response(awaited(response).await?, true, &stream_service)
    }))
}

pub(super) type Brokered = tokio::sync::oneshot::Receiver<Result<HttpResponse, String>>;

async fn awaited(response: Brokered) -> Result<HttpResponse, String> {
    response
        .await
        .map_err(|_| "guest fetch was abandoned".to_string())?
}

fn broker(
    scope: &mut v8::PinScope,
    mut request: celld_applications::invocation_wire::HttpRequest,
    redirect: String,
    target: Option<celld_applications::invocation_wire::ObjectCall>,
    service: Option<celld_applications::invocation_wire::ServiceCall>,
    resource: Option<celld_applications::resource_wire::ResourceCall>,
) -> Result<Brokered, String> {
    if request.body.len() > celld_applications::invocation_wire::MAX_BODY_BYTES {
        if (target.is_none() && service.is_none() && resource.is_none())
            || request.body.len() > celld_applications::invocation_wire::MAX_OBJECT_CALL_BYTES
        {
            return Err("object call arguments exceed limit".into());
        }
        let body = std::mem::take(&mut request.body);
        request.validate().map_err(|error| error.to_string())?;
        request.body = body;
    } else {
        request.validate().map_err(|error| error.to_string())?;
    }
    // Module evaluation precedes the scheduler's broker, and the scheduler
    // refuses a context that is not a live event's: without an event there is
    // no attribution or output gate, so the original refusal stands.
    let sender = EGRESS.get().ok_or(FETCH_OUTSIDE_EVENT)?;
    let (reply, response) = tokio::sync::oneshot::channel();
    sender
        .send(GuestFetch {
            context: Arc::downgrade(&event_context(scope)),
            request,
            redirect,
            target,
            service,
            resource,
            upload: None,
            dial: None,
            reply,
        })
        .map_err(|_| "guest egress broker stopped")?;
    Ok(response)
}

/// Original namespace-stub `fetch` or, with `rpc`, `stub.method(...)` in guest
/// mode. The host decides whether `cell` is in a namespace this handler is
/// bound to, runs the object's event and returns its value or error. An RPC's
/// request body is its argument bytes and the promise answers the result bytes.
pub(super) fn object_call(
    scope: &mut v8::PinScope,
    cell: &str,
    name: Option<String>,
    rpc: Option<String>,
    request: celld_applications::invocation_wire::HttpRequest,
) -> Result<u64, String> {
    use futures_util::StreamExt;
    let (class, id) = cell.split_once(':').ok_or("malformed object scope")?;
    let target = celld_applications::invocation_wire::ObjectCall {
        object: celld_applications::invocation_wire::ObjectTarget {
            class: class.into(),
            id: id.into(),
            name,
        },
        rpc: rpc.clone(),
    };
    let response = broker(scope, request, "manual".into(), Some(target), None, None)?;
    let stream_service = http_stream_service();
    Ok(if rpc.is_some() {
        asyncrt::enqueue(async move {
            let response = awaited(response).await?;
            let mut bytes = response.body;
            if let Some(mut stream) = response.stream {
                while let Some(chunk) = stream.next().await {
                    bytes.extend(chunk?);
                }
            }
            Ok::<_, String>(bytes)
        })
    } else {
        asyncrt::enqueue(async move {
            encode_http_response(awaited(response).await?, true, &stream_service)
        })
    })
}

/// Preserve the original service response/RPC encoding and cancellable op
/// registry. Only an alias crosses; the host resolves its pinned binding.
pub(super) fn service_call(
    scope: &mut v8::PinScope,
    binding: String,
    invocation: celld_applications::invocation_wire::ServiceInvocation,
    request: celld_applications::invocation_wire::HttpRequest,
    upload: Option<HttpChunkStream>,
    cancel: Option<tokio::sync::oneshot::Receiver<()>>,
    cancel_guard: Option<DoCallCancelGuard>,
) -> Result<u64, String> {
    use futures_util::StreamExt;
    invocation.validate().map_err(|error| error.to_string())?;
    let rpc = invocation.rpc.is_some();
    let service = celld_applications::invocation_wire::ServiceCall {
        binding: celld_applications::Name::try_from(binding).map_err(|error| error.to_string())?,
        invocation,
    };
    let response = if let Some(upload) = upload {
        request.validate().map_err(|error| error.to_string())?;
        let (reply, response) = tokio::sync::oneshot::channel();
        EGRESS
            .get()
            .ok_or(FETCH_OUTSIDE_EVENT)?
            .send(GuestFetch {
                context: Arc::downgrade(&event_context(scope)),
                request,
                redirect: "manual".into(),
                target: None,
                service: Some(service),
                resource: None,
                upload: Some(upload),
                dial: None,
                reply,
            })
            .map_err(|_| "guest egress broker stopped")?;
        response
    } else {
        broker(scope, request, "manual".into(), None, Some(service), None)?
    };
    let stream_service = http_stream_service();
    Ok(asyncrt::enqueue(async move {
        let mut cancel_guard = cancel_guard;
        let work = async move {
            let mut response = awaited(response).await?;
            if rpc {
                if let Some(mut stream) = response.stream.take() {
                    while let Some(chunk) = stream.next().await {
                        response.body.extend(chunk?);
                    }
                }
                Ok::<asyncrt::OpOut, String>(response.body.into())
            } else {
                encode_http_response(response, true, &stream_service).map(Into::into)
            }
        };
        let result = if let Some(mut cancel) = cancel {
            crate::asyncrt::select! {
                result = work => result,
                _ = &mut cancel => Err("Service binding call cancelled".into()),
            }
        } else {
            work.await
        };
        if let Some(guard) = cancel_guard.as_mut() {
            guard.disarm();
        }
        result
    }))
}

pub(super) fn resource_request(
    scope: &mut v8::PinScope,
    binding: String,
    operation: celld_applications::resource_wire::ResourceOperation,
    body: Vec<u8>,
) -> Result<Brokered, String> {
    resource_http_request(
        scope,
        binding,
        operation,
        celld_applications::invocation_wire::HttpRequest {
            method: "POST".into(),
            url: "https://resource.celld.internal/".into(),
            headers: vec![],
            body,
        },
    )
}

/// A resource operation whose request is itself the operation's input, like
/// an `ASSETS.fetch()` call.
pub(super) fn resource_http_request(
    scope: &mut v8::PinScope,
    binding: String,
    operation: celld_applications::resource_wire::ResourceOperation,
    request: celld_applications::invocation_wire::HttpRequest,
) -> Result<Brokered, String> {
    let call = celld_applications::resource_wire::ResourceCall {
        binding: celld_applications::Name::try_from(binding).map_err(|error| error.to_string())?,
        operation,
    };
    call.validate().map_err(|error| error.to_string())?;
    broker(scope, request, "manual".into(), None, None, Some(call))
}

pub(super) fn enabled(scope: &mut v8::PinScope) -> bool {
    scope
        .get_slot::<Arc<ActorRuntimeState>>()
        .is_some_and(|state| state.guest_storage_enabled.load(Ordering::Acquire))
}

pub(super) fn current_channel(scope: &mut v8::PinScope) -> Result<Arc<Channel>, String> {
    event_context(scope)
        .guest_storage
        .get()
        .context("guest storage requires its owning event")
        .map_err(|error| error.to_string())
        .cloned()
}

/// The object the current event runs for.
pub(super) fn current_cell(scope: &mut v8::PinScope) -> Result<String, String> {
    current_channel(scope).map(|channel| channel.cell.clone())
}

pub(super) fn channel(
    scope: &mut v8::PinScope,
    cell: &str,
) -> Result<Option<Arc<Channel>>, String> {
    if !enabled(scope) {
        return Ok(None);
    }
    let channel = current_channel(scope)?;
    if channel.cell != cell {
        return Err("guest storage belongs to another object".into());
    }
    Ok(Some(channel))
}

fn terminate(scope: &mut v8::PinScope, channel: &Channel, message: &str) {
    channel.retire();
    let context = event_context(scope);
    *actor_runtime_state(scope).termination.lock().unwrap() = Some(ExecutionTermination {
        error: format!("guest storage retired: {message}"),
        actor_scope: Some(channel.cell.clone()),
        context_id: context.continuation_id(),
    });
    scope.terminate_execution();
}

pub(super) fn call(
    scope: &mut v8::PinScope,
    channel: &Channel,
    operation: Operation,
) -> Result<Value, String> {
    let result = execution_turn(channel).and_then(|turn| channel.call(operation, turn));
    checked(scope, channel, result)
}

// CPED selects the resource origin, while CurrentGuard identifies the actual
// checkpoint driver. Borrow only that driver's admitted ordinary turn; the
// host independently verifies the token, grant, VM and native input hold.
fn execution_turn(channel: &Channel) -> Result<Option<String>> {
    anyhow::ensure!(
        !channel.effects_closed.load(Ordering::Acquire),
        "guest lifecycle channel refuses storage effects"
    );
    // A foreign checkpoint must not park on an origin whose own admission is
    // waiting for the native gate that this checkpoint's driver holds. The
    // scheduler keeps same-object admissions apart from running turns.
    match channel.state.try_lock() {
        Ok(state) if state.turn.is_some() => return Ok(None),
        Ok(_) => {}
        Err(std::sync::TryLockError::WouldBlock) => {
            anyhow::bail!("guest storage origin is waiting for native admission")
        }
        Err(std::sync::TryLockError::Poisoned(error)) => panic!("{error}"),
    }
    let driver = current_context();
    let driver = driver
        .guest_storage
        .get()
        .context("guest storage requires its active execution turn")?;
    anyhow::ensure!(
        driver.cell == channel.cell,
        "foreign checkpoint requires its object's execution turn"
    );
    let state = driver.state.lock().unwrap();
    anyhow::ensure!(
        state.stream.is_some(),
        "guest execution turn channel retired"
    );
    Ok(Some(state.turn.clone().context(
        "guest storage requires its active execution turn",
    )?))
}

fn in_turn(operation: Operation, turn: Option<String>) -> Operation {
    match turn {
        Some(turn) => Operation::InExecutionTurn {
            turn,
            operation: Box::new(operation),
        },
        None => operation,
    }
}

fn checked<T>(scope: &mut v8::PinScope, channel: &Channel, result: Result<T>) -> Result<T, String> {
    let result = result.map_err(|error| error.to_string());
    if channel.effects_closed.load(Ordering::Acquire) {
        return result;
    }
    if let Err(message) = &result {
        // A pending admission owns the mutex and still has its stream.
        let retired = channel
            .state
            .try_lock()
            .is_ok_and(|state| state.stream.is_none());
        if retired {
            terminate(scope, channel, message);
        }
    }
    result
}

pub(super) fn unexpected(scope: &mut v8::PinScope, channel: &Channel, operation: &str) -> String {
    let message = format!("invalid {operation} reply");
    terminate(scope, channel, &message);
    message
}

/// The original harness keeps numeric local event labels. Only opaque host
/// capabilities cross the wire; labels neither select a host gate nor grant
/// admission. As in original `op_gate_acquire`, the operation driver owns the
/// gate: a foreign origin's block is held by the driver's ordinary turn and
/// retains the origin through its cross-entry claim. The host lets only that
/// origin grant borrow the driver's turn while the block is held.
pub(super) fn critical_start(
    scope: &mut v8::PinScope,
    cell: &str,
    event: u64,
    origin: &Arc<IoContext>,
    driver: &Arc<IoContext>,
) -> Result<(), String> {
    let channel = driver
        .guest_storage
        .get()
        .context("guest critical section has no driver channel")
        .map_err(|error| error.to_string())?;
    if channel.cell != cell {
        return Err("guest storage belongs to another object".into());
    }
    if channel.effects_closed.load(Ordering::Acquire) {
        return Err("guest lifecycle channel refuses critical sections".into());
    }
    // Never lock the origin's channel here: its own admission can be waiting
    // for the very gate this driver holds.
    let foreign = if Arc::ptr_eq(origin, driver) {
        None
    } else {
        let origin_channel = origin
            .guest_storage
            .get()
            .context("guest critical section origin has no storage channel")
            .map_err(|error| error.to_string())?;
        if origin_channel.cell != cell {
            return Err("foreign checkpoint requires its object's execution turn".into());
        }
        if origin_channel.effects_closed.load(Ordering::Acquire) {
            return Err("guest lifecycle channel refuses critical sections".into());
        }
        let claim = origin.claim_cross_entry_gate().ok_or(RETIRED_INPUT_GATE)?;
        Some((origin_channel.grant.clone(), claim))
    };
    let result = (|| {
        channel.require_turn(cell)?;
        anyhow::ensure!(
            driver.continuation_id() == channel.owner.get().copied(),
            "guest critical section belongs to another event"
        );
        let mut state = channel.state.lock().unwrap();
        anyhow::ensure!(
            state.critical_sections.len() < 64,
            "guest critical section depth limit reached"
        );
        anyhow::ensure!(
            !state
                .critical_sections
                .iter()
                .any(|(label, ..)| *label == event),
            "guest critical section label already active"
        );
        let (origin, claim) = foreign.unzip();
        match channel.exchange(&mut state, Operation::CriticalSectionStart { origin })? {
            Value::CriticalSection { critical_section }
                if wire::valid_handle(&critical_section) =>
            {
                state
                    .critical_sections
                    .push((event, critical_section, claim));
                Ok(())
            }
            _ => {
                state.stream = None;
                state.native_hold_retained = true;
                anyhow::bail!("invalid guest critical-section reply")
            }
        }
    })();
    checked(scope, channel, result)
}

pub(super) fn critical_finish(
    scope: &mut v8::PinScope,
    cell: &str,
    event: u64,
    driver: &Arc<IoContext>,
    failed: bool,
) -> Result<(), String> {
    let channel = driver
        .guest_storage
        .get()
        .context("guest critical section has no driver channel")
        .map_err(|error| error.to_string())?;
    if channel.effects_closed.load(Ordering::Acquire) {
        if channel.cell != cell {
            return Err("guest storage belongs to another object".into());
        }
        // The cancellation turn will discard this local label after admission.
        // Never wait for the channel mutex or release the native claim here.
        return Ok(());
    }
    let result = (|| {
        channel.require_turn(cell)?;
        anyhow::ensure!(
            driver.continuation_id() == channel.owner.get().copied(),
            "guest critical section belongs to another event"
        );
        let mut state = channel.state.lock().unwrap();
        // Original stale/duplicate local release is harmless. It cannot
        // consume a different event's live opaque capability.
        let Some(index) = state
            .critical_sections
            .iter()
            .position(|(label, ..)| *label == event)
        else {
            return Ok(None);
        };
        anyhow::ensure!(
            index + 1 == state.critical_sections.len(),
            "guest critical section must finish in nesting order"
        );
        let critical_section = state.critical_sections[index].1.clone();
        match channel.exchange(
            &mut state,
            Operation::CriticalSectionFinish {
                critical_section,
                failed,
            },
        )? {
            Value::Unit {} => {
                // A failed block latches the host gate until trusted settlement.
                state.native_hold_retained |= failed;
                Ok(state.critical_sections.pop().and_then(|(.., claim)| claim))
            }
            _ => {
                state.stream = None;
                state.native_hold_retained = true;
                anyhow::bail!("invalid guest critical-section finish reply")
            }
        }
    })();
    // Releasing the origin claim can drop its context; keep that outside the
    // channel mutex, as release_cell_gate does outside the gate map.
    checked(scope, channel, result).map(drop)
}

pub(super) fn actor_name(
    scope: &mut v8::PinScope,
    channel: &Channel,
) -> Result<Option<String>, String> {
    match call(scope, channel, Operation::GetActorName {})? {
        Value::ActorName { name } => Ok(name),
        _ => Err(unexpected(scope, channel, "actor name")),
    }
}

pub(super) fn unit(
    scope: &mut v8::PinScope,
    channel: &Channel,
    operation: Operation,
) -> Result<(), String> {
    match call(scope, channel, operation)? {
        Value::Unit {} => Ok(()),
        _ => Err(unexpected(scope, channel, "unit")),
    }
}

pub(super) fn entries(
    scope: &mut v8::PinScope,
    channel: &Channel,
    operation: Operation,
) -> Result<Vec<(String, storage::StoredValue)>, String> {
    let name = if matches!(operation, Operation::Get { .. }) {
        "get"
    } else {
        "list"
    };
    match call(scope, channel, operation)? {
        Value::Entries { entries } => Ok(entries
            .into_iter()
            .map(|(key, value)| {
                (
                    key,
                    match value {
                        wire::StoredValue::Json { value } => {
                            storage::StoredValue::LegacyJson(value)
                        }
                        wire::StoredValue::V8 { value } => storage::StoredValue::V8(value),
                    },
                )
            })
            .collect()),
        _ => Err(unexpected(scope, channel, name)),
    }
}

pub(super) fn sql_row(row: Vec<wire::SqlValue>) -> Vec<storage::SqlValue> {
    row.into_iter()
        .map(|value| match value {
            wire::SqlValue::Null {} => storage::SqlValue::Null,
            wire::SqlValue::Integer { value } => storage::SqlValue::Integer(value),
            wire::SqlValue::Real { bits } => storage::SqlValue::Real(f64::from_bits(bits)),
            wire::SqlValue::Text { value } => storage::SqlValue::Text(value),
            wire::SqlValue::Blob { value } => storage::SqlValue::Blob(value),
        })
        .collect()
}

pub(super) fn transaction_control(
    scope: &mut v8::PinScope,
    channel: &Channel,
    action: &str,
    nested: bool,
    savepoint: &str,
) -> Result<(), String> {
    if channel.effects_closed.load(Ordering::Acquire) {
        if matches!(action, "rollback" | "rollback_explicit") {
            // Unwind the original JS transaction wrapper without treating a
            // deliberately refused native rollback as database corruption.
            // This retires only an owned guest label; native rollback remains
            // mandatory at trusted joined settlement. Never wait on an ordinary
            // admission mutex or accept this outside its own cleanup turn.
            if current_context().continuation_id() != channel.owner.get().copied() {
                return Err("guest rollback requires its owning cleanup turn".into());
            }
            let mut state = channel
                .state
                .try_lock()
                .map_err(|_| "guest rollback requires joined cleanup admission")?;
            if !state.cleanup_admitted || !state.cleanup_only || state.turn.is_none() {
                return Err("guest rollback requires admitted cleanup turn".into());
            }
            let (label, ..) = state
                .transactions
                .last()
                .ok_or("guest transaction is no longer active")?;
            if label != savepoint || nested != (state.transactions.len() > 1) {
                return Err("guest rollback must unwind owning transaction nesting order".into());
            }
            state.transactions.pop();
            state.native_hold_retained = true;
            return Ok(());
        }
        return Err("guest lifecycle channel refuses storage effects".into());
    }
    // An origin with its own turn holds its savepoints. A foreign
    // continuation's savepoint is held by the checkpoint's driver, as its
    // blocks are: the driver's session keeps the gate and rollback duty while
    // the host admits only this origin's grant inside it. Never lock the
    // origin while its own admission can be waiting for that driver's gate.
    let own = match channel.state.try_lock() {
        Ok(state) => state.turn.is_some(),
        Err(std::sync::TryLockError::WouldBlock) => false,
        Err(std::sync::TryLockError::Poisoned(error)) => panic!("{error}"),
    };
    let driver_context = current_context();
    let (holder, origin) = match driver_context.guest_storage.get() {
        Some(driver) if !own && !std::ptr::eq(driver.as_ref(), channel) => {
            (driver.as_ref(), Some(channel.grant.clone()))
        }
        _ => (channel, None),
    };
    let result = (|| {
        if origin.is_some() {
            anyhow::ensure!(
                holder.cell == channel.cell,
                "foreign checkpoint requires its object's execution turn"
            );
            anyhow::ensure!(
                !holder.effects_closed.load(Ordering::Acquire),
                "guest lifecycle channel refuses storage effects"
            );
        }
        let mut state = match holder.state.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock) => {
                anyhow::bail!("guest storage requires its active execution turn")
            }
            Err(std::sync::TryLockError::Poisoned(error)) => panic!("{error}"),
        };
        anyhow::ensure!(
            state.turn.is_some(),
            "guest storage requires its active execution turn"
        );
        if action == "start" {
            anyhow::ensure!(
                state.transactions.iter().all(|(.., held)| held == &origin),
                "transaction nesting mixes events"
            );
            anyhow::ensure!(
                nested == !state.transactions.is_empty(),
                "transaction nesting does not match owning event"
            );
            anyhow::ensure!(
                state.transactions.len() < 128 && !savepoint.is_empty() && savepoint.len() <= 256,
                "invalid transaction label or depth limit reached"
            );
            anyhow::ensure!(
                !state
                    .transactions
                    .iter()
                    .any(|(label, ..)| label == savepoint),
                "transaction label already active"
            );
            let start = Operation::TransactionStart {
                origin: origin.clone(),
            };
            match holder.exchange(&mut state, start)? {
                Value::Transaction { transaction } if wire::valid_handle(&transaction) => {
                    state
                        .transactions
                        .push((savepoint.to_owned(), transaction, origin));
                    Ok(())
                }
                _ => {
                    state.stream = None;
                    state.native_hold_retained = true;
                    anyhow::bail!("invalid transaction-start reply")
                }
            }
        } else {
            let action = match action {
                "commit" => wire::TransactionAction::Commit,
                "rollback" => wire::TransactionAction::Rollback,
                "rollback_explicit" => wire::TransactionAction::RollbackExplicit,
                _ => anyhow::bail!("unknown transaction action"),
            };
            let (label, transaction, held) = state
                .transactions
                .last()
                .context("transaction is no longer active")?;
            anyhow::ensure!(
                label == savepoint && held == &origin && nested == (state.transactions.len() > 1),
                "transaction must finish in owning event nesting order"
            );
            let transaction = transaction.clone();
            match holder.exchange(
                &mut state,
                Operation::TransactionFinish {
                    transaction,
                    action,
                },
            )? {
                Value::Unit {} => {
                    state.transactions.pop();
                    Ok(())
                }
                _ => {
                    state.stream = None;
                    state.native_hold_retained = true;
                    anyhow::bail!("invalid transaction-finish reply")
                }
            }
        }
    })();
    checked(scope, holder, result)
}

pub(super) fn alarm(scope: &mut v8::PinScope, channel: &Channel) -> Result<Option<i64>, String> {
    match call(scope, channel, Operation::GetAlarm {})? {
        Value::Alarm { at_ms } => Ok(at_ms),
        _ => Err(unexpected(scope, channel, "alarm")),
    }
}

pub(super) fn get(
    scope: &mut v8::PinScope,
    channel: &Channel,
    keys: Vec<String>,
) -> Result<Vec<(String, storage::StoredValue)>, String> {
    entries(scope, channel, Operation::Get { keys })
}

pub(super) fn put(
    scope: &mut v8::PinScope,
    channel: &Channel,
    entries: &[(String, Vec<u8>)],
) -> Result<(), String> {
    let entries = entries
        .iter()
        .map(|(key, value)| CloneEntry {
            key: key.clone(),
            value: value.clone(),
        })
        .collect();
    put_clones(scope, channel, entries)
}

fn put_clones(
    scope: &mut v8::PinScope,
    channel: &Channel,
    entries: Vec<CloneEntry>,
) -> Result<(), String> {
    match call(scope, channel, Operation::PutClone { entries })? {
        Value::Unit {} => Ok(()),
        _ => {
            terminate(scope, channel, "invalid put reply");
            Err("invalid put reply".into())
        }
    }
}

pub(super) fn queue(channel: &Channel, entries: Vec<(String, Vec<u8>)>) -> Result<(), String> {
    let turn = execution_turn(channel).map_err(|error| error.to_string())?;
    let mut state = channel.state.lock().unwrap();
    if state.cleanup_only {
        return Err("guest lifecycle channel refuses pending storage effects".into());
    }
    if state.stream.is_none() || (state.turn.is_none() && turn.is_none()) {
        return Err("guest storage requires its active execution turn".into());
    }
    if entries.is_empty() {
        return Ok(());
    }
    let entries: Vec<_> = entries
        .into_iter()
        .map(|(key, value)| CloneEntry { key, value })
        .collect();
    // Charge the exact JSON/base64 frame size, including keys and separators.
    // Do not retain an unbounded batch that can never fit on the wire.
    let pending_bytes = state
        .pending_bytes
        .saturating_add(serialized_json_bytes(&entries) - 2)
        .saturating_add(usize::from(!state.pending.is_empty()));
    let overhead = serialized_json_bytes(&Request {
        version: wire::VERSION,
        grant: channel.grant.clone(),
        operation: in_turn(
            Operation::PutClone {
                entries: Vec::new(),
            },
            turn,
        ),
    });
    if pending_bytes.saturating_add(overhead) > wire::MAX_MESSAGE_BYTES {
        return Err("guest pending puts exceed storage message limit".into());
    }
    state.pending_bytes = pending_bytes;
    state.pending.extend(entries);
    Ok(())
}

pub(super) fn flush(scope: &mut v8::PinScope, channel: &Channel) -> Result<(), String> {
    if channel.effects_closed.load(Ordering::Acquire) {
        return Ok(());
    }
    // A pending admission owns the mutex; queue() refused that origin, so
    // nothing can be pending for this checkpoint to flush.
    let entries = match channel.state.try_lock() {
        Ok(mut state) => {
            state.pending_bytes = 0;
            std::mem::take(&mut state.pending)
        }
        Err(std::sync::TryLockError::WouldBlock) => return Ok(()),
        Err(std::sync::TryLockError::Poisoned(error)) => panic!("{error}"),
    };
    if entries.is_empty() {
        return Ok(());
    }
    put_clones(scope, channel, entries)
}

pub(super) fn cancel(channel: &Channel) {
    if channel.effects_closed.load(Ordering::Acquire) {
        return;
    }
    if let Ok(mut state) = channel.state.try_lock() {
        state.pending.clear();
        state.pending_bytes = 0;
    }
}

#[cfg(test)]
#[path = "../unit_tests/js/guest_storage/tests.rs"]
pub(crate) mod tests;
