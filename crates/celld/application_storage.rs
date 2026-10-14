//! Trusted host half of synchronous guest storage. The caller holds the
//! original storage/runtime lock and admits the effect through the Actor.
//! This adapter neither opens files nor grants ownership or durability proof.

use crate::storage::{self, Cells};
use anyhow::{ensure, Context, Result};
use celld_applications::storage_wire::{
    self as wire, Operation, Outcome, Reply, Request, TransactionAction, Value,
};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use subtle::ConstantTimeEq;

#[path = "application_storage/facet_lifetime.rs"]
mod facet_lifetime;

const MAX_CURSORS: usize = 128;
const MAX_TRANSACTIONS: usize = 128;
const MAX_CRITICAL_SECTIONS: usize = 64;
const MAX_FACET_EVENTS: usize = 64;
const CRITICAL_SECTION_FAILED: &str = "guest critical section failed; object residency retired";

pub(crate) fn handle() -> Result<String> {
    let mut bytes = [0; 32];
    getrandom::fill(&mut bytes)
        .map_err(|error| anyhow::anyhow!("storage entropy failed: {error}"))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// One host-admitted event at a fenced native connection. The optional VM
/// cursor table has a separate lifetime; it cannot revive this event's grant.
pub struct StorageSession {
    scope: String,
    epoch: u64,
    container: Option<Arc<crate::container::CellContainer>>,
    pending_container_run: Option<u64>,
    residency: std::sync::Arc<std::sync::atomic::AtomicBool>,
    grant: String,
    revoked: bool,
    alarm: Option<(i64, celld_applications::invocation_wire::AlarmInvocation)>,
    cursors: HashMap<String, u64>,
    /// The original sync `kv.list()` cursor: opaque handle and native id.
    sync_list: Option<(String, u64)>,
    // One immutable D1 reply per event; continuation never executes SQL again.
    d1_reply: Option<(String, usize)>,
    d1_request: Option<(String, usize)>,
    cursor_residency: Option<Arc<CursorResidency>>,
    turn: Option<String>,
    turn_lease: Option<Arc<ExecutionTurn>>,
    /// Opaque capability and, for a foreign-driven block, its origin grant.
    critical_sections: Vec<(String, Option<String>)>,
    critical_failed: bool,
    abort_reason: Option<String>,
    /// Opaque token, native savepoint and optional delegated origin grant.
    transactions: Vec<(String, String, Option<String>)>,
    next_savepoint: u64,
    /// Sockets this event accepted. Whatever no upgrade response claimed is
    /// released when the event settles, as nothing can ever deliver it.
    accepted_sockets: Vec<u64>,
    input_gate: crate::js::GuestInputGate,
    /// A facet event session names the live root it lives in; every operation
    /// checks it, so a facet of a retired root can never act on a successor.
    parent: Option<ParentLink>,
    /// Facet event sessions by grant, and the native facet connections this
    /// session uses. Both end with this session; the guest supplies neither scope.
    facets: HashMap<String, StorageSession>,
    facet_cells: Vec<facet_lifetime::Lease>,
    /// For a facet event session, its facet's names from the root down; its
    /// replication stream derives from them and the root.
    facet_names: Vec<String>,
    /// The facet a pending `FacetOpen` opens and the file its activated stream
    /// answered, taken by the native turn.
    facet_file: Option<(String, (std::path::PathBuf, bool))>,
}

#[derive(Clone)]
struct ParentLink {
    scope: String,
    epoch: u64,
    residency: Arc<AtomicBool>,
}

/// Read cursors belong to one VM and one native connection, independently of
/// event grants. No admission, Actor request or native pointer escapes here.
pub(crate) struct CursorResidency {
    id: String,
    scope: String,
    epoch: u64,
    connection: Arc<AtomicBool>,
    owner: Arc<AtomicBool>,
    retired: AtomicBool,
    cursors: Mutex<HashMap<String, u64>>,
    turns: Mutex<HashMap<String, std::sync::Weak<ExecutionTurn>>>,
}

/// Host-only lease; a token cannot outlive its session or borrow a native
/// transaction/critical hold. The one exception is a block the driver holds
/// for a foreign origin: only that origin grant may borrow while it is held.
/// Validation and effects run under the Cells lock.
struct ExecutionTurn {
    event: u64,
    delegation_closed: AtomicBool,
    borrowable: AtomicBool,
    delegate: Mutex<Option<String>>,
}

impl CursorResidency {
    fn new(session: &StorageSession, owner: Arc<AtomicBool>) -> Result<Self> {
        ensure!(owner.load(Ordering::Acquire), "guest cursor owner retired");
        Ok(Self {
            id: handle()?,
            scope: session.scope.clone(),
            epoch: session.epoch,
            connection: session.residency.clone(),
            owner,
            retired: AtomicBool::new(false),
            cursors: Mutex::new(HashMap::new()),
            turns: Mutex::new(HashMap::new()),
        })
    }

    fn matches(&self, session: &StorageSession) -> bool {
        !self.retired.load(Ordering::Acquire) && self.connection_matches(session)
    }

    fn connection_matches(&self, session: &StorageSession) -> bool {
        self.scope == session.scope
            && self.epoch == session.epoch
            && Arc::ptr_eq(&self.connection, &session.residency)
    }

    pub(crate) fn scope(&self) -> &str {
        &self.scope
    }

    /// Only read statements reach this table, so retirement cannot commit a
    /// write or manufacture an output proof. Stale cleanup never enters a new
    /// connection, including one opened again at the same ownership epoch.
    pub(crate) fn close(&self, cells: &Cells) {
        let _installed = cells.install();
        let mut cursors = self.cursors.lock().expect("guest cursors poisoned");
        let current = storage::activation_epoch(&self.scope) == Some(self.epoch)
            && storage::activation_residency(&self.scope)
                .is_some_and(|token| Arc::ptr_eq(&token, &self.connection));
        for (_, native) in cursors.drain() {
            if current {
                storage::sql_cursor_close(native);
            }
        }
    }
}

/// Owned by the transport/VM. The fixed bound avoids retaining an unlimited
/// number of object tables; application limits will replace it when wired.
pub(crate) struct CursorResidencies {
    live: Arc<AtomicBool>,
    objects: Mutex<HashMap<String, (crate::runtime::RuntimeManager, Arc<CursorResidency>)>>,
    closing: tokio::sync::Mutex<()>,
}

impl Default for CursorResidencies {
    fn default() -> Self {
        Self {
            live: Arc::new(AtomicBool::new(true)),
            objects: Mutex::new(HashMap::new()),
            closing: tokio::sync::Mutex::new(()),
        }
    }
}

impl CursorResidencies {
    pub(crate) fn attach(
        &self,
        session: &mut StorageSession,
        runtime: &crate::runtime::RuntimeManager,
    ) -> Result<String> {
        ensure!(
            self.live.load(Ordering::Acquire),
            "guest cursor owner retired"
        );
        session.check_residency()?;
        ensure!(
            session.cursors.is_empty() && session.cursor_residency.is_none(),
            "event cursor residency already selected"
        );
        let mut objects = self.objects.lock().expect("guest cursor registry poisoned");
        ensure!(
            self.live.load(Ordering::Acquire),
            "guest cursor owner retired"
        );
        // Reopening the same cell retires its old native cursors in Cells.
        if objects
            .get(&session.scope)
            .is_some_and(|(_, resident)| !resident.matches(session))
        {
            objects.remove(&session.scope);
        }
        if !objects.contains_key(&session.scope) {
            // ponytail: 1024 resident objects per VM; application limits can
            // replace this ceiling when configurable guest limits are enforced.
            ensure!(
                objects.len() < 1024,
                "guest cursor residency capacity reached; renew VM"
            );
            let resident = Arc::new(CursorResidency::new(session, self.live.clone())?);
            objects.insert(session.scope.clone(), (runtime.clone(), resident));
        }
        let resident = objects
            .get(&session.scope)
            .expect("selected guest object")
            .1
            .clone();
        session.cursor_residency = Some(resident.clone());
        Ok(resident.id.clone())
    }

    pub(crate) fn retire(&self) {
        self.live.store(false, Ordering::Release);
    }

    pub(crate) async fn close_object(&self, scope: &str, id: &str) {
        let entry = self
            .objects
            .lock()
            .expect("guest cursor registry poisoned")
            .get(scope)
            .filter(|(_, resident)| resident.id == id)
            .cloned();
        if let Some((runtime, resident)) = entry {
            resident.retired.store(true, Ordering::Release);
            runtime.close_guest_read_cursors(&resident).await;
            let mut objects = self.objects.lock().expect("guest cursor registry poisoned");
            if objects
                .get(scope)
                .is_some_and(|(_, current)| Arc::ptr_eq(current, &resident))
            {
                objects.remove(scope);
            }
        }
    }

    pub(crate) async fn close(&self) {
        self.retire();
        // All owners await the same settlement. If a waiter is cancelled,
        // unfinalized entries stay registered for the next cleanup owner.
        let _closing = self.closing.lock().await;
        let objects: Vec<_> = self
            .objects
            .lock()
            .expect("guest cursor registry poisoned")
            .values()
            .cloned()
            .collect();
        for (runtime, resident) in objects {
            runtime.close_guest_read_cursors(&resident).await;
            self.objects
                .lock()
                .expect("guest cursor registry poisoned")
                .remove(resident.scope());
        }
    }
}

pub(crate) enum StorageAttempt {
    Reply(Reply),
    Wait {
        gate: crate::js::InputGateWait,
        request: Request,
    },
}

/// Host-only samples for the original output-gate/turn-activity path. These
/// are never fields in Reply and cannot be supplied by an untrusted guest.
pub struct OutputObservation {
    pub epoch: u64,
    pub position: Option<u64>,
    pub observed_position: Option<u64>,
    pub in_transaction: bool,
    pub alarm: celld_logic::wake::AlarmSnapshot,
    pub(crate) root_gate: Option<storage::RootGate>,
}

/// A host-created event baseline. Its private session identity prevents a
/// caller from confusing concurrent objects or a replacement VM at one epoch.
pub struct StorageEvent {
    grant: String,
    before: u64,
    root: Option<storage::RootGate>,
}

/// Non-serializable output authority, prepared under the original storage
/// lock. Guest response bytes and proof claims are never inputs to this type.
pub struct PreparedOutput {
    scope: String,
    ticket: crate::actor::GateTicket,
    residency: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    /// A facet's output also waits for every write its stream committed.
    facet: Option<(String, u64)>,
}

impl PreparedOutput {
    pub(crate) fn position(&self) -> Option<u64> {
        self.ticket.position
    }
    fn resident(&self) -> bool {
        self.residency
            .as_ref()
            .is_none_or(|residency| residency.load(std::sync::atomic::Ordering::Acquire))
    }
    /// A facet's own writes are proved on its stream before the root's gate,
    /// as any effect can reveal one, this event's or an earlier one's.
    async fn prove_facet(&self) -> Result<()> {
        match &self.facet {
            Some((stream, epoch)) => crate::js::prove_facet_stream(stream.clone(), *epoch).await,
            None => Ok(()),
        }
    }

    /// Keep the sampled epoch through routing. The original Actor owns the
    /// proof, ownership check and request pin; no guest-specific proof path.
    pub async fn gate(self, app: &crate::actor::AppHandle) -> Result<()> {
        ensure!(self.resident(), "output storage residency changed");
        self.prove_facet().await?;
        app.gate_cell_output(self.scope.clone(), self.ticket)
            .await
            .map_err(|error| anyhow::anyhow!("guest output gate refused: {error:?}"))?;
        ensure!(
            self.resident(),
            "output storage residency changed during proof"
        );
        Ok(())
    }

    /// An invocation that already holds an Actor activity must prove output
    /// under that original request. Root/facet outputs require a root request;
    /// they cannot borrow a child request's ownership authority.
    pub(crate) async fn gate_at(
        self,
        app: &crate::actor::AppHandle,
        cell: &str,
        request: u64,
    ) -> Result<()> {
        ensure!(
            self.scope == cell,
            "output requires its owning cell request"
        );
        ensure!(self.resident(), "output storage residency changed");
        self.prove_facet().await?;
        app.gate_output(request, self.ticket)
            .await
            .map_err(|error| anyhow::anyhow!("guest output gate refused: {error:?}"))?;
        ensure!(
            self.resident(),
            "output storage residency changed during proof"
        );
        Ok(())
    }
}

impl OutputObservation {
    pub fn root_output_gate(&self) -> Option<(&str, u64, u64, Option<u64>)> {
        self.root_gate
            .as_ref()
            .map(|gate| (gate.cell.as_str(), gate.epoch, gate.position, gate.observed))
    }
}

impl StorageSession {
    pub fn new(cells: &Cells, scope: String, epoch: u64) -> Result<Self> {
        ensure!(
            celld_logic::cell::valid_cell_scope(&scope),
            "invalid resident cell scope"
        );
        let _installed = cells.install();
        ensure!(
            storage::activation_epoch(&scope) == Some(epoch) && epoch > 0,
            "storage residency is not active at the supplied epoch"
        );
        let residency =
            storage::activation_residency(&scope).context("storage residency disappeared")?;
        Ok(Self {
            input_gate: crate::js::GuestInputGate::new(scope.clone(), residency.clone()),
            residency,
            scope,
            epoch,
            grant: handle()?,
            revoked: false,
            alarm: None,
            container: None,
            pending_container_run: None,
            cursors: HashMap::new(),
            sync_list: None,
            d1_reply: None,
            d1_request: None,
            cursor_residency: None,
            turn: None,
            turn_lease: None,
            critical_sections: Vec::new(),
            critical_failed: false,
            abort_reason: None,
            transactions: Vec::new(),
            next_savepoint: 0,
            accepted_sockets: Vec::new(),
            parent: None,
            facets: HashMap::new(),
            facet_cells: Vec::new(),
            facet_names: Vec::new(),
            facet_file: None,
        })
    }

    pub(crate) fn bind_container(
        &mut self,
        cell: Arc<crate::container::CellContainer>,
    ) -> Result<()> {
        self.check_residency_with(false)?;
        ensure!(
            cell.scope() == self.scope,
            "container belongs to another native object"
        );
        ensure!(
            self.container.is_none(),
            "container already attached to this event"
        );
        self.container = Some(cell);
        Ok(())
    }

    /// The native container of this event's object, while its residency holds.
    pub(crate) fn container(&self) -> Result<Arc<crate::container::CellContainer>> {
        self.check_residency_with(false)?;
        self.container
            .clone()
            .context("this Durable Object has no container")
    }

    /// Hand this event's pending start to its owned native effect. A start
    /// never taken is closed by `revoke`.
    pub(crate) fn take_container_run(
        &mut self,
        run: u64,
    ) -> Result<Arc<crate::container::CellContainer>> {
        let cell = self.container()?;
        ensure!(
            self.pending_container_run == Some(run),
            "container start has no pending run"
        );
        self.pending_container_run = None;
        Ok(cell)
    }

    pub(crate) fn alarm(&self) -> Option<celld_applications::invocation_wire::AlarmInvocation> {
        self.alarm.as_ref().map(|(_, alarm)| alarm.clone())
    }

    /// Claim only the exact firing the native scheduler selected. The original
    /// active-alarm generation preserves explicit re-arm/delete precedence.
    pub(crate) fn claim_alarm(&mut self, cells: &Cells, scheduled_ms: i64) -> Result<()> {
        let _installed = cells.install();
        self.check_residency()?;
        let now_ms = crate::ownership_store::now_ms().min(i64::MAX as u64) as i64;
        ensure!(
            now_ms >= scheduled_ms,
            "alarm dispatched before its deadline"
        );
        ensure!(
            storage::active_alarm_scheduled_time(&self.scope).is_none(),
            "alarm already claimed"
        );
        let (at_ms, retry) =
            storage::due_alarm_entry(&self.scope, now_ms).context("alarm is no longer due")?;
        ensure!(at_ms == scheduled_ms, "stale alarm dispatch");
        storage::begin_alarm_handler(&self.scope, at_ms);
        self.alarm = Some((
            now_ms,
            celld_applications::invocation_wire::AlarmInvocation {
                scheduled_ms: at_ms,
                retry,
            },
        ));
        Ok(())
    }

    pub fn grant(&self) -> &str {
        &self.grant
    }

    fn check_residency(&self) -> Result<()> {
        self.check_residency_with(false)
    }

    fn check_residency_with(&self, cleanup: bool) -> Result<()> {
        ensure!(!self.revoked, "storage session was revoked");
        ensure!(self.resident(), "storage residency changed");
        if let Some(root) = &self.parent {
            ensure!(
                storage::activation_epoch(&root.scope) == Some(root.epoch)
                    && storage::activation_residency(&root.scope)
                        .is_some_and(|current| Arc::ptr_eq(&current, &root.residency)),
                "facet root residency changed"
            );
        }
        if let Some(resident) = &self.cursor_residency {
            ensure!(
                resident.owner.load(Ordering::Acquire),
                "guest cursor owner retired"
            );
            ensure!(
                resident.connection_matches(self)
                    && (cleanup || !resident.retired.load(Ordering::Acquire)),
                "guest cursor storage residency changed"
            );
        }
        Ok(())
    }

    fn resident(&self) -> bool {
        storage::activation_epoch(&self.scope) == Some(self.epoch)
            && storage::activation_residency(&self.scope)
                .is_some_and(|current| std::sync::Arc::ptr_eq(&current, &self.residency))
    }

    pub fn begin_event(&self, cells: &Cells) -> Result<StorageEvent> {
        let _installed = cells.install();
        self.check_residency()?;
        Ok(StorageEvent {
            grant: self.grant.clone(),
            before: storage::event_start_position(&self.scope)
                .context("resident storage disappeared")?,
            root: storage::embedded_root_gate(&self.scope),
        })
    }

    /// Call for response, error, external effect or socket output before it
    /// becomes externally visible. Sample in the owning turn, then release
    /// that lock before awaiting PreparedOutput::gate.
    pub fn prepare_output(
        &self,
        cells: &Cells,
        event: &StorageEvent,
        channel: celld_logic::Channel,
    ) -> Result<PreparedOutput> {
        let _installed = cells.install();
        if let Some(reason) = &self.abort_reason {
            anyhow::bail!("__CELLD_ACTOR_ABORT__:{reason}");
        }
        self.check_residency()?;
        ensure!(
            !self.critical_failed && self.critical_sections.is_empty(),
            CRITICAL_SECTION_FAILED
        );
        self.event_output(event, channel)
    }

    // Internal post-cleanup sampler: a retired grant cannot authorize guest
    // operations, but the host must prove writes committed by cursor cleanup.
    fn event_output(
        &self,
        event: &StorageEvent,
        channel: celld_logic::Channel,
    ) -> Result<PreparedOutput> {
        ensure!(self.resident(), "storage residency changed");
        ensure!(
            event.grant == self.grant,
            "event belongs to another storage session"
        );
        let sample = storage::output_gate_sample(&self.scope, event.before, event.root.clone())?;
        ensure!(
            sample.epoch.is_some(),
            "output storage residency disappeared"
        );
        Ok(PreparedOutput {
            // A facet's output belongs to its root, and can remain valid
            // after the facet's connection is closed. If that root lives in a
            // different Cells owner, retain the original RootGate epoch and
            // let the Actor enforce its ownership/proof as it does for JS.
            residency: storage::activation_residency(&sample.cell),
            facet: sample.facet_stream.zip(sample.epoch),
            scope: sample.cell,
            ticket: crate::actor::GateTicket {
                channel,
                position: sample.position,
                observed: sample.observed,
                epoch: sample.epoch,
            },
        })
    }

    /// Retire the event under the native turn before any output or activity
    /// release. A pre-cleanup sample failure refuses the handler result even
    /// if closing its unfinished cursor subsequently commits the SQL write.
    #[cfg(test)]
    pub(crate) fn finish_event(
        &mut self,
        cells: &Cells,
        event: &StorageEvent,
    ) -> Result<(PreparedOutput, Option<anyhow::Error>)> {
        self.finish_event_with_alarm(cells, event, None)
    }

    pub(crate) fn finish_event_with_alarm(
        &mut self,
        cells: &Cells,
        event: &StorageEvent,
        outcome: Option<(bool, bool)>,
    ) -> Result<(PreparedOutput, Option<anyhow::Error>)> {
        let _installed = cells.install();
        let refused = self
            .prepare_output(cells, event, celld_logic::Channel::Response)
            .err();
        self.revoke(cells)?;
        // Roll back any unfinished guest transaction before closing the native
        // claim. Its consuming/retry commit is included in this output sample.
        if let Some((now_ms, _)) = self.alarm.take() {
            let (ok, counted) = outcome.unwrap_or((false, false));
            storage::finish_alarm_handler_with_retry_policy(
                &self.scope,
                ok && refused.is_none(),
                now_ms,
                counted && refused.is_none(),
            );
        }
        let output = self.event_output(event, celld_logic::Channel::Response)?;
        Ok((output, refused))
    }

    /// `admitted` is the trusted Actor's current authority/admission verdict,
    /// not a wire field. Epoch equality alone does not prove a live node lease.
    /// The host must still gate all externally visible results and errors.
    pub fn execute(&mut self, cells: &Cells, admitted: bool, request: Request) -> Reply {
        match self.try_execute(cells, admitted, request) {
            StorageAttempt::Reply(reply) => reply,
            StorageAttempt::Wait { .. } => Reply {
                version: wire::VERSION,
                outcome: Outcome::Error {
                    message: "cell input gate is closed; host must wait before executing".into(),
                },
            },
        }
    }

    /// A blocked native turn returns its waiter and untouched request. The
    /// host waits outside the pool, then resamples authority inside a new turn.
    pub(crate) fn try_execute(
        &mut self,
        cells: &Cells,
        admitted: bool,
        request: Request,
    ) -> StorageAttempt {
        self.try_operation(cells, admitted, request, false)
    }

    pub(crate) fn try_cleanup_turn(
        &mut self,
        cells: &Cells,
        live: bool,
        request: Request,
    ) -> StorageAttempt {
        self.try_operation(cells, live, request, true)
    }

    fn try_operation(
        &mut self,
        cells: &Cells,
        admitted: bool,
        request: Request,
        cleanup: bool,
    ) -> StorageAttempt {
        if !bool::from(request.grant.as_bytes().ct_eq(self.grant.as_bytes())) {
            return self.try_facet(cells, admitted, request, cleanup);
        }
        let _installed = cells.install();
        let checked = (|| {
            ensure!(admitted, "host refused storage admission");
            request.validate()?;
            ensure!(
                !cleanup
                    || matches!(
                        request.operation,
                        Operation::TurnStart {} | Operation::TurnFinish { .. }
                    ),
                "cleanup channel only permits execution turns"
            );
            self.check_residency_with(cleanup)?;
            // Failure revokes effects immediately. Owned turns still gate guest
            // unwinding, while only trusted joined settlement can roll back and
            // release the failed hold to its already queued native waiters.
            ensure!(
                !self.critical_failed
                    || matches!(
                        request.operation,
                        Operation::TurnStart {}
                            | Operation::TurnFinish { .. }
                            | Operation::CriticalSectionFinish { .. }
                    ),
                CRITICAL_SECTION_FAILED
            );
            ensure!(
                bool::from(request.grant.as_bytes().ct_eq(self.grant.as_bytes())),
                "unknown storage handle"
            );
            if let Operation::InExecutionTurn { turn, .. } = &request.operation {
                self.check_execution_turn(turn)?;
                return Ok(None);
            }
            ensure!(
                self.cursor_residency.is_none()
                    || self.turn.is_some()
                    || matches!(request.operation, Operation::TurnStart {}),
                "guest operation requires an active execution turn"
            );
            self.input_gate.enter(matches!(
                request.operation,
                Operation::TransactionStart { .. }
                    | Operation::TurnStart {}
                    | Operation::CriticalSectionStart { .. }
            ))
        })();
        let result = match checked {
            Ok(Some(gate)) => return StorageAttempt::Wait { gate, request },
            // Interrupted execution may already hold this event's turn. Its
            // unwind retains that exact hold; it cannot acquire another event.
            Ok(None)
                if cleanup
                    && matches!(request.operation, Operation::TurnStart {})
                    && self.turn.is_some() =>
            {
                Ok(Value::Turn {
                    turn: self.turn.clone().unwrap(),
                })
            }
            Ok(None) => match request.operation {
                Operation::InExecutionTurn { operation, .. } => self.apply(*operation),
                Operation::FacetOpen { owner, name, class } => {
                    self.facet_open(cells, owner, name, class)
                }
                Operation::FacetAbort { owner, name, class } => {
                    self.facet_abort(cells, owner, name, class)
                }
                Operation::FacetDelete { name } => self.facet_delete(cells, &name),
                operation => self.apply(operation),
            },
            Err(error) => Err(error),
        };
        if let Some(lease) = &self.turn_lease {
            // Once unwinding starts, even a later refused request on the
            // ordinary channel cannot make this cleanup hold borrowable again.
            if cleanup {
                lease.delegation_closed.store(true, Ordering::Release);
            }
            // Held blocks/savepoints admit only the single foreign origin they
            // run for; the driver's own or mixed-origin holds admit no borrower.
            let mut origins = self
                .critical_sections
                .iter()
                .map(|(_, origin)| origin)
                .chain(self.transactions.iter().map(|(.., origin)| origin));
            let delegate = origins.next().and_then(|first| {
                first
                    .clone()
                    .filter(|first| origins.all(|other| other.as_ref() == Some(first)))
            });
            let held = !self.critical_sections.is_empty() || !self.transactions.is_empty();
            lease.borrowable.store(
                !cleanup
                    && !lease.delegation_closed.load(Ordering::Acquire)
                    && !self.critical_failed
                    && (!held || delegate.is_some())
                    && self.abort_reason.is_none(),
                Ordering::Release,
            );
            *lease.delegate.lock().expect("guest turn delegate poisoned") = delegate;
        }
        if self.transactions.is_empty()
            && self.turn.is_none()
            && self.critical_sections.is_empty()
            && !self.critical_failed
        {
            self.input_gate.release();
        }
        StorageAttempt::Reply(Reply {
            version: wire::VERSION,
            outcome: match result {
                Ok(value) => Outcome::Ok { value },
                Err(error) => Outcome::Error {
                    message: error.to_string(),
                },
            },
        })
    }

    pub(crate) fn wait_input(&mut self, cells: &Cells) -> Result<Option<crate::js::InputGateWait>> {
        let _installed = cells.install();
        // VM retirement revokes effects, but cannot revoke the trusted host's
        // obligation to roll back this event under its original input hold.
        ensure!(!self.revoked, "storage session was revoked");
        ensure!(self.resident(), "storage residency changed");
        self.input_gate.enter(false)
    }

    /// An early reply must also wait for this event's own execution/transaction
    /// hold. Cleanup's wait_input deliberately bypasses that hold for rollback.
    pub(crate) fn wait_output(&self, cells: &Cells) -> Result<Option<crate::js::InputGateWait>> {
        let _installed = cells.install();
        self.check_residency()?;
        Ok(crate::js::cell_gate_wait(&self.scope))
    }

    /// Call before host output publication. An unfinished RETURNING cursor
    /// fails the original write-position sample rather than inventing proof.
    pub fn sample_output(&self, cells: &Cells) -> Result<OutputObservation> {
        let _installed = cells.install();
        self.check_residency()?;
        let position = storage::write_position(&self.scope)?;
        let sample = storage::sync_sample(&self.scope)?.context("resident storage disappeared")?;
        Ok(OutputObservation {
            epoch: sample.epoch,
            position,
            observed_position: storage::observed_position(&self.scope, position),
            in_transaction: sample.in_transaction,
            alarm: storage::alarm_snapshot(&self.scope)?,
            root_gate: storage::embedded_root_gate(&self.scope),
        })
    }

    /// Close capabilities before cleanup, including when rollback fails. The
    /// caller must quarantine/discard on failed cleanup and prove any resulting
    /// committed position before retaining the cell for a successor.
    pub fn revoke(&mut self, cells: &Cells) -> Result<()> {
        self.revoked = true;
        crate::js::ws_release_unclaimed(&self.scope, &std::mem::take(&mut self.accepted_sockets));
        self.retire_facets(cells);
        if let Some(run) = self.pending_container_run.take() {
            if let Some(cell) = &self.container {
                cell.cancel_pending_run(run);
            }
        }
        self.container = None;
        self.d1_reply = None;
        self.d1_request = None;
        self.close_execution_turn();
        self.critical_failed |= !self.critical_sections.is_empty();
        self.critical_sections.clear();
        let _installed = cells.install();
        if self.critical_failed {
            if let Some(resident) = &self.cursor_residency {
                resident.retired.store(true, Ordering::Release);
                resident.close(cells);
            }
        }
        if !self.resident() {
            self.cursors.clear();
            self.sync_list = None;
            self.transactions.clear();
            self.release_settled_gate();
            return Ok(());
        }
        for (_, native) in self.cursors.drain() {
            storage::sql_cursor_close(native);
        }
        if let Some((_, native)) = self.sync_list.take() {
            storage::sync_list_close(native);
        }
        while let Some((_, savepoint, _)) = self.transactions.last() {
            storage::transaction_control(
                &self.scope,
                "rollback",
                self.transactions.len() > 1,
                savepoint,
            )
            .map_err(anyhow::Error::msg)?;
            self.transactions.pop();
        }
        self.release_settled_gate();
        Ok(())
    }

    fn release_settled_gate(&mut self) {
        self.input_gate
            .release_with(if let Some(reason) = &self.abort_reason {
                Err(format!("__CELLD_ACTOR_ABORT__:{reason}"))
            } else if self.critical_failed {
                Err(CRITICAL_SECTION_FAILED.into())
            } else {
                Ok(())
            });
    }

    fn d1_next(&mut self) -> Result<Value> {
        let (reply, offset) = self.d1_reply.as_mut().context("no pending D1 reply")?;
        let mut end =
            (*offset + celld_applications::invocation_wire::MAX_CHUNK_BYTES).min(reply.len());
        while !reply.is_char_boundary(end) {
            end -= 1;
        }
        let chunk = reply[*offset..end].to_owned();
        *offset = end;
        let complete = end == reply.len();
        if complete {
            self.d1_reply = None;
        }
        Ok(Value::D1 {
            reply: chunk,
            complete,
        })
    }

    fn apply(&mut self, operation: Operation) -> Result<Value> {
        use Operation::*;
        match operation {
            InExecutionTurn { .. } => anyhow::bail!("execution turn was not validated"),
            FacetOpen { .. } | FacetClose { .. } | FacetAbort { .. } | FacetDelete { .. } => {
                anyhow::bail!("facet operation was not routed")
            }
            AbortObject { reason } => {
                let resident = self
                    .cursor_residency
                    .as_ref()
                    .context("object abort requires a guest object residency")?;
                ensure!(
                    self.turn.is_some(),
                    "object abort requires its execution turn"
                );
                self.abort_reason = Some(reason);
                self.critical_failed = true;
                resident.retired.store(true, Ordering::Release);
                // Keep the original hold and transaction until the supervisor
                // reaps the process. Abort never discards persistent storage.
                Ok(Value::Unit {})
            }
            Get { keys } => Ok(Value::Entries {
                entries: storage::get_many_stored(&self.scope, &keys)?
                    .into_iter()
                    .map(|(key, value)| (key, stored(value)))
                    .collect(),
            }),
            PutJson { entries } => {
                let entries: Vec<_> = entries
                    .into_iter()
                    .map(|(key, value)| (key, value.to_string()))
                    .collect();
                storage::put_many_json(&self.scope, &entries)?;
                Ok(Value::Unit {})
            }
            PutClone { entries } => {
                let entries: Vec<_> = entries
                    .into_iter()
                    .map(|entry| (entry.key, entry.value))
                    .collect();
                storage::put_many_serialized(&self.scope, &entries)?;
                Ok(Value::Unit {})
            }
            Delete { keys } => Ok(Value::Count {
                count: storage::delete_many(&self.scope, &keys)? as u64,
            }),
            DeleteAll { delete_alarm } => {
                storage::delete_all_with_alarm(&self.scope, delete_alarm)?;
                Ok(Value::Unit {})
            }
            List { options } => Ok(Value::Entries {
                entries: storage::list_stored_with_options(
                    &self.scope,
                    options.begin.as_deref(),
                    options.end.as_deref(),
                    options.start_after.as_deref(),
                    options.prefix.as_deref(),
                    options.limit,
                    options.reverse,
                )?
                .into_iter()
                .map(|(key, value)| (key, stored(value)))
                .collect(),
            }),
            SyncListStart { options } => {
                let opaque = handle()?;
                self.sync_list = None;
                let native = storage::sync_list_start(
                    &self.scope,
                    options.begin.as_deref(),
                    options.end.as_deref(),
                    options.start_after.as_deref(),
                    options.prefix.as_deref(),
                    options.limit,
                    options.reverse,
                )?;
                self.sync_list = Some((opaque.clone(), native));
                Ok(Value::Cursor {
                    cursor: Some(opaque),
                    columns: Vec::new(),
                    row: None,
                    rows_written: 0,
                    reused_cached_query: false,
                })
            }
            SyncListNext { cursor } => {
                let native = self
                    .sync_list
                    .as_ref()
                    .filter(|(opaque, _)| *opaque == cursor)
                    .map(|(_, native)| *native)
                    .context("sync KV cursor was invalidated")?;
                let entry = storage::sync_list_next(native);
                if !matches!(entry, Ok(Some(_))) {
                    self.sync_list = None;
                }
                Ok(Value::Entries {
                    entries: entry?
                        .into_iter()
                        .map(|(key, value)| (key, stored(value)))
                        .collect(),
                })
            }
            SqlStart { query, bindings } => {
                // ponytail: bound native statements per residency; expose this
                // through application limits when limits management is wired.
                ensure!(
                    self.cursors.len()
                        + self.cursor_residency.as_ref().map_or(0, |resident| resident
                            .cursors
                            .lock()
                            .expect("guest cursors poisoned")
                            .len())
                        < MAX_CURSORS,
                    "resident SQL cursor limit reached"
                );
                let opaque = handle()?;
                let bindings: Vec<_> = bindings.into_iter().map(sql_binding).collect();
                let (native, columns, row, rows_written, reused_cached_query) =
                    storage::sql_cursor_start_values(&self.scope, &query, &bindings)
                        .map_err(anyhow::Error::msg)?;
                let cursor = if native == 0 {
                    None
                } else {
                    match &self.cursor_residency {
                        Some(resident) if storage::sql_cursor_readonly(native) == Some(true) => {
                            resident
                                .cursors
                                .lock()
                                .expect("guest cursors poisoned")
                                .insert(opaque.clone(), native);
                        }
                        _ => {
                            self.cursors.insert(opaque.clone(), native);
                        }
                    }
                    Some(opaque)
                };
                Ok(Value::Cursor {
                    cursor,
                    columns,
                    row: row.map(sql_row),
                    rows_written,
                    reused_cached_query,
                })
            }
            SqlNext { cursor } => {
                let native = self
                    .cursors
                    .get(&cursor)
                    .copied()
                    .or_else(|| {
                        self.cursor_residency.as_ref().and_then(|resident| {
                            resident
                                .cursors
                                .lock()
                                .expect("guest cursors poisoned")
                                .get(&cursor)
                                .copied()
                        })
                    })
                    .context("SQL cursor is no longer active")?;
                match storage::sql_cursor_next(native) {
                    Ok((row, rows_written)) => {
                        if row.is_none() {
                            self.remove_cursor(&cursor);
                        }
                        Ok(Value::Row {
                            row: row.map(sql_row),
                            rows_written,
                        })
                    }
                    Err(error) => {
                        self.remove_cursor(&cursor);
                        Err(anyhow::Error::msg(error))
                    }
                }
            }
            SqlClose { cursor } => {
                if let Some(native) = self.remove_cursor(&cursor) {
                    storage::sql_cursor_close(native);
                }
                Ok(Value::Unit {})
            }
            SqlIngest { input } => {
                let (remainder, rows_written, statement_count) =
                    storage::sql_ingest(&self.scope, &input).map_err(anyhow::Error::msg)?;
                Ok(Value::Ingest {
                    remainder,
                    rows_written,
                    statement_count,
                })
            }
            SqlMetadata {} => {
                let (last_insert_rowid, changes) =
                    storage::sql_write_meta(&self.scope).map_err(anyhow::Error::msg)?;
                Ok(Value::Metadata {
                    last_insert_rowid,
                    changes,
                })
            }
            SqlSize {} => Ok(Value::Size {
                bytes: storage::sql_database_size(&self.scope).map_err(anyhow::Error::msg)?,
            }),
            D1Run { request } => {
                ensure!(
                    self.scope
                        .split_once(':')
                        .is_some_and(|(class, _)| class == crate::deploy::D1_CLASS),
                    "D1 requires its internal database object"
                );
                ensure!(self.d1_request.is_none(), "D1 request upload is incomplete");
                self.d1_reply = None;
                self.d1_reply = Some((storage::d1_run_json(&self.scope, &request), 0));
                self.d1_next()
            }
            D1Next {} => self.d1_next(),
            D1RequestStart { bytes } => {
                ensure!(
                    self.scope
                        .split_once(':')
                        .is_some_and(|(class, _)| class == crate::deploy::D1_CLASS),
                    "D1 requires its internal database object"
                );
                ensure!(
                    self.d1_request.is_none(),
                    "D1 request upload already started"
                );
                self.d1_reply = None;
                self.d1_request = Some((String::new(), bytes as usize));
                Ok(Value::Unit {})
            }
            D1RequestChunk { data } => {
                let (request, expected) = self
                    .d1_request
                    .as_mut()
                    .context("D1 request upload was not started")?;
                if request.len().saturating_add(data.len()) > *expected {
                    // An oversize chunk abandons the upload instead of wedging D1.
                    self.d1_request = None;
                    anyhow::bail!("D1 request exceeds declared size");
                }
                request.push_str(&data);
                Ok(Value::Unit {})
            }
            D1RequestEnd {} => {
                let (request, expected) = self
                    .d1_request
                    .take()
                    .context("D1 request upload was not started")?;
                ensure!(
                    request.len() == expected,
                    "D1 request ended before declared size"
                );
                self.d1_reply = Some((storage::d1_run_json(&self.scope, &request), 0));
                self.d1_next()
            }
            TurnStart {} => {
                ensure!(self.turn.is_none(), "execution turn is already active");
                let turn = handle()?;
                let lease = Arc::new(ExecutionTurn {
                    event: self.input_gate.event(),
                    delegation_closed: AtomicBool::new(false),
                    borrowable: AtomicBool::new(false),
                    delegate: Mutex::new(None),
                });
                if let Some(resident) = &self.cursor_residency {
                    let mut turns = resident.turns.lock().expect("guest turns poisoned");
                    turns.retain(|_, lease| lease.strong_count() > 0);
                    turns.insert(turn.clone(), Arc::downgrade(&lease));
                }
                self.turn_lease = Some(lease);
                self.turn = Some(turn.clone());
                Ok(Value::Turn { turn })
            }
            TurnFinish { turn } => {
                ensure!(
                    self.turn.as_deref() == Some(turn.as_str()),
                    "execution turn is no longer active"
                );
                self.close_execution_turn();
                // A facet event's reply waits for its stream's proof, which
                // the caller makes after this turn (`facet_stream`).
                if self.parent.is_some() {
                    self.facet_critical_error()?;
                }
                Ok(Value::Unit {})
            }
            CriticalSectionStart { origin } => {
                ensure!(
                    self.critical_sections.len() < MAX_CRITICAL_SECTIONS,
                    "critical section depth limit reached"
                );
                // A foreign origin names a different event of this residency,
                // never this session's own grant.
                ensure!(
                    origin.as_ref().is_none_or(|origin| {
                        !bool::from(origin.as_bytes().ct_eq(self.grant.as_bytes()))
                    }),
                    "critical section origin names its own driver"
                );
                let critical_section = handle()?;
                self.critical_sections
                    .push((critical_section.clone(), origin));
                Ok(Value::CriticalSection { critical_section })
            }
            CriticalSectionFinish {
                critical_section,
                failed,
            } => {
                ensure!(
                    self.critical_sections
                        .last()
                        .is_some_and(|(held, _)| held == &critical_section),
                    "critical section must finish in nesting order"
                );
                self.critical_sections.pop();
                self.critical_failed |= failed;
                Ok(Value::Unit {})
            }
            TransactionStart { origin } => {
                ensure!(
                    self.transactions.len() < MAX_TRANSACTIONS,
                    "resident transaction depth limit reached"
                );
                ensure!(
                    origin.as_ref().is_none_or(|origin| {
                        !bool::from(origin.as_bytes().ct_eq(self.grant.as_bytes()))
                    }),
                    "transaction origin names its own driver"
                );
                // A savepoint nests only inside the same event's savepoints.
                ensure!(
                    self.transactions
                        .last()
                        .is_none_or(|(.., parent)| parent == &origin),
                    "transaction nesting mixes events"
                );
                let token = handle()?;
                self.next_savepoint = self
                    .next_savepoint
                    .checked_add(1)
                    .context("savepoint counter exhausted")?;
                let savepoint = format!("cells_tx_{}", self.next_savepoint);
                storage::transaction_control(
                    &self.scope,
                    "start",
                    !self.transactions.is_empty(),
                    &savepoint,
                )
                .map_err(anyhow::Error::msg)?;
                self.transactions.push((token.clone(), savepoint, origin));
                Ok(Value::Transaction { transaction: token })
            }
            TransactionFinish {
                transaction,
                action,
            } => {
                let (token, savepoint, _) = self
                    .transactions
                    .last()
                    .context("transaction is no longer active")?;
                ensure!(
                    token == &transaction,
                    "transaction must finish in nesting order"
                );
                let action = match action {
                    TransactionAction::Commit => "commit",
                    TransactionAction::Rollback => "rollback",
                    TransactionAction::RollbackExplicit => "rollback_explicit",
                };
                storage::transaction_control(
                    &self.scope,
                    action,
                    self.transactions.len() > 1,
                    savepoint,
                )
                .map_err(anyhow::Error::msg)?;
                self.transactions.pop();
                Ok(Value::Unit {})
            }
            GetAlarm {} => Ok(Value::Alarm {
                at_ms: storage::get_alarm(&self.scope),
            }),
            SetAlarm { at_ms } => {
                storage::set_alarm(&self.scope, at_ms)?;
                Ok(Value::Unit {})
            }
            DeleteAlarm {} => {
                storage::delete_alarm(&self.scope)?;
                Ok(Value::Unit {})
            }
            ContainerState {} => {
                let cell = self
                    .container
                    .as_ref()
                    .context("this Durable Object has no container")?;
                Ok(Value::ContainerState {
                    running: cell.running(),
                    run: cell.current_run(),
                })
            }
            ContainerBeginRun {} => {
                let cell = self
                    .container
                    .as_ref()
                    .context("this Durable Object has no container")?;
                ensure!(
                    self.pending_container_run.is_none() && !cell.running(),
                    "start() cannot be called on a container that is already running."
                );
                let run = cell.begin_run();
                self.pending_container_run = Some(run);
                Ok(Value::ContainerState { running: true, run })
            }
            SocketAccept { ws, accepted, tags } => {
                // ponytail: one event may hold 1024 unclaimed sockets; a
                // per-application limit replaces it when limits are configurable.
                ensure!(
                    accepted || self.accepted_sockets.len() < 1024,
                    "too many unclaimed WebSockets in one event"
                );
                let id = crate::js::ws_accept_guest(&self.scope, accepted.then_some(ws), tags)?;
                if !accepted {
                    self.accepted_sockets.push(id);
                }
                Ok(Value::Socket { ws: id })
            }
            SocketList { tag } => Ok(Value::Sockets {
                sockets: crate::js::ws_guest_sockets(&self.scope, tag.as_deref())
                    .into_iter()
                    .map(|(ws, tags, attachment)| wire::SocketRow {
                        ws,
                        tags,
                        attachment,
                    })
                    .collect(),
            }),
            SocketAttachment { ws, attachment } => {
                crate::js::ws_guest_attachment(&self.scope, ws, attachment)?;
                Ok(Value::Unit {})
            }
            SocketAutoResponse { pair } => {
                crate::js::ws_auto_response_install(&self.scope, pair);
                Ok(Value::Unit {})
            }
            SocketAutoResponseGet {} => Ok(Value::AutoResponse {
                pair: crate::js::ws_auto_response_pair(&self.scope),
            }),
            SocketAutoResponseTimestamp { ws } => Ok(Value::AutoResponseTimestamp {
                millis: crate::js::ws_guest_auto_response_timestamp(&self.scope, ws)?,
            }),
            GetActorName {} => Ok(Value::ActorName {
                name: storage::get_actor_name(&self.scope)?,
            }),
        }
    }

    pub(crate) fn has_facet(&self, grant: &str) -> bool {
        self.facets
            .iter()
            .any(|(held, child)| held == grant || child.has_facet(grant))
    }

    /// Revoke every facet event session and release this session's connection
    /// leases. The facets' files and replication streams belong to the root.
    fn retire_facets(&mut self, cells: &Cells) {
        for (_, mut child) in std::mem::take(&mut self.facets) {
            let _ = child.revoke(cells);
        }
        let _installed = cells.install();
        for lease in self.facet_cells.drain(..) {
            if facet_lifetime::release(&lease) {
                storage::close(&lease.scope);
            }
        }
    }

    fn root_link(&self) -> ParentLink {
        self.parent.clone().unwrap_or_else(|| ParentLink {
            scope: self.scope.clone(),
            epoch: self.epoch,
            residency: self.residency.clone(),
        })
    }

    /// An operation under a facet grant: this live session routes it, so the
    /// facet is usable only while its root is. Ending one event's session is
    /// done here, by the session that holds it.
    fn try_facet(
        &mut self,
        cells: &Cells,
        admitted: bool,
        request: Request,
        cleanup: bool,
    ) -> StorageAttempt {
        let _installed = cells.install();
        let routed = (|| {
            ensure!(admitted, "host refused storage admission");
            request.validate()?;
            self.check_residency_with(cleanup)?;
            ensure!(
                !self.critical_failed && self.abort_reason.is_none(),
                CRITICAL_SECTION_FAILED
            );
            if let Operation::FacetClose { facet } = &request.operation {
                if *facet == request.grant && self.facets.contains_key(facet) {
                    let mut child = self.facets.remove(facet).expect("held facet event");
                    let checked = child.facet_critical_error();
                    child.revoke(cells)?;
                    checked?;
                    return Ok(None);
                }
            }
            self.facets
                .iter()
                .find(|(held, child)| **held == request.grant || child.has_facet(&request.grant))
                .map(|(held, _)| Some(held.clone()))
                .context("unknown storage handle")
        })();
        let key = match routed {
            Ok(Some(key)) => key,
            Ok(None) => {
                return StorageAttempt::Reply(Reply {
                    version: wire::VERSION,
                    outcome: Outcome::Ok {
                        value: Value::Unit {},
                    },
                })
            }
            Err(error) => {
                return StorageAttempt::Reply(Reply {
                    version: wire::VERSION,
                    outcome: Outcome::Error {
                        message: error.to_string(),
                    },
                })
            }
        };
        let child = self.facets.get_mut(&key).expect("routed facet event");
        child.try_operation(cells, admitted, request, cleanup)
    }

    /// A facet whose SQL failed fatally cannot answer for its event.
    fn facet_critical_error(&self) -> Result<()> {
        match storage::sql_critical_error(&self.scope) {
            Some(error) => anyhow::bail!(error),
            None => Ok(()),
        }
    }

    /// The session of this tree that holds `grant`.
    fn session_of(&mut self, grant: &str) -> Option<&mut StorageSession> {
        if bool::from(self.grant.as_bytes().ct_eq(grant.as_bytes())) {
            return Some(self);
        }
        self.facets
            .values_mut()
            .find_map(|child| child.session_of(grant))
    }

    /// The replication stream and epoch of the facet whose event holds
    /// `grant`. `None` for a root object, whose stream the Actor proves.
    pub(crate) fn facet_stream(&mut self, grant: &str) -> Option<(String, u64)> {
        let session = self.session_of(grant)?;
        let root = session.parent.as_ref()?;
        Some((
            crate::engine_api::facet_cell(&root.scope, &session.facet_names),
            session.epoch,
        ))
    }

    /// What a `FacetOpen` would open: the facet's scope, and its names from the
    /// root down.
    fn facet_plan(&self, owner: &str, name: &str, class: &str) -> Result<(String, Vec<String>)> {
        let root_class = self.scope.split(':').next().unwrap_or_default();
        ensure!(
            self.parent.is_some() || !crate::deploy::is_reserved_class(root_class),
            "facets are not available to this object"
        );
        ensure!(
            !class.contains(':')
                && celld_logic::cell::valid_cell_scope(class)
                && !crate::deploy::is_reserved_class(class),
            "invalid facet class"
        );
        ensure!(
            self.facets.len() < MAX_FACET_EVENTS,
            "facet event limit reached"
        );
        let facet = crate::js::facet_scope(class, &self.scope, owner, name);
        ensure!(
            celld_logic::cell::valid_cell_scope(&facet),
            "invalid facet scope"
        );
        let mut names = self.facet_names.clone();
        names.push(name.to_string());
        Ok((facet, names))
    }

    /// Before a `FacetOpen` takes its native turn: activate the facet's
    /// replication stream, which only the host loop can do. The root and epoch
    /// are this session's own, so the guest names no stream. A stream is
    /// activated once per root activation, and a repeat answers the same file.
    pub(crate) async fn open_facet_stream(&mut self, request: &Request) -> Result<()> {
        let Operation::FacetOpen { owner, name, class } = &request.operation else {
            return Ok(());
        };
        let Some(session) = self.session_of(&request.grant) else {
            // The native turn refuses an unknown handle.
            return Ok(());
        };
        let (facet, names) = session.facet_plan(owner, name, class)?;
        let root = session.root_link();
        // Without a host loop there is no replicator: the file sits beside the
        // root's database (`facet_open`).
        let file = crate::js::request_facet_file(&root.scope, root.epoch, names)
            .await
            .map_err(anyhow::Error::msg)?;
        session.facet_file = file.map(|file| (facet, file));
        Ok(())
    }

    /// A request the host loop refused is an error reply, so the guest sees
    /// why and its storage stream stays up.
    pub(crate) fn error_reply(error: &anyhow::Error) -> Reply {
        Reply {
            version: wire::VERSION,
            outcome: Outcome::Error {
                message: error.to_string(),
            },
        }
    }

    /// The requests whose native turn is followed by a host-loop step
    /// (`finish_facet_request`). Small enough to keep a copy of.
    pub(crate) fn facet_followup(request: &Request) -> Option<Request> {
        matches!(
            request.operation,
            Operation::FacetDelete { .. } | Operation::TurnFinish { .. }
        )
        .then(|| request.clone())
    }

    /// After the native turn of `request` replied: what only the host loop can
    /// finish. A deleted facet's streams go, locally and in the bucket. A
    /// facet event's turn reply waits for every write the facet's stream
    /// committed, as workerd's facet output gate holds a facet's reply: the
    /// root can reveal that write once it has the reply.
    ///
    /// Returns the reply to send. A host-loop step that fails (a stream that
    /// cannot be proved, a refused delete) is an error reply in place of the
    /// native one: the guest sees why, never an acknowledgement of a write that
    /// is not durable, and its storage stream stays up.
    pub(crate) async fn finish_facet_request(
        &mut self,
        followup: Option<&Request>,
        reply: Reply,
    ) -> Reply {
        let Some(request) = followup else {
            return reply;
        };
        match self.finish_facet_step(request, &reply).await {
            Ok(()) => reply,
            Err(error) => Self::error_reply(&error),
        }
    }

    async fn finish_facet_step(&mut self, request: &Request, reply: &Reply) -> Result<()> {
        if !matches!(reply.outcome, Outcome::Ok { .. }) {
            return Ok(());
        }
        match &request.operation {
            Operation::FacetDelete { name } => {
                let Some(session) = self.session_of(&request.grant) else {
                    return Ok(());
                };
                let root = session.root_link();
                let mut names = session.facet_names.clone();
                names.push(name.clone());
                crate::js::request_facet_delete(&root.scope, root.epoch, names)
                    .await
                    .map_err(anyhow::Error::msg)
            }
            Operation::TurnFinish { .. } => match self.facet_stream(&request.grant) {
                Some((stream, epoch)) => crate::js::prove_facet_stream(stream, epoch).await,
                None => Ok(()),
            },
            _ => Ok(()),
        }
    }

    fn facet_open(
        &mut self,
        cells: &Cells,
        owner: String,
        name: String,
        class: String,
    ) -> Result<Value> {
        let (facet, names) = self.facet_plan(&owner, &name, &class)?;
        let parent =
            storage::storage_identity(&self.scope)?.context("storage residency disappeared")?;
        ensure!(parent.epoch == self.epoch, "storage residency changed");
        let (path, restored) = match self.facet_file.take() {
            Some((opened, file)) if opened == facet => file,
            _ if !crate::js::facet_streams_replicated() => {
                crate::js::facet_file_beside_root(&parent, &names)
            }
            _ => anyhow::bail!("the facet's stream was not opened"),
        };
        if storage::activation_epoch(&facet).is_some_and(|epoch| epoch != parent.epoch) {
            facet_lifetime::invalidate(&self.root_link().residency, &facet);
            storage::close(&facet);
        }
        if storage::activation_epoch(&facet).is_none() {
            // ponytail: no sqlite-vec for facets until compatibility reaches them.
            storage::open_embedded(&facet, &parent, &name, &path, restored, false)?;
        } else {
            storage::refresh_embedded_root(&facet, &parent);
        }
        let mut child = StorageSession::new(cells, facet.clone(), parent.epoch)?;
        child.parent = Some(self.root_link());
        child.facet_names = names;
        let lease = facet_lifetime::retain(&self.root_link().residency, &facet);
        self.facet_cells.push(lease);
        let grant = child.grant.clone();
        self.facets.insert(grant.clone(), child);
        Ok(Value::Facet {
            scope: facet,
            grant,
            depth: parent.facet_path.len() as u32 + 1,
        })
    }

    /// `facets.abort`: the facet and every facet below it stop, as in workerd.
    fn facet_abort(
        &mut self,
        cells: &Cells,
        owner: String,
        name: String,
        class: String,
    ) -> Result<Value> {
        let facet = crate::js::facet_scope(&class, &self.scope, &owner, &name);
        let mut path = self.facet_names.clone();
        path.push(name);
        let mut doomed = storage::embedded_facets_below(&self.root_link().scope, &path);
        doomed.push(facet);
        self.close_facets(cells, &doomed);
        Ok(Value::Unit {})
    }

    /// `facets.delete`: the facet and every facet below it go with their
    /// streams, so close what is open before their files are removed.
    fn facet_delete(&mut self, cells: &Cells, name: &str) -> Result<Value> {
        let parent =
            storage::storage_identity(&self.scope)?.context("storage residency disappeared")?;
        let mut path = parent.facet_path.clone();
        path.push(name.to_string());
        let doomed = storage::embedded_facets_from(&parent.root_scope, &path);
        self.close_facets(cells, &doomed);
        storage::delete_legacy_facet(&parent, name)?;
        if !crate::js::facet_streams_replicated() {
            crate::js::remove_facet_file(&crate::js::facet_file_beside_root(&parent, &path).0);
        }
        Ok(Value::Unit {})
    }

    /// Revoke the events of the facets `doomed` and close their cells.
    fn close_facets(&mut self, cells: &Cells, doomed: &[String]) {
        self.revoke_facet_events(cells, doomed);
        let root = self.root_link();
        for scope in doomed {
            facet_lifetime::invalidate(&root.residency, scope);
            storage::close(scope);
        }
        self.facet_cells
            .retain(|lease| !doomed.contains(&lease.scope));
    }

    fn revoke_facet_events(&mut self, cells: &Cells, doomed: &[String]) {
        let ended: Vec<String> = self
            .facets
            .iter()
            .filter(|(_, child)| doomed.contains(&child.scope))
            .map(|(grant, _)| grant.clone())
            .collect();
        for grant in ended {
            if let Some(mut child) = self.facets.remove(&grant) {
                let _ = child.revoke(cells);
            }
        }
        for child in self.facets.values_mut() {
            child.revoke_facet_events(cells, doomed);
        }
    }

    fn remove_cursor(&mut self, cursor: &str) -> Option<u64> {
        self.cursors.remove(cursor).or_else(|| {
            self.cursor_residency.as_ref().and_then(|resident| {
                resident
                    .cursors
                    .lock()
                    .expect("guest cursors poisoned")
                    .remove(cursor)
            })
        })
    }

    fn check_execution_turn(&self, turn: &str) -> Result<()> {
        ensure!(
            self.turn.is_none()
                && self.transactions.is_empty()
                && self.critical_sections.is_empty()
                && !self.critical_failed
                && self.abort_reason.is_none(),
            "origin cannot borrow an execution turn while holding one"
        );
        let resident = self
            .cursor_residency
            .as_ref()
            .context("execution turn requires VM residency")?;
        let lease = resident
            .turns
            .lock()
            .expect("guest turns poisoned")
            .get(turn)
            .and_then(std::sync::Weak::upgrade)
            .context("execution turn is not active in this VM residency")?;
        ensure!(
            lease.borrowable.load(Ordering::Acquire),
            "execution turn holds a transaction or critical section"
        );
        if let Some(delegate) = &*lease.delegate.lock().expect("guest turn delegate poisoned") {
            ensure!(
                bool::from(delegate.as_bytes().ct_eq(self.grant.as_bytes())),
                "execution turn holds a transaction or critical section"
            );
        }
        ensure!(
            crate::js::GuestInputGate::is_current_holder(&self.scope, &self.residency, lease.event),
            "execution turn no longer holds native admission"
        );
        Ok(())
    }

    fn close_execution_turn(&mut self) {
        if let (Some(turn), Some(resident)) = (self.turn.take(), &self.cursor_residency) {
            resident
                .turns
                .lock()
                .expect("guest turns poisoned")
                .remove(&turn);
        }
        self.turn_lease = None;
    }
}

fn stored(value: storage::StoredValue) -> wire::StoredValue {
    match value {
        storage::StoredValue::LegacyJson(value) => wire::StoredValue::Json { value },
        storage::StoredValue::V8(value) => wire::StoredValue::V8 { value },
    }
}

fn sql_binding(value: wire::SqlValue) -> rusqlite::types::Value {
    match value {
        wire::SqlValue::Null {} => rusqlite::types::Value::Null,
        wire::SqlValue::Integer { value } => rusqlite::types::Value::Integer(value),
        wire::SqlValue::Real { bits } => rusqlite::types::Value::Real(f64::from_bits(bits)),
        wire::SqlValue::Text { value } => rusqlite::types::Value::Text(value),
        wire::SqlValue::Blob { value } => rusqlite::types::Value::Blob(value),
    }
}

fn sql_row(row: Vec<storage::SqlValue>) -> Vec<wire::SqlValue> {
    row.into_iter()
        .map(|value| match value {
            storage::SqlValue::Null => wire::SqlValue::Null {},
            storage::SqlValue::Integer(value) => wire::SqlValue::Integer { value },
            storage::SqlValue::Real(value) => wire::SqlValue::Real {
                bits: value.to_bits(),
            },
            storage::SqlValue::Text(value) => wire::SqlValue::Text { value },
            storage::SqlValue::Blob(value) => wire::SqlValue::Blob { value },
        })
        .collect()
}

// Tests drive real processes, sockets, files and wall-clock deadlines on their
// own runtime, outside the Actor execution domain.
#[allow(clippy::disallowed_methods)]
#[cfg(all(test, unix))]
#[path = "unit_tests/application_storage/tests.rs"]
pub(crate) mod tests;
