//! Firecracker-guest-only items of the JS engine that have no upstream
//! counterpart. They live here, not in `js.rs`, so upstream merges stay small;
//! `js` re-exports them, so every path is unchanged.

use super::*;

/// Only this original rejection branch consumes a guest alarm's retry limit.
#[derive(Debug)]
pub(crate) struct GuestAlarmRejection;
impl std::fmt::Display for GuestAlarmRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("alarm promise rejected")
    }
}
impl std::error::Error for GuestAlarmRejection {}

/// Host-owned transaction hold for one guest event. Reuses the process-wide
/// gate and waiter queue that original JavaScript deliveries already consult.
/// The guest never chooses the event id, cell scope or ownership context.
pub(crate) struct GuestInputGate {
    context: Arc<IoContext>,
    cell: String,
    residency: Arc<std::sync::atomic::AtomicBool>,
    event: celld_logic::gate::EventId,
    held: bool,
}

pub(crate) type InputGateWait = tokio::sync::oneshot::Receiver<Result<(), String>>;

impl GuestInputGate {
    pub(crate) fn event(&self) -> celld_logic::gate::EventId {
        self.event
    }

    pub(crate) fn is_current_holder(cell: &str, residency: &Arc<AtomicBool>, event: u64) -> bool {
        cell_gates().lock().unwrap().get(cell).is_some_and(|gate| {
            gate.gate.is_held_by(event)
                && gate
                    .guest_residency
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, residency))
        })
    }

    pub(crate) fn new(cell: String, residency: Arc<std::sync::atomic::AtomicBool>) -> Self {
        Self {
            context: IoContext::new(),
            cell,
            residency,
            event: NEXT_GATE_EVENT.fetch_add(1, Ordering::Relaxed),
            held: false,
        }
    }

    /// Called inside the original native turn, atomically with the operation
    /// it admits. Nested storage transactions retain the same outer hold.
    pub(crate) fn enter(&mut self, hold: bool) -> Result<Option<InputGateWait>> {
        if self.held {
            return Ok(None);
        }
        if !hold {
            return Ok(cell_gate_wait(&self.cell));
        }
        match acquire_cell_gate(
            &self.context,
            &self.cell,
            self.event,
            None,
            &mut None,
            Some(self.residency.clone()),
        ) {
            CellGateAcquisition::Acquired => {
                self.held = true;
                Ok(None)
            }
            CellGateAcquisition::Waiting { gate, .. } => Ok(Some(gate)),
            CellGateAcquisition::Retired => anyhow::bail!(RETIRED_INPUT_GATE),
        }
    }

    pub(crate) fn release(&mut self) {
        self.release_with(Ok(()));
    }

    pub(crate) fn release_with(&mut self, outcome: Result<(), String>) {
        if self.held && self.context.release_input_gate(&self.cell, self.event) {
            self.held = false;
            release_cell_gate(&self.cell, self.event, outcome);
        }
    }
}

impl Drop for GuestInputGate {
    fn drop(&mut self) {
        // A normal finish releases only after native rollback/commit. An
        // abandoned holder refuses its existing waiters as original JS does;
        // the owning driver must quarantine any failed native settlement.
        let _ = abandon_context_input_gates(&self.context);
    }
}

/// Called by the native connection destructor, including same-epoch reopen
/// and whole-Cells retirement. Only the matching guest incarnation is removed.
pub(crate) fn retire_guest_input_gate(cell: &str, residency: &Arc<std::sync::atomic::AtomicBool>) {
    let retired = {
        let mut gates = cell_gates().lock().unwrap();
        if gates.get(cell).is_some_and(|gate| {
            gate.guest_residency
                .as_ref()
                .is_some_and(|token| Arc::ptr_eq(token, residency))
        }) {
            gates.remove(cell)
        } else {
            None
        }
    };
    if let Some(mut retired) = retired {
        drop(retired.clear_holder());
        wake_gate_waiters(
            cell,
            Err("guest input gate storage residency changed".into()),
        );
    }
}

#[cfg(test)]
pub(crate) fn live_input_gate_waiters(cell: &str) -> usize {
    gate_waiters()
        .lock()
        .unwrap()
        .get(cell)
        .map_or(0, |waiters| {
            waiters.iter().filter(|waiter| !waiter.is_closed()).count()
        })
}

#[cfg(any(target_os = "linux", test))]
fn guest_available_from_meminfo(meminfo: &str) -> Option<u64> {
    if meminfo.len() > 16 * 1024 {
        return None;
    }
    // One owner for this line: machine::meminfo_available_bytes. It refuses a
    // duplicate `MemAvailable`, which this reader used to silently accept.
    crate::machine::meminfo_available_bytes(meminfo)
}

#[cfg(target_os = "linux")]
#[allow(clippy::disallowed_methods)] // Bounded kernel telemetry, never application data.
fn guest_available_memory() -> Option<u64> {
    let file = std::fs::File::open("/proc/meminfo").ok()?;
    let mut reader = std::io::Read::take(file, 16 * 1024 + 1);
    let mut meminfo = String::new();
    std::io::Read::read_to_string(&mut reader, &mut meminfo).ok()?;
    guest_available_from_meminfo(&meminfo)
}

#[cfg(not(target_os = "linux"))]
fn guest_available_memory() -> Option<u64> {
    None
}

#[cfg(test)]
#[test]
fn guest_available_memory_is_bounded_and_checks_units() {
    assert_eq!(
        guest_available_from_meminfo("MemTotal: 9 kB\nMemAvailable: 7 kB\n"),
        Some(7168)
    );
    for invalid in [
        "MemFree: 7 kB",
        "MemAvailable: 7 B",
        "MemAvailable: 7 kB extra",
        "MemAvailable: 18446744073709551615 kB",
    ] {
        assert_eq!(guest_available_from_meminfo(invalid), None);
    }
    assert_eq!(
        guest_available_from_meminfo(&format!("MemAvailable: 7 kB\n{}", " ".repeat(16 * 1024))),
        None
    );
    // The shared parser refuses a duplicate line instead of taking the first.
    assert_eq!(
        guest_available_from_meminfo("MemAvailable: 7 kB\nMemAvailable: 9 kB\n"),
        None
    );
}

/// `__queue_consumer()`: the consumer settings the host catalog holds for the
/// running event's own Queue cell, as JSON, or `undefined` for a queue that is
/// not an application's or has no consumer. The cell is the event's, never an
/// argument, so no cell can read another's settings.
pub(super) fn op_queue_consumer(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    #[cfg(target_os = "linux")]
    if crate::application_vm::hosted().is_some() {
        let context = event_context(scope);
        let Some(cell) = current_cell_event(&context).map(|frame| frame.storage) else {
            return loader_throw(scope, "a Queue consumer lookup needs a cell event");
        };
        if let Some(consumer) = context
            .application_scope
            .get()
            .filter(|metadata| metadata.scope == cell)
            .and_then(|metadata| metadata.queue.clone())
            .and_then(|entry| entry.consumer_wire())
        {
            rv.set(
                v8::String::new(scope, &consumer.to_string())
                    .unwrap()
                    .into(),
            );
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (scope, &mut rv);
}

/// `__cron_schedule()`: the expressions the host catalog holds for the running
/// event's own `.cron` cell, as a JSON array (empty once its trigger is gone), or
/// `undefined` for a cell that is not an application's. The cell is the event's,
/// never an argument.
pub(super) fn op_cron_schedule(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    #[cfg(target_os = "linux")]
    if crate::application_vm::hosted().is_some() {
        let context = event_context(scope);
        let Some(cell) = current_cell_event(&context).map(|frame| frame.storage) else {
            return loader_throw(scope, "a cron schedule lookup needs a cell event");
        };
        if let Some(entry) = context
            .application_scope
            .get()
            .filter(|metadata| metadata.scope == cell)
            .and_then(|metadata| metadata.cron.as_ref())
        {
            let crons: Vec<_> = entry
                .schedule
                .iter()
                .map(|s| s.expression.as_str())
                .collect();
            rv.set(
                v8::String::new(scope, &serde_json::json!(crons).to_string())
                    .unwrap()
                    .into(),
            );
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (scope, &mut rv);
}

/// `__cron_dispatch(cron, scheduledTime)`: run one scheduled event of the
/// running event's own application `.cron` cell in its trigger's pinned VM, and
/// resolve with the guest's JSON result. The target is the host catalog's.
pub(super) fn op_cron_dispatch(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    #[cfg(target_os = "linux")]
    {
        let cron = args.get(0).to_rust_string_lossy(scope);
        let scheduled_ms = args.get(1).number_value(scope).unwrap_or(-1.0) as i64;
        let cell = current_cell_event(&event_context(scope)).map(|frame| frame.storage);
        let id = asyncrt::enqueue(async move {
            let cell = cell.ok_or("a cron dispatch needs a cell event")?;
            let hosted = crate::application_vm::hosted().ok_or("no application VM host")?;
            hosted
                .dispatch_scheduled(&cell, &cron, scheduled_ms)
                .await
                .map(asyncrt::OpOut::Str)
                .map_err(|error| format!("{error:#}"))
        });
        rv.set(promise_for(scope, id));
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (scope, args, &mut rv);
}

/// Keep the originating guest event alive until a loaded descendant drains.
/// The op is attributed synchronously; its sender moves into the actual driver.
pub(super) fn loaded_worker_drain(
    scope: &mut v8::PinScope,
) -> Option<tokio::sync::oneshot::Sender<()>> {
    // Stateless drivers need no CPED token; module defaults have no driver.
    if !guest_activity::enabled()
        || (operation_continuation_id(scope).is_none() && !event_context(scope).has_event_driver())
    {
        return None;
    }
    let (drained, receive) = tokio::sync::oneshot::channel();
    asyncrt::enqueue_io_context(async move {
        let _ = receive.await;
        Ok::<String, String>(String::new())
    });
    Some(drained)
}

/// Ask the host loop to activate the stream of the facet at `names` below
/// `root`. `None` when no host loop runs a replicator.
pub(crate) async fn request_facet_file(
    root: &str,
    epoch: u64,
    names: Vec<String>,
) -> Result<Option<(std::path::PathBuf, bool)>, String> {
    let Some(sender) = FACET_TX.get() else {
        return Ok(None);
    };
    let (reply, receive) = tokio::sync::oneshot::channel();
    let request = FacetReq::Open {
        root: root.to_string(),
        epoch,
        names,
        reply,
    };
    sender
        .send(request)
        .map_err(|_| "the facet channel closed".to_string())?;
    receive
        .await
        .map_err(|_| "the facet open was dropped".to_string())?
        .map(Some)
        .map_err(|error| format!("open the facet database: {error:#}"))
}

/// The file of a facet when no host loop runs the replicator: beside the
/// root's database and named after it, as workerd names a facet
/// `<id>.<facet>.sqlite`.
pub(crate) fn facet_file_beside_root(
    parent: &storage::StorageIdentity,
    names: &[String],
) -> (std::path::PathBuf, bool) {
    let stream = crate::engine_api::facet_cell(&parent.root_scope, names);
    let facet = stream[parent.root_scope.len()..]
        .trim_start_matches('/')
        .replace('/', "-");
    let root = std::path::Path::new(&parent.root_path);
    let file = root.file_name().unwrap().to_string_lossy();
    (root.with_file_name(format!("{file}.{facet}.sqlite")), false)
}

/// Whether a host loop replicates the streams of facets.
pub(crate) fn facet_streams_replicated() -> bool {
    FACET_TX.get().is_some()
}

/// Ask the host loop to delete the stream of the facet at `names` below
/// `root` and every stream below it. Without a host loop the files beside the
/// root are the caller's to remove (`remove_facet_file`).
pub(crate) async fn request_facet_delete(
    root: &str,
    epoch: u64,
    names: Vec<String>,
) -> Result<(), String> {
    let Some(sender) = FACET_TX.get() else {
        return Ok(());
    };
    let (reply, receive) = tokio::sync::oneshot::channel();
    let request = FacetReq::Delete {
        root: root.to_string(),
        epoch,
        names,
        reply,
    };
    sender
        .send(request)
        .map_err(|_| "the facet channel closed".to_string())?;
    receive
        .await
        .map_err(|_| "the facet delete was dropped".to_string())?
        .map_err(|error| format!("delete facet storage: {error:#}"))
}

/// Remove a facet database kept beside its root's (see `facet_file_beside_root`).
pub(crate) fn remove_facet_file(path: &std::path::Path) {
    for suffix in ["", "-wal", "-shm"] {
        let mut file = path.to_path_buf().into_os_string();
        file.push(suffix);
        let _ = asyncrt::fs().remove_file(std::path::Path::new(&file));
    }
}

pub(super) fn op_rpc_signal_guest(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    #[cfg(unix)]
    let guest = guest_storage::enabled(scope);
    #[cfg(not(unix))]
    let guest = false;
    rv.set(v8::Boolean::new(scope, guest).into());
}

/// Signals a service caller declared for callee events, mirrored under the
/// caller's ids with a count of the events that hold each.
// ponytail: the first host-assigned owner wins a mirror, so a service already
// trusted with a signal could race its true source to another callee and abort
// it there; owner-qualified signal ids in the marker close this if it matters.
static REMOTE_SIGNALS: OnceLock<std::sync::Mutex<HashMap<RequestId, (String, usize)>>> =
    OnceLock::new();

/// Mirror caller-declared signals as `Live` entries and return the ids this
/// event now holds. An id that is already a local signal, or a mirror of
/// another owner, is not held: the first source stays authoritative, so a
/// relayed abort cannot touch it, nor can a receiver that merely saw the id.
pub(crate) fn rpc_signal_declare_remote(owner: &str, ids: &[RequestId]) -> Vec<RequestId> {
    let mut remote = REMOTE_SIGNALS.get_or_init(Default::default).lock().unwrap();
    let mut signals = rpc_signals().lock().unwrap();
    let mut held = Vec::new();
    for id in ids {
        if let Some((holder, count)) = remote.get_mut(id) {
            if holder != owner {
                continue;
            }
            *count += 1;
        } else if signals.contains_key(id) {
            continue;
        } else {
            remote.insert(*id, (owner.to_owned(), 1));
            signals.insert(*id, RpcSignalState::Live(HashMap::new()));
        }
        held.push(*id);
    }
    held
}

/// Drop one event's hold on its mirrors. A mirror nobody holds is released
/// exactly like a collected source: waiters resolve and never abort.
pub(crate) fn rpc_signal_release_remote(ids: &[RequestId]) {
    let mut remote = REMOTE_SIGNALS.get_or_init(Default::default).lock().unwrap();
    for id in ids {
        match remote.get_mut(id) {
            Some((_, count)) if *count > 1 => *count -= 1,
            Some(_) => {
                remote.remove(id);
                rpc_signal_release(*id);
            }
            None => {}
        }
    }
}

/// A signal subscription: `(subscription, immediate, receiver)`.
pub(super) type SignalSubscription = (
    RequestId,
    Option<Option<Vec<u8>>>,
    tokio::sync::oneshot::Receiver<Option<Vec<u8>>>,
);

/// Subscribe to a signal: `(subscription, immediate, receiver)`, where
/// `immediate` is the settled outcome (`Some(None)` = released).
pub(super) fn rpc_signal_subscribe(signal: RequestId) -> SignalSubscription {
    let subscription = next_request_id();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let immediate = {
        let mut signals = rpc_signals().lock().unwrap();
        match signals.get_mut(&signal) {
            Some(RpcSignalState::Live(subscribers)) => {
                subscribers.insert(subscription, tx);
                None
            }
            Some(RpcSignalState::Aborted(reason)) => Some(Some(reason.clone())),
            None => Some(None),
        }
    };
    (subscription, immediate, rx)
}

/// The abort reason of `signal`, or `None` once it is released.
pub(super) async fn rpc_signal_settle(
    signal: RequestId,
    subscription: RequestId,
    immediate: Option<Option<Vec<u8>>>,
    rx: tokio::sync::oneshot::Receiver<Option<Vec<u8>>>,
) -> Option<Vec<u8>> {
    let _guard = RpcSignalSubscriptionGuard {
        signal,
        subscription,
    };
    match immediate {
        Some(reason) => reason,
        None => rx.await.unwrap_or(None),
    }
}

/// Resolves with the reason when `signal` aborts, `None` when it is released.
pub(crate) async fn rpc_signal_watch(signal: RequestId) -> Option<Vec<u8>> {
    let (subscription, immediate, rx) = rpc_signal_subscribe(signal);
    rpc_signal_settle(signal, subscription, immediate, rx).await
}

/// The trusted application pool takes a queue cell's name from its host, while
/// every other Worker still re-derives the scope from its own namespace key.
#[cfg(all(test, target_os = "linux"))]
pub(crate) fn check_host_verified_actor_names() {
    Engine::init();
    let directory = tempfile::tempdir().unwrap();
    for trusted in [true, false] {
        let digit = if trusted { "7" } else { "8" };
        let scope = format!(
            "{}:{}",
            celld_logic::queue::RESERVED_CLASS,
            digit.repeat(64)
        );
        let mut config = WorkerConfig::new(WorkerConfigOptions {
            src: "export default { fetch() { return new Response('ok'); } };".into(),
            script_name: "celld-application-kv".into(),
            do_classes: vec![],
            bindings: vec![],
            r2_bindings: vec![],
            d1_bindings: vec![],
            kv_bindings: vec![],
            queue_bindings: vec![],
            queue_consumers: vec![],
            workflow_bindings: vec![],
            vars: vec![],
            node: "check".into(),
            modules: vec![],
            compat: Default::default(),
        });
        // A script name alone, even this pool's, proves nothing.
        if trusted {
            config = config.with_host_verified_names();
        }
        let mut worker = Worker::load_config(Arc::new(config)).unwrap();
        let path = directory.path().join(format!("{trusted}.sqlite"));
        worker
            .own_cell(
                &scope,
                Some(CellStorage {
                    path: path.to_str().unwrap(),
                    epoch: 1,
                    replicated_wake: false,
                    vfs: None,
                }),
            )
            .unwrap();
        let named = worker.set_id_name(&scope, "jobs");
        if trusted {
            named.unwrap();
            // The name is persisted, so an alarm-driven activation recovers it.
            let persisted = {
                let _cells = worker.lock();
                storage::get_actor_name(&scope).unwrap()
            };
            assert_eq!(persisted.as_deref(), Some("jobs"));
            assert!(worker.set_id_name(&scope, "other").is_err());
        } else {
            let error = named.unwrap_err().to_string();
            assert!(error.contains("missing namespace key"), "{error}");
            let _cells = worker.lock();
            assert_eq!(storage::get_actor_name(&scope).unwrap(), None);
        }
    }
}

pub(super) fn op_guest_cleanup_track(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Some(context) =
        reaction_or_current_context(scope).filter(|context| context.has_guest_storage())
    else {
        return;
    };
    rv.set_bool(true);
    if let Ok(promise) = args.get(0).try_cast::<v8::Promise>() {
        context
            .events
            .lock()
            .unwrap()
            .guest_callbacks
            .push(v8::Global::new(scope, promise));
    }
}

#[cfg(test)]
mod rpc_signal_mirror_tests {
    use super::*;

    #[test]
    fn mirrors_are_counted_never_alias_local_signals_and_release_unaborted() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (mirror, local) = (next_request_id(), next_request_id());
        rpc_signals()
            .lock()
            .unwrap()
            .insert(local, RpcSignalState::Live(HashMap::new()));
        // A declared id naming a local signal is not mirrored, so the relay
        // cannot abort it; the mirrored id is held once per declaration.
        assert_eq!(
            rpc_signal_declare_remote("a", &[mirror, local, mirror]),
            vec![mirror, mirror]
        );
        // Another owner that merely learned the id holds nothing, so its abort
        // frame is ignored and its release cannot drop the real owner's hold.
        assert!(rpc_signal_declare_remote("b", &[mirror]).is_empty());
        let waiting = runtime.spawn(rpc_signal_watch(mirror));
        let aborted = runtime.block_on(async {
            tokio::task::yield_now().await;
            rpc_signal_abort(mirror, vec![7]);
            waiting.await.unwrap()
        });
        assert_eq!(aborted, Some(vec![7]), "abort reaches the mirrored waiter");
        // An abort that precedes the subscription is still delivered.
        assert_eq!(runtime.block_on(rpc_signal_watch(mirror)), Some(vec![7]));
        rpc_signal_release_remote(&[mirror]);
        assert!(
            rpc_signals().lock().unwrap().contains_key(&mirror),
            "other event still holds it"
        );
        rpc_signal_release_remote(&[mirror]);
        assert!(!rpc_signals().lock().unwrap().contains_key(&mirror));
        // A mirror released before any abort resolves waiters "released".
        let second = next_request_id();
        rpc_signal_declare_remote("a", &[second]);
        let waiting = runtime.spawn(rpc_signal_watch(second));
        let released = runtime.block_on(async {
            tokio::task::yield_now().await;
            rpc_signal_release_remote(&[second]);
            waiting.await.unwrap()
        });
        assert_eq!(
            released, None,
            "an unaborted mirror never fires after release"
        );
        assert!(matches!(
            rpc_signals().lock().unwrap().get(&local),
            Some(RpcSignalState::Live(_))
        ));
        rpc_signal_release(local);
    }
}

#[cfg(test)]
mod loader_drain_context_tests {
    use super::*;

    #[test]
    fn stateless_driver_ownership_outlives_handler_frame() {
        let context = IoContext::new();
        assert!(context.continuation_id().is_none());
        assert!(!context.has_event_driver());
        context.mark_event_driver();
        context.begin_event();
        assert!(context.end_event().is_some());
        context.seal_wait_until();
        assert!(!context.accepts_wait_until());
        assert!(context.has_event_driver());
        assert!(!IoContext::new().has_event_driver());
        assert!(context.close_handed_ops().is_empty());
        assert!(!context.has_event_driver());
    }
}

impl WorkerConfig {
    pub(crate) fn with_keyed_namespaces(
        mut self,
        namespaces: Vec<(String, String, String)>,
    ) -> Self {
        self.keyed_namespaces = namespaces;
        self
    }

    pub(crate) fn with_container_aliases(mut self, aliases: Vec<(String, String)>) -> Self {
        self.container_aliases = aliases;
        self
    }

    /// The trusted application pool: its host derived each cell's scope from the
    /// bound resource and names the cell itself, and the pool holds no namespace
    /// keys to derive that scope again. A script name cannot prove this, so only
    /// this construction path sets it.
    pub(crate) fn with_host_verified_names(mut self) -> Self {
        self.host_verified_names = true;
        self
    }
}

impl Worker {
    /// Guest diagnostics run between turns. Child permits are tried, never
    /// awaited, and overlapping heap, allocator, and RSS counters stay separate.
    pub(crate) fn memory_sample(&self) -> celld_applications::invocation_wire::GuestMemorySample {
        use celld_applications::invocation_wire::{
            AllocatorMemory, GuestMemorySample, HeapMemory, LibcMemory,
        };
        let heap = |bytes: HeapBytes| HeapMemory {
            used_bytes: bytes.used,
            physical_bytes: bytes.physical,
            external_bytes: bytes.external,
        };
        let root = self.heap_bytes();
        let mut sample = GuestMemorySample {
            rss_bytes: crate::memory::resident_bytes(),
            available_bytes: guest_available_memory(),
            root: root.map(heap).unwrap_or_default(),
            total: root.map(heap).unwrap_or_default(),
            isolates: u32::from(root.is_some()),
            busy_isolates: 0,
            loading_isolates: 0,
            complete: root.is_some(),
            jemalloc: crate::memory::allocator_stats().map(|stats| AllocatorMemory {
                allocated_bytes: stats.allocated_bytes,
                active_bytes: stats.active_bytes,
                resident_bytes: stats.resident_bytes,
                mapped_bytes: stats.mapped_bytes,
                retained_bytes: stats.retained_bytes,
            }),
            libc_malloc: crate::memory::libc_malloc_stats().map(|stats| LibcMemory {
                in_use_bytes: stats.in_use_bytes,
                free_bytes: stats.free_bytes,
                mmap_bytes: stats.mmap_bytes,
                arena_bytes: stats.arena_bytes,
            }),
        };
        let mut slots = Vec::new();
        match loader_registry().try_lock() {
            Ok(registry) => {
                for entry in registry.values() {
                    match &*entry.state.borrow() {
                        LoaderState::Ready(slot) => slots.push(slot.clone()),
                        LoaderState::Loading => {
                            sample.loading_isolates = sample.loading_isolates.saturating_add(1);
                            sample.complete = false;
                        }
                        LoaderState::Failed(_) => {}
                    }
                }
            }
            Err(_) => sample.complete = false,
        }
        if let Some(facets) = ROOT_FACETS.get() {
            match facets.try_lock() {
                Ok(facets) => {
                    for entry in facets.values() {
                        sample.complete &= entry.pending == 0;
                        slots.extend(entry.open.iter().map(|facet| facet.slot.clone()));
                    }
                }
                Err(_) => sample.complete = false,
            }
        }
        // Loaded children and recorded facets never include the root's own
        // isolate. Several stubs/facets can keep the same child slot alive.
        let mut seen = std::collections::HashSet::new();
        for slot in slots {
            if !seen.insert(Arc::as_ptr(&slot)) {
                continue;
            }
            match slot.try_heap_bytes() {
                Ok(Some(bytes)) => {
                    sample.isolates = sample.isolates.saturating_add(1);
                    sample.total.used_bytes = sample.total.used_bytes.saturating_add(bytes.used);
                    sample.total.physical_bytes =
                        sample.total.physical_bytes.saturating_add(bytes.physical);
                    sample.total.external_bytes =
                        sample.total.external_bytes.saturating_add(bytes.external);
                }
                Ok(None) => {}
                Err(()) => {
                    sample.isolates = sample.isolates.saturating_add(1);
                    sample.busy_isolates = sample.busy_isolates.saturating_add(1);
                    sample.complete = false;
                }
            }
        }
        sample
    }

    /// Native guest operations enter the same lock and storage context as a
    /// JS turn. The synchronous callback cannot retain that lock across I/O.
    pub(crate) fn storage_turn<T>(
        &mut self,
        cell: &str,
        epoch: u64,
        f: impl FnOnce(&storage::Cells) -> T,
    ) -> Result<(T, Vec<(String, celld_logic::wake::AlarmSnapshot)>)> {
        let inner = self.inner.as_ref().context("cell worker was retired")?;
        let (_locker, _installed) = inner.lock();
        anyhow::ensure!(
            storage::activation_epoch(cell) == Some(epoch),
            "guest storage activation changed"
        );
        let result = f(&inner.cells);
        Ok((result, storage::take_alarm_moves()))
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn turn_begin_application_cell(
        &mut self,
        job: CellJob,
        trace: Option<crate::telemetry::TraceContext>,
        metadata: Option<Arc<crate::application_vm::ScopeMetadata>>,
    ) -> (Option<InFlight>, Vec<Op>) {
        if crate::application_vm::hosted().is_some()
            && self
                .inner
                .as_ref()
                .is_some_and(|inner| inner.runtime_state.application_storage != metadata.is_some())
        {
            job.fail(anyhow!(
                "cell event application classification does not match its isolate"
            ));
            return (None, Vec::new());
        }
        let context = metadata.and_then(|metadata| {
            self.inner.as_ref().map(|inner| {
                let context = IoContext::tracked(&inner.runtime_state);
                let _ = context.application_scope.set(metadata);
                context
            })
        });
        self.turn_begin_cell_with_context(job, trace, context)
    }
}

impl IoContext {
    pub(super) fn mark_event_driver(&self) {
        self.event_driver.store(true, Ordering::Release);
    }

    pub(super) fn has_event_driver(&self) -> bool {
        self.event_driver.load(Ordering::Acquire)
    }

    pub(super) fn has_guest_storage(&self) -> bool {
        #[cfg(unix)]
        {
            self.guest_storage.get().is_some()
        }
        #[cfg(not(unix))]
        {
            false
        }
    }
}
