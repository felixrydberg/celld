//! One application's owned guest transport. Reply readiness and joined drain
//! are distinct; only the original owned event driver executes storage/proof.

use crate::application_guest::{read_frame, read_frame_limited, write_frame, GuestStorage};
use crate::application_vm::lifecycle::{EventHold, Gate, Phase};
use crate::asyncrt::{timeout, timeout_at};
use crate::js::HttpResponse;
use anyhow::{ensure, Context, Result};
use celld_applications::invocation_wire::{
    GuestControl, GuestMemorySample, HostControl, HttpRequest, InvokeHttp, ObjectCall,
    ObjectTarget, ResponseFlow, SocketEvent, SocketFrame, SocketInvocation, StorageConnect,
    MAX_CHUNK_BYTES, SOCKET_WINDOW,
};
use celld_applications::storage_wire::{Operation, Outcome, Request, Value, VERSION};
use celld_applications::{ApplicationId, Language, ResourceAddress, ResourceKind};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use subtle::ConstantTimeEq;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const MAX_ACTIVE: usize = 32;
// ponytail: a queue consumer may hold at most half of a VM's event slots, so
// batches cannot starve HTTP and object events. Several queues on one handler
// can still add up to the cap each; a per-handler admission would replace this.
pub(crate) const QUEUE_MAX_CONCURRENCY: u16 = (MAX_ACTIVE / 2) as u16;
const MAX_EVENTS: usize = 65536;
const IO_DEADLINE: Duration = Duration::from_secs(5);
const CANCEL_GRACE: Duration = Duration::from_secs(1);
// ponytail: fixed per-event brokered fetch bound; application limits can
// replace it once egress limits are configurable.
const MAX_FETCHES: usize = 16;

struct Pending {
    execution: Option<EventHold>,
    retirement: bool,
    grant: String,
    storage: Option<oneshot::Sender<UnixStream>>,
    response: Option<oneshot::Sender<HttpResponse>>,
    response_seen: bool,
    terminal: Option<oneshot::Sender<GuestControl>>,
    terminal_seen: bool,
    connected: bool,
    cleanup_connected: bool,
    access: Option<GuestStorage>,
    cancelled: bool,
    deadline: Option<Instant>,
    settled: CancellationToken,
    stream: Option<PendingStream>,
    /// Guest credits for a streamed request body, forwarded to its driver.
    request: Option<(ResponseFlow, mpsc::Sender<u64>)>,
    /// Brokered fetches; dropping one stops its host task.
    fetches: HashMap<String, BrokeredFetch>,
    /// The node's Actor, for this event's guest-requested object calls.
    app: Option<crate::actor::AppHandle>,
    /// Frames this object event sent on its sockets, bound for the forwarder
    /// that proves and releases them. Bounded by the credit window.
    sockets: Option<mpsc::Sender<(SocketTarget, SocketFrame)>>,
    /// Outbound WebSockets this event dialed, by their fetch. Dropping one
    /// closes its connection, so no dial outlives its event or its fetch.
    dials: HashMap<String, DialedSocket>,
    /// Upgrades of callee objects' sockets this event was handed in a fetch
    /// head. Its reply can return each one once; what it keeps is released.
    issued_sockets: HashMap<u64, GuestSocket>,
}

/// Where one of a guest's socket frames goes.
enum SocketTarget {
    /// A hibernatable socket of the event's own object, by registry id.
    Registry(u64),
    /// A socket the event dialed, by its fetch.
    Dial(String),
}

/// A connected outbound socket the host pump owns on the guest's behalf. It
/// lives and dies with its event, as a Worker's socket does with its request.
struct DialedSocket {
    id: u64,
}

impl Drop for DialedSocket {
    fn drop(&mut self) {
        crate::js::ws_close(self.id, 1001, "request ended");
        crate::js::ws_unregister(self.id);
    }
}

/// How long a dialed WebSocket's handshake may take; the connector itself
/// gives up at ten seconds.
const DIAL_DEADLINE: Duration = Duration::from_secs(15);

/// A dialed socket's inbound frames as the chunks one credit each carries:
/// the frame's `WsPull` bytes, split to the chunk bound and flagged so the
/// guest can reassemble them. The queue ahead of this is the original bounded
/// one, so a guest that stops pulling stops the connection's reads. A close ends
/// the stream; the greeting `Open` is already in the response head.
pub(crate) fn dial_chunks(
    inbound: crate::js::WsPullReceiver,
) -> impl futures_util::Stream<Item = std::result::Result<Vec<u8>, String>> + Send {
    use celld_applications::invocation_wire::{DIAL_LAST, DIAL_MORE};
    futures_util::stream::unfold(
        (inbound, std::collections::VecDeque::<Vec<u8>>::new(), false),
        |(mut inbound, mut pieces, mut closed)| async move {
            loop {
                if let Some(piece) = pieces.pop_front() {
                    return Some((Ok(piece), (inbound, pieces, closed)));
                }
                if closed {
                    return None;
                }
                let frame = inbound.recv().await.unwrap_or_else(|| {
                    // The connection went without a close frame.
                    crate::js::WsPull::Close(1006, String::new(), false)
                });
                if matches!(frame, crate::js::WsPull::Open(_)) {
                    continue;
                }
                closed = matches!(frame, crate::js::WsPull::Close(..));
                let bytes = frame.encode();
                let mut rest = bytes.as_slice();
                while !rest.is_empty() {
                    let (data, tail) = rest.split_at(rest.len().min(MAX_CHUNK_BYTES - 1));
                    let mut piece = Vec::with_capacity(data.len() + 1);
                    piece.push(if tail.is_empty() {
                        DIAL_LAST
                    } else {
                        DIAL_MORE
                    });
                    piece.extend_from_slice(data);
                    pieces.push_back(piece);
                    rest = tail;
                }
            }
        },
    )
}

/// A hibernatable socket an application object accepted and a response now
/// carries, claimed in the host registry. It belongs to the one upgrade that
/// consumes it (`into_target`); dropped before that, the socket is released
/// as a connection that never opened, so no registry entry outlives its reply.
pub struct GuestSocket {
    app: crate::actor::AppHandle,
    target: crate::js::WsTarget,
    armed: bool,
}

/// Parked tunnel upgrades are issued to a guest above this base, so a remote
/// owner's upgrade never shares an id with a local registry socket.
const REMOTE_UPGRADE_BASE: u64 = 1 << 52;

/// How the node drops a peer's parked 101 nobody took; installed by the node.
static RELEASE_PARKED: std::sync::OnceLock<fn(u64)> = std::sync::OnceLock::new();

pub(crate) fn install_parked_release(release: fn(u64)) {
    let _ = RELEASE_PARKED.set(release);
}

impl GuestSocket {
    /// A peer owner's upgrade, parked on this node until the client's own
    /// socket upgrades and the two are spliced. Nothing of it is in this
    /// node's registry; dropped unclaimed, only the parked stream is released.
    pub(crate) fn remote(app: crate::actor::AppHandle, target: crate::js::WsTarget) -> Self {
        Self {
            app,
            target,
            armed: true,
        }
    }

    /// The id the stateless caller is handed and returns.
    pub(crate) fn key(&self) -> u64 {
        self.target
            .tunnel
            .map_or(self.target.id, |parked| REMOTE_UPGRADE_BASE + parked)
    }

    pub(crate) fn new(app: crate::actor::AppHandle, id: u64, scope: String) -> Self {
        Self {
            app,
            target: crate::js::WsTarget {
                id,
                scope,
                tunnel: None,
            },
            armed: true,
        }
    }

    /// The upgrade took the socket; the connection owns its cleanup now.
    pub fn into_target(mut self) -> crate::js::WsTarget {
        self.armed = false;
        self.target.clone()
    }

    pub(crate) async fn register(&self) -> Result<()> {
        self.app
            .websocket_opened(
                self.target.scope.clone(),
                self.target.id,
                celld_logic::WebSocketKind::Hibernatable,
            )
            .await
    }
}

impl Drop for GuestSocket {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(parked) = self.target.tunnel {
            if let Some(release) = RELEASE_PARKED.get() {
                release(parked);
            }
            return;
        }
        let (app, target) = (self.app.clone(), self.target.clone());
        crate::asyncrt::spawn(abandon_socket(app, target)).detach();
    }
}

/// The original rejection of an accepted socket that no client ever got: the
/// object learns of an unclean close, then the core and registry forget it.
async fn abandon_socket(app: crate::actor::AppHandle, target: crate::js::WsTarget) {
    #[cfg(target_os = "linux")]
    if let Some(applications) = crate::application_vm::hosted() {
        let _ = applications
            .dispatch_socket(
                &app,
                &target.scope,
                target.id,
                SocketEvent::Close {
                    code: 1006,
                    reason: String::new(),
                    was_clean: false,
                },
                Vec::new(),
            )
            .await;
    }
    app.websocket_closed(target.scope.clone(), target.id);
    crate::js::ws_unregister(target.id);
}

/// Take the upgrade a guest's reply names. An object event names a socket its
/// own object accepted; a stateless event only one the host issued it.
fn claim_upgrade(pending: &mut Pending, ws: u64) -> Result<crate::js::HttpResponseWebSocket> {
    let app = pending.app.clone().context("socket upgrade has no Actor")?;
    let socket = match &pending.access {
        Some(access) => {
            let object = access.object()?;
            let scope = format!("{}:{}", object.class, object.id);
            ensure!(
                crate::js::ws_scope_of(ws).as_deref() == Some(scope.as_str()),
                "upgrade names a socket outside its object"
            );
            crate::js::ws_claim_guest(ws)?;
            GuestSocket::new(app, ws, scope)
        }
        None => pending
            .issued_sockets
            .remove(&ws)
            .context("upgrade names a socket this event was not given")?,
    };
    Ok(crate::js::HttpResponseWebSocket::Guest(Box::new(socket)))
}

struct BrokeredFetch {
    flow: ResponseFlow,
    pulls: mpsc::Sender<u64>,
    _stop: tokio_util::sync::DropGuard,
    request: Option<RequestUpload>,
    /// A service RPC's declared signals, and the relay of their aborts to the
    /// callee event. Only a declared id can be relayed.
    signals: HashSet<String>,
    relay: mpsc::UnboundedSender<(String, Vec<u8>)>,
}

/// Aborts of a caller's declared signals, bound for one callee event.
pub(crate) type SignalRelay = mpsc::UnboundedReceiver<(String, Vec<u8>)>;

async fn next_relay(relay: &mut Option<SignalRelay>) -> Option<(String, Vec<u8>)> {
    match relay {
        Some(relay) => relay.recv().await,
        None => std::future::pending().await,
    }
}

struct RequestUpload {
    flow: ResponseFlow,
    remaining: Option<u64>,
    items: mpsc::Sender<std::result::Result<Option<Vec<u8>>, String>>,
}

impl RequestUpload {
    fn push(&mut self, sequence: u64, data: Option<Vec<u8>>) -> Result<()> {
        if let Some(data) = &data {
            self.flow.chunk(sequence)?;
            if let Some(remaining) = self.remaining.as_mut() {
                ensure!(
                    data.len() as u64 <= *remaining,
                    "object request exceeds declared size"
                );
                *remaining -= data.len() as u64;
            }
        } else {
            self.flow.end(sequence)?;
            ensure!(
                self.remaining.is_none_or(|remaining| remaining == 0),
                "object upload ended before declared size"
            );
        }
        self.items
            .try_send(Ok(data))
            .context("object upload exceeded credit")
    }
}

type BodyRequest = oneshot::Sender<std::result::Result<Option<Vec<u8>>, String>>;

struct PendingStream {
    flow: ResponseFlow,
    head_seen: bool,
    demand: mpsc::Sender<BodyRequest>,
    chunks: mpsc::Sender<GuestControl>,
    dropped: CancellationToken,
}

struct ResponseBody {
    demand: mpsc::Sender<BodyRequest>,
    dropped: CancellationToken,
    // Admission follows an escaped stream even after its head is dropped.
    // A stateless fetch has no storage output to prove or Actor request.
    _output: Option<Arc<crate::application_guest::GuestOutput>>,
    execution: Option<EventHold>,
    ended: bool,
}

impl ResponseBody {
    fn finish(&mut self) {
        self.ended = true;
        self.execution.take();
    }
}

impl Drop for ResponseBody {
    fn drop(&mut self) {
        if !self.ended {
            self.dropped.cancel();
        }
    }
}

fn response(
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    stream: Option<crate::js::HttpChunkStream>,
) -> HttpResponse {
    HttpResponse {
        status,
        headers,
        body,
        stream,
        websocket: None,
        write_position: None,
        observed_position: None,
    }
}

fn buffered(response_body: celld_applications::invocation_wire::HttpResponse) -> HttpResponse {
    response(
        response_body.status,
        response_body.headers,
        response_body.body,
        None,
    )
}

fn body_stream(body: ResponseBody) -> crate::js::HttpChunkStream {
    Box::pin(futures_util::stream::unfold(body, |mut body| async move {
        if body.ended {
            return None;
        }
        let (reply, receive) = oneshot::channel();
        let result = if body.demand.send(reply).await.is_ok() {
            receive
                .await
                .unwrap_or_else(|_| Err("guest response transport retired".into()))
        } else {
            Err("guest response transport retired".into())
        };
        match result {
            Ok(Some(bytes)) => Some((Ok(bytes), body)),
            Ok(None) => {
                body.finish();
                None
            }
            Err(error) => {
                body.dropped.cancel();
                body.finish();
                Some((Err(error), body))
            }
        }
    }))
}

struct Registry {
    running: bool,
    /// Separate from event ownership; a dropped waiter retains its nonce.
    memory_sample: Option<(String, oneshot::Sender<GuestMemorySample>)>,
    idle_probe: Option<(String, oneshot::Sender<bool>)>,
    pending: HashMap<String, Pending>,
    // ponytail: bounded VM-lifetime replay ledger, shared by guest adapters.
    // Renew the VM at MAX_EVENTS; a session sequence protocol can remove it.
    retired: HashSet<String>,
    retiring_objects: HashSet<String>,
    // Joined control is earlier than native rollback/proof settlement. Keep
    // native event witnesses across pending-record removal and transport loss.
    native_events: Vec<(String, GuestStorage)>,
    retired_objects: HashSet<String>,
}

impl Registry {
    fn idle_reply(&mut self, probe: &str, idle: bool) -> Result<()> {
        let (expected, _) = self.idle_probe.as_ref().context("unsolicited idle reply")?;
        ensure!(probe == expected, "idle reply belongs to another request");
        let (_, reply) = self.idle_probe.take().expect("registered idle probe");
        let _ = reply.send(idle);
        Ok(())
    }

    fn memory_reply(&mut self, sample: &str, stats: GuestMemorySample) -> Result<()> {
        let (expected, _) = self
            .memory_sample
            .as_ref()
            .context("unsolicited memory sample reply")?;
        ensure!(
            sample == expected,
            "memory sample reply belongs to another request"
        );
        let (_, reply) = self.memory_sample.take().expect("registered memory sample");
        // Timeout/cancellation drops the receiver; matching late replies are harmless.
        let _ = reply.send(stats);
        Ok(())
    }
}

/// A handler's declared services: its own name and, for each binding, the
/// service it reaches and the entrypoint it names.
type ServiceBindings = (
    celld_applications::Name,
    HashMap<celld_applications::Name, (celld_applications::Name, Option<String>)>,
);

/// A brokered fetch's request body: its declared length and the credited chunks.
type FetchUpload = (
    Option<u64>,
    mpsc::Receiver<std::result::Result<Option<Vec<u8>>, String>>,
);

enum Outbound {
    Frame(Vec<u8>),
    Fence(oneshot::Sender<()>),
}

struct Shared {
    execution_gate: std::sync::OnceLock<Arc<Gate>>,
    idle_lifecycle: bool,
    resources: std::sync::OnceLock<HashMap<celld_applications::Name, ResourceAddress>>,
    application: ApplicationId,
    registry: Mutex<Registry>,
    outbound: mpsc::Sender<Outbound>,
    stop: CancellationToken,
    retired: CancellationToken,
    /// Cancelled by the guest's `Condemn`: no new leases; the pool retires the VM once idle.
    condemned: CancellationToken,
    cursors: crate::application_storage::CursorResidencies,
    owner_pid: Option<u32>,
    reaped: CancellationToken,
    reply_drain: bool,
    resident_objects: bool,
    object_retirement: bool,
    response_streams: bool,
    memory_diagnostics: bool,
    drained: Notify,
    // ponytail: serialize object stops within a VM; use per-residency locks
    // if measured bulk-retirement latency requires parallel object cleanup.
    object_stops: tokio::sync::Mutex<()>,
    /// The namespaces the VM's pinned handler is bound to; unset refuses every
    /// guest object call.
    bound: std::sync::OnceLock<Bound>,
    services: std::sync::OnceLock<ServiceBindings>,
    service_routes: std::sync::OnceLock<std::sync::Weak<crate::application_vm::Routes>>,
    /// Names this transport's declared signals to the callees it calls.
    signal_owner: String,
    /// The pinned handler, installed with `services` from the immutable
    /// deployment; it selects the handler's static assets.
    handler: std::sync::OnceLock<celld_applications::Name>,
    /// The handler's Container bindings, pinned by the host at launch.
    containers: std::sync::OnceLock<Vec<crate::application_container::Binding>>,
}

struct Bound {
    namespaces: Vec<(ResourceAddress, String)>,
    deadline: Duration,
}

/// The VM supervisor owns this object and must kill/reap the VM on retirement.
/// Dropping the owner retires escaped clients. This transport does not launch
/// a VM or authorize a deployment; its endpoints/identity come from bootstrap.
pub(crate) struct Dispatcher {
    client: Client,
    task: Option<crate::asyncrt::TaskHandle<Result<()>>>,
}

#[derive(Clone)]
pub(crate) struct Client(Arc<Shared>);

/// A native cell remembers its guest through a weak owner; the cursor owner
/// already references this runtime, so a strong link would form a cycle.
#[derive(Clone)]
pub(crate) struct GuestObject {
    owner: std::sync::Weak<Shared>,
    target: celld_applications::invocation_wire::ObjectTarget,
    residency: String,
}

impl GuestObject {
    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        self.owner.ptr_eq(&other.owner)
    }
    pub(crate) async fn retire(
        &self,
        runtime: crate::runtime::RuntimeManager,
        cell: String,
        epoch: u64,
    ) -> Result<()> {
        let Some(owner) = self.owner.upgrade() else {
            return Ok(());
        };
        let object = self.clone();
        crate::asyncrt::spawn(async move {
            let client = Client(owner);
            client.retire_object(runtime, cell, epoch, object).await
        })
        .await
        .context("guest object retirement driver panicked")?
    }
}

impl Dispatcher {
    pub(crate) async fn connect(
        application: ApplicationId,
        adapter: Language,
        mut control: UnixStream,
        storage: UnixListener,
        owner_pid: Option<u32>,
    ) -> Result<Self> {
        let ready = timeout(IO_DEADLINE, read_frame_limited(&mut control, 512))
            .await
            .context("guest readiness timed out")??
            .context("guest closed before readiness")?;
        let (
            reply_drain,
            resident_objects,
            object_retirement,
            response_streams,
            memory_diagnostics,
            idle_lifecycle,
        ) = match GuestControl::decode(&ready)? {
            GuestControl::Ready {
                adapter: received,
                execution_turns: true,
                quiescent_terminals: true,
                cleanup_turns: true,
                callback_turns: true,
                response_turns: true,
                response_streams,
                memory_diagnostics,
                idle_lifecycle,
                reply_drain,
                resident_objects,
                object_retirement,
                ..
            } if received == adapter && (!response_streams || reply_drain) => (
                reply_drain,
                resident_objects,
                object_retirement,
                response_streams,
                memory_diagnostics,
                idle_lifecycle,
            ),
            _ => anyhow::bail!("guest adapter readiness mismatch"),
        };
        ensure!(
            !idle_lifecycle || reply_drain,
            "idle lifecycle requires joined reply/drain"
        );
        ensure!(
            !object_retirement || (resident_objects && owner_pid.is_some()),
            "object retirement requires an owned guest process"
        );
        let (outbound, receive) = mpsc::channel(MAX_ACTIVE * 2 + 1);
        let shared = Arc::new(Shared {
            execution_gate: std::sync::OnceLock::new(),
            idle_lifecycle,
            application,
            registry: Mutex::new(Registry {
                running: true,
                memory_sample: None,
                idle_probe: None,
                pending: HashMap::new(),
                retired: HashSet::new(),
                retiring_objects: HashSet::new(),
                native_events: Vec::new(),
                retired_objects: HashSet::new(),
            }),
            outbound,
            stop: CancellationToken::new(),
            retired: CancellationToken::new(),
            condemned: CancellationToken::new(),
            cursors: Default::default(),
            owner_pid,
            reaped: CancellationToken::new(),
            reply_drain,
            resident_objects,
            object_retirement,
            response_streams,
            memory_diagnostics,
            drained: Notify::new(),
            object_stops: tokio::sync::Mutex::new(()),
            bound: std::sync::OnceLock::new(),
            services: std::sync::OnceLock::new(),
            resources: std::sync::OnceLock::new(),
            service_routes: std::sync::OnceLock::new(),
            signal_owner: crate::application_storage::handle()?,
            handler: std::sync::OnceLock::new(),
            containers: std::sync::OnceLock::new(),
        });
        let task = crate::asyncrt::spawn(run(shared.clone(), control, storage, receive));
        Ok(Self {
            client: Client(shared),
            task: Some(task),
        })
    }

    pub(crate) fn client(&self) -> Client {
        self.client.clone()
    }
    pub(crate) fn retirement(&self) -> CancellationToken {
        self.client.0.retired.clone()
    }

    pub(crate) async fn shutdown(mut self) -> Result<()> {
        {
            let mut registry = self
                .client
                .0
                .registry
                .lock()
                .expect("guest registry poisoned");
            ensure!(
                registry.running && !self.client.0.stop.is_cancelled(),
                "guest transport retired"
            );
            // Stop admission before waiting; a reply is not joined event drain.
            registry.running = false;
        }
        timeout(IO_DEADLINE, async {
            loop {
                let notified = self.client.0.drained.notified();
                tokio::pin!(notified);
                // Register the waiter before inspecting the mutex-protected
                // record, so a final removal cannot lose its notification.
                notified.as_mut().enable();
                ensure!(!self.client.0.stop.is_cancelled(), "guest transport retired during drain");
                if self.client.0.registry.lock().expect("guest registry poisoned").pending.is_empty() {
                    return Ok::<(), anyhow::Error>(());
                }
                crate::asyncrt::select! {
                    _ = &mut notified => {},
                    _ = self.client.0.stop.cancelled() => anyhow::bail!("guest transport retired during drain"),
                }
            }
        }).await.context("guest drain timed out")??;
        let message = HostControl::Shutdown { version: VERSION }.encode()?;
        self.client
            .0
            .outbound
            .try_send(Outbound::Frame(message))
            .context("guest shutdown queue unavailable")?;
        timeout(IO_DEADLINE, self.task.take().expect("owned guest task"))
            .await
            .context("guest shutdown timed out")?
            .context("guest dispatcher panicked")?
    }

    #[cfg(test)]
    pub(crate) async fn join(mut self) -> Result<()> {
        self.task
            .take()
            .expect("owned guest task")
            .await
            .context("guest dispatcher panicked")?
    }
}

impl Drop for Dispatcher {
    fn drop(&mut self) {
        if let Some(gate) = self.client.0.execution_gate.get() {
            gate.retire();
        }
        {
            let mut registry = self
                .client
                .0
                .registry
                .lock()
                .expect("guest registry poisoned");
            registry.memory_sample = None;
            registry.idle_probe = None;
        }
        self.client.0.cursors.retire();
        self.client.0.stop.cancel();
    }
}

struct Registration {
    shared: Arc<Shared>,
    id: String,
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut registry = self
            .shared
            .registry
            .lock()
            .expect("guest registry poisoned");
        let Some(pending) = registry.pending.get_mut(&self.id) else {
            return;
        };
        if pending.terminal_seen {
            registry.pending.remove(&self.id);
            registry.retired.insert(self.id.clone());
            self.shared.drained.notify_waiters();
            return;
        }
        pending.cancelled = true;
        pending.storage.take();
        pending.response.take();
        pending.terminal.take();
        pending.deadline = Some(Instant::now() + CANCEL_GRACE);
        let cancel = HostControl::Cancel {
            version: VERSION,
            invocation: self.id.clone(),
        };
        if cancel.encode().ok().is_none_or(|bytes| {
            self.shared
                .outbound
                .try_send(Outbound::Frame(bytes))
                .is_err()
        }) {
            // Never lose a cancellation because the bounded queue is full.
            self.shared.stop.cancel();
        }
    }
}

impl Client {
    pub(crate) fn install_execution_gate(&self, gate: Arc<Gate>) -> Result<()> {
        {
            let registry = self.0.registry.lock().expect("guest registry poisoned");
            ensure!(
                registry.running
                    && registry.pending.is_empty()
                    && registry
                        .native_events
                        .iter()
                        .all(|(_, event)| event.is_finished()),
                "execution gate must precede event publication"
            );
        }
        self.0
            .execution_gate
            .set(gate)
            .map_err(|_| anyhow::anyhow!("guest execution gate already installed"))
    }

    pub(crate) fn supports_idle_lifecycle(&self) -> bool {
        self.0.idle_lifecycle
    }

    async fn enter_execution(
        &self,
        deadline_ms: u64,
        cancellation: &CancellationToken,
    ) -> Result<Option<EventHold>> {
        ensure!(
            crate::asyncrt::mono_ms() < deadline_ms,
            "guest execution deadline exceeded"
        );
        ensure!(
            !self.0.stop.is_cancelled() && !cancellation.is_cancelled(),
            "guest execution admission cancelled"
        );
        match self.0.execution_gate.get() {
            Some(gate) => crate::asyncrt::select! {
                hold = gate.enter(deadline_ms, cancellation) => hold.map(Some),
                _ = self.0.stop.cancelled() => anyhow::bail!("guest transport retired during admission"),
            },
            None => Ok(None),
        }
    }

    pub(crate) async fn writer_fence(&self, deadline_ms: u64) -> Result<()> {
        let (reply, receive) = oneshot::channel();
        timeout_at(deadline_ms, self.0.outbound.send(Outbound::Fence(reply)))
            .await
            .context("guest writer fence queue deadline exceeded")?
            .context("guest control writer stopped")?;
        timeout_at(deadline_ms, receive)
            .await
            .context("guest writer fence deadline exceeded")?
            .context("guest writer fence retired")
    }

    pub(crate) async fn idle_probe(&self, deadline_ms: u64) -> Result<bool> {
        ensure!(
            self.0.idle_lifecycle,
            "guest does not support idle lifecycle"
        );
        ensure!(
            crate::asyncrt::mono_ms() < deadline_ms,
            "guest idle probe deadline exceeded"
        );
        ensure!(
            self.0
                .execution_gate
                .get()
                .is_none_or(|gate| gate.status().phase == Phase::Pausing),
            "guest admission is not closed for idle probe"
        );
        let probe = crate::application_storage::handle()?;
        let bytes = HostControl::IdleProbe {
            version: VERSION,
            probe: probe.clone(),
        }
        .encode()?;
        let receive = {
            let mut registry = self.0.registry.lock().expect("guest registry poisoned");
            ensure!(
                registry.running && !self.0.stop.is_cancelled(),
                "guest transport retired"
            );
            ensure!(
                registry.pending.is_empty()
                    && registry.idle_probe.is_none()
                    && registry.memory_sample.is_none(),
                "guest idle probe unavailable"
            );
            let (reply, receive) = oneshot::channel();
            self.0
                .outbound
                .try_send(Outbound::Frame(bytes))
                .context("guest idle probe queue unavailable")?;
            registry.idle_probe = Some((probe, reply));
            receive
        };
        // Preserve the nonce after timeout/cancellation until a matching reply or retirement.
        crate::asyncrt::select! {
            result = timeout_at(deadline_ms, receive) => result.context("guest idle probe deadline exceeded")?.context("guest idle probe retired"),
            _ = self.0.stop.cancelled() => anyhow::bail!("guest transport retired"),
        }
    }

    /// Fixed once from the pinned deployment and its registered resources.
    pub(crate) fn bind_containers(
        &self,
        containers: Vec<crate::application_container::Binding>,
    ) -> Result<()> {
        self.0
            .containers
            .set(containers)
            .map_err(|_| anyhow::anyhow!("guest containers already bound"))
    }

    /// The pinned Container this alias names must belong to the issuing
    /// object's class and namespace; the object comes from the route.
    fn authorize_container(&self, address: &ResourceAddress, access: &GuestStorage) -> Result<()> {
        let object = access.object()?;
        ensure!(
            self.0
                .containers
                .get()
                .is_some_and(|all| all
                    .iter()
                    .any(|item| item.address == *address
                        && item.authorizes(&self.0.application, object))),
            "container is not bound to this object"
        );
        Ok(())
    }

    /// Give a native object event its Container, when the object's class has
    /// one. The class and namespace come from the route-authenticated object,
    /// never from the guest; the spec is the registered resource's.
    async fn attach_container(&self, access: &GuestStorage) -> Result<()> {
        let Ok(object) = access.object() else {
            return Ok(());
        };
        let Some(binding) = self.0.containers.get().and_then(|all| {
            all.iter()
                .find(|item| item.authorizes(&self.0.application, object))
        }) else {
            return Ok(());
        };
        let engine = crate::container::engine().await?;
        let cell = engine
            .attach_with_spec(
                &format!("{}:{}", object.class, object.id),
                binding.spec.clone(),
            )
            .await?;
        access.bind_container(cell).await
    }

    pub(crate) fn bind_service_routes(
        &self,
        routes: std::sync::Weak<crate::application_vm::Routes>,
    ) -> Result<()> {
        self.0
            .service_routes
            .set(routes)
            .map_err(|_| anyhow::anyhow!("guest service router already bound"))
    }
    /// Installed by the supervisor from immutable deployment metadata, never
    /// by a guest frame. An alias grants only its application-local handler.
    pub(crate) fn bind_services(
        &self,
        deployment: celld_applications::Name,
        handler: &celld_applications::Handler,
    ) -> Result<()> {
        let services = crate::application_javascript_guest::service_bindings(handler)?
            .into_iter()
            .map(|(alias, target, entrypoint)| {
                Ok((
                    celld_applications::Name::try_from(alias)?,
                    (celld_applications::Name::try_from(target)?, entrypoint),
                ))
            })
            .collect::<Result<_>>()?;
        let resources = handler
            .bindings
            .iter()
            .filter_map(|(alias, target)| match target {
                celld_applications::BindingTarget::Resource { resource } => Some((
                    alias.clone(),
                    ResourceAddress {
                        application: self.0.application.clone(),
                        resource: resource.clone(),
                    },
                )),
                _ => None,
            })
            .collect();
        self.0
            .resources
            .set(resources)
            .map_err(|_| anyhow::anyhow!("guest resource aliases already bound"))?;
        self.0
            .handler
            .set(handler.id.clone())
            .map_err(|_| anyhow::anyhow!("guest handler already bound"))?;
        self.0
            .services
            .set((deployment, services))
            .map_err(|_| anyhow::anyhow!("guest services already bound"))
    }

    fn resource_target(
        &self,
        call: &celld_applications::resource_wire::ResourceCall,
    ) -> Result<ResourceAddress> {
        call.validate()?;
        let resource = self
            .0
            .resources
            .get()
            .and_then(|bindings| bindings.get(&call.binding))
            .context("resource alias is not bound to this handler")?;
        let kind = match call.operation {
            celld_applications::resource_wire::ResourceOperation::R2 { .. } => ResourceKind::R2,
            celld_applications::resource_wire::ResourceOperation::Assets {} => {
                ResourceKind::StaticAssets
            }
            celld_applications::resource_wire::ResourceOperation::Container { .. } => {
                ResourceKind::Container
            }
        };
        ensure!(
            resource.resource.kind == kind,
            "resource alias kind does not match operation"
        );
        Ok(resource.clone())
    }

    fn service_target(
        &self,
        call: &celld_applications::invocation_wire::ServiceCall,
    ) -> Result<(celld_applications::Name, celld_applications::Name)> {
        let (deployment, services) = self.0.services.get().context("no services are bound")?;
        let (target, entrypoint) = services
            .get(&call.binding)
            .context("service is not bound to this handler")?;
        ensure!(
            entrypoint.as_deref().unwrap_or("default")
                == call.invocation.entrypoint.as_deref().unwrap_or("default"),
            "service entrypoint is not bound to this handler"
        );
        Ok((deployment.clone(), target.clone()))
    }
    async fn retire_object(
        &self,
        runtime: crate::runtime::RuntimeManager,
        cell: String,
        epoch: u64,
        object: GuestObject,
    ) -> Result<()> {
        if self
            .0
            .registry
            .lock()
            .expect("guest registry poisoned")
            .retired_objects
            .contains(&object.residency)
        {
            return Ok(());
        }
        let deadline_ms = crate::application_guest::execution_deadline_ms(IO_DEADLINE)?;
        let execution = if self.0.stop.is_cancelled() {
            None
        } else {
            match self.enter_execution(deadline_ms, &self.0.stop).await {
                Ok(execution) => execution,
                Err(_) => {
                    self.retire();
                    self.0.reaped.cancelled().await;
                    return Ok(());
                }
            }
        };
        let _stop = match timeout_at(deadline_ms, self.0.object_stops.lock()).await {
            Ok(stop) => stop,
            Err(_) => {
                self.retire();
                self.0.reaped.cancelled().await;
                return Ok(());
            }
        };
        let active: Vec<_> = {
            let mut registry = self.0.registry.lock().expect("guest registry poisoned");
            if registry.retired_objects.contains(&object.residency) {
                return Ok(());
            }
            registry.retiring_objects.insert(object.residency.clone());
            registry
                .native_events
                .retain(|(_, event)| !event.is_finished());
            registry
                .native_events
                .iter()
                .filter(|(residency, _)| residency == &object.residency)
                .map(|(_, event)| event.clone())
                .collect()
        };
        let completed = || {
            self.0
                .registry
                .lock()
                .expect("guest registry poisoned")
                .retired_objects
                .insert(object.residency.clone());
        };
        // A forced stop cannot close a connection under a live guest event.
        // Idle retirement uses the per-object lifecycle; active/uncooperative
        // work retains native holds until the supervisor has actually reaped.
        if !active.is_empty() || self.0.stop.is_cancelled() {
            self.retire();
            self.0.reaped.cancelled().await;
            for access in active {
                access.wait_finished().await;
            }
            completed();
            return Ok(());
        }
        let begin = async {
            let admission = runtime.admit_storage_retirement(&cell, epoch)?;
            GuestStorage::retirement(
                runtime.clone(),
                admission,
                self.0.application.clone(),
                object.target.clone(),
                deadline_ms,
                execution,
            )
            .await
        };
        let access = match timeout_at(deadline_ms, begin).await {
            Ok(Ok(access)) => access,
            _ => {
                self.retire();
                self.0.reaped.cancelled().await;
                completed();
                return Ok(());
            }
        };
        let result = timeout_at(
            deadline_ms,
            self.event(
                access.clone(),
                None,
                None,
                None,
                Some(&object.residency),
                None,
                None,
            ),
        )
        .await;
        if !matches!(result, Ok(Ok(_))) {
            self.retire();
            self.0.reaped.cancelled().await;
        }
        access.finish_retirement().await?;
        self.0.cursors.close_object(&cell, &object.residency).await;
        completed();
        Ok(())
    }
    pub(crate) fn confirm_vm_reaped(
        &self,
        pid: u32,
        child: &mut tokio::process::Child,
    ) -> Result<()> {
        ensure!(
            self.0.owner_pid == Some(pid) && child.id().is_none() && child.try_wait()?.is_some(),
            "owning VM was not reaped"
        );
        self.0.reaped.cancel();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn confirm_fixture_reaped(&self, child: &mut std::process::Child) -> Result<()> {
        ensure!(
            self.0.owner_pid == Some(child.id()) && child.try_wait()?.is_some(),
            "owning guest was not reaped"
        );
        self.0.reaped.cancel();
        Ok(())
    }

    pub(crate) async fn close_read_cursors(&self) {
        self.0.cursors.close().await;
    }

    pub(crate) fn retire(&self) {
        if let Some(gate) = self.0.execution_gate.get() {
            gate.retire();
        }
        {
            let mut registry = self.0.registry.lock().expect("guest registry poisoned");
            registry.memory_sample = None;
            registry.idle_probe = None;
        }
        self.0.cursors.retire();
        self.0.stop.cancel();
    }

    /// Fix, once, the namespaces and classes this VM's handler may call.
    pub(crate) fn bind(
        &self,
        namespaces: Vec<(ResourceAddress, String)>,
        deadline: Duration,
    ) -> Result<()> {
        self.0
            .bound
            .set(Bound {
                namespaces,
                deadline,
            })
            .map_err(|_| anyhow::anyhow!("guest namespaces already bound"))
    }

    #[cfg(test)]
    pub(crate) async fn invoke(
        &self,
        app: crate::actor::AppHandle,
        resource: ResourceAddress,
        class: String,
        name: String,
        timeout: Duration,
        request: HttpRequest,
    ) -> Result<crate::application_guest::GuestCompletion<HttpResponse>> {
        self.invoke_request(app, resource, class, name, None, timeout, request, None)
            .await
    }

    #[cfg(test)]
    // Each parameter names one independent input of a guest object call.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn invoke_request(
        &self,
        app: crate::actor::AppHandle,
        resource: ResourceAddress,
        class: String,
        name: String,
        request_id: Option<crate::js::RequestId>,
        timeout: Duration,
        request: HttpRequest,
        body: Option<crate::js::HttpChunkStream>,
    ) -> Result<crate::application_guest::GuestCompletion<HttpResponse>> {
        let cell = resource.cell_scope(&class, &name)?;
        let object = ObjectTarget {
            class,
            id: cell.split_once(':').expect("validated scope").1.into(),
            name: Some(name),
        };
        self.invoke_object(
            app, resource, object, request_id, timeout, request, body, None,
        )
        .await
    }

    /// The guest names no resource: an object's id must authenticate under a
    /// namespace this VM's handler is bound to, and its class must be one the
    /// handler declares.
    fn namespace(&self, object: &ObjectTarget) -> Result<(ResourceAddress, Duration)> {
        let bound = self.0.bound.get().context("no namespaces are bound")?;
        let resource = bound
            .namespaces
            .iter()
            .find(|(resource, class)| {
                crate::application_javascript_guest::namespace_owns_object(resource, class, object)
            })
            .map(|(resource, _)| resource)
            .context("object is not in a namespace bound to this handler")?;
        Ok((resource.clone(), bound.deadline))
    }

    /// A guest's call to an object. Routing, admission and the callee's own
    /// output gate are the ordinary scoped event's.
    pub(crate) async fn call_object(
        &self,
        app: crate::actor::AppHandle,
        call: ObjectCall,
        request: HttpRequest,
    ) -> Result<crate::application_guest::GuestCompletion<HttpResponse>> {
        call.object.validate()?;
        let (resource, deadline) = self.namespace(&call.object)?;
        self.invoke_object(
            app,
            resource,
            call.object,
            None,
            deadline,
            request,
            None,
            call.rpc,
        )
        .await
    }

    /// Only native alarm discovery uses the authenticated id without an
    /// instance name. Guest object calls always use `namespace` above.
    fn alarm_namespace(&self, object: &ObjectTarget) -> Result<(ResourceAddress, Duration)> {
        object.validate()?;
        let bound = self.0.bound.get().context("no namespaces are bound")?;
        let resource = bound
            .namespaces
            .iter()
            .find(|(resource, class)| {
                matches!(
                    resource.resource.kind,
                    ResourceKind::DurableObject | ResourceKind::Workflow
                ) && crate::application_javascript_guest::native_namespace_owns_object(
                    resource, class, object,
                )
            })
            .map(|(resource, _)| resource)
            .context("alarm is not in a guest namespace bound to this handler")?;
        Ok((resource.clone(), bound.deadline))
    }

    pub(crate) async fn invoke_alarm(
        &self,
        app: crate::actor::AppHandle,
        object: ObjectTarget,
        request_id: crate::js::RequestId,
        scheduled_ms: i64,
        cancelled: CancellationToken,
    ) -> Result<crate::application_guest::GuestCompletion<HttpResponse>> {
        let (resource, deadline) = self.alarm_namespace(&object)?;
        let deadline_ms = crate::application_guest::execution_deadline_ms(deadline)?;
        let execution = self.enter_execution(deadline_ms, &cancelled).await?;
        let client = self.clone();
        crate::application_guest::invoke_object_event(
            app,
            resource,
            object,
            Some(request_id),
            deadline_ms,
            execution,
            Some((scheduled_ms, cancelled)),
            None,
            move |access, reply| async move {
                client
                    .event(
                        access,
                        Some(HttpRequest {
                            method: "POST".into(),
                            url: "http://celld.internal/alarm".into(),
                            headers: vec![],
                            body: vec![],
                        }),
                        None,
                        Some(reply),
                        None,
                        None,
                        None,
                    )
                    .await
            },
        )
        .await
    }

    /// A guest's own call: a scoped event here when this node owns the object,
    /// otherwise the owner's, reached through the original Durable Object call
    /// path. The completion, when local, keeps the callee's admission.
    async fn call_or_forward(
        &self,
        app: crate::actor::AppHandle,
        call: ObjectCall,
        request: HttpRequest,
    ) -> Result<(
        HttpResponse,
        Option<crate::application_guest::GuestCompletion<HttpResponse>>,
    )> {
        call.object.validate()?;
        let (resource, deadline) = self.namespace(&call.object)?;
        let deadline_ms = crate::application_guest::execution_deadline_ms(deadline)?;
        let local = if matches!(
            resource.resource.kind,
            ResourceKind::Kv | ResourceKind::Queue
        ) {
            native_call(
                app,
                resource,
                ObjectCall {
                    object: call.object.clone(),
                    rpc: call.rpc.clone(),
                },
                request.clone(),
                deadline,
            )
            .await
            .map(|response| (response, None))
        } else {
            self.invoke_object(
                app,
                resource,
                call.object.clone(),
                None,
                deadline,
                request.clone(),
                None,
                call.rpc.clone(),
            )
            .await
            .and_then(|mut completion| {
                let response =
                    std::mem::replace(&mut completion.result, Err(anyhow::anyhow!("taken")))?;
                Ok((response, Some(completion)))
            })
        };
        match local {
            Ok(result) => Ok(result),
            Err(error) if error.is::<crate::application_guest::RemoteOwner>() => {
                let ObjectCall { object, rpc } = call;
                let body = crate::js::RequestBody::Bytes(request.body.into());
                let (reply, receive) = oneshot::channel();
                ensure!(
                    crate::js::submit_do_call(crate::js::DoCallReq {
                        request_id: None,
                        cancel: None,
                        deliver_abort_to_handler: false,
                        scope: format!("{}:{}", object.class, object.id),
                        name: object.name,
                        url: request.url,
                        method: request.method,
                        body_guard: crate::js::RequestBodyGuard::of(&body),
                        body,
                        headers: request.headers,
                        reply,
                        order: None,
                        parent: None,
                        guest: Some(crate::js::GuestCall {
                            application: self.0.application.clone(),
                            rpc,
                        }),
                    }),
                    "no proxy channel"
                );
                let response = timeout_at(deadline_ms, receive)
                    .await
                    .context("guest object call to its owner timed out")?
                    .context("proxy dropped")??;
                Ok((response, None))
            }
            Err(error) => {
                // A reply may already be sent: only the logs show a failed waitUntil call.
                tracing::warn!(object = %call.object.class, error = %format!("{error:#}"), "guest object call failed");
                Err(error)
            }
        }
    }

    // Each parameter names one independent input of a guest object call.
    #[allow(clippy::too_many_arguments)]
    async fn invoke_object(
        &self,
        app: crate::actor::AppHandle,
        resource: ResourceAddress,
        object: ObjectTarget,
        request_id: Option<crate::js::RequestId>,
        timeout: Duration,
        mut request: HttpRequest,
        mut body: Option<crate::js::HttpChunkStream>,
        rpc: Option<String>,
    ) -> Result<crate::application_guest::GuestCompletion<HttpResponse>> {
        let deadline_ms = crate::application_guest::execution_deadline_ms(timeout)?;
        let execution = self.enter_execution(deadline_ms, &self.0.stop).await?;
        ensure!(
            resource.application == self.0.application,
            "guest transport belongs to another application"
        );
        if request.body.len() > celld_applications::invocation_wire::MAX_BODY_BYTES {
            ensure!(
                body.is_none()
                    && request.body.len()
                        <= celld_applications::invocation_wire::MAX_OBJECT_CALL_BYTES,
                "object request exceeds limit"
            );
            let bytes = bytes::Bytes::from(std::mem::take(&mut request.body));
            body = Some(Box::pin(futures_util::stream::unfold(
                bytes,
                |mut remaining| async {
                    if remaining.is_empty() {
                        None
                    } else {
                        let chunk = remaining.split_to(remaining.len().min(MAX_CHUNK_BYTES));
                        Some((Ok(chunk.to_vec()), remaining))
                    }
                },
            )));
        }
        ensure!(
            self.0
                .registry
                .lock()
                .expect("guest registry poisoned")
                .running
                && !self.0.stop.is_cancelled(),
            "guest transport retired"
        );
        let client = self.clone();
        crate::application_guest::invoke_object(
            app,
            resource,
            object,
            request_id,
            deadline_ms,
            execution,
            move |access, reply| async move {
                client
                    .event(access, Some(request), body, Some(reply), None, rpc, None)
                    .await
            },
        )
        .await
    }

    /// An original stateless Worker fetch in this pinned deployment's VM. It
    /// has no object, storage session or durable write, so no output proof;
    /// the public admission covers waitUntil work and a streamed body.
    pub(crate) async fn invoke_stateless(
        &self,
        app: &crate::actor::AppHandle,
        deadline: Duration,
        request: HttpRequest,
        body: Option<crate::js::HttpChunkStream>,
    ) -> Result<HttpResponse> {
        self.stateless(app, deadline, request, body, false, None, None)
            .await
    }

    pub(crate) async fn invoke_service(
        &self,
        app: &crate::actor::AppHandle,
        deadline: Duration,
        request: HttpRequest,
        service: celld_applications::invocation_wire::ServiceInvocation,
        body: Option<crate::js::HttpChunkStream>,
        relay: Option<SignalRelay>,
    ) -> Result<HttpResponse> {
        self.stateless(app, deadline, request, body, false, Some(service), relay)
            .await
    }

    /// An original queue consumer batch in this pinned deployment's VM, run as
    /// a stateless event. The queue cell's lease handoff is already
    /// output-gated, so there is no storage grant or output proof here. It
    /// waits for event capacity within `deadline` rather than failing the
    /// batch; a lost race for the last slot still fails, and the lease expires.
    pub(crate) async fn invoke_queue(
        &self,
        app: &crate::actor::AppHandle,
        deadline: Duration,
        batch: &crate::js::QueueBatch,
    ) -> Result<crate::js::QueueDispatchResult> {
        let started = Instant::now();
        while self.pending() >= MAX_ACTIVE {
            // ponytail: polls; a registry notify if queue waits get hot.
            ensure!(
                started.elapsed() < deadline,
                "guest event capacity unavailable"
            );
            crate::asyncrt::sleep(Duration::from_millis(25)).await;
        }
        let batch = crate::application_guest::encode_queue_batch(batch)?;
        let response = self
            .stateless(
                app,
                deadline.saturating_sub(started.elapsed()),
                HttpRequest {
                    method: "POST".into(),
                    url: "https://queue.celld.internal/".into(),
                    headers: vec![],
                    body: vec![],
                },
                Some(Box::pin(futures_util::stream::once(async move {
                    Ok::<_, String>(batch)
                }))),
                true,
                None,
                None,
            )
            .await?;
        ensure!(
            response.status == 200 && response.stream.is_none(),
            "guest queue handler answered status {}",
            response.status
        );
        crate::application_guest::decode_queue_result(&response.body)
    }

    // Each parameter names one independent input of a stateless guest event.
    #[allow(clippy::too_many_arguments)]
    async fn stateless(
        &self,
        app: &crate::actor::AppHandle,
        deadline: Duration,
        request: HttpRequest,
        body: Option<crate::js::HttpChunkStream>,
        queue: bool,
        service: Option<celld_applications::invocation_wire::ServiceInvocation>,
        relay: Option<SignalRelay>,
    ) -> Result<HttpResponse> {
        let deadline_ms = crate::application_guest::execution_deadline_ms(deadline)?;
        let execution = self.enter_execution(deadline_ms, &self.0.stop).await?;
        ensure!(
            self.0
                .registry
                .lock()
                .expect("guest registry poisoned")
                .running
                && !self.0.stop.is_cancelled(),
            "guest transport retired"
        );
        let public = app.admit_public().context("node is draining")?;
        let (reply, ready) = oneshot::channel();
        let client = self.clone();
        // Dropping this before the reply cancels the guest event.
        let handle = app.clone();
        let mut event = Box::pin(async move {
            client
                .stateless_event(
                    handle, request, body, queue, service, relay, reply, execution,
                )
                .await
        });
        let mut ready = ready;
        let response = crate::asyncrt::select! {
            response = &mut ready => response.context("guest stateless event retired before reply")?,
            result = &mut event => {
                result?;
                // The event can send its reply and drain within one poll.
                return ready
                    .try_recv()
                    .context("guest stateless event ended without a reply")?;
            }
            _ = crate::asyncrt::sleep_until(deadline_ms) => anyhow::bail!("guest execution deadline exceeded"),
        };
        crate::asyncrt::spawn(async move {
            let _public = public;
            if let Err(error) = event.await {
                tracing::warn!(%error, "guest stateless work failed after reply");
            }
        });
        response
    }

    /// Reply/drain, streams and cancellation route exactly as for `event`.
    // Each parameter names one independent input of a stateless guest event.
    #[allow(clippy::too_many_arguments)]
    async fn stateless_event(
        &self,
        app: crate::actor::AppHandle,
        request: HttpRequest,
        body: Option<crate::js::HttpChunkStream>,
        queue: bool,
        service: Option<celld_applications::invocation_wire::ServiceInvocation>,
        mut relay: Option<SignalRelay>,
        reply: oneshot::Sender<Result<HttpResponse>>,
        execution: Option<EventHold>,
    ) -> Result<()> {
        ensure!(
            self.0.reply_drain,
            "stateless guest requires split reply/drain"
        );
        let id = crate::application_storage::handle()?;
        let bytes = HostControl::InvokeHttp(Box::new(InvokeHttp {
            version: VERSION,
            invocation: id.clone(),
            // Never connected: a stateless event opens no guest storage.
            storage_grant: crate::application_storage::handle()?,
            storage_residency: None,
            object: None,
            request,
            request_stream: body.is_some(),
            rpc: None,
            queue,
            service,
            alarm: None,
            socket: None,
        }))
        .encode()?;
        let (terminal, result) = oneshot::channel();
        let (response, response_ready) = oneshot::channel();
        let (demand, demand_ready) = mpsc::channel(1);
        let (chunks, chunk_ready) = mpsc::channel(1);
        let body_dropped = CancellationToken::new();
        let (request_pulls, request_credits) = mpsc::channel(1);
        let (frames, frame_ready) = mpsc::channel(SOCKET_WINDOW);
        {
            let mut registry = self.0.registry.lock().expect("guest registry poisoned");
            ensure!(
                registry.running && !self.0.stop.is_cancelled(),
                "guest transport retired"
            );
            ensure!(
                registry.pending.len() < MAX_ACTIVE,
                "guest invocation capacity reached"
            );
            if registry.retired.len() + registry.pending.len() >= MAX_EVENTS {
                self.0.stop.cancel();
                anyhow::bail!("guest replay ledger capacity reached; renew VM");
            }
            ensure!(
                !registry.retired.contains(&id) && !registry.pending.contains_key(&id),
                "guest invocation nonce collision"
            );
            registry.pending.insert(
                id.clone(),
                Pending {
                    execution: execution.clone(),
                    retirement: false,
                    grant: crate::application_storage::handle()?,
                    storage: None,
                    response: Some(response),
                    response_seen: false,
                    terminal: Some(terminal),
                    terminal_seen: false,
                    connected: true,
                    cleanup_connected: false,
                    access: None,
                    cancelled: false,
                    deadline: None,
                    settled: CancellationToken::new(),
                    stream: self.0.response_streams.then(|| PendingStream {
                        flow: ResponseFlow::default(),
                        head_seen: false,
                        demand,
                        chunks,
                        dropped: body_dropped.clone(),
                    }),
                    request: body
                        .is_some()
                        .then(|| (ResponseFlow::default(), request_pulls)),
                    fetches: HashMap::new(),
                    app: Some(app),
                    sockets: Some(frames),
                    dials: HashMap::new(),
                    issued_sockets: HashMap::new(),
                },
            );
        }
        let _registration = Registration {
            shared: self.0.clone(),
            id: id.clone(),
        };
        if self.0.outbound.try_send(Outbound::Frame(bytes)).is_err() {
            self.0.stop.cancel();
            anyhow::bail!("guest invocation queue unavailable");
        }
        if let Some(hold) = &execution {
            hold.mark_admitted();
        }
        let mut reply = Some(reply);
        tokio::pin!(result, response_ready);
        let request_pump = self.request_body(&id, body, request_credits);
        tokio::pin!(request_pump);
        let body = self.response_body(&id, None, demand_ready, chunk_ready);
        tokio::pin!(body);
        let forwarder = self.forward_sockets(&id, None, frame_ready);
        tokio::pin!(forwarder);
        let mut streaming = false;
        let mut body_done = false;
        let mut joined = None;
        let mut response_open = true;
        let mut responded = false;
        let terminal = loop {
            if (!streaming || body_done) && joined.is_some() {
                break joined.take().expect("joined terminal");
            }
            crate::asyncrt::select_biased! {
                "a ready response is delivered before a tied terminal so the terminal is judged as work after the response";
                body = &mut response_ready, if response_open => {
                    response_open = false;
                    if let Ok(body) = body {
                        streaming = body.stream.is_some();
                        reply.take().expect("stateless guest reply channel").send(Ok(body))
                            .map_err(|_| anyhow::anyhow!("guest reply consumer retired"))?;
                        responded = true;
                    }
                }
                terminal = &mut result, if joined.is_none() => {
                    let terminal = terminal.context("guest control retired")?;
                    if matches!(terminal, GuestControl::Drained { .. }) && streaming && !body_done {
                        joined = Some(terminal);
                    } else { break terminal; }
                }
                result = &mut body, if streaming && !body_done => { result?; body_done = true; },
                _ = body_dropped.cancelled(), if streaming && !body_done =>
                    anyhow::bail!("guest response consumer retired"),
                result = &mut request_pump => result?,
                result = &mut forwarder => { result?; anyhow::bail!("guest socket forwarder stopped") },
                item = next_relay(&mut relay) => match item {
                    Some((signal, reason)) => self.send(HostControl::SignalAbort {
                        version: VERSION,
                        invocation: id.clone(),
                        signal,
                        reason,
                    }).await?,
                    None => relay = None,
                },
            }
        };
        if matches!(terminal, GuestControl::Drained { .. }) {
            // The frames of the sockets this event dialed precede its drain.
            if let Some(pending) = self
                .0
                .registry
                .lock()
                .expect("guest registry poisoned")
                .pending
                .get_mut(&id)
            {
                pending.sockets = None;
            }
            forwarder.await?;
        }
        match terminal {
            GuestControl::Drained { .. } => Ok(()),
            GuestControl::Failed { message, .. } if responded => {
                tracing::warn!(%message, "guest background work failed");
                Ok(())
            }
            GuestControl::Failed { message, .. } => {
                anyhow::bail!("guest handler failed: {message}")
            }
            GuestControl::Cancelled { .. } => anyhow::bail!("guest handler cancelled"),
            _ => anyhow::bail!("unexpected stateless guest terminal"),
        }
    }

    /// Observe memory without admitting work or changing event drain/idle state.
    #[cfg(test)]
    pub(crate) async fn memory_sample(&self) -> Result<Option<GuestMemorySample>> {
        if self
            .0
            .execution_gate
            .get()
            .is_some_and(|gate| gate.status().phase != Phase::Running)
        {
            return Ok(None);
        }
        let sample = crate::application_storage::handle()?;
        let message = HostControl::MemorySample {
            version: VERSION,
            sample: sample.clone(),
        }
        .encode()?;
        let receive = {
            let mut registry = self.0.registry.lock().expect("guest registry poisoned");
            ensure!(
                registry.running && !self.0.stop.is_cancelled(),
                "guest transport retired"
            );
            if !self.0.memory_diagnostics || registry.memory_sample.is_some() {
                return Ok(None);
            }
            let (reply, receive) = oneshot::channel();
            // Enqueue while holding the registry lock: cancellation cannot leave
            // a nonce registered for a request that never reached the writer.
            self.0
                .outbound
                .try_send(Outbound::Frame(message))
                .context("guest diagnostic queue unavailable")?;
            registry.memory_sample = Some((sample, reply));
            receive
        };
        crate::asyncrt::select! {
            result = timeout(Duration::from_secs(1), receive) => {
                match result {
                    Ok(reply) => Ok(Some(reply.context("guest memory sample retired")?)),
                    // Keep the nonce until its matching reply or retirement.
                    Err(_) => Ok(None),
                }
            },
            _ = self.0.stop.cancelled() => anyhow::bail!("guest transport retired"),
        }
    }

    /// Whether the guest asked to be replaced (`GuestControl::Condemn`).
    pub(crate) fn is_condemned(&self) -> bool {
        self.0.condemned.is_cancelled()
    }

    /// Guest events not yet retired, including work after its reply.
    pub(crate) fn pending(&self) -> usize {
        self.0
            .registry
            .lock()
            .expect("guest registry poisoned")
            .pending
            .len()
    }

    #[cfg(test)]
    pub(crate) fn joined(&self, id: &str) -> bool {
        self.0
            .registry
            .lock()
            .expect("guest registry poisoned")
            .pending
            .get(id)
            .is_some_and(|pending| pending.terminal_seen)
    }

    // Each parameter names one independent input of a guest event.
    #[allow(clippy::too_many_arguments)]
    async fn event(
        &self,
        access: GuestStorage,
        request: Option<HttpRequest>,
        body: Option<crate::js::HttpChunkStream>,
        mut reply: Option<oneshot::Sender<Result<HttpResponse>>>,
        retiring: Option<&str>,
        rpc: Option<String>,
        socket: Option<SocketInvocation>,
    ) -> Result<Option<HttpResponse>> {
        let execution = access.execution_hold();
        ensure!(
            crate::asyncrt::mono_ms() < access.execution_deadline_ms(),
            "guest event deadline exceeded before delivery"
        );
        ensure!(
            self.0.execution_gate.get().is_none() || execution.is_some(),
            "native guest event lacks execution ownership"
        );

        ensure!(
            access.application()? == &self.0.application,
            "guest transport belongs to another application"
        );
        ensure!(
            retiring.is_some() || !self.0.reply_drain || reply.is_some(),
            "guest reply channel unavailable"
        );
        let id = crate::application_storage::handle()?;
        let storage_residency = access.retain_read_cursors(&self.0.cursors).await?;
        if retiring.is_none() {
            self.attach_container(&access).await?;
        }
        if let Some(expected) = retiring {
            ensure!(
                storage_residency == expected,
                "object retirement residency changed"
            );
        } else if self.0.object_retirement {
            access
                .register_retirement(GuestObject {
                    owner: Arc::downgrade(&self.0),
                    target: access.object()?.clone(),
                    residency: storage_residency.clone(),
                })
                .await?;
        }
        let socket_event = socket.is_some();
        let invoke = if retiring.is_some() {
            HostControl::RetireObject {
                version: VERSION,
                invocation: id.clone(),
                storage_grant: access.grant().into(),
                storage_residency: storage_residency.clone(),
                object: access.object()?.clone(),
            }
        } else {
            HostControl::InvokeHttp(Box::new(InvokeHttp {
                version: VERSION,
                invocation: id.clone(),
                storage_grant: access.grant().into(),
                storage_residency: Some(storage_residency.clone()),
                object: if self.0.resident_objects {
                    Some(access.object()?.clone())
                } else {
                    None
                },
                request: request.context("guest HTTP request unavailable")?,
                request_stream: body.is_some(),
                rpc,
                queue: false,
                service: None,
                alarm: access.alarm(),
                socket,
            }))
        };
        let bytes = invoke.encode()?;
        let (storage, channel) = oneshot::channel();
        let (terminal, result) = oneshot::channel();
        let (response, response_ready) = oneshot::channel();
        let settled = CancellationToken::new();
        let (demand, demand_ready) = mpsc::channel(1);
        let (chunks, chunk_ready) = mpsc::channel(1);
        let body_dropped = CancellationToken::new();
        let (request_pulls, request_credits) = mpsc::channel(1);
        let (frames, frame_ready) = mpsc::channel(SOCKET_WINDOW);
        {
            let mut registry = self.0.registry.lock().expect("guest registry poisoned");
            ensure!(
                registry.running && !self.0.stop.is_cancelled(),
                "guest transport retired"
            );
            ensure!(
                retiring.is_some() || !registry.retiring_objects.contains(&storage_residency),
                "guest object is retiring"
            );
            ensure!(
                registry.pending.len() < MAX_ACTIVE,
                "guest invocation capacity reached"
            );
            if registry.retired.len() + registry.pending.len() >= MAX_EVENTS {
                self.0.stop.cancel();
                anyhow::bail!("guest replay ledger capacity reached; renew VM");
            }
            ensure!(
                !registry.retired.contains(&id) && !registry.pending.contains_key(&id),
                "guest invocation nonce collision"
            );
            registry.pending.insert(
                id.clone(),
                Pending {
                    execution: execution.clone(),
                    retirement: retiring.is_some(),
                    grant: access.grant().into(),
                    storage: Some(storage),
                    response: Some(response),
                    response_seen: false,
                    terminal: Some(terminal),
                    terminal_seen: false,
                    connected: false,
                    cleanup_connected: false,
                    access: Some(access.clone()),
                    cancelled: false,
                    deadline: Some(Instant::now() + IO_DEADLINE),
                    settled: settled.clone(),
                    stream: (self.0.response_streams && retiring.is_none()).then(|| {
                        PendingStream {
                            flow: ResponseFlow::default(),
                            head_seen: false,
                            demand,
                            chunks,
                            dropped: body_dropped.clone(),
                        }
                    }),
                    request: body
                        .is_some()
                        .then(|| (ResponseFlow::default(), request_pulls)),
                    fetches: HashMap::new(),
                    app: access.output().ok().map(|output| output.app.clone()),
                    sockets: retiring.is_none().then_some(frames),
                    dials: HashMap::new(),
                    issued_sockets: HashMap::new(),
                },
            );
            registry
                .native_events
                .retain(|(_, event)| !event.is_finished());
            registry
                .native_events
                .push((storage_residency, access.clone()));
        }
        let _registration = Registration {
            shared: self.0.clone(),
            id: id.clone(),
        };
        access.register_settlement(crate::application_guest::GuestSettlement {
            event: settled.clone(),
            process: self.0.reaped.clone(),
        })?;
        if self.0.outbound.try_send(Outbound::Frame(bytes)).is_err() {
            self.0.stop.cancel();
            anyhow::bail!("guest invocation queue unavailable");
        }
        // A streamed RPC uploads its arguments before it connects storage, so
        // the pump must already be answering credits while we wait.
        let request_pump = self.request_body(&id, body, request_credits);
        tokio::pin!(request_pump);
        let mut socket = crate::asyncrt::select! {
            socket = channel => socket.context("guest storage connection retired")?,
            result = &mut request_pump => {
                result?;
                anyhow::bail!("guest request pump stopped")
            }
        };
        let storage = async {
            while let Some(bytes) = read_frame(&mut socket).await? {
                let request = Request::decode(&bytes)?;
                let abort = matches!(request.operation, Operation::AbortObject { .. });
                let reply = access
                    .execute_while(request, || {
                        !self.0.stop.is_cancelled() && !settled.is_cancelled()
                    })
                    .await?;
                if abort
                    && matches!(
                        reply.outcome,
                        Outcome::Ok {
                            value: Value::Unit {}
                        }
                    )
                {
                    // Native refusal is latched before retiring the transport.
                    // Do not rely on the guest to honor its termination call.
                    self.retire();
                    anyhow::bail!("guest object aborted");
                }
                timeout(IO_DEADLINE, write_frame(&mut socket, &reply.encode()?))
                    .await
                    .context("guest storage reply timed out")??;
            }
            // Closing storage is not a terminal HTTP event. The control reader
            // stays alive until a result, cancellation or supervisor retirement.
            std::future::pending::<Result<()>>().await
        };
        tokio::pin!(storage);
        tokio::pin!(result, response_ready);
        let body = self.response_body(&id, Some(&access), demand_ready, chunk_ready);
        tokio::pin!(body);
        let forwarder = async {
            if retiring.is_some() {
                std::future::pending().await
            } else {
                self.forward_sockets(&id, Some(&access), frame_ready).await
            }
        };
        tokio::pin!(forwarder);
        let mut streaming = false;
        let mut body_done = false;
        let mut joined = None;
        let alarm = access.alarm().is_some();
        // An alarm or a socket event answers nothing a client reads: its
        // terminal is its only result.
        let silent = alarm || socket_event;
        let mut response_open = self.0.reply_drain && retiring.is_none() && !silent;
        let mut responded = false;
        let terminal = loop {
            if (!streaming || body_done) && joined.is_some() {
                break joined.take().expect("joined terminal");
            }
            crate::asyncrt::select_biased! {
                "a ready response is delivered before a tied terminal so the terminal is judged as work after the response";
                body = &mut response_ready, if response_open => {
                    response_open = false;
                    if let Ok(body) = body {
                        // The upgrade's socket joins the core while this event still
                        // pins its object; a refusal drops the response and the socket.
                        if let Some(crate::js::HttpResponseWebSocket::Guest(socket)) = &body.websocket {
                            socket.register().await?;
                        }
                        streaming = body.stream.is_some();
                        reply.take().expect("scoped guest reply channel").send(Ok(body))
                            .map_err(|_| anyhow::anyhow!("guest reply consumer retired"))?;
                        responded = true;
                    }
                }
                terminal = &mut result, if joined.is_none() => {
                    let terminal = terminal.context("guest control retired")?;
                    if matches!(terminal, GuestControl::Drained { .. }) && streaming && !body_done {
                        joined = Some(terminal);
                    } else { break terminal; }
                }
                result = &mut body, if streaming && !body_done => { result?; body_done = true; },
                _ = body_dropped.cancelled(), if streaming && !body_done =>
                    anyhow::bail!("guest response consumer retired"),
                result = &mut request_pump => result?,
                result = &mut forwarder => { result?; anyhow::bail!("guest socket forwarder stopped") },
                result = &mut storage => { result?; anyhow::bail!("guest storage pump stopped") },
            }
        };
        if matches!(terminal, GuestControl::Drained { .. }) {
            // Every frame precedes the drain on the ordered control stream, so
            // closing the queue lets the forwarder release them all before this
            // event settles; a failed event's unreleased frames are dropped.
            if let Some(pending) = self
                .0
                .registry
                .lock()
                .expect("guest registry poisoned")
                .pending
                .get_mut(&id)
            {
                pending.sockets = None;
            }
            forwarder.await?;
        }
        match terminal {
            GuestControl::HttpResult(result) => Ok(Some(buffered(result.response))),
            GuestControl::Drained { .. } if silent => Ok(Some(buffered(
                celld_applications::invocation_wire::HttpResponse {
                    status: 204,
                    headers: vec![],
                    body: vec![],
                },
            ))),
            GuestControl::Drained { .. } => Ok(None),
            GuestControl::ObjectRetired { .. } => Ok(None),
            GuestControl::Failed { message, .. } if responded => {
                // Joined background failure cannot replace a settled handler's
                // reply. Native settlement/proof can still refuse its output.
                tracing::warn!(%message, "guest background work failed");
                Ok(None)
            }
            GuestControl::Failed { message, .. } if alarm => {
                Err(crate::application_guest::AlarmHandlerFailure(message).into())
            }
            GuestControl::Failed { message, .. } => {
                anyhow::bail!("guest handler failed: {message}")
            }
            GuestControl::Cancelled { .. } => anyhow::bail!("guest handler cancelled"),
            GuestControl::Ready { .. }
            | GuestControl::Condemn { .. }
            | GuestControl::MemorySample { .. }
            | GuestControl::IdleStatus { .. }
            | GuestControl::HttpReply(_) => {
                unreachable!("nonterminal cannot route as a terminal")
            }
            GuestControl::HttpStreamHead(_)
            | GuestControl::ResponseChunk { .. }
            | GuestControl::ResponseEnd { .. } => {
                unreachable!("stream frames cannot route as terminals")
            }
            GuestControl::RequestPull { .. }
            | GuestControl::FetchStart { .. }
            | GuestControl::FetchRequestChunk { .. }
            | GuestControl::FetchRequestEnd { .. }
            | GuestControl::FetchPull { .. }
            | GuestControl::FetchCancel { .. }
            | GuestControl::FetchSignal { .. }
            | GuestControl::SocketFrame { .. } => {
                unreachable!("event I/O cannot route as a terminal")
            }
        }
    }

    /// Releases an event's frames on its sockets, in order: each is proved under
    /// the event's own output gate (an object event; a stateless one has no
    /// storage to prove), then enters the socket's stream, and only then
    /// returns its credit to the guest. Its sockets are its own object's
    /// hibernatable ones and the outbound sockets it dialed.
    async fn forward_sockets(
        &self,
        id: &str,
        access: Option<&GuestStorage>,
        mut frames: mpsc::Receiver<(SocketTarget, SocketFrame)>,
    ) -> Result<()> {
        use crate::js::WsOut;
        let scope = match access {
            Some(access) => {
                let object = access.object()?;
                Some(format!("{}:{}", object.class, object.id))
            }
            None => None,
        };
        let credit = || async {
            self.send(HostControl::SocketCredit {
                version: VERSION,
                invocation: id.into(),
                count: 1,
            })
            .await
        };
        while let Some((target, frame)) = frames.recv().await {
            // A socket that already closed drops its frames, as in the original;
            // another object's socket is a guest fault.
            let ws = match target {
                SocketTarget::Registry(ws) => {
                    let scope = scope
                        .as_deref()
                        .context("a stateless event has no registered sockets")?;
                    match crate::js::ws_scope_of(ws) {
                        Some(owner) => {
                            ensure!(
                                owner == scope,
                                "guest sent a frame on a socket outside its object"
                            );
                            Some(ws)
                        }
                        None => None,
                    }
                }
                SocketTarget::Dial(fetch) => self
                    .0
                    .registry
                    .lock()
                    .expect("guest registry poisoned")
                    .pending
                    .get(id)
                    .and_then(|pending| pending.dials.get(&fetch))
                    .map(|dialed| dialed.id),
            };
            let Some(ws) = ws else {
                credit().await?;
                continue;
            };
            if let Some(access) = access {
                if let Err(error) = access.output()?.prove(access).await {
                    // The frames describe a write the fleet may never have.
                    if let Some(scope) = &scope {
                        crate::js::ws_close_scope(scope, 1011, "durability unproven");
                    }
                    return Err(error);
                }
            }
            crate::js::ws_emit_batch(vec![(
                ws,
                match frame {
                    SocketFrame::Text { data } => WsOut::Text(data),
                    SocketFrame::Binary { data } => WsOut::Binary(data),
                    SocketFrame::Close { code, reason } => WsOut::Close(code, reason),
                },
            )]);
            credit().await?;
        }
        Ok(())
    }

    /// Dial an outbound WebSocket for a guest event, through the node's own
    /// connector (`outbound_websocket_task`): the host resolves, connects,
    /// negotiates and answers pings; the guest sees the handshake outcome and,
    /// frame by frame under its own credit, what the peer sends.
    async fn dial(
        &self,
        invocation: &str,
        fetch: &str,
        request: HttpRequest,
        dial: celld_applications::invocation_wire::SocketDial,
    ) -> Result<(u16, Vec<(String, String)>, crate::js::HttpChunkStream)> {
        use futures_util::StreamExt;
        let id = crate::js::ws_next_id();
        let (pull, inbound) = crate::js::ws_pull_channel();
        let (reply, opened) = oneshot::channel();
        crate::js::outbound_ws_tx()
            .context("this node cannot open outbound WebSockets")?
            .send(crate::js::OutboundWsReq {
                scope: String::new(),
                id,
                url: request.url,
                protocols: dial.protocols,
                pull: Some(pull),
                headers: request.headers,
                want_response: dial.response,
                target: None,
                facet: None,
                reply,
            })
            .map_err(|_| anyhow::anyhow!("the outbound WebSocket connector stopped"))?;
        let opened = timeout(DIAL_DEADLINE, opened)
            .await
            .context("outbound WebSocket handshake timed out")?
            .context("the outbound WebSocket connector dropped the request")??;
        if let Some(declined) = opened.declined {
            // The server answered without upgrading: an ordinary response.
            let body = futures_util::stream::once(std::future::ready(Ok(declined.body)));
            return Ok((declined.status, declined.headers, body.boxed()));
        }
        // From here the socket is registered for output; the guard closes it.
        let guard = DialedSocket { id };
        {
            let mut registry = self.0.registry.lock().expect("guest registry poisoned");
            let pending = registry
                .pending
                .get_mut(invocation)
                .context("guest event retired")?;
            // `FetchCancel` removes the fetch and its dial in one lock. The
            // guest can cancel while this dial is still in flight, so register
            // only while its fetch is still pending: otherwise the dial outlives
            // the fetch that owns it, leaking a MAX_FETCHES slot and a peer
            // connection that no cancel can reach. The guard unwinds when the
            // early return drops it.
            ensure!(
                pending.fetches.contains_key(fetch),
                "dialed fetch was cancelled"
            );
            ensure!(
                pending.dials.len() < MAX_FETCHES,
                "too many outbound WebSockets in one event"
            );
            pending.dials.insert(fetch.to_owned(), guard);
        }
        let headers = opened
            .protocol
            .map(|protocol| vec![("sec-websocket-protocol".to_owned(), protocol)])
            .unwrap_or_default();
        Ok((101, headers, dial_chunks(inbound).boxed()))
    }

    /// A message or close of a hibernatable socket, run through the original
    /// `webSocketMessage`/`webSocketClose` of the socket's object.
    pub(crate) async fn invoke_socket(
        &self,
        app: crate::actor::AppHandle,
        object: ObjectTarget,
        ws: u64,
        event: SocketEvent,
        body: Vec<u8>,
    ) -> Result<crate::application_guest::GuestCompletion<HttpResponse>> {
        let (resource, deadline) = self.alarm_namespace(&object)?;
        let deadline_ms = crate::application_guest::execution_deadline_ms(deadline)?;
        let execution = self.enter_execution(deadline_ms, &self.0.stop).await?;
        let client = self.clone();
        let socket = SocketInvocation { ws, event };
        crate::application_guest::invoke_object_event(
            app,
            resource,
            object,
            None,
            deadline_ms,
            execution,
            None,
            Some(ws),
            move |access, reply| async move {
                client
                    .event(
                        access,
                        Some(HttpRequest {
                            method: "POST".into(),
                            url: "http://celld.internal/socket".into(),
                            headers: vec![],
                            body,
                        }),
                        None,
                        Some(reply),
                        None,
                        None,
                        Some(socket),
                    )
                    .await
            },
        )
        .await
    }

    /// Hands a callee's accepted socket to the stateless event that asked for
    /// the call, returning the id its reply may give back once.
    fn issue_socket(&self, invocation: &str, socket: GuestSocket) -> Result<u64> {
        let id = socket.key();
        let mut registry = self.0.registry.lock().expect("guest registry poisoned");
        let pending = registry
            .pending
            .get_mut(invocation)
            .context("guest event retired")?;
        ensure!(
            pending.access.is_none() && pending.issued_sockets.len() < 16,
            "this event cannot take a callee's socket"
        );
        pending.issued_sockets.insert(id, socket);
        Ok(id)
    }

    async fn send(&self, message: HostControl) -> Result<()> {
        self.0
            .outbound
            .send(Outbound::Frame(message.encode()?))
            .await
            .map_err(|_| anyhow::anyhow!("guest control writer stopped"))
    }

    /// Answers each guest credit with one request-body item, so an upload
    /// holds at most one bounded chunk in flight. A stalled upload parks only
    /// this pump; storage, control and the response keep moving.
    async fn request_body(
        &self,
        id: &str,
        body: Option<crate::js::HttpChunkStream>,
        mut credits: mpsc::Receiver<u64>,
    ) -> Result<()> {
        use futures_util::StreamExt;
        let Some(mut body) = body else {
            return std::future::pending().await;
        };
        let mut held = Vec::new();
        loop {
            let sequence = credits
                .recv()
                .await
                .context("guest request credit retired")?;
            while held.is_empty() {
                match body.next().await {
                    Some(Ok(chunk)) => held = chunk,
                    end => {
                        let error = matches!(end, Some(Err(_)))
                            .then(|| "request body ended before completion".to_owned());
                        self.request_ledger(id, sequence, true)?;
                        self.send(HostControl::RequestEnd {
                            version: VERSION,
                            invocation: id.into(),
                            sequence,
                            error,
                        })
                        .await?;
                        return std::future::pending().await;
                    }
                }
            }
            let rest = held.split_off(held.len().min(MAX_CHUNK_BYTES));
            let data = std::mem::replace(&mut held, rest);
            self.request_ledger(id, sequence, false)?;
            self.send(HostControl::RequestChunk {
                version: VERSION,
                invocation: id.into(),
                sequence,
                data,
            })
            .await?;
        }
    }

    fn request_ledger(&self, id: &str, sequence: u64, end: bool) -> Result<()> {
        let mut registry = self.0.registry.lock().expect("guest registry poisoned");
        let (flow, _) = registry
            .pending
            .get_mut(id)
            .and_then(|pending| pending.request.as_mut())
            .context("guest request stream retired")?;
        if end {
            flow.end(sequence)
        } else {
            flow.chunk(sequence)
        }
    }

    /// Runs alongside storage/control. A proof wait cannot park the pump which
    /// releases the original native transaction/turn that closes output.
    async fn response_body(
        &self,
        id: &str,
        access: Option<&GuestStorage>,
        mut demand: mpsc::Receiver<BodyRequest>,
        mut chunks: mpsc::Receiver<GuestControl>,
    ) -> Result<()> {
        let output = access.map(GuestStorage::output).transpose()?;
        loop {
            let consumer = demand
                .recv()
                .await
                .context("guest response consumer retired")?;
            let sequence = {
                let mut registry = self.0.registry.lock().expect("guest registry poisoned");
                let pending = registry
                    .pending
                    .get_mut(id)
                    .context("guest event retired")?;
                let stream = pending
                    .stream
                    .as_mut()
                    .context("guest response stream unavailable")?;
                ensure!(
                    stream.head_seen && !pending.cancelled,
                    "guest response stream retired"
                );
                let sequence = stream.flow.next_sequence();
                stream.flow.grant(sequence)?;
                pending.deadline = Some(Instant::now() + IO_DEADLINE);
                sequence
            };
            self.0
                .outbound
                .try_send(Outbound::Frame(
                    HostControl::ResponsePull {
                        version: VERSION,
                        invocation: id.into(),
                        sequence,
                    }
                    .encode()?,
                ))
                .context("guest response pull queue unavailable")?;
            let frame = chunks
                .recv()
                .await
                .context("guest response transport retired")?;
            // Guest positions are never consulted. The original admitted
            // event samples and gates under its trusted Actor request.
            if let (Some(output), Some(access)) = (&output, access) {
                output.prove(access).await?;
            }
            match frame {
                GuestControl::ResponseChunk { data, .. } => {
                    consumer
                        .send(Ok(Some(data)))
                        .map_err(|_| anyhow::anyhow!("guest response consumer retired"))?;
                }
                GuestControl::ResponseEnd { error, .. } => {
                    let result = if error.is_some() {
                        Err("guest response producer failed".into())
                    } else {
                        Ok(None)
                    };
                    consumer
                        .send(result)
                        .map_err(|_| anyhow::anyhow!("guest response consumer retired"))?;
                    return Ok(());
                }
                _ => anyhow::bail!("unexpected guest response body frame"),
            }
        }
    }
}

/// The one request a KV namespace admits: its own shard and a client method.
fn authorize_kv(resource: &ResourceAddress, call: &ObjectCall) -> Result<String> {
    call.object.validate()?;
    ensure!(
        resource.resource.kind == ResourceKind::Kv
            && crate::application_javascript_guest::namespace_owns_object(
                resource,
                celld_logic::kv::RESERVED_CLASS,
                &call.object
            ),
        "invalid application KV shard"
    );
    let method = call
        .rpc
        .clone()
        .context("application KV requires an RPC method")?;
    ensure!(
        matches!(
            method.as_str(),
            "__kvGet" | "__kvPut" | "__kvDelete" | "__kvList"
        ),
        "unsupported application KV method"
    );
    Ok(method)
}

/// The one request a queue admits: its own cell and a producer method.
fn authorize_queue(resource: &ResourceAddress, call: &ObjectCall) -> Result<String> {
    call.object.validate()?;
    ensure!(
        resource.resource.kind == ResourceKind::Queue
            && crate::application_javascript_guest::namespace_owns_object(
                resource,
                celld_logic::queue::RESERVED_CLASS,
                &call.object
            ),
        "invalid application queue"
    );
    let method = call
        .rpc
        .clone()
        .context("application queue requires an RPC method")?;
    ensure!(
        matches!(method.as_str(), "__queueSend" | "__queueMetrics"),
        "unsupported application queue method"
    );
    Ok(method)
}

/// The caller has resolved an exact bound resource/class pair. The native
/// original KV and Queue builtins need their activation epoch and gated egress,
/// which are never credentials of the application's VM.
pub(crate) async fn native_call(
    app: crate::actor::AppHandle,
    resource: ResourceAddress,
    call: ObjectCall,
    request: HttpRequest,
    deadline: Duration,
) -> Result<HttpResponse> {
    let method = match resource.resource.kind {
        ResourceKind::Queue => authorize_queue(&resource, &call)?,
        _ => authorize_kv(&resource, &call)?,
    };
    #[cfg(target_os = "linux")]
    if let Some(host) = crate::application_vm::hosted() {
        host.register_cell(&resource, &call.object).await?;
    }
    ensure!(!deadline.is_zero(), "native deadline must be positive");
    let queue_name = (resource.resource.kind == ResourceKind::Queue)
        .then(|| celld_logic::queue::cell_name(resource.resource.id.as_str()).to_owned());
    // Once admitted, the owned task settles the original event and output
    // proof even when a VM/caller disconnects or its deadline expires.
    let (reply, receive) = oneshot::channel();
    crate::asyncrt::spawn(async move {
        let result = async {
            let _public = app.admit_continuation().context("node is draining")?;
            let cell = format!("{}:{}", call.object.class, call.object.id);
            let routed = timeout(deadline, app.request(cell.clone()))
                .await
                .context("native routing deadline exceeded")?
                .map_err(|error| anyhow::anyhow!("native route refused: {error:?}"))?;
            if routed.route != celld_logic::Route::Local {
                return Err(crate::application_guest::RemoteOwner.into());
            }
            let local = app.local_request(routed.request, cell.clone(), None, "application_kv");
            let completed = local
                .run(async {
                    app.runtime
                        .as_ref()
                        .context("native runtime is unavailable")?
                        .rpc(
                            cell,
                            // authorization pinned the canonical scope; the
                            // trusted pool holds no application namespace keys
                            // but takes a queue's name from the host, never
                            // from the caller.
                            queue_name,
                            method,
                            crate::js::RpcData::V8(request.body.into()),
                            None,
                        )
                        .await
                })
                .await;
            let answer = completed.result.map_err(|failure| match failure {
                crate::actor::LocalRequestFailure::Handler(error) => error,
                crate::actor::LocalRequestFailure::OutputGate { verdict, handler } => {
                    let message = format!("application native output refused: {verdict:?}");
                    match handler {
                        Some(error) => error.context(message),
                        None => anyhow::anyhow!(message),
                    }
                }
            })?;
            let crate::js::RpcData::V8(bytes) = answer.data else {
                anyhow::bail!("native call answered JSON to a structured-clone call");
            };
            Ok(response(
                200,
                vec![("content-type".into(), "application/octet-stream".into())],
                bytes.to_vec(),
                None,
            ))
        }
        .await;
        let _ = reply.send(result);
    });
    timeout(deadline, receive)
        .await
        .context("native call deadline exceeded")?
        .context("native driver stopped before settlement")?
}

/// The trusted host performs one guest fetch, or its object call. Whatever it
/// can reveal must be durable first: an object event's output gate is sampled
/// and proven before the request leaves, exactly as for its response. The
/// reply body follows one guest credit per bounded chunk.
// A brokered fetch carries one handle per independent channel it owns.
#[allow(clippy::too_many_arguments)]
async fn broker_fetch(
    client: Client,
    invocation: String,
    fetch: String,
    mut request: HttpRequest,
    redirect: String,
    target: Option<ObjectCall>,
    service: Option<celld_applications::invocation_wire::ServiceCall>,
    resource: Option<celld_applications::resource_wire::ResourceCall>,
    app: Option<crate::actor::AppHandle>,
    access: Option<GuestStorage>,
    mut credits: mpsc::Receiver<u64>,
    stop: CancellationToken,
    upload: Option<FetchUpload>,
    relay: SignalRelay,
    dial: Option<celld_applications::invocation_wire::SocketDial>,
    execution: Option<EventHold>,
) {
    let _execution = execution;
    use futures_util::StreamExt;
    let ledger = |sequence: u64, end: bool| -> Result<()> {
        let mut registry = client.0.registry.lock().expect("guest registry poisoned");
        let pending = registry
            .pending
            .get_mut(&invocation)
            .context("guest event retired")?;
        let state = pending
            .fetches
            .get_mut(&fetch)
            .context("guest fetch retired")?;
        if end {
            state.flow.end(sequence)?;
            Ok(())
        } else {
            state.flow.chunk(sequence)
        }
    };
    let end = |sequence: u64, error: Option<String>| HostControl::FetchEnd {
        version: VERSION,
        invocation: invocation.clone(),
        fetch: fetch.clone(),
        sequence,
        error,
    };
    let work = async {
        if let Some(access) = &access {
            access.output()?.prove(access).await?;
        }
        let mut service_body = None;
        if let Some((length, mut chunks)) = upload {
            // Authenticate the namespace before retaining a large body. The
            // declaring length adds no resource or storage authority.
            if let Some(call) = target.as_ref() {
                client.namespace(&call.object)?;
            } else if let Some(call) = service.as_ref() {
                client.service_target(call)?;
            } else if let Some(call) = resource.as_ref() {
                client.resource_target(call)?;
            } else {
                anyhow::bail!("upload is not a bound call");
            }
            if service.is_some() {
                service_body = Some(service_request_body(
                    client.clone(),
                    invocation.clone(),
                    fetch.clone(),
                    chunks,
                    access.clone(),
                    _execution.clone(),
                ));
            } else {
                let length = length.context("object upload requires a declared length")?;
                ensure!(
                    length <= celld_applications::invocation_wire::MAX_OBJECT_CALL_BYTES as u64,
                    "object request exceeds limit"
                );
                request.body = Vec::new();
                for sequence in 0.. {
                    {
                        let mut registry =
                            client.0.registry.lock().expect("guest registry poisoned");
                        let upload = registry
                            .pending
                            .get_mut(&invocation)
                            .context("event retired")?
                            .fetches
                            .get_mut(&fetch)
                            .context("fetch retired")?
                            .request
                            .as_mut()
                            .context("upload retired")?;
                        upload.flow.grant(sequence)?;
                    }
                    client
                        .send(HostControl::FetchRequestPull {
                            version: VERSION,
                            invocation: invocation.clone(),
                            fetch: fetch.clone(),
                            sequence,
                        })
                        .await?;
                    match chunks
                        .recv()
                        .await
                        .context("object upload retired")?
                        .map_err(anyhow::Error::msg)?
                    {
                        Some(chunk) => {
                            ensure!(
                                request.body.len().saturating_add(chunk.len()) <= length as usize,
                                "object request exceeds declared size"
                            );
                            request.body.extend(chunk);
                        }
                        None => break,
                    }
                }
                ensure!(
                    request.body.len() == length as usize,
                    "object upload size mismatch"
                );
            }
        }
        // The callee's admission and gates live as long as its reply streams.
        let mut _callee = None;
        let mut issued = None;
        let (status, headers, mut body): (_, _, crate::js::HttpChunkStream) =
            if let Some(resource) = resource {
                let address = client.resource_target(&resource)?;
                let response = match resource.operation {
                    celld_applications::resource_wire::ResourceOperation::R2 { operation } => {
                        let fleet = crate::host_channels::R2_STORE
                            .get()
                            .context("R2 bindings need a fleet bucket")?;
                        fleet.validate_r2_bucket_name(address.resource.id.as_str())?;
                        let bucket = if fleet.backend == crate::bucket::StorageBackend::Local {
                            address.storage_root().trim_end_matches('/').to_owned()
                        } else {
                            address.resource.id.as_str().to_owned()
                        };
                        crate::js::r2_ops::broker_operation(
                            bucket,
                            address.storage_root(),
                            operation,
                            request.body,
                        )
                        .await
                        .map_err(anyhow::Error::msg)?
                    }
                    celld_applications::resource_wire::ResourceOperation::Container {
                        operation,
                    } => {
                        let access = access
                            .as_ref()
                            .context("container call has no object event")?;
                        client.authorize_container(&address, access)?;
                        crate::js::container::broker_operation(access, operation, request)
                            .await
                            .map_err(anyhow::Error::msg)?
                    }
                    celld_applications::resource_wire::ResourceOperation::Assets {} => {
                        let deployment = client
                            .0
                            .services
                            .get()
                            .map(|(deployment, _)| deployment)
                            .context("guest assets call has no pinned deployment")?;
                        let handler = client
                            .0
                            .handler
                            .get()
                            .context("guest assets call has no pinned handler")?;
                        client
                            .0
                            .service_routes
                            .get()
                            .and_then(std::sync::Weak::upgrade)
                            .context("application asset router is unavailable")?
                            .asset_resolver(&client.0.application, deployment, handler)
                            .await?
                            .context("handler declares no static assets")?
                            .binding_response(&request.url, &request.method, &request.headers)
                            .await?
                    }
                };
                let body = futures_util::stream::once(std::future::ready(Ok(response.body)));
                let body = match response.stream {
                    Some(stream) => body.chain(stream).boxed(),
                    None => body.boxed(),
                };
                (response.status, response.headers, body)
            } else if let Some(mut service) = service {
                let (deployment, handler) = client.service_target(&service)?;
                service.invocation.signal_owner = Some(client.0.signal_owner.clone());
                let app = app.context("guest service call has no Actor")?;
                let hosted = client
                    .0
                    .service_routes
                    .get()
                    .and_then(std::sync::Weak::upgrade)
                    .context("application service router is unavailable")?;
                let response = hosted
                    .call_service(
                        &app,
                        &client.0.application,
                        &deployment,
                        &handler,
                        request,
                        service.invocation,
                        service_body,
                        Some(relay),
                    )
                    .await?;
                let body = futures_util::stream::once(std::future::ready(Ok(response.body)));
                let body = match response.stream {
                    Some(stream) => body.chain(stream).boxed(),
                    None => body.boxed(),
                };
                (response.status, response.headers, body)
            } else if let Some(dial) = dial {
                client.dial(&invocation, &fetch, request, dial).await?
            } else if let Some(call) = target {
                let app = app.context("guest object call has no Actor")?;
                let client_app = Some(app.clone());
                let (mut response, completion) = client.call_or_forward(app, call, request).await?;
                _callee = completion;
                match response.websocket.take() {
                    None => {}
                    Some(crate::js::HttpResponseWebSocket::Guest(socket)) => {
                        issued = Some(client.issue_socket(&invocation, *socket)?);
                    }
                    // The owner is another node: its 101 is parked here and the
                    // client's own socket will be spliced to it.
                    Some(crate::js::HttpResponseWebSocket::Cell(target))
                        if target.tunnel.is_some() =>
                    {
                        let socket = GuestSocket::remote(
                            client_app.clone().context("remote upgrade has no Actor")?,
                            target,
                        );
                        issued = Some(client.issue_socket(&invocation, socket)?);
                    }
                    Some(_) => anyhow::bail!(
                        "this WebSocket response kind is not connected in application guests"
                    ),
                }
                let body = futures_util::stream::once(std::future::ready(Ok(response.body)));
                let body = match response.stream {
                    Some(stream) => body.chain(stream).boxed(),
                    None => body.boxed(),
                };
                (response.status, response.headers, body)
            } else {
                let response = egress_client(&redirect)?
                    .request(
                        reqwest::Method::from_bytes(request.method.as_bytes())?,
                        &request.url,
                    )
                    .headers(
                        request
                            .headers
                            .iter()
                            .map(|(name, value)| {
                                Ok((
                                    reqwest::header::HeaderName::from_bytes(name.as_bytes())?,
                                    reqwest::header::HeaderValue::from_str(value)?,
                                ))
                            })
                            .collect::<Result<reqwest::header::HeaderMap>>()?,
                    )
                    .body(request.body)
                    .send()
                    .await?;
                let headers = response
                    .headers()
                    .iter()
                    .filter_map(|(name, value)| {
                        Some((name.to_string(), value.to_str().ok()?.to_owned()))
                    })
                    .collect();
                let status = response.status().as_u16();
                let body = response.bytes_stream().map(|chunk| {
                    chunk
                        .map(|chunk| chunk.to_vec())
                        .map_err(|error| error.to_string())
                });
                (status, headers, body.boxed())
            };
        let head = HostControl::FetchHead {
            version: VERSION,
            invocation: invocation.clone(),
            fetch: fetch.clone(),
            status,
            headers,
            socket: issued,
        };
        client.send(head).await?;
        let mut held = bytes::Bytes::new();
        loop {
            let sequence = credits.recv().await.context("guest fetch credit retired")?;
            while held.is_empty() {
                match body.next().await {
                    Some(Ok(chunk)) => held = chunk.into(),
                    Some(Err(error)) => {
                        ledger(sequence, true)?;
                        client
                            .send(end(sequence, Some(format!("fetch body failed: {error}"))))
                            .await?;
                        finish_fetch(&client, &invocation, &fetch);
                        return Ok::<(), anyhow::Error>(());
                    }
                    None => {
                        ledger(sequence, true)?;
                        client.send(end(sequence, None)).await?;
                        finish_fetch(&client, &invocation, &fetch);
                        return Ok(());
                    }
                }
            }
            let data = held.split_to(held.len().min(MAX_CHUNK_BYTES)).to_vec();
            ledger(sequence, false)?;
            client
                .send(HostControl::FetchChunk {
                    version: VERSION,
                    invocation: invocation.clone(),
                    fetch: fetch.clone(),
                    sequence,
                    data,
                })
                .await?;
        }
    };
    crate::asyncrt::select! {
        result = work => if let Err(error) = result {
            // A refusal before the head carries no credit; the guest rejects
            // its fetch promise. The state is dropped either way.
            let message: String = format!("fetch failed: {error:#}").chars().take(4096).collect();
            let _ = client.send(end(0, Some(message))).await;
            if let Some(pending) = client.0.registry.lock().expect("guest registry poisoned").pending.get_mut(&invocation) {
                pending.fetches.remove(&fetch);
            }
        },
        _ = stop.cancelled() => {}
    }
}

fn service_request_body(
    client: Client,
    invocation: String,
    fetch: String,
    chunks: mpsc::Receiver<std::result::Result<Option<Vec<u8>>, String>>,
    access: Option<GuestStorage>,
    execution: Option<EventHold>,
) -> crate::js::HttpChunkStream {
    Box::pin(futures_util::stream::unfold(
        (
            client, invocation, fetch, chunks, access, execution, 0u64, false,
        ),
        |(client, invocation, fetch, mut chunks, access, execution, sequence, ended)| async move {
            if ended {
                return None;
            }
            let item = async {
                {
                    let mut registry = client.0.registry.lock().expect("guest registry poisoned");
                    let upload = registry
                        .pending
                        .get_mut(&invocation)
                        .context("event retired")?
                        .fetches
                        .get_mut(&fetch)
                        .context("fetch retired")?
                        .request
                        .as_mut()
                        .context("upload retired")?;
                    upload.flow.grant(sequence)?;
                }
                client
                    .send(HostControl::FetchRequestPull {
                        version: VERSION,
                        invocation: invocation.clone(),
                        fetch: fetch.clone(),
                        sequence,
                    })
                    .await?;
                let item = chunks.recv().await.context("service upload retired")?;
                // A later chunk/error can reveal storage observed after the
                // start. Sample the original event again before releasing it.
                if let Some(access) = &access {
                    access.output()?.prove(access).await?;
                }
                item.map_err(anyhow::Error::msg)
            }
            .await;
            match item {
                Ok(Some(chunk)) => Some((
                    Ok(chunk),
                    (
                        client,
                        invocation,
                        fetch,
                        chunks,
                        access,
                        execution,
                        sequence + 1,
                        false,
                    ),
                )),
                Ok(None) => None,
                Err(error) => Some((
                    Err(error.to_string()),
                    (
                        client, invocation, fetch, chunks, access, execution, sequence, true,
                    ),
                )),
            }
        },
    ))
}

fn finish_fetch(client: &Client, invocation: &str, fetch: &str) {
    // Retain the stop guard until the terminal is queued. Removing it sooner
    // cancels this task while a full outbound queue delays FetchEnd.
    if let Some(pending) = client
        .0
        .registry
        .lock()
        .expect("guest registry poisoned")
        .pending
        .get_mut(invocation)
    {
        pending.fetches.remove(fetch);
        pending.dials.remove(fetch);
    }
}

/// The host's network client for brokered fetches, one per original redirect
/// mode. The guest has no network device; this is its only egress.
pub(crate) fn egress_client(redirect: &str) -> Result<reqwest::Client> {
    static CLIENTS: std::sync::OnceLock<[reqwest::Client; 3]> = std::sync::OnceLock::new();
    let clients = CLIENTS.get_or_init(|| {
        let timeout = Duration::from_secs(
            crate::env_vars::positive_or("CELLD_FETCH_TIMEOUT_S", 120).unwrap_or(120),
        );
        let build = |policy| {
            reqwest::Client::builder()
                .redirect(policy)
                .timeout(timeout)
                .build()
                .expect("egress client")
        };
        [
            build(reqwest::redirect::Policy::limited(20)),
            build(reqwest::redirect::Policy::none()),
            build(reqwest::redirect::Policy::custom(|attempt| {
                attempt.error("redirect forbidden by fetch redirect mode")
            })),
        ]
    });
    Ok(match redirect {
        "follow" => clients[0].clone(),
        "manual" => clients[1].clone(),
        "error" => clients[2].clone(),
        _ => anyhow::bail!("unknown fetch redirect mode"),
    })
}

struct Retire(Arc<Shared>);
impl Drop for Retire {
    fn drop(&mut self) {
        if let Some(gate) = self.0.execution_gate.get() {
            gate.retire();
        }
        self.0.cursors.retire();
        let mut registry = self.0.registry.lock().expect("guest registry poisoned");
        registry.running = false;
        registry.pending.clear();
        registry.memory_sample = None;
        registry.idle_probe = None;
        self.0.drained.notify_waiters();
        self.0.stop.cancel();
        self.0.retired.cancel();
    }
}

async fn run(
    shared: Arc<Shared>,
    control: UnixStream,
    listener: UnixListener,
    mut outbound: mpsc::Receiver<Outbound>,
) -> Result<()> {
    let _retire = Retire(shared.clone());
    let (mut reader, mut writer) = control.into_split();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        while let Some(message) = outbound.recv().await {
            match message {
                Outbound::Frame(bytes) => {
                    timeout(IO_DEADLINE, write_frame(&mut writer, &bytes))
                        .await
                        .context("guest control write timed out")??;
                }
                Outbound::Fence(reply) => {
                    let _ = reply.send(());
                }
            }
        }
        anyhow::bail!("guest control writer stopped")
    });
    let read = shared.clone();
    tasks.spawn(async move {
        while let Some(bytes) = read_frame(&mut reader).await? {
            let terminal = GuestControl::decode(&bytes)?;
            if let GuestControl::MemorySample { sample, stats, .. } = terminal {
                ensure!(read.memory_diagnostics, "unadvertised memory sample reply");
                read.registry
                    .lock()
                    .expect("guest registry poisoned")
                    .memory_reply(&sample, stats)?;
                continue;
            }
            if let GuestControl::IdleStatus { probe, idle, .. } = terminal {
                ensure!(read.idle_lifecycle, "unadvertised idle reply");
                read.registry
                    .lock()
                    .expect("guest registry poisoned")
                    .idle_reply(&probe, idle)?;
                continue;
            }
            if let GuestControl::Condemn { reason, .. } = &terminal {
                tracing::warn!(%reason, "guest condemned its VM");
                read.condemned.cancel();
                continue;
            }
            let id = terminal
                .invocation()
                .context("duplicate guest readiness")?
                .to_owned();
            let mut registry = read.registry.lock().expect("guest registry poisoned");
            // A guest body pump can release a credit or a fetch after its
            // event's terminal. The host already stopped that I/O, so such a
            // frame from a known event is late and harmless.
            if matches!(
                terminal,
                GuestControl::RequestPull { .. }
                    | GuestControl::FetchPull { .. }
                    | GuestControl::FetchCancel { .. }
                    | GuestControl::FetchSignal { .. }
                    | GuestControl::FetchRequestChunk { .. }
                    | GuestControl::FetchRequestEnd { .. }
            ) && (registry.retired.contains(&id)
                || registry.pending.get(&id).is_some_and(|pending| {
                    pending.terminal_seen
                        || (pending.cancelled
                            && matches!(
                                terminal,
                                GuestControl::FetchRequestChunk { .. }
                                    | GuestControl::FetchRequestEnd { .. }
                            ))
                }))
            {
                continue;
            }
            let pending = registry
                .pending
                .get_mut(&id)
                .context("guest terminal names an unknown or retired invocation")?;
            ensure!(!pending.terminal_seen, "duplicate guest terminal");
            if terminal.is_event_io() {
                // A streamed RPC assembles its arguments before native turn
                // admission. Credits alone authorize no storage or effects.
                ensure!(
                    pending.connected
                        || (matches!(terminal, GuestControl::RequestPull { .. })
                            && pending.request.is_some()),
                    "guest event I/O before its connection"
                );
                match terminal {
                    GuestControl::RequestPull { sequence, .. } => {
                        let (flow, credits) = pending
                            .request
                            .as_mut()
                            .context("guest pulled an unstreamed request")?;
                        flow.grant(sequence)?;
                        credits
                            .try_send(sequence)
                            .context("guest request credit queue unavailable")?;
                        // Upload progress keeps an unconnected event alive.
                        if !pending.connected && !pending.cancelled {
                            pending.deadline = Some(Instant::now() + IO_DEADLINE);
                        }
                    }
                    GuestControl::FetchStart {
                        fetch,
                        request,
                        redirect,
                        target,
                        service,
                        resource,
                        request_stream,
                        request_bytes,
                        dial,
                        ..
                    } => {
                        ensure!(!pending.cancelled, "guest fetch from a cancelled event");
                        ensure!(
                            pending.fetches.len() < MAX_FETCHES
                                && !pending.fetches.contains_key(&fetch),
                            "guest fetch limit reached or duplicate fetch"
                        );
                        let (pulls, credits) = mpsc::channel(1);
                        let stop = CancellationToken::new();
                        let (relay, relays) = mpsc::unbounded_channel();
                        let signals: HashSet<String> = service
                            .iter()
                            .flat_map(|call| call.invocation.signals.iter().cloned())
                            .collect();
                        let (upload, request_upload) = match (request_bytes, request_stream) {
                            (length, true) | (length @ Some(_), false) => {
                                let (items, chunks) = mpsc::channel(1);
                                (
                                    Some((length, chunks)),
                                    Some(RequestUpload {
                                        flow: ResponseFlow::default(),
                                        remaining: length,
                                        items,
                                    }),
                                )
                            }
                            (None, false) => (None, None),
                        };
                        // Registered before its task starts: a dialing fetch
                        // proves it is still pending under the registry lock, and
                        // `FetchCancel` removes the fetch and its dial together in
                        // one lock. Spawning first would let the task observe its
                        // own registration as missing.
                        pending.fetches.insert(
                            fetch.clone(),
                            BrokeredFetch {
                                flow: ResponseFlow::default(),
                                pulls,
                                _stop: stop.drop_guard(),
                                request: request_upload,
                                signals,
                                relay,
                            },
                        );
                        crate::asyncrt::spawn(broker_fetch(
                            Client(read.clone()),
                            id.clone(),
                            fetch,
                            request,
                            redirect,
                            target.map(|call| *call),
                            service.map(|call| *call),
                            resource.map(|call| *call),
                            pending.app.clone(),
                            pending.access.clone(),
                            credits,
                            stop.clone(),
                            upload,
                            relays,
                            dial,
                            pending.execution.clone(),
                        ));
                    }
                    GuestControl::FetchPull {
                        fetch, sequence, ..
                    } => {
                        let state = pending
                            .fetches
                            .get_mut(&fetch)
                            .context("guest pulled an unknown fetch")?;
                        state.flow.grant(sequence)?;
                        state
                            .pulls
                            .try_send(sequence)
                            .context("guest fetch credit queue unavailable")?;
                    }
                    GuestControl::FetchRequestChunk {
                        fetch,
                        sequence,
                        data,
                        ..
                    } => {
                        // A late frame can follow the guest's FetchCancel.
                        let Some(state) = pending.fetches.get_mut(&fetch) else {
                            continue;
                        };
                        let upload = state.request.as_mut().context("chunk for buffered fetch")?;
                        upload.push(sequence, Some(data))?;
                    }
                    GuestControl::FetchRequestEnd {
                        fetch,
                        sequence,
                        error,
                        ..
                    } => {
                        let Some(state) = pending.fetches.get_mut(&fetch) else {
                            continue;
                        };
                        let upload = state.request.as_mut().context("end for buffered fetch")?;
                        if let Some(error) = error {
                            upload.flow.end(sequence)?;
                            upload
                                .items
                                .try_send(Err(error))
                                .context("service upload error exceeded credit")?;
                        } else {
                            upload.push(sequence, None)?;
                        }
                    }
                    GuestControl::FetchCancel { fetch, .. } => {
                        pending.fetches.remove(&fetch);
                        pending.dials.remove(&fetch);
                    }
                    GuestControl::SocketFrame {
                        ws, frame, fetch, ..
                    } => {
                        // A frame in flight when the host cancelled is late and harmless.
                        if pending.cancelled {
                            continue;
                        }
                        pending
                            .sockets
                            .as_ref()
                            .context("guest sent a socket frame outside an object event")?
                            .try_send((
                                match fetch {
                                    Some(fetch) => SocketTarget::Dial(fetch),
                                    None => SocketTarget::Registry(ws),
                                },
                                frame,
                            ))
                            .context("guest socket frame exceeded its credit")?;
                    }
                    GuestControl::FetchSignal {
                        fetch,
                        signal,
                        reason,
                        ..
                    } => {
                        // The fetch can end before its signal fires: nothing listens.
                        let Some(state) = pending.fetches.get(&fetch) else {
                            continue;
                        };
                        ensure!(
                            state.signals.contains(&signal),
                            "guest signaled an undeclared service signal"
                        );
                        let _ = state.relay.send((signal, reason));
                    }
                    _ => unreachable!("classified as event I/O"),
                }
                continue;
            }
            if let GuestControl::HttpStreamHead(head) = terminal {
                ensure!(
                    !pending.retirement && read.response_streams && read.reply_drain,
                    "guest did not advertise response streams"
                );
                ensure!(
                    pending.connected && !pending.response_seen,
                    "duplicate or unconnected guest stream head"
                );
                let stream = pending
                    .stream
                    .as_mut()
                    .context("guest stream unavailable")?;
                stream.head_seen = true;
                pending.response_seen = true;
                if let Some(sender) = pending.response.take() {
                    let output = pending
                        .access
                        .as_ref()
                        .map(GuestStorage::output)
                        .transpose()?;
                    let body = body_stream(ResponseBody {
                        demand: stream.demand.clone(),
                        dropped: stream.dropped.clone(),
                        _output: output,
                        execution: pending.execution.clone(),
                        ended: false,
                    });
                    let _ =
                        sender.send(response(head.status, head.headers, Vec::new(), Some(body)));
                }
                continue;
            }
            if matches!(
                terminal,
                GuestControl::ResponseChunk { .. } | GuestControl::ResponseEnd { .. }
            ) {
                ensure!(
                    read.response_streams && pending.connected && pending.response_seen,
                    "unadvertised or premature guest response body"
                );
                let stream = pending
                    .stream
                    .as_mut()
                    .context("guest stream unavailable")?;
                ensure!(stream.head_seen, "guest response body before stream head");
                match &terminal {
                    GuestControl::ResponseChunk { sequence, .. } => stream.flow.chunk(*sequence)?,
                    GuestControl::ResponseEnd { sequence, .. } => stream.flow.end(*sequence)?,
                    _ => unreachable!(),
                }
                pending.deadline = if pending.cancelled {
                    pending.deadline
                } else {
                    None
                };
                if !pending.cancelled {
                    stream
                        .chunks
                        .try_send(terminal)
                        .context("guest response chunk queue unavailable")?;
                }
                continue;
            }
            if let GuestControl::HttpReply(result) = terminal {
                ensure!(!pending.retirement, "retirement emitted an HTTP reply");
                ensure!(
                    read.reply_drain,
                    "guest did not advertise reply/drain support"
                );
                ensure!(
                    pending.connected && !pending.response_seen,
                    "duplicate or unconnected guest reply"
                );
                pending.response_seen = true;
                let socket = result.socket;
                let mut upgrade = buffered(result.response);
                if let Some(ws) = socket {
                    match claim_upgrade(pending, ws) {
                        Ok(websocket) => upgrade.websocket = Some(websocket),
                        Err(error) => {
                            // A bad upgrade fails this request, not the whole VM.
                            tracing::warn!(%error, "guest WebSocket upgrade refused");
                            upgrade =
                                response(502, vec![], b"invalid WebSocket upgrade".to_vec(), None);
                        }
                    }
                }
                if let Some(response) = pending.response.take() {
                    let _ = response.send(upgrade);
                }
                continue;
            }
            match &terminal {
                GuestControl::ObjectRetired { .. } => ensure!(
                    pending.retirement && pending.connected && !pending.response_seen,
                    "unexpected object retirement terminal"
                ),
                GuestControl::Drained { .. } => ensure!(
                    !pending.retirement && read.reply_drain && pending.response_seen,
                    "guest drain before reply"
                ),
                GuestControl::HttpResult(_) => ensure!(
                    !pending.retirement && !read.reply_drain,
                    "split guest sent legacy terminal response"
                ),
                _ => {}
            }
            if matches!(terminal, GuestControl::Drained { .. }) {
                ensure!(
                    pending
                        .stream
                        .as_ref()
                        .is_none_or(|stream| !stream.head_seen || stream.flow.is_closed()),
                    "guest drain before response body end"
                );
            }
            pending.terminal_seen = true;
            pending.response.take();
            pending.settled.cancel();
            if pending.cancelled {
                registry.pending.remove(&id);
                registry.retired.insert(id);
                read.drained.notify_waiters();
            } else if let Some(reply) = pending.terminal.take() {
                let _ = reply.send(terminal);
            }
        }
        let registry = read.registry.lock().expect("guest registry poisoned");
        ensure!(
            !registry.running && registry.pending.is_empty(),
            "guest control ended unexpectedly"
        );
        Ok(())
    });
    let accept = shared.clone();
    tasks.spawn(async move {
        let mut cleanup = JoinSet::new();
        // Facet event pumps end with their socket; a facet's failure ends only it.
        let mut facets = JoinSet::new();
        loop {
            let (mut socket, _) = crate::asyncrt::select! {
                accepted = listener.accept() => accepted?,
                result = cleanup.join_next(), if !cleanup.is_empty() => {
                    result.context("guest cleanup pump missing")?.context("guest cleanup pump panicked")??;
                    continue;
                }
                _ = facets.join_next(), if !facets.is_empty() => continue,
            };
            // Only bootstrap is serialized, for at most IO_DEADLINE. Each
            // admitted event's storage traffic is pumped by its own driver.
            let hello = timeout(IO_DEADLINE, read_frame_limited(&mut socket, 512))
                .await
                .context("guest storage handshake timed out")??
                .context("guest storage handshake ended")?;
            let hello = StorageConnect::decode(&hello)?;
            let mut registry = accept.registry.lock().expect("guest registry poisoned");
            if registry.retired.contains(&hello.invocation) {
                // A late connection can never recover retired authority. No
                // operation frame is read from it, regardless of its grant.
                continue;
            }
            let pending = registry
                .pending
                .get_mut(&hello.invocation)
                .context("guest storage names an unknown invocation")?;
            if hello.facet {
                // Only a live facet event of this root event may connect, and
                // it speaks for that facet's grant alone.
                if pending.cancelled || facets.len() >= 256 {
                    continue;
                }
                let access = pending.access.clone().context("guest facet admission unavailable")?;
                let owner = accept.clone();
                let grant = hello.grant.clone();
                let execution = pending.execution.clone();
                facets.spawn(async move {
                    let _execution = execution;
                    if !access.has_facet(&grant).await {
                        return;
                    }
                    let served = async {
                        while let Some(bytes) = read_frame(&mut socket).await? {
                            let reply = access
                                .execute_while(Request::decode(&bytes)?, || !owner.stop.is_cancelled())
                                .await?;
                            timeout(IO_DEADLINE, write_frame(&mut socket, &reply.encode()?))
                                .await
                                .context("guest facet reply timed out")??;
                        }
                        Ok::<(), anyhow::Error>(())
                    };
                    if let Err(error) = served.await {
                        tracing::debug!(%error, "guest facet storage ended");
                    }
                });
                continue;
            }
            ensure!(
                bool::from(pending.grant.as_bytes().ct_eq(hello.grant.as_bytes())),
                "guest storage grant mismatch"
            );
            if hello.cleanup {
                ensure!(!pending.terminal_seen && !pending.cleanup_connected, "duplicate or settled guest cleanup connection");
                ensure!(cleanup.len() < MAX_ACTIVE, "guest cleanup capacity reached");
                let access = pending.access.clone().context("guest cleanup admission unavailable")?;
                pending.cleanup_connected = true;
                let owner = accept.clone();
                let execution = pending.execution.clone();
                cleanup.spawn(async move {
                    let _execution = execution;
                    while let Some(bytes) = read_frame(&mut socket).await? {
                        let reply = access.cleanup_turn(Request::decode(&bytes)?, || !owner.stop.is_cancelled()).await?;
                        timeout(IO_DEADLINE, write_frame(&mut socket, &reply.encode()?)).await.context("guest cleanup reply timed out")??;
                    }
                    Ok::<(), anyhow::Error>(())
                });
                continue;
            }
            if pending.cancelled {
                continue;
            }
            ensure!(!pending.connected, "duplicate guest storage connection");
            pending.connected = true;
            pending.deadline = None;
            if let Some(reply) = pending.storage.take() {
                let _ = reply.send(socket);
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    });
    let deadlines = shared.clone();
    tasks.spawn(async move {
        loop {
            crate::asyncrt::sleep(Duration::from_millis(25)).await;
            let registry = deadlines.registry.lock().expect("guest registry poisoned");
            if let Some(pending) = registry.pending.values().find(|pending| {
                pending
                    .deadline
                    .is_some_and(|deadline| deadline <= Instant::now())
            }) {
                anyhow::bail!(
                    "guest event connection or cancellation deadline exceeded \
                     (cancelled={}, connected={}, replied={}, retirement={})",
                    pending.cancelled,
                    pending.connected,
                    pending.response_seen,
                    pending.retirement
                );
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    });
    let result = crate::asyncrt::select! {
        result = tasks.join_next() => result.context("guest dispatcher lost its pumps")
            .and_then(|joined| joined.context("guest transport pump panicked"))
            .and_then(|result| result),
        _ = shared.stop.cancelled() => Err(anyhow::anyhow!("guest transport stopped")),
    };
    if let Err(error) = &result {
        tracing::warn!(%error, "guest dispatcher retired");
    }
    // Retire grants before finalizing resident reads, including protocol faults
    // and normal shutdown. Event drivers retain their own rollback/proof path.
    shared.stop.cancel();
    shared.cursors.close().await;
    result
}

/// What the scripted peer saw: each dial's id, and each close it was sent.
#[cfg(test)]
#[path = "unit_tests/application_guest_transport/tests.rs"]
pub(crate) mod tests;

#[cfg(test)]
// These Unix-socket peers test host observation outside tenant event scheduling.
#[allow(clippy::disallowed_methods)]
#[path = "unit_tests/application_guest_transport/memory_sample_tests.rs"]
mod memory_sample_tests;
