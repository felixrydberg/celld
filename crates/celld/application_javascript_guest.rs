// The guest runtime executes inside the application VM, not in the node's
// execution domain. It uses the guest's own clocks, tasks and filesystem.
#![allow(clippy::disallowed_methods)]

//! Original V8 runtime inside a credential-free Linux guest. It multiplexes
//! object events, separates reply from drain and advertises the lifecycle
//! capabilities qualified by T02-T05.

use crate::js::{
    self, guest_storage::Channel, CellJob, ModuleSource, RequestBody, Worker, WorkerConfig,
    WorkerConfigOptions,
};
use anyhow::{ensure, Context, Result};
use celld_applications::invocation_wire::{
    GuestControl, HostControl, HttpResult, HttpStreamHead, ResponseFlow, StorageConnect,
    MAX_CHUNK_BYTES,
};
use celld_applications::storage_wire::{self as wire, VERSION};
use celld_applications::{BindingTarget, Language, ResourceAddress, ResourceKind};
use futures_util::{future::poll_fn, stream::FuturesUnordered, Stream};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::future::Future;
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::{Component, Path};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

const MAX_ACTIVE: usize = 32;

/// The control reader owns no V8 state. A bounded mailbox keeps control
/// reception independent of event storage and native input-gate waits.
struct Control {
    writer: File,
    reader: Option<std::thread::JoinHandle<()>>,
    incoming: mpsc::Receiver<Result<HostControl>>,
}

impl Control {
    fn new(writer: File) -> Result<Self> {
        let mut reader = writer.try_clone()?;
        let deadline = libc::timeval {
            tv_sec: 0,
            tv_usec: 0,
        };
        // SAFETY: the live timeval disables idle read timeouts on this owned
        // control socket. Shutdown below interrupts even a partial frame.
        let result = unsafe {
            libc::setsockopt(
                reader.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVTIMEO,
                &deadline as *const _ as *const libc::c_void,
                std::mem::size_of_val(&deadline) as libc::socklen_t,
            )
        };
        ensure!(
            result == 0,
            "control deadline: {}",
            std::io::Error::last_os_error()
        );
        let (send, incoming) = mpsc::channel(MAX_ACTIVE);
        let reader = std::thread::spawn(move || loop {
            let frame = wire::read_frame(&mut reader);
            let message = match frame {
                Ok(Some(frame)) => HostControl::decode(&frame),
                Ok(None) => break,
                Err(error) => Err(error),
            };
            let failed = message.is_err();
            if send.blocking_send(message).is_err() || failed {
                break;
            }
        });
        Ok(Self {
            writer,
            reader: Some(reader),
            incoming,
        })
    }

    async fn send(&self, message: GuestControl) -> Result<()> {
        // A condemnation goes out first: the host must read it before the reply
        // of the event that condemned the VM, or it leases the broken VM again.
        let frames = js::guest_storage::take_condemn()
            .into_iter()
            .chain([message])
            .map(|message| message.encode())
            .collect::<Result<Vec<_>>>()?;
        let mut writer = self.writer.try_clone()?;
        tokio::task::spawn_blocking(js::guest_activity::blocking(move || {
            frames
                .iter()
                .try_for_each(|bytes| wire::write_frame(&mut writer, bytes))
        }))
        .await
        .context("guest control writer panicked")??;
        Ok(())
    }
}

impl Drop for Control {
    fn drop(&mut self) {
        self.incoming.close();
        // SAFETY: shutdown operates on the owned socket and unblocks its
        // reader. Descriptor ownership remains with the two File values.
        unsafe {
            libc::shutdown(self.writer.as_raw_fd(), libc::SHUT_RDWR);
        }
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

struct Operation(js::Op);
impl Future for Operation {
    type Output = (u64, Result<crate::asyncrt::OpOut, String>);
    fn poll(mut self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let id = self.0 .0;
        self.0 .1.as_mut().poll(context).map(|result| (id, result))
    }
}

struct Running {
    channel: Arc<Channel>,
    entry: Option<js::InFlight>,
    response: Response,
    ops: FuturesUnordered<Operation>,
    admission: Option<JoinHandle<Result<()>>>,
    delivery: Option<(u64, Result<crate::asyncrt::OpOut, String>)>,
    poll_requested: bool,
    reply_sent: bool,
    failure: Option<String>,
    turns: usize,
    cancel_started: bool,
    body: Option<OutgoingBody>,
    sockets: SocketOut,
}

struct OutgoingBody {
    source: js::HttpChunkStream,
    flow: ResponseFlow,
}

enum Response {
    Http(oneshot::Receiver<Result<js::HttpResponse>>),
    /// An object RPC answers its tagged result bytes under response credits.
    Rpc(RpcReply),
    /// An original queue consumer batch answers its settlement record.
    Queue(oneshot::Receiver<Result<js::QueueDispatchResult>>),
    Alarm {
        result: oneshot::Receiver<Result<(celld_logic::wake::AlarmSnapshot, Option<u64>)>>,
        counts_against_limit: bool,
    },
    /// A hibernatable socket's message or close, answered by the original
    /// `webSocketMessage`/`webSocketClose`. Its frames go out as they are sent.
    Socket(SocketReply),
    Retirement {
        residency: String,
        ack: oneshot::Receiver<Result<Option<u64>>>,
    },
}

enum SocketReply {
    Message(oneshot::Receiver<Result<js::WsDispatch>>),
    Closed(oneshot::Receiver<Result<Option<u64>>>),
}

impl SocketReply {
    fn try_recv(&mut self) -> Result<Result<()>, oneshot::error::TryRecvError> {
        match self {
            Self::Message(reply) => reply.try_recv().map(|result| result.map(|_| ())),
            Self::Closed(reply) => reply.try_recv().map(|result| result.map(|_| ())),
        }
    }
}

/// Frames an event sent on its object's hibernatable sockets. The host grants
/// a window of credits; nothing leaves beyond it, and the event cannot drain
/// while frames wait, so the host sees them all before `Drained`.
struct SocketOut {
    credit: usize,
    queue: std::collections::VecDeque<GuestControl>,
}

impl Default for SocketOut {
    fn default() -> Self {
        Self {
            credit: celld_applications::invocation_wire::SOCKET_WINDOW,
            queue: Default::default(),
        }
    }
}

impl SocketOut {
    /// Written on the control stream itself, so a frame can never be overtaken
    /// by its event's `Drained`, which the same stream carries.
    async fn pump(&mut self, control: &Control) -> Result<()> {
        while self.credit > 0 {
            let Some(frame) = self.queue.pop_front() else {
                break;
            };
            self.credit -= 1;
            control.send(frame).await?;
        }
        Ok(())
    }
}

/// The worker's own reply channel, read directly. Forwarding it through a
/// task let the entry finish before the reply arrived ("ended without a
/// response").
enum RpcReply {
    Object(oneshot::Receiver<Result<js::RpcOutcome>>),
    Service(oneshot::Receiver<Result<Vec<u8>>>),
}

impl RpcReply {
    fn try_recv(&mut self) -> Result<Result<js::RpcOutcome>, oneshot::error::TryRecvError> {
        match self {
            Self::Object(reply) => reply.try_recv(),
            Self::Service(reply) => match reply.try_recv() {
                // A dropped worker reply fails only this event.
                Err(oneshot::error::TryRecvError::Closed) => {
                    Ok(Err(anyhow::anyhow!("guest service RPC retired")))
                }
                received => received.map(|result| {
                    result.map(|bytes| js::RpcOutcome {
                        data: js::RpcData::V8(bytes.into()),
                        write_position: None,
                        observed_position: None,
                    })
                }),
            },
        }
    }
}

type ChannelStart = Result<(Arc<Channel>, Result<()>)>;

struct Cancelling {
    channel: Arc<Channel>,
    running: Option<Running>,
    admission: Option<JoinHandle<Result<()>>>,
}

impl Drop for Running {
    fn drop(&mut self) {
        if let Some(entry) = &mut self.entry {
            entry.abandon();
        }
        // Dropping a blocking admission task cannot cancel its syscall. The
        // socket deadline bounds teardown; a failed run exits the entire VM.
        self.channel.retire();
    }
}

enum Phase {
    Starting(
        celld_applications::invocation_wire::InvokeHttp,
        Option<JoinHandle<ChannelStart>>,
    ),
    Running(Running),
    Cancelling(Cancelling),
    Retiring {
        residency: String,
        task: JoinHandle<ChannelStart>,
    },
}

struct Event {
    invocation: String,
    cell: String,
    deadline: tokio::time::Instant,
    phase: Phase,
    cancelled: bool,
    rpc_upload: Option<JoinHandle<Result<Vec<u8>>>>,
}

impl Event {
    fn start_cancellation(&mut self) {
        let Phase::Running(running) = &self.phase else {
            return;
        };
        if running.admission.is_some() || running.cancel_started {
            return;
        }
        let channel = running.channel.clone();
        // Effects close now; schedule() admits the cleanup turn.
        channel.request_cleanup();
        let previous = std::mem::replace(
            &mut self.phase,
            Phase::Cancelling(Cancelling {
                channel,
                running: None,
                admission: None,
            }),
        );
        let Phase::Running(running) = previous else {
            unreachable!()
        };
        let Phase::Cancelling(cancelling) = &mut self.phase else {
            unreachable!()
        };
        cancelling.running = Some(running);
    }
}

/// Body and egress pumps run beside events and hand their frames to the main
/// loop, which stays the only control writer.
struct Io {
    outbound: mpsc::UnboundedSender<GuestControl>,
    outgoing: mpsc::UnboundedReceiver<GuestControl>,
    egress: mpsc::UnboundedReceiver<js::guest_storage::GuestFetch>,
    sockets: mpsc::UnboundedReceiver<js::guest_storage::GuestSocketFrame>,
    requests: HashMap<String, mpsc::Sender<BodyItem>>,
    fetches: HashMap<String, (String, mpsc::Sender<BodyItem>)>,
    /// Sockets this VM dialed: the guest's id for each, and its fetch.
    dials: HashMap<u64, String>,
    uploads: HashMap<String, (bytes::Bytes, ResponseFlow)>,
    streamed_uploads: HashMap<String, mpsc::Sender<u64>>,
    /// Per service-RPC event: the signal ids its caller declared, and the
    /// mirrored subset it holds in the signal registry.
    signals: HashMap<String, (Vec<String>, Vec<u128>)>,
}

enum BodyItem {
    Head(u16, Vec<(String, String)>, Option<u64>),
    Chunk(u64, Vec<u8>),
    End(u64, Option<String>),
}

/// RPC structured-clone bytes and a queue batch must be complete before
/// entering a native turn. The original request pump bounds each transfer;
/// this bounds the assembly.
fn rpc_upload(
    invoke: &celld_applications::invocation_wire::InvokeHttp,
    io: &mut Io,
) -> Option<JoinHandle<Result<Vec<u8>>>> {
    if !invoke.request_stream
        || (invoke.rpc.is_none()
            && !invoke.queue
            && invoke
                .service
                .as_ref()
                .is_none_or(|service| service.rpc.is_none()))
    {
        return None;
    }
    let (limit, what) = if invoke.queue {
        (
            celld_applications::invocation_wire::MAX_QUEUE_BATCH_BYTES,
            "queue batch",
        )
    } else {
        (
            celld_applications::invocation_wire::MAX_OBJECT_CALL_BYTES,
            "RPC arguments",
        )
    };
    let (sender, mut receiver) = mpsc::channel(1);
    let (pump, items) = mpsc::channel(2);
    io.requests.insert(invoke.invocation.clone(), pump);
    tokio::spawn(js::guest_activity::track(request_pump(
        invoke.invocation.clone(),
        sender,
        items,
        io.outbound.clone(),
    )));
    Some(tokio::spawn(js::guest_activity::track(async move {
        let mut args = Vec::new();
        while let Some(chunk) = receiver.recv().await {
            let chunk = chunk.map_err(anyhow::Error::msg)?;
            ensure!(
                args.len().saturating_add(chunk.len()) <= limit,
                "{what} exceed limit"
            );
            args.extend(chunk);
        }
        Ok(args)
    })))
}

impl Io {
    /// A frame for a retired pump is late and harmless; one beyond its single
    /// credit is a protocol violation.
    fn route(
        pumps: &mut HashMap<String, mpsc::Sender<BodyItem>>,
        key: &str,
        item: BodyItem,
    ) -> Result<()> {
        let Some(pump) = pumps.get(key) else {
            return Ok(());
        };
        let end = matches!(item, BodyItem::End(..));
        match pump.try_send(item) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Closed(_)) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                anyhow::bail!("host body frame exceeded its credit")
            }
        }
        if end {
            pumps.remove(key);
        }
        Ok(())
    }
}

/// One credited request body: each JS demand becomes one host credit.
async fn request_pump(
    invocation: String,
    sender: mpsc::Sender<Result<Vec<u8>, String>>,
    mut items: mpsc::Receiver<BodyItem>,
    outbound: mpsc::UnboundedSender<GuestControl>,
) {
    let mut flow = ResponseFlow::default();
    loop {
        let Ok(permit) = sender.reserve().await else {
            return;
        };
        let sequence = flow.next_sequence();
        if flow.grant(sequence).is_err()
            || outbound
                .send(GuestControl::RequestPull {
                    version: VERSION,
                    invocation: invocation.clone(),
                    sequence,
                })
                .is_err()
        {
            return;
        }
        match items.recv().await {
            Some(BodyItem::Chunk(received, data))
                if received == sequence && flow.chunk(received).is_ok() =>
            {
                permit.send(Ok(data))
            }
            Some(BodyItem::End(received, error))
                if received == sequence && flow.end(received).is_ok() =>
            {
                if let Some(error) = error {
                    permit.send(Err(error));
                }
                return;
            }
            _ => {
                permit.send(Err("request body stream failed".into()));
                return;
            }
        }
    }
}

/// Stream transfer preserves one bounded credit and never waits in the main
/// scheduler, so a producer can need another guest turn to yield its bytes.
async fn service_upload(
    invocation: String,
    fetch: String,
    mut source: js::HttpChunkStream,
    mut credits: mpsc::Receiver<u64>,
    outbound: mpsc::UnboundedSender<GuestControl>,
) {
    use futures_util::StreamExt;
    let mut remaining = bytes::Bytes::new();
    let mut flow = ResponseFlow::default();
    while let Some(sequence) = credits.recv().await {
        if flow.grant(sequence).is_err() {
            return;
        }
        while remaining.is_empty() {
            let next = crate::asyncrt::select! {
                next = source.next() => next,
                _ = outbound.closed() => return,
                _ = credits.recv() => return,
            };
            match next {
                Some(Ok(bytes)) => remaining = bytes.into(),
                end => {
                    let error = match end {
                        Some(Err(error)) => Some(error.chars().take(4096).collect()),
                        _ => None,
                    };
                    let _ = outbound.send(GuestControl::FetchRequestEnd {
                        version: VERSION,
                        invocation,
                        fetch,
                        sequence,
                        error,
                    });
                    return;
                }
            }
        }
        if flow.chunk(sequence).is_err() {
            return;
        }
        let data = remaining
            .split_to(remaining.len().min(MAX_CHUNK_BYTES))
            .to_vec();
        if outbound
            .send(GuestControl::FetchRequestChunk {
                version: VERSION,
                invocation: invocation.clone(),
                fetch: fetch.clone(),
                sequence,
                data,
            })
            .is_err()
        {
            return;
        }
    }
}

/// A frame JavaScript queued but the scheduler has not yet attributed goes out
/// on the control stream before its event's `Drained`: an event with such a
/// frame is not finished, and the frame cannot be overtaken by the terminal.
#[cfg(test)]
#[path = "unit_tests/application_javascript_guest/tests.rs"]
pub(crate) mod tests;

/// The fetch head of a callee object's upgrade reaches the handler as a bodiless
/// 101 the handler can return: no stream, the host's id as a plain bound target,
/// and the host's fetch is cancelled since there is no body to pull.
///
/// One brokered fetch: the host's head resolves the original fetch promise,
/// then each JS demand for its body becomes one host credit.
async fn fetch_pump(
    invocation: String,
    fetch: String,
    mut reply: oneshot::Sender<Result<js::HttpResponse, String>>,
    mut items: mpsc::Receiver<BodyItem>,
    outbound: mpsc::UnboundedSender<GuestControl>,
) {
    let cancel = || {
        let _ = outbound.send(GuestControl::FetchCancel {
            version: VERSION,
            invocation: invocation.clone(),
            fetch: fetch.clone(),
        });
    };
    let head = crate::asyncrt::select! {
        item = items.recv() => item,
        _ = reply.closed() => return cancel(),
    };
    let (status, headers, socket) = match head {
        Some(BodyItem::Head(status, headers, socket)) => (status, headers, socket),
        Some(BodyItem::End(_, error)) => {
            let _ = reply.send(Err(
                error.unwrap_or_else(|| "fetch ended without a response".into())
            ));
            return;
        }
        _ => {
            let _ = reply.send(Err("fetch failed".into()));
            return cancel();
        }
    };
    let (sender, receiver) = mpsc::channel(1);
    // A socket upgrade has no body to pull, and a streamed body would make the
    // reply an ordinary stream: the handler could not return it as its 101.
    let upgrade = socket.is_some();
    let response = js::HttpResponse {
        status,
        headers,
        body: Vec::new(),
        stream: (!upgrade).then(|| {
            Box::pin(tokio_stream::wrappers::ReceiverStream::new(receiver)) as js::HttpChunkStream
        }),
        // The host issued this id for one callee object's accepted socket; the
        // reply may hand it back once, and the host checks it then.
        websocket: socket.map(|id| {
            js::HttpResponseWebSocket::Cell(js::WsTarget {
                id,
                scope: String::new(),
                tunnel: None,
            })
        }),
        write_position: None,
        observed_position: None,
    };
    if reply.send(Ok(response)).is_err() || upgrade {
        return cancel();
    }
    let mut flow = ResponseFlow::default();
    loop {
        let Ok(permit) = sender.reserve().await else {
            return cancel();
        };
        let sequence = flow.next_sequence();
        if flow.grant(sequence).is_err()
            || outbound
                .send(GuestControl::FetchPull {
                    version: VERSION,
                    invocation: invocation.clone(),
                    fetch: fetch.clone(),
                    sequence,
                })
                .is_err()
        {
            return;
        }
        match items.recv().await {
            Some(BodyItem::Chunk(received, data))
                if received == sequence && flow.chunk(received).is_ok() =>
            {
                permit.send(Ok(data))
            }
            Some(BodyItem::End(received, error))
                if received == sequence && flow.end(received).is_ok() =>
            {
                if let Some(error) = error {
                    permit.send(Err(error));
                }
                return;
            }
            None => {
                permit.send(Err("fetch stream retired".into()));
                return;
            }
            _ => {
                permit.send(Err("fetch stream failed".into()));
                return cancel();
            }
        }
    }
}

/// Forwards one declared service-RPC signal's abort to the host for as long as
/// the brokered fetch lives (`fetch_pump` ends, closing `fetch`). Subscribing to
/// the registry, not a JS object, also relays a signal this VM only mirrors.
async fn signal_relay(
    invocation: String,
    fetch: String,
    signal: String,
    id: u128,
    pump: mpsc::Sender<BodyItem>,
    outbound: mpsc::UnboundedSender<GuestControl>,
) {
    let reason = crate::asyncrt::select! {
        reason = js::rpc_signal_watch(id) => reason,
        _ = pump.closed() => None,
    };
    if let Some(reason) = reason {
        let _ = outbound.send(GuestControl::FetchSignal {
            version: VERSION,
            invocation,
            fetch,
            signal,
            reason,
        });
    }
}

enum Action {
    Send(GuestControl),
    Fetch(Box<js::guest_storage::GuestFetch>),
    Socket(js::guest_storage::GuestSocketFrame),
    Control(Option<Result<HostControl>>),
    Started(usize, ChannelStart),
    RpcUploaded(usize, Result<Vec<u8>>),
    Admitted(usize, Result<()>),
    Operation(usize, u64, Result<crate::asyncrt::OpOut, String>),
    Body(usize, Option<Result<Vec<u8>, String>>),
    RetirementAdmitted(usize, ChannelStart),
    CleanupAdmitted(usize, Result<()>),
}

fn start_channel(
    cell: String,
    invocation: String,
    grant: String,
    cleanup_only: bool,
) -> JoinHandle<ChannelStart> {
    tokio::task::spawn_blocking(js::guest_activity::blocking(move || {
        let invocation_id = invocation.clone();
        let mut stream = connect(5001)?;
        wire::write_frame(
            &mut stream,
            &StorageConnect {
                version: VERSION,
                invocation,
                grant: grant.clone(),
                cleanup: false,
                facet: false,
            }
            .encode()?,
        )?;
        let channel = Channel::from_stream(cell, grant, stream)?;
        channel.set_invocation(invocation_id);
        if cleanup_only {
            channel.restrict_to_cleanup();
        }
        let admitted = channel.begin_turn();
        // Preserve the channel even if a cancelled ordinary admission refuses.
        // Before entry it has performed no application effects; cleanup still
        // names this exact native event and cannot restore its effect authority.
        Ok((channel, admitted))
    }))
}

fn acquire(channel: Arc<Channel>) -> JoinHandle<Result<()>> {
    tokio::task::spawn_blocking(js::guest_activity::blocking(move || channel.begin_turn()))
}

/// Native admission waits outside V8, but owns its channel until the gate
/// opens, so a foreign checkpoint cannot use that origin meanwhile. An event
/// with JavaScript state keeps at most one admission per object in flight,
/// and only a holder is admitted while another event of that object can
/// retain its native gate; the holder that must release the gate is never
/// queued behind its waiters. A starting event has no continuations to run,
/// so it may queue natively behind a holder as the original delivery does.
// ponytail: a running event whose admission is pending while a just-started
// event or a holder runs can see its foreign effects refused (never parked)
// during those turns; closing it needs a multiplexed admission protocol.
fn schedule(events: &mut [Event]) {
    fn in_flight(event: &Event) -> bool {
        match &event.phase {
            Phase::Starting(..) => false,
            Phase::Running(running) => running.admission.is_some(),
            Phase::Cancelling(cancelling) => cancelling.admission.is_some(),
            Phase::Retiring { .. } => true,
        }
    }
    // Inspect only channels with no admission in flight; see in_flight.
    fn holds_gate(event: &Event) -> bool {
        match &event.phase {
            Phase::Running(running) => running.channel.may_hold_native_gate(),
            Phase::Cancelling(cancelling) => cancelling.channel.may_hold_native_gate(),
            Phase::Starting(..) | Phase::Retiring { .. } => false,
        }
    }
    // Observe completed operations first: an established event's next turn
    // is admitted ahead of a new event, as its native request arrived first.
    for event in events.iter_mut() {
        if let Phase::Running(running) = &mut event.phase {
            if running.admission.is_none() && running.delivery.is_none() {
                let mut context = TaskContext::from_waker(std::task::Waker::noop());
                if let Poll::Ready(Some(delivery)) =
                    Pin::new(&mut running.ops).poll_next(&mut context)
                {
                    running.delivery = Some(delivery);
                }
            }
        }
    }
    let starting = |event: &Event| matches!(event.phase, Phase::Starting(..));
    let order: Vec<_> = (0..events.len())
        .filter(|index| !starting(&events[*index]))
        .chain((0..events.len()).filter(|index| starting(&events[*index])))
        .collect();
    for index in order {
        let wanted = match &events[index].phase {
            Phase::Starting(_, task) => task.is_none() && events[index].rpc_upload.is_none(),
            Phase::Running(running) => {
                running.admission.is_none()
                    && (running.delivery.is_some() || running.poll_requested)
                    && !(events[index].cancelled && !running.cancel_started)
            }
            Phase::Cancelling(cancelling) => cancelling.admission.is_none(),
            Phase::Retiring { .. } => false,
        };
        if !wanted {
            continue;
        }
        // A holder's native turn is immediate, and a pending admission may be
        // waiting for exactly that holder's gate: never queue it behind others.
        let cell = &events[index].cell;
        if !holds_gate(&events[index])
            && (events
                .iter()
                .any(|other| &other.cell == cell && in_flight(other))
                || (!starting(&events[index])
                    && events.iter().enumerate().any(|(other, event)| {
                        other != index && &event.cell == cell && holds_gate(event)
                    })))
        {
            continue;
        }
        let event = &mut events[index];
        match &mut event.phase {
            // A stateless handler has no storage connection or native gate.
            Phase::Starting(invoke, task) if invoke.object.is_none() => {
                *task = Some(tokio::spawn(async { Ok((Channel::detached(), Ok(()))) }));
            }
            Phase::Starting(invoke, task) => {
                *task = Some(start_channel(
                    event.cell.clone(),
                    event.invocation.clone(),
                    invoke.storage_grant.clone(),
                    false,
                ));
            }
            Phase::Running(running) => running.admission = Some(acquire(running.channel.clone())),
            Phase::Cancelling(cancelling) => {
                cancelling.admission = Some(cleanup(
                    cancelling.channel.clone(),
                    event.invocation.clone(),
                ));
            }
            Phase::Retiring { .. } => unreachable!(),
        }
    }
}

fn cleanup(channel: Arc<Channel>, invocation: String) -> JoinHandle<Result<()>> {
    channel.request_cleanup();
    tokio::task::spawn_blocking(js::guest_activity::blocking(move || {
        if channel.is_detached() {
            return Ok(());
        }
        channel.begin_cleanup(invocation, connect(5001)?)
    }))
}

/// Host cancellation drops the event's ordinary storage stream, and can race
/// the end of a turn. That event alone unwinds through its cleanup channel,
/// which reuses the native turn the host still holds; the VM keeps serving.
fn lost_ordinary_stream(event: &mut Event) -> Result<()> {
    ensure!(
        !matches!(&event.phase, Phase::Running(running)
            if matches!(running.response, Response::Retirement { .. })),
        "guest retirement turn release failed"
    );
    event.cancelled = true;
    event.deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    event.start_cancellation();
    Ok(())
}

async fn finish(channel: Arc<Channel>) -> Result<()> {
    tokio::task::spawn_blocking(js::guest_activity::blocking(move || channel.finish_turn()))
        .await
        .context("guest turn release panicked")?
}

async fn next(
    control: &mut Control,
    io: &mut Io,
    events: &mut [Event],
    cursor: &mut usize,
) -> Result<Action> {
    let deadline = events.iter().map(|event| event.deadline).min();
    let ready = poll_fn(|context| {
        if let Poll::Ready(Some(message)) = io.outgoing.poll_recv(context) {
            return Poll::Ready(Ok(Action::Send(message)));
        }
        if let Poll::Ready(message) = control.incoming.poll_recv(context) {
            return Poll::Ready(Ok(Action::Control(message)));
        }
        if let Poll::Ready(Some(fetch)) = io.egress.poll_recv(context) {
            return Poll::Ready(Ok(Action::Fetch(Box::new(fetch))));
        }
        if let Poll::Ready(Some(frame)) = io.sockets.poll_recv(context) {
            return Poll::Ready(Ok(Action::Socket(frame)));
        }
        for offset in 0..events.len() {
            let index = (*cursor + offset) % events.len();
            if let Some(upload) = &mut events[index].rpc_upload {
                if let Poll::Ready(result) = Pin::new(upload).poll(context) {
                    return Poll::Ready(Ok(Action::RpcUploaded(
                        index,
                        result.context("RPC upload task panicked")?,
                    )));
                }
                continue;
            }
            let action = match &mut events[index].phase {
                Phase::Starting(_, None) => None,
                Phase::Starting(_, Some(task)) => match Pin::new(task).poll(context) {
                    Poll::Ready(result) => Some(Action::Started(
                        index,
                        result.context("guest storage connector panicked")?,
                    )),
                    Poll::Pending => None,
                },
                Phase::Running(event) => {
                    if let Some(task) = &mut event.admission {
                        match Pin::new(task).poll(context) {
                            Poll::Ready(result) => Some(Action::Admitted(
                                index,
                                result.context("guest admission panicked")?,
                            )),
                            Poll::Pending => None,
                        }
                    } else {
                        // A completed operation waits for its scheduled turn.
                        let operation = match event.delivery {
                            Some(_) => None,
                            None => match Pin::new(&mut event.ops).poll_next(context) {
                                Poll::Ready(Some((id, result))) => {
                                    Some(Action::Operation(index, id, result))
                                }
                                _ => None,
                            },
                        };
                        operation.or_else(|| {
                            let body = event.body.as_mut()?;
                            if !body.flow.has_credit() {
                                return None;
                            }
                            match body.source.as_mut().poll_next(context) {
                                Poll::Ready(item) => Some(Action::Body(index, item)),
                                Poll::Pending => None,
                            }
                        })
                    }
                }
                Phase::Retiring { task, .. } => match Pin::new(task).poll(context) {
                    Poll::Ready(result) => Some(Action::RetirementAdmitted(
                        index,
                        result.context("guest retirement connector panicked")?,
                    )),
                    Poll::Pending => None,
                },
                Phase::Cancelling(Cancelling {
                    admission: None, ..
                }) => None,
                Phase::Cancelling(Cancelling {
                    admission: Some(task),
                    ..
                }) => match Pin::new(task).poll(context) {
                    Poll::Ready(result) => Some(Action::CleanupAdmitted(
                        index,
                        result.context("guest cleanup admission panicked")?,
                    )),
                    Poll::Pending => None,
                },
            };
            if let Some(action) = action {
                *cursor = index + 1;
                return Poll::Ready(Ok(action));
            }
        }
        Poll::Pending
    });
    if let Some(deadline) = deadline {
        tokio::time::timeout_at(deadline, ready)
            .await
            .context("guest joined event deadline")?
    } else {
        ready.await
    }
}

/// The one WebSocket response an application guest can carry: a hibernatable
/// 101 upgrade, with no body, whose socket the host registry owns.
pub(crate) fn socket_upgrade_supported(response: &js::HttpResponse) -> bool {
    match &response.websocket {
        None => true,
        Some(js::HttpResponseWebSocket::Cell(target)) => {
            response.status == 101
                && target.tunnel.is_none()
                && response.stream.is_none()
                && response.body.is_empty()
        }
        Some(_) => false,
    }
}

/// The frames JavaScript has sent and the scheduler has not yet attributed, and
/// the fetch of each socket this VM dialed.
type SocketFrames<'a> = (
    &'a mut mpsc::UnboundedReceiver<js::guest_storage::GuestSocketFrame>,
    &'a HashMap<u64, String>,
);

/// Attribute one frame to the event that sent it and send what its credit
/// allows. A frame of an event that already ended has nowhere to go.
async fn route_socket_frame(
    control: &Control,
    events: &mut [Event],
    dials: &HashMap<u64, String>,
    frame: js::guest_storage::GuestSocketFrame,
) -> Result<()> {
    let Some(event) = events.iter_mut().find(|event| {
        !event.cancelled
            && matches!(&event.phase, Phase::Running(running)
                if running.entry.as_ref().is_some_and(|entry| entry.sent(&frame)))
    }) else {
        return Ok(());
    };
    let invocation = event.invocation.clone();
    // A dialed socket's frame names the fetch that carries it.
    let fetch = if frame.dial {
        match dials.get(&frame.ws) {
            Some(fetch) => Some(fetch.clone()),
            None => return Ok(()),
        }
    } else {
        None
    };
    let Phase::Running(running) = &mut event.phase else {
        unreachable!()
    };
    // ponytail: the queue is bounded per event; a streaming API
    // with JS-visible backpressure would replace failing the event.
    if running.sockets.queue.len() >= 1024 {
        running
            .failure
            .get_or_insert_with(|| "WebSocket send queue exhausted".into());
        return Ok(());
    }
    running.sockets.queue.push_back(GuestControl::SocketFrame {
        version: VERSION,
        invocation,
        ws: if fetch.is_some() { 0 } else { frame.ws },
        fetch,
        frame: frame.frame,
    });
    running.sockets.pump(control).await
}

/// Adopt operations attributed by the original CPED/mailbox machinery, then
/// publish responses independently of remaining referenced event work. Frames
/// JavaScript already sent are attributed first: an event with one unsent is
/// not finished, and it must not drain ahead of it.
async fn publish(
    control: &Control,
    events: &mut Vec<Event>,
    driver: Option<usize>,
    residencies: &mut HashMap<String, String>,
    retired_residencies: &mut HashSet<String>,
    frames: Option<SocketFrames<'_>>,
) -> Result<()> {
    if let Some((sockets, dials)) = frames {
        while let Ok(frame) = sockets.try_recv() {
            route_socket_frame(control, events, dials, frame).await?;
        }
    }
    for index in (0..events.len()).rev() {
        let invocation = events[index].invocation.clone();
        let cancelled = events[index].cancelled;
        let Phase::Running(event) = &mut events[index].phase else {
            continue;
        };
        // Admission owns the channel's I/O mutex while waiting for the native
        // gate. Even finished() can inspect that mutex for critical claims.
        // Keep driving the gate owner until admission joins; publication and
        // cleanup must not inspect a pending event from a foreign turn.
        if event.admission.is_some() || (cancelled && !event.cancel_started) {
            continue;
        }
        if let Some(entry) = &mut event.entry {
            let handed = entry.take_handed_ops();
            if !handed.is_empty() {
                event.ops.extend(handed.into_iter().map(Operation));
            }
        }
        if !cancelled && !event.reply_sent && event.failure.is_none() {
            match &mut event.response {
                Response::Retirement { ack, .. } => match ack.try_recv() {
                    Ok(Ok(position)) => {
                        ensure!(position.is_none(), "guest release supplied native proof");
                        event.reply_sent = true;
                    }
                    Ok(Err(error)) => return Err(error).context("guest object release failed"),
                    Err(oneshot::error::TryRecvError::Empty) => {}
                    Err(error) => return Err(error).context("guest release response disappeared"),
                },
                Response::Rpc(outcome) => match outcome.try_recv() {
                    Ok(Ok(outcome)) => {
                        let js::RpcData::V8(body) = outcome.data else {
                            anyhow::bail!("guest RPC answered a legacy JSON flavor");
                        };
                        if body.len() > celld_applications::invocation_wire::MAX_BODY_BYTES {
                            control
                                .send(GuestControl::HttpStreamHead(HttpStreamHead {
                                    version: VERSION,
                                    invocation: invocation.clone(),
                                    status: 200,
                                    headers: vec![],
                                }))
                                .await?;
                            // Reuse HTTP's credited, bounded response pump; keep
                            // the tagged RPC result unchanged for its decoder.
                            event.body = Some(OutgoingBody {
                                source: Box::pin(futures_util::stream::unfold(
                                    body,
                                    |mut remaining| async {
                                        if remaining.is_empty() {
                                            None
                                        } else {
                                            let chunk = remaining
                                                .split_to(remaining.len().min(MAX_CHUNK_BYTES));
                                            Some((Ok(chunk.to_vec()), remaining))
                                        }
                                    },
                                )),
                                flow: ResponseFlow::default(),
                            });
                        } else {
                            control
                                .send(GuestControl::HttpReply(HttpResult {
                                    version: VERSION,
                                    invocation: invocation.clone(),
                                    response: celld_applications::invocation_wire::HttpResponse {
                                        status: 200,
                                        body: body.to_vec(),
                                        headers: vec![],
                                    },
                                    socket: None,
                                }))
                                .await?;
                        }
                        event.reply_sent = true;
                    }
                    Ok(Err(error)) => {
                        event.failure = Some(error.to_string().chars().take(1024).collect())
                    }
                    Err(oneshot::error::TryRecvError::Empty) => {}
                    Err(error) => return Err(error).context("guest RPC outcome disappeared"),
                },
                Response::Alarm {
                    result,
                    counts_against_limit,
                } => match result.try_recv() {
                    Ok(Ok((alarm, position))) => {
                        ensure!(
                            alarm.at_ms().is_none() && position.is_none(),
                            "guest alarm supplied native proof"
                        );
                        // Keep the standard reply/drain handshake, but the host
                        // publishes alarm success only after joined drain.
                        control
                            .send(GuestControl::HttpReply(HttpResult {
                                version: VERSION,
                                invocation: invocation.clone(),
                                response: celld_applications::invocation_wire::HttpResponse {
                                    status: 204,
                                    headers: vec![],
                                    body: vec![],
                                },
                                socket: None,
                            }))
                            .await?;
                        event.reply_sent = true;
                    }
                    Ok(Err(error)) => {
                        *counts_against_limit = error.is::<js::GuestAlarmRejection>();
                        event.failure = Some(format!("{error:#}").chars().take(1024).collect());
                    }
                    Err(oneshot::error::TryRecvError::Empty) => {}
                    Err(error) => return Err(error).context("guest alarm result disappeared"),
                },
                Response::Socket(result) => match result.try_recv() {
                    Ok(Ok(())) => {
                        control
                            .send(GuestControl::HttpReply(HttpResult {
                                version: VERSION,
                                invocation: invocation.clone(),
                                response: celld_applications::invocation_wire::HttpResponse {
                                    status: 204,
                                    headers: vec![],
                                    body: vec![],
                                },
                                socket: None,
                            }))
                            .await?;
                        event.reply_sent = true;
                    }
                    Ok(Err(error)) => {
                        event.failure = Some(format!("{error:#}").chars().take(1024).collect())
                    }
                    Err(oneshot::error::TryRecvError::Empty) => {}
                    Err(error) => return Err(error).context("guest socket result disappeared"),
                },
                Response::Queue(result) => match result.try_recv() {
                    Ok(Ok(result)) => {
                        control
                            .send(GuestControl::HttpReply(HttpResult {
                                version: VERSION,
                                invocation: invocation.clone(),
                                response: celld_applications::invocation_wire::HttpResponse {
                                    status: 200,
                                    body: crate::application_guest::encode_queue_result(&result)?,
                                    headers: vec![],
                                },
                                socket: None,
                            }))
                            .await?;
                        event.reply_sent = true;
                    }
                    Ok(Err(error)) => {
                        event.failure = Some(error.to_string().chars().take(1024).collect())
                    }
                    Err(oneshot::error::TryRecvError::Empty) => {}
                    Err(error) => return Err(error).context("guest queue result disappeared"),
                },
                Response::Http(response) => match response.try_recv() {
                    Ok(Ok(response)) if !socket_upgrade_supported(&response) => {
                        event.failure = Some(
                            "this WebSocket response kind is not connected in application guests"
                                .into(),
                        );
                    }
                    Ok(Ok(response)) => {
                        // A hibernatable upgrade names its socket by the host id; the
                        // host checks that id against this event before anything leaves.
                        let socket = match &response.websocket {
                            Some(js::HttpResponseWebSocket::Cell(target)) => Some(target.id),
                            _ => None,
                        };
                        ensure!(
                            response.write_position.is_none()
                                && response.observed_position.is_none(),
                            "guest supplied native proof positions"
                        );
                        if let Some(source) = response.stream {
                            ensure!(
                                response.body.is_empty(),
                                "stream response also supplied buffered bytes"
                            );
                            control
                                .send(GuestControl::HttpStreamHead(HttpStreamHead {
                                    version: VERSION,
                                    invocation: invocation.clone(),
                                    status: response.status,
                                    headers: response.headers,
                                }))
                                .await?;
                            event.body = Some(OutgoingBody {
                                source,
                                flow: ResponseFlow::default(),
                            });
                        } else {
                            control
                                .send(GuestControl::HttpReply(HttpResult {
                                    version: VERSION,
                                    invocation: invocation.clone(),
                                    response: celld_applications::invocation_wire::HttpResponse {
                                        status: response.status,
                                        body: response.body,
                                        headers: response.headers,
                                    },
                                    socket,
                                }))
                                .await?;
                        }
                        event.reply_sent = true;
                    }
                    Ok(Err(error)) => {
                        event.failure = Some(error.to_string().chars().take(1024).collect())
                    }
                    Err(oneshot::error::TryRecvError::Empty) => {}
                    Err(error) => return Err(error).context("guest response disappeared"),
                },
            }
        }
        if event.entry.as_ref().is_none_or(|entry| entry.finished())
            && event.body.as_ref().is_none_or(|body| body.flow.is_closed())
            && event.admission.is_none()
            && (cancelled || event.sockets.queue.is_empty())
        {
            ensure!(
                cancelled || event.reply_sent || event.failure.is_some(),
                "guest event ended without a response"
            );
            // Drop all operation futures/resolvers and close storage before
            // Drained. It is adapter drain, never a host durability receipt.
            let failure = event.failure.take();
            let uncounted_alarm = failure.is_some()
                && matches!(
                    event.response,
                    Response::Alarm {
                        counts_against_limit: false,
                        ..
                    }
                );
            let retirement = match &event.response {
                Response::Retirement { residency, .. } => Some(residency.clone()),
                Response::Http(_)
                | Response::Rpc(_)
                | Response::Queue(_)
                | Response::Socket(_)
                | Response::Alarm { .. } => None,
            };
            if let Some(residency) = &retirement {
                ensure!(
                    residencies.remove(&events[index].cell) == Some(residency.clone()),
                    "object residency changed during retirement"
                );
                ensure!(
                    retired_residencies.insert(residency.clone()),
                    "object retired twice"
                );
            }
            if uncounted_alarm {
                tracing::warn!(
                    invocation,
                    message = failure.as_deref().unwrap(),
                    "guest alarm failed outside counted promise rejection"
                );
            }
            events.remove(index);
            control
                .send(if retirement.is_some() {
                    GuestControl::ObjectRetired {
                        version: VERSION,
                        invocation,
                    }
                } else if cancelled || uncounted_alarm {
                    GuestControl::Cancelled {
                        version: VERSION,
                        invocation,
                    }
                } else if let Some(message) = failure {
                    GuestControl::Failed {
                        version: VERSION,
                        invocation,
                        message,
                    }
                } else {
                    GuestControl::Drained {
                        version: VERSION,
                        invocation,
                    }
                })
                .await?;
        } else if driver.is_some_and(|driver| driver != index) {
            // A foreign checkpoint can settle this event's promise. Give the
            // original owner a fresh admitted poll; poll-only turns do not
            // recursively request more polls.
            event.poll_requested = true;
        }
    }
    // Requested polls are admitted by schedule().
    Ok(())
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    version: u16,
    resource: ResourceAddress,
    source: String,
    do_classes: Vec<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactConfiguration {
    #[serde(default)]
    do_classes: Vec<String>,
    #[serde(default)]
    durable_objects: ArtifactDurableObjects,
    #[serde(default)]
    bindings: Vec<ArtifactMetadataBinding>,
    #[serde(default)]
    modules: Vec<ArtifactModule>,
    #[serde(default)]
    compatibility_date: Option<String>,
    #[serde(default)]
    compatibility_flags: Vec<String>,
    /// The Pyodide release a Python handler needs: the node attaches its
    /// shared runtime drive of that release (see `application_pyodide`), from
    /// which the `shared` modules load. The guest ignores the value.
    #[serde(default)]
    #[allow(dead_code)]
    pyodide: Option<String>,
    /// The original normalized push-consumer shape; see `queue_consumers`.
    #[serde(default)]
    queue_consumers: Vec<crate::protocol::QueueConsumerConfig>,
    #[serde(default)]
    services: Vec<ArtifactService>,
    #[serde(default)]
    workflows: Vec<ArtifactWorkflow>,
    #[serde(default)]
    r2_buckets: Vec<ArtifactR2>,
    /// The original Worker Loader declarations: only a binding name, as in
    /// Wrangler. Each must be bound to a `worker_loader` resource alias.
    #[serde(default)]
    worker_loaders: Vec<ArtifactLoader>,
    /// The original deployment manifest's asset reference, verbatim; the
    /// index and bodies are this application's uploaded artifacts.
    #[serde(default)]
    assets: Option<crate::protocol::AssetManifestRef>,
    /// Which class owns which Container binding. Only accepted here; the
    /// host and guest read it through `application_container::declarations`.
    #[serde(default)]
    #[allow(dead_code)]
    containers: Vec<crate::application_container::Declaration>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactLoader {
    binding: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactR2 {
    binding: String,
    bucket_name: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactWorkflow {
    name: String,
    binding: String,
    class_name: String,
    #[serde(default)]
    script_name: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactService {
    binding: String,
    service: String,
    #[serde(default)]
    entrypoint: Option<String>,
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactDurableObjects {
    #[serde(default)]
    bindings: Vec<ArtifactObjectBinding>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactObjectBinding {
    name: String,
    class_name: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactMetadataBinding {
    #[serde(rename = "type")]
    kind: String,
    name: String,
    #[serde(default)]
    class_name: String,
    #[serde(default)]
    workflow_name: Option<String>,
    #[serde(default)]
    bucket_name: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactModule {
    name: String,
    path: String,
    kind: ArtifactModuleKind,
    /// Read from the shared runtime drive instead of the handler's image.
    #[serde(default)]
    shared: bool,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum ArtifactModuleKind {
    EsModule,
    Text,
    Wasm,
    /// Binary content, imported as an `ArrayBuffer` (Wrangler's Data rule).
    Data,
}

/// An empty list is a stateless handler; object classes need a namespace.
fn validate_classes(classes: &[String]) -> Result<()> {
    ensure!(classes.len() <= 128, "invalid JavaScript guest classes");
    let mut seen = HashSet::new();
    ensure!(
        classes.iter().all(|class| !class.is_empty()
            && !class.contains(':')
            && celld_logic::cell::valid_cell_scope(class)
            && !crate::deploy::is_reserved_class(class)
            // Not in `is_reserved_class`, which also gates operator routes: a
            // guest object of class `.cron` would start in the trusted pool.
            && class != celld_logic::cron::RESERVED_CLASS
            && seen.insert(class)),
        "invalid or duplicate guest class"
    );
    Ok(())
}

pub(crate) fn artifact_path(path: &str) -> Result<()> {
    ensure!(
        !path.is_empty()
            && path.len() <= 1024
            && !path.contains(['\\', ':', '\0'])
            && path.split('/').all(|part| !matches!(part, "" | "." | ".."))
            && Path::new(path)
                .components()
                .all(|part| matches!(part, Component::Normal(_))),
        "invalid JavaScript artifact path or module name"
    );
    Ok(())
}

fn read_artifact(root: &Path, relative: &str, remaining: &mut usize) -> Result<Vec<u8>> {
    artifact_path(relative)?;
    // The guest mount is read-only and user code has not started. Check every
    // component: uploaded images need not have come through the safe packager.
    let mut path = root.to_path_buf();
    for part in Path::new(relative).components() {
        path.push(part);
        let metadata = std::fs::symlink_metadata(&path)?;
        ensure!(
            !metadata.file_type().is_symlink(),
            "symlink in JavaScript artifact"
        );
    }
    ensure!(
        path.canonicalize()?.starts_with(root) && path.is_file(),
        "JavaScript artifact file escapes root or is not regular"
    );
    let mut file = File::open(path)?;
    ensure!(
        file.metadata()?.len() <= *remaining as u64,
        "JavaScript artifact source budget exceeded"
    );
    let mut bytes = Vec::new();
    (&mut file)
        .take(*remaining as u64 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= *remaining,
        "JavaScript artifact source budget exceeded"
    );
    *remaining -= bytes.len();
    Ok(bytes)
}

pub(crate) struct PreparedApplication {
    resources: Vec<ResourceAddress>,
    loader_bindings: Vec<String>,
    /// `env` namespaces: (binding alias, class, namespace key of its resource).
    namespaces: Vec<(String, String, String)>,
    options: WorkerConfigOptions,
    services: Vec<(String, String, Option<String>)>,
    asset_binding: Option<String>,
    container_aliases: Vec<(String, String)>,
}

/// Shared by guest bootstrap and the trusted VM/peer authorization paths.
/// An alias chooses a declared class under its bound resource; declarations
/// never grant the Cartesian product of all classes and all resources.
pub(crate) fn object_namespaces(
    application: &celld_applications::ApplicationId,
    handler: &celld_applications::Handler,
) -> Result<Vec<(String, ResourceAddress, String)>> {
    service_bindings(handler)?;
    let config: ArtifactConfiguration = serde_json::from_value(handler.worker_config.clone())
        .map_err(|_| anyhow::anyhow!("unsupported JavaScript artifact configuration"))?;
    validate_classes(&config.do_classes)?;
    resolve_object_namespaces(application, handler, &config)
}

pub(crate) fn service_bindings(
    handler: &celld_applications::Handler,
) -> Result<Vec<(String, String, Option<String>)>> {
    let config: ArtifactConfiguration = serde_json::from_value(handler.worker_config.clone())
        .map_err(|_| anyhow::anyhow!("unsupported JavaScript artifact configuration"))?;
    let mut declarations = HashMap::new();
    for service in config.services {
        ensure!(handler.bindings.iter().any(|(alias, target)| alias.as_str() == service.binding
            && matches!(target, BindingTarget::Handler { handler } if handler.as_str() == service.service))
            && service.entrypoint.as_ref().is_none_or(|name| !name.is_empty() && name.len() <= 1024)
            && declarations.insert(service.binding, service.entrypoint).is_none(),
            "invalid, duplicate or unbound JavaScript service binding");
    }
    Ok(handler
        .bindings
        .iter()
        .filter_map(|(alias, target)| {
            let BindingTarget::Handler { handler } = target else {
                return None;
            };
            Some((
                alias.as_str().to_owned(),
                handler.as_str().to_owned(),
                declarations.remove(alias.as_str()).flatten(),
            ))
        })
        .collect())
}

fn workflow_bindings(
    handler: &celld_applications::Handler,
    config: &ArtifactConfiguration,
) -> Result<Vec<js::WorkflowBinding>> {
    let mut names = HashSet::new();
    let mut aliases = HashSet::new();
    let mut workflows = Vec::new();
    let declarations = config
        .workflows
        .iter()
        .map(|workflow| {
            (
                workflow.binding.as_str(),
                workflow.name.as_str(),
                workflow.class_name.as_str(),
                workflow.script_name.as_deref(),
            )
        })
        .chain(
            config
                .bindings
                .iter()
                .filter(|binding| binding.kind == "workflow")
                .map(|binding| {
                    (
                        binding.name.as_str(),
                        binding.workflow_name.as_deref().unwrap_or(""),
                        binding.class_name.as_str(),
                        None,
                    )
                }),
        );
    for (alias, name, class, script) in declarations {
        validate_classes(&[class.to_owned()])?;
        ensure!(script.is_none_or(|script| script == handler.id.as_str()) && names.insert(name) && aliases.insert(alias)
            && handler.bindings.iter().any(|(bound, target)| bound.as_str() == alias && matches!(target, BindingTarget::Resource {resource} if resource.kind == ResourceKind::Workflow && resource.id.as_str() == name)),
            "invalid, duplicate or unbound JavaScript workflow binding");
        workflows.push(js::WorkflowBinding {
            environment: alias.into(),
            workflow: name.into(),
            class: class.into(),
        });
    }
    ensure!(handler.bindings.iter().all(|(alias,target)| !matches!(target, BindingTarget::Resource {resource} if resource.kind == ResourceKind::Workflow) || aliases.contains(alias.as_str())), "Workflow resource requires its declared user class");
    Ok(workflows)
}

/// The original push consumers a handler declares in its `worker_config`, under
/// the original rules. Every consumer queue and dead-letter queue must be a
/// registered Queue resource of the handler's own application (the resource id
/// is the queue name), and concurrency is bounded by what a VM can run.
pub(crate) fn queue_consumers(
    handler: &celld_applications::Handler,
    registered: &HashSet<celld_applications::ResourceRef>,
) -> Result<Vec<crate::protocol::QueueConsumerConfig>> {
    let config: ArtifactConfiguration = serde_json::from_value(handler.worker_config.clone())
        .map_err(|_| anyhow::anyhow!("unsupported JavaScript artifact configuration"))?;
    let mut consumers = config.queue_consumers;
    // The normalized type drops fields it does not know; a round trip refuses them.
    let none = serde_json::json!([]);
    let declared = handler
        .worker_config
        .get("queue_consumers")
        .unwrap_or(&none);
    ensure!(
        *declared == serde_json::to_value(&consumers)?,
        "unsupported queue consumer configuration"
    );
    crate::protocol::validate_queue_consumers(&consumers)?;
    let queue = |name: &str| {
        celld_applications::Name::try_from(name.to_owned())
            .ok()
            .is_some_and(|id| {
                registered.contains(&celld_applications::ResourceRef {
                    kind: ResourceKind::Queue,
                    id,
                })
            })
    };
    for consumer in &mut consumers {
        ensure!(
            queue(&consumer.queue) && consumer.dead_letter_queue.as_deref().is_none_or(queue),
            "queue consumer names a queue that is not a registered resource of its application"
        );
        let cap = crate::application_guest_transport::QUEUE_MAX_CONCURRENCY;
        consumer.max_concurrency = Some(consumer.max_concurrency.map_or(cap, |n| n.min(cap)));
    }
    Ok(consumers)
}

/// The handler's static assets: its one `static_assets` binding alias and the
/// declared index reference. Declaring one without the other is refused.
pub(crate) fn asset_binding(
    handler: &celld_applications::Handler,
) -> Result<Option<(String, crate::protocol::AssetManifestRef)>> {
    let config: ArtifactConfiguration = serde_json::from_value(handler.worker_config.clone())
        .map_err(|_| anyhow::anyhow!("unsupported JavaScript artifact configuration"))?;
    let mut aliases = handler
        .bindings
        .iter()
        .filter(|&(_, target)| {
            matches!(target, BindingTarget::Resource { resource } if resource.kind == ResourceKind::StaticAssets)
        })
        .map(|(alias, _)| alias.as_str().to_owned());
    match (config.assets, aliases.next(), aliases.next()) {
        (None, None, _) => Ok(None),
        (Some(reference), Some(alias), None) => Ok(Some((alias, reference))),
        _ => anyhow::bail!(
            "static assets need `worker_config.assets` and exactly one static_assets binding"
        ),
    }
}

fn r2_bindings(
    handler: &celld_applications::Handler,
    config: &ArtifactConfiguration,
) -> Result<Vec<(String, String)>> {
    let mut declarations = HashSet::new();
    for (alias, bucket) in config
        .r2_buckets
        .iter()
        .map(|binding| (binding.binding.as_str(), Some(binding.bucket_name.as_str())))
        .chain(
            config
                .bindings
                .iter()
                .filter(|binding| binding.kind == "r2_bucket")
                .map(|binding| (binding.name.as_str(), binding.bucket_name.as_deref())),
        )
    {
        ensure!(declarations.insert(alias) && bucket.is_some_and(|bucket| handler.bindings.iter().any(|(bound, target)| bound.as_str() == alias && matches!(target, BindingTarget::Resource {resource} if resource.kind == ResourceKind::R2 && resource.id.as_str() == bucket))), "invalid, duplicate or unbound JavaScript R2 binding");
    }
    ensure!(
        config
            .bindings
            .iter()
            .all(|binding| binding.kind == "r2_bucket"
                && binding.class_name.is_empty()
                && binding.workflow_name.is_none()
                || binding.kind != "r2_bucket" && binding.bucket_name.is_none()),
        "invalid JavaScript R2 binding metadata"
    );
    // The guest receives only aliases. The native broker derives bucket paths
    // from the registered application's ResourceAddress.
    Ok(handler.bindings.iter().filter(|&(_, target)| matches!(target, BindingTarget::Resource {resource} if resource.kind == ResourceKind::R2)).map(|(alias, _)| (alias.as_str().to_owned(), alias.as_str().to_owned())).collect())
}

/// Each declared loader is bound to exactly one `worker_loader` resource alias
/// and each such alias is declared. Loaders are VM-local, so the host needs no
/// address for them.
fn loader_bindings(
    handler: &celld_applications::Handler,
    config: &ArtifactConfiguration,
) -> Result<Vec<String>> {
    let mut declared = HashSet::new();
    for loader in &config.worker_loaders {
        ensure!(
            declared.insert(loader.binding.as_str())
                && handler
                    .bindings
                    .iter()
                    .any(|(alias, target)| alias.as_str() == loader.binding
                        && matches!(target, BindingTarget::Resource { resource }
                        if resource.kind == ResourceKind::WorkerLoader)),
            "invalid, duplicate or unbound JavaScript worker loader"
        );
    }
    let bound = handler.bindings.iter().filter(|(_, target)| {
        matches!(target, BindingTarget::Resource { resource } if resource.kind == ResourceKind::WorkerLoader)
    });
    ensure!(
        bound.count() == declared.len(),
        "JavaScript worker loaders must be declared in worker_config.worker_loaders"
    );
    Ok(config
        .worker_loaders
        .iter()
        .map(|loader| loader.binding.clone())
        .collect())
}

fn resolve_object_namespaces(
    application: &celld_applications::ApplicationId,
    handler: &celld_applications::Handler,
    config: &ArtifactConfiguration,
) -> Result<Vec<(String, ResourceAddress, String)>> {
    let workflows = workflow_bindings(handler, config)?;
    r2_bindings(handler, config)?;
    loader_bindings(handler, config)?;
    crate::application_container::declarations(handler)?;
    let mut namespaces = Vec::new();
    let mut classes_by_alias = HashMap::new();
    ensure!(
        config
            .bindings
            .iter()
            .all(|binding| binding.kind == "durable_object_namespace"
                && binding.workflow_name.is_none()
                || binding.kind == "workflow"
                || binding.kind == "r2_bucket"),
        "unsupported JavaScript binding metadata"
    );
    let declarations = config
        .durable_objects
        .bindings
        .iter()
        .map(|binding| (&binding.name, &binding.class_name))
        .chain(
            config
                .bindings
                .iter()
                .filter(|binding| binding.kind == "durable_object_namespace")
                .map(|binding| (&binding.name, &binding.class_name)),
        );
    for (name, class_name) in declarations {
        ensure!(
            config.do_classes.contains(class_name)
                && handler.bindings.iter().any(|(alias, target)| {
                    alias.as_str() == name
                        && matches!(target, BindingTarget::Resource { resource }
                            if resource.kind == ResourceKind::DurableObject)
                })
                && classes_by_alias
                    .insert(name.as_str(), class_name.as_str())
                    .is_none(),
            "invalid, duplicate or unbound JavaScript object binding"
        );
    }
    let mut bound_classes = HashSet::new();
    for (alias, target) in &handler.bindings {
        let BindingTarget::Resource { resource } = target else {
            continue;
        };
        ensure!(
            matches!(
                resource.kind,
                ResourceKind::DurableObject
                    | ResourceKind::D1
                    | ResourceKind::Kv
                    | ResourceKind::Queue
                    | ResourceKind::Workflow
                    | ResourceKind::R2
                    | ResourceKind::StaticAssets
                    | ResourceKind::Container
                    | ResourceKind::WorkerLoader
            ),
            "this resource kind cannot be bound to a JavaScript handler"
        );
        if matches!(
            resource.kind,
            ResourceKind::R2
                | ResourceKind::StaticAssets
                | ResourceKind::Container
                | ResourceKind::WorkerLoader
        ) {
            continue;
        }
        let address = ResourceAddress {
            application: application.clone(),
            resource: resource.clone(),
        };
        if resource.kind == ResourceKind::Workflow {
            ensure!(
                workflows
                    .iter()
                    .any(|workflow| workflow.environment == alias.as_str()),
                "unbound workflow namespace"
            );
            namespaces.push((
                alias.as_str().to_owned(),
                address,
                crate::deploy::workflow_class(resource.id.as_str()),
            ));
            continue;
        }
        let class = match resource.kind {
            ResourceKind::D1 => crate::deploy::D1_CLASS,
            ResourceKind::Kv => celld_logic::kv::RESERVED_CLASS,
            ResourceKind::Queue => celld_logic::queue::RESERVED_CLASS,
            _ => {
                let class = classes_by_alias
                    .get(alias.as_str())
                    .copied()
                    .or(match config.do_classes.as_slice() {
                        [class] => Some(class.as_str()),
                        _ => None,
                    })
                    .context("Durable Object binding requires an explicit declared class")?;
                bound_classes.insert(class);
                class
            }
        };
        namespaces.push((alias.as_str().to_owned(), address, class.to_owned()));
    }
    // A handler without object resources declares no classes; classes need a namespace.
    ensure!(
        bound_classes.len() == config.do_classes.len(),
        "JavaScript object classes and bound namespaces must be declared together"
    );
    Ok(namespaces)
}

/// Internal KV and Queue address exactly one original cell under the resource
/// key: the KV shard, or the queue the resource names. A valid HMAC for another
/// hidden cell never grants another index or queue.
pub(crate) fn namespace_owns_object(
    resource: &ResourceAddress,
    class: &str,
    object: &celld_applications::invocation_wire::ObjectTarget,
) -> bool {
    if class != object.class || !resource.owns(object) {
        return false;
    }
    if resource.resource.kind == ResourceKind::Workflow {
        return class == crate::deploy::workflow_class(resource.resource.id.as_str())
            && object.name.as_ref().is_some_and(|name| {
                name.strip_prefix(resource.resource.id.as_str())
                    .is_some_and(|suffix| suffix.starts_with('/') && suffix.len() > 1)
            });
    }
    let (reserved, name) = match resource.resource.kind {
        ResourceKind::Kv => (
            celld_logic::kv::RESERVED_CLASS,
            celld_logic::kv::cell_name("namespace", 0),
        ),
        ResourceKind::Queue => (
            celld_logic::queue::RESERVED_CLASS,
            celld_logic::queue::cell_name(resource.resource.id.as_str()).to_owned(),
        ),
        _ => return true,
    };
    class == reserved
        && object.name.as_ref().is_none_or(|actual| actual == &name)
        && resource
            .cell_scope(class, &name)
            .is_ok_and(|scope| scope == format!("{}:{}", object.class, object.id))
}

/// Native activation and alarm discovery have a catalog-authenticated cell id,
/// without a guest-supplied Workflow instance name. Guest calls retain the
/// stricter name check in `namespace_owns_object`.
pub(crate) fn native_namespace_owns_object(
    resource: &ResourceAddress,
    declared: &str,
    object: &celld_applications::invocation_wire::ObjectTarget,
) -> bool {
    if resource.resource.kind == ResourceKind::Workflow {
        declared == object.class
            && declared == crate::deploy::workflow_class(resource.resource.id.as_str())
            && resource.owns(object)
    } else {
        namespace_owns_object(resource, declared, object)
    }
}

/// `shared` is the mounted runtime drive of a Python handler: modules marked
/// `shared` load from there, so the handler's own image holds only its code.
pub(crate) fn load_application_with(
    bootstrap: &Path,
    artifact: &Path,
    shared: Option<&Path>,
) -> Result<PreparedApplication> {
    use celld_applications::bootstrap::{Bootstrap, DISK_BYTES};
    let mut raw = Vec::new();
    File::open(bootstrap)?
        .take(DISK_BYTES as u64 + 1)
        .read_to_end(&mut raw)?;
    let bootstrap = Bootstrap::decode_disk(&raw)?;
    ensure!(
        bootstrap.handler.language == Language::Javascript,
        "bootstrap is not JavaScript"
    );
    let config: ArtifactConfiguration =
        serde_json::from_value(bootstrap.handler.worker_config.clone())
            .map_err(|_| anyhow::anyhow!("unsupported JavaScript artifact configuration"))?;
    validate_classes(&config.do_classes)?;
    ensure!(
        config.modules.len() < 4096,
        "JavaScript artifact module count exceeded"
    );
    let resolved = resolve_object_namespaces(&bootstrap.application, &bootstrap.handler, &config)?;
    let services = service_bindings(&bootstrap.handler)?;
    let r2_bindings = r2_bindings(&bootstrap.handler, &config)?;
    let loader_bindings = loader_bindings(&bootstrap.handler, &config)?;
    let asset_binding = asset_binding(&bootstrap.handler)?.map(|(alias, _)| alias);
    let workflow_bindings = workflow_bindings(&bootstrap.handler, &config)?;
    let mut do_classes = config.do_classes.clone();
    do_classes.extend(
        resolved
            .iter()
            .filter(|(_, address, _)| address.resource.kind == ResourceKind::Workflow)
            .map(|(_, _, class)| class.clone()),
    );
    // A queue resource is the original producer binding, by the resource's
    // name, under its own key. The original delivery delay default applies.
    let queue_bindings = resolved
        .iter()
        .filter(|(_, address, _)| address.resource.kind == ResourceKind::Queue)
        .map(|(alias, address, _)| js::QueueBinding {
            environment: alias.clone(),
            queue: address.resource.id.as_str().to_owned(),
            delivery_delay: 0,
        })
        .collect();
    let mut resources = Vec::new();
    let namespaces = resolved
        .into_iter()
        .map(|(alias, address, class)| {
            if !resources.contains(&address) {
                resources.push(address.clone());
            }
            let key = address.namespace_key(&class);
            (alias, class, key)
        })
        .collect();
    let root = artifact.canonicalize()?;
    ensure!(root.is_dir(), "JavaScript artifact root is not a directory");
    let shared = shared.map(Path::canonicalize).transpose()?;
    let mut remaining = crate::applications::MAX_ARTIFACT_BYTES;
    let mut shared_remaining = crate::applications::MAX_ARTIFACT_BYTES;
    let source = String::from_utf8(read_artifact(
        &root,
        &bootstrap.handler.entrypoint,
        &mut remaining,
    )?)
    .context("JavaScript entrypoint is not UTF-8")?;
    let mut names = HashSet::new();
    let mut modules = Vec::new();
    for module in config.modules {
        let canonical_name = module.name.strip_prefix("./").unwrap_or(&module.name);
        artifact_path(canonical_name)?;
        ensure!(
            canonical_name != "worker.js"
                && canonical_name != bootstrap.handler.entrypoint
                && names.insert(canonical_name.to_owned()),
            "duplicate or entry-shadowing JavaScript module name"
        );
        let bytes = if module.shared {
            let shared = shared
                .as_deref()
                .context("shared module without a runtime drive")?;
            read_artifact(shared, &module.path, &mut shared_remaining)?
        } else {
            read_artifact(&root, &module.path, &mut remaining)?
        };
        let source = match module.kind {
            ArtifactModuleKind::EsModule => ModuleSource::EsModule(
                String::from_utf8(bytes).context("JavaScript module is not UTF-8")?,
            ),
            ArtifactModuleKind::Text => {
                ModuleSource::Text(String::from_utf8(bytes).context("text module is not UTF-8")?)
            }
            ArtifactModuleKind::Wasm => ModuleSource::Wasm(bytes.into()),
            ArtifactModuleKind::Data => ModuleSource::Data(bytes.into()),
        };
        modules.push((canonical_name.to_owned(), source));
    }
    let metadata = serde_json::json!({"compatibility_date":config.compatibility_date,"compatibility_flags":config.compatibility_flags});
    Ok(PreparedApplication {
        resources,
        loader_bindings,
        namespaces,
        asset_binding,
        container_aliases: crate::application_container::declarations(&bootstrap.handler)?
            .into_iter()
            .map(|item| (item.class_name, item.binding))
            .collect(),
        services: services
            .into_iter()
            .map(|(alias, _, entrypoint)| (alias.clone(), alias, entrypoint))
            .collect(),
        options: WorkerConfigOptions {
            src: source,
            script_name: bootstrap.application.storage_root(),
            do_classes,
            bindings: vec![],
            r2_bindings,
            d1_bindings: vec![],
            kv_bindings: vec![],
            queue_bindings,
            // The guest only needs the declaration: a consumer must export a
            // queue handler. Delivery settings come from the host catalog.
            queue_consumers: config.queue_consumers,
            workflow_bindings,
            vars: bootstrap
                .handler
                .environment
                .into_iter()
                .map(|(name, value)| (name.as_str().to_owned(), value))
                .collect(),
            node: "guest".into(),
            modules,
            compat: crate::worker_compat(&metadata),
        },
    })
}

pub async fn run_application(
    bootstrap: &Path,
    artifact: &Path,
    shared: Option<&Path>,
) -> Result<()> {
    js::guest_activity::enable();
    let began = std::time::Instant::now();
    let prepared = load_application_with(bootstrap, artifact, shared)?;
    let artifact_loaded = began.elapsed();
    js::Engine::init();
    let engine_initialized = began.elapsed();
    let config = WorkerConfig::new(prepared.options)
        .with_keyed_namespaces(prepared.namespaces)
        .with_services(prepared.services)
        .with_asset_binding(prepared.asset_binding)
        .with_container_aliases(prepared.container_aliases)
        .with_loaders(prepared.loader_bindings);
    let config_built = began.elapsed();
    let worker = Worker::load_guest_config(Arc::new(config))?;
    let worker_loaded = began.elapsed();
    let ms = |elapsed: std::time::Duration| elapsed.as_secs_f64() * 1000.0;
    tracing::info!(
        event = "guest_application_initialization",
        artifact_ms = ms(artifact_loaded),
        engine_ms = ms(engine_initialized - artifact_loaded),
        config_ms = ms(config_built - engine_initialized),
        worker_ms = ms(worker_loaded - config_built),
        total_ms = ms(worker_loaded),
        "application guest initialization timing"
    );
    // This records adapter bootstrap, not readiness or a durability proof.
    tracing::info!("CELLD_JAVASCRIPT_APPLICATION_BOOTSTRAP_LOADED");
    run_worker(prepared.resources, worker).await
}

/// The socket is genuinely AF_VSOCK, not a UnixStream with a different family.
/// OwnedFd closes every failed path. Only the fixed host protocol ports
/// are selected here; the application never receives the descriptor or grant.
pub(crate) fn connect(port: u32) -> Result<File> {
    // SAFETY: socket has no pointer arguments and returns a fresh descriptor.
    let raw = unsafe { libc::socket(libc::AF_VSOCK, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
    ensure!(
        raw >= 0,
        "create vsock: {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: raw is a newly opened descriptor, now owned only by this guard.
    let socket = unsafe { OwnedFd::from_raw_fd(raw) };
    let deadline = libc::timeval {
        tv_sec: 5,
        tv_usec: 0,
    };
    for option in [libc::SO_RCVTIMEO, libc::SO_SNDTIMEO] {
        // SAFETY: pointer and length describe the live timeval, and fd is owned.
        let result = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                option,
                &deadline as *const _ as *const libc::c_void,
                std::mem::size_of_val(&deadline) as libc::socklen_t,
            )
        };
        ensure!(
            result == 0,
            "vsock deadline: {}",
            std::io::Error::last_os_error()
        );
    }
    // SAFETY: sockaddr_vm consists of integer/byte fields, all valid when zero.
    let mut address: libc::sockaddr_vm = unsafe { std::mem::zeroed() };
    address.svm_family = libc::AF_VSOCK as libc::sa_family_t;
    address.svm_cid = libc::VMADDR_CID_HOST;
    address.svm_port = port;
    // SAFETY: address/length match sockaddr_vm and remain live for connect.
    let result = unsafe {
        libc::connect(
            socket.as_raw_fd(),
            &address as *const _ as *const libc::sockaddr,
            std::mem::size_of_val(&address) as libc::socklen_t,
        )
    };
    ensure!(
        result == 0,
        "connect host vsock: {}",
        std::io::Error::last_os_error()
    );
    Ok(File::from(socket))
}

pub async fn run(path: &Path) -> Result<()> {
    js::guest_activity::enable();
    let mut bytes = Vec::new();
    File::open(path)?
        .take(wire::MAX_MESSAGE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= wire::MAX_MESSAGE_BYTES,
        "guest configuration exceeds limit"
    );
    let config: Configuration = serde_json::from_slice(&bytes)?;
    ensure!(
        config.version == VERSION,
        "invalid JavaScript guest configuration"
    );
    validate_classes(&config.do_classes)?;
    js::Engine::init();
    let resource = config.resource;
    ensure!(
        resource.resource.kind == ResourceKind::DurableObject,
        "guest namespace must be a Durable Object resource"
    );
    let worker = Worker::load_guest_config(Arc::new(WorkerConfig::new(WorkerConfigOptions {
        src: config.source,
        script_name: resource.storage_root(),
        do_classes: config.do_classes,
        bindings: vec![],
        r2_bindings: vec![],
        d1_bindings: vec![],
        kv_bindings: vec![],
        queue_bindings: vec![],
        queue_consumers: vec![],
        workflow_bindings: vec![],
        vars: vec![],
        node: "guest".into(),
        modules: vec![],
        compat: Default::default(),
    })))?;
    run_worker(vec![resource], worker).await
}

async fn run_worker(resources: Vec<ResourceAddress>, mut worker: Worker) -> Result<()> {
    let mut control = Control::new(connect(5002)?)?;
    control
        .send(GuestControl::Ready {
            version: VERSION,
            adapter: Language::Javascript,
            execution_turns: true,
            // T02-T05 qualified joined terminals, cleanup, callback admission
            // and the host response path on actual VMs. Public execution
            // availability stays separately gated by independent-host proof.
            quiescent_terminals: true,
            cleanup_turns: true,
            callback_turns: true,
            response_turns: true,
            reply_drain: true,
            resident_objects: true,
            object_retirement: true,
            response_streams: true,
            memory_diagnostics: true,
            idle_lifecycle: true,
        })
        .await?;
    let mut retired = HashSet::new();
    let mut residencies = HashMap::new();
    let mut retired_residencies = HashSet::new();
    let mut events = Vec::new();
    let mut cursor = 0;
    let (outbound, outgoing) = mpsc::unbounded_channel();
    let (egress, egress_requests) = mpsc::unbounded_channel();
    js::guest_storage::install_egress(egress)?;
    // A facet event is its own storage stream under the root event's invocation.
    js::guest_storage::install_facet_connector(|cell, invocation, grant| {
        let mut stream = connect(5001)?;
        wire::write_frame(
            &mut stream,
            &StorageConnect {
                version: VERSION,
                invocation: invocation.into(),
                grant: grant.into(),
                cleanup: false,
                facet: true,
            }
            .encode()?,
        )?;
        let channel = Channel::from_stream(cell.into(), grant.into(), stream)?;
        channel.set_invocation(invocation.into());
        Ok(channel)
    });
    let (sockets, socket_frames) = mpsc::unbounded_channel();
    js::guest_storage::install_sockets(sockets)?;
    let mut io = Io {
        outbound,
        outgoing,
        egress: egress_requests,
        sockets: socket_frames,
        requests: HashMap::new(),
        fetches: HashMap::new(),
        dials: HashMap::new(),
        uploads: HashMap::new(),
        streamed_uploads: HashMap::new(),
        signals: HashMap::new(),
    };
    let mut shutting_down = false;
    loop {
        if shutting_down && events.is_empty() {
            break;
        }
        schedule(&mut events);
        io.requests
            .retain(|invocation, _| events.iter().any(|event| &event.invocation == invocation));
        io.fetches.retain(|_, (invocation, _)| {
            events.iter().any(|event| &event.invocation == invocation)
        });
        io.uploads.retain(|fetch, _| io.fetches.contains_key(fetch));
        io.dials.retain(|_, fetch| io.fetches.contains_key(fetch));
        io.streamed_uploads
            .retain(|fetch, _| io.fetches.contains_key(fetch));
        io.signals.retain(|invocation, (_, held)| {
            let live = events.iter().any(|event| &event.invocation == invocation);
            if !live {
                // ponytail: a mirror lives only as long as its callee event, so a
                // signal retained past the event never fires (the original's
                // "source released" state); a VM-scoped hub keyed by caller VM + id
                // would keep retained signals live.
                js::rpc_signal_release_remote(held);
            }
            live
        });
        match next(&mut control, &mut io, &mut events, &mut cursor).await? {
            Action::Send(message) => control.send(message).await?,
            Action::Fetch(request) => {
                let mut request = *request;
                let live = events.iter().find(|event| {
                    !event.cancelled
                        && matches!(&event.phase, Phase::Running(running)
                        if running.entry.as_ref().is_some_and(|entry| entry.issued(&request)))
                });
                let Some(event) = live else {
                    let _ = request
                        .reply
                        .send(Err(js::guest_storage::FETCH_OUTSIDE_EVENT.into()));
                    continue;
                };
                ensure!(io.fetches.len() < 1024, "guest fetch registry exhausted");
                let invocation = event.invocation.clone();
                let fetch = crate::application_storage::handle()?;
                let (pump, items) = mpsc::channel(2);
                io.fetches.insert(fetch.clone(), (invocation.clone(), pump));
                let request_stream = request.upload.is_some();
                let signals = request
                    .service
                    .as_ref()
                    .map_or_else(Vec::new, |service| service.invocation.signals.clone());
                if let Some(source) = request.upload.take() {
                    let (credits, pulls) = mpsc::channel(1);
                    io.streamed_uploads.insert(fetch.clone(), credits);
                    tokio::spawn(js::guest_activity::track(service_upload(
                        invocation.clone(),
                        fetch.clone(),
                        source,
                        pulls,
                        io.outbound.clone(),
                    )));
                }
                let request_bytes = if request.request.body.len()
                    > celld_applications::invocation_wire::MAX_BODY_BYTES
                {
                    let body = std::mem::take(&mut request.request.body);
                    let length = body.len() as u64;
                    io.uploads
                        .insert(fetch.clone(), (body.into(), ResponseFlow::default()));
                    Some(length)
                } else {
                    None
                };
                control
                    .send(GuestControl::FetchStart {
                        version: VERSION,
                        invocation: invocation.clone(),
                        fetch: fetch.clone(),
                        request: request.request,
                        redirect: request.redirect,
                        target: request.target.map(Box::new),
                        service: request.service.map(Box::new),
                        resource: request.resource.map(Box::new),
                        request_stream,
                        request_bytes,
                        dial: request.dial.as_ref().map(|(dial, _)| dial.clone()),
                    })
                    .await?;
                if let Some((_, ws)) = request.dial {
                    io.dials.insert(ws, fetch.clone());
                }
                for signal in signals {
                    let id = js::parse_request_id(&signal).context("invalid service signal id")?;
                    tokio::spawn(js::guest_activity::track(signal_relay(
                        invocation.clone(),
                        fetch.clone(),
                        signal,
                        id,
                        io.fetches[&fetch].1.clone(),
                        io.outbound.clone(),
                    )));
                }
                tokio::spawn(js::guest_activity::track(fetch_pump(
                    invocation,
                    fetch,
                    request.reply,
                    items,
                    io.outbound.clone(),
                )));
            }
            Action::Socket(frame) => {
                route_socket_frame(&control, &mut events, &io.dials, frame).await?;
            }
            Action::Control(Some(Ok(HostControl::IdleProbe { probe, .. }))) => {
                let idle = events.is_empty()
                    && js::guest_activity::idle()
                    && control.incoming.is_empty()
                    && io.outgoing.is_empty()
                    && io.egress.is_empty()
                    && io.sockets.is_empty()
                    && io.requests.is_empty()
                    && io.fetches.is_empty()
                    && io.dials.is_empty()
                    && io.uploads.is_empty()
                    && io.streamed_uploads.is_empty()
                    && io.signals.is_empty();
                control
                    .send(GuestControl::IdleStatus {
                        version: VERSION,
                        probe,
                        idle,
                    })
                    .await?;
            }
            Action::Control(Some(Ok(HostControl::MemorySample { sample, .. }))) => {
                // The control action runs after the preceding turn released V8.
                let stats = worker.memory_sample();
                control
                    .send(GuestControl::MemorySample {
                        version: VERSION,
                        sample,
                        stats,
                    })
                    .await?;
            }
            Action::Control(Some(Ok(HostControl::FetchRequestPull {
                invocation,
                fetch,
                sequence,
                ..
            }))) => {
                // One already-issued credit may arrive while cancellation
                // retires the event; it cannot restore the upload or authority.
                if !events
                    .iter()
                    .any(|event| event.invocation == invocation && !event.cancelled)
                {
                    continue;
                }
                let (owner, _) = io
                    .fetches
                    .get(&fetch)
                    .context("request credit for unknown fetch")?;
                ensure!(owner == &invocation, "request credit for another event");
                if let Some(sender) = io.streamed_uploads.get(&fetch) {
                    sender
                        .try_send(sequence)
                        .context("duplicate service upload credit")?;
                    continue;
                }
                let (remaining, flow) = io
                    .uploads
                    .get_mut(&fetch)
                    .context("request credit for buffered fetch")?;
                flow.grant(sequence)?;
                let message = if remaining.is_empty() {
                    flow.end(sequence)?;
                    GuestControl::FetchRequestEnd {
                        version: VERSION,
                        invocation,
                        fetch: fetch.clone(),
                        sequence,
                        error: None,
                    }
                } else {
                    let data = remaining
                        .split_to(remaining.len().min(MAX_CHUNK_BYTES))
                        .to_vec();
                    flow.chunk(sequence)?;
                    GuestControl::FetchRequestChunk {
                        version: VERSION,
                        invocation,
                        fetch: fetch.clone(),
                        sequence,
                        data,
                    }
                };
                let ended = remaining.is_empty() && flow.is_closed();
                control.send(message).await?;
                if ended {
                    io.uploads.remove(&fetch);
                }
            }
            Action::RpcUploaded(index, result) => {
                events[index].rpc_upload = None;
                let Phase::Starting(invoke, _) = &mut events[index].phase else {
                    anyhow::bail!("RPC upload completed after entry");
                };
                invoke.request_stream = false;
                match result {
                    Ok(body) => invoke.request.body = body,
                    Err(_) => {
                        // A failed upload fails only this event: it unwinds
                        // like a pre-entry cancellation and never enters V8.
                        io.requests.remove(&events[index].invocation);
                        events[index].cancelled = true;
                        events[index].deadline =
                            tokio::time::Instant::now() + Duration::from_secs(5);
                    }
                }
            }
            Action::Control(Some(Ok(HostControl::RequestChunk {
                invocation,
                sequence,
                data,
                ..
            }))) => Io::route(
                &mut io.requests,
                &invocation,
                BodyItem::Chunk(sequence, data),
            )?,
            Action::Control(Some(Ok(HostControl::RequestEnd {
                invocation,
                sequence,
                error,
                ..
            }))) => Io::route(
                &mut io.requests,
                &invocation,
                BodyItem::End(sequence, error),
            )?,
            Action::Control(Some(Ok(
                message @ (HostControl::FetchHead { .. }
                | HostControl::FetchChunk { .. }
                | HostControl::FetchEnd { .. }),
            ))) => {
                let (invocation, fetch, item) = match message {
                    HostControl::FetchHead {
                        invocation,
                        fetch,
                        status,
                        headers,
                        socket,
                        ..
                    } => (invocation, fetch, BodyItem::Head(status, headers, socket)),
                    HostControl::FetchChunk {
                        invocation,
                        fetch,
                        sequence,
                        data,
                        ..
                    } => (invocation, fetch, BodyItem::Chunk(sequence, data)),
                    HostControl::FetchEnd {
                        invocation,
                        fetch,
                        sequence,
                        error,
                        ..
                    } => (invocation, fetch, BodyItem::End(sequence, error)),
                    _ => unreachable!(),
                };
                if let Some((owner, pump)) = io.fetches.get(&fetch) {
                    ensure!(owner == &invocation, "fetch frame names another invocation");
                    let end = matches!(item, BodyItem::End(..));
                    if let Err(mpsc::error::TrySendError::Full(_)) = pump.try_send(item) {
                        anyhow::bail!("host fetch frame exceeded its credit");
                    }
                    if end {
                        io.fetches.remove(&fetch);
                    }
                }
            }
            Action::Control(Some(Ok(HostControl::SocketCredit {
                invocation, count, ..
            }))) => {
                // A credit can follow the event's drain; nothing waits on it.
                let Some(event) = events
                    .iter_mut()
                    .find(|event| event.invocation == invocation)
                else {
                    ensure!(
                        retired.contains(&invocation),
                        "socket credit names an unknown event"
                    );
                    continue;
                };
                if let Phase::Running(running) = &mut event.phase {
                    let sockets = &mut running.sockets;
                    ensure!(
                        sockets.credit + count as usize
                            <= celld_applications::invocation_wire::SOCKET_WINDOW,
                        "host socket credit exceeded the window"
                    );
                    sockets.credit += count as usize;
                    sockets.pump(&control).await?;
                }
                publish(
                    &control,
                    &mut events,
                    None,
                    &mut residencies,
                    &mut retired_residencies,
                    Some((&mut io.sockets, &io.dials)),
                )
                .await?;
            }
            Action::Control(Some(Ok(HostControl::Shutdown { .. }))) => shutting_down = true,
            Action::Control(Some(Ok(HostControl::InvokeHttp(invoke)))) => {
                ensure!(!shutting_down, "invocation after guest shutdown");
                ensure!(
                    events.len() < MAX_ACTIVE,
                    "guest active event limit exceeded"
                );
                ensure!(
                    retired.len() < 65536 && retired.insert(invoke.invocation.clone()),
                    "guest replay ledger exhausted or duplicate invocation"
                );
                // No object target is the original stateless Worker fetch. It
                // carries no residency and never opens guest storage.
                let Some(object) = invoke.object.as_ref() else {
                    ensure!(
                        invoke.storage_residency.is_none(),
                        "stateless invocation names an object residency"
                    );
                    if let Some(service) = invoke.service.as_ref().filter(|s| !s.signals.is_empty())
                    {
                        let ids: Vec<_> = service
                            .signals
                            .iter()
                            .filter_map(|signal| js::parse_request_id(signal))
                            .collect();
                        io.signals.insert(
                            invoke.invocation.clone(),
                            (
                                service.signals.clone(),
                                js::rpc_signal_declare_remote(
                                    service.signal_owner.as_deref().unwrap_or_default(),
                                    &ids,
                                ),
                            ),
                        );
                    }
                    events.push(Event {
                        invocation: invoke.invocation.clone(),
                        cell: String::new(),
                        deadline: tokio::time::Instant::now() + Duration::from_secs(35),
                        cancelled: false,
                        rpc_upload: rpc_upload(&invoke, &mut io),
                        phase: Phase::Starting(*invoke, None),
                    });
                    continue;
                };
                let cell = format!("{}:{}", object.class, object.id);
                ensure!(
                    resources.iter().any(|resource| resource.owns(object)),
                    "object is outside configured guest namespace"
                );
                let residency = invoke
                    .storage_residency
                    .as_ref()
                    .context("object has no residency")?;
                ensure!(
                    !retired_residencies.contains(residency),
                    "object residency was retired"
                );
                ensure!(
                    !events.iter().any(|event: &Event| event.cell == cell
                        && (matches!(event.phase, Phase::Retiring { .. })
                            || matches!(&event.phase, Phase::Running(running)
                                if matches!(running.response, Response::Retirement { .. })))),
                    "object is retiring"
                );
                match residencies.get(&cell).cloned() {
                    Some(previous) if &previous != residency => {
                        // The trusted host retired this residency after a failed
                        // critical section or abort. The original reset dropped
                        // the instance; adopt the successor only once no event
                        // of the old incarnation remains, and never readmit it.
                        ensure!(
                            !events.iter().any(|event: &Event| event.cell == cell),
                            "object residency changed while active; guest must retire"
                        );
                        ensure!(
                            retired_residencies.len() < 65536
                                && retired_residencies.insert(previous),
                            "retired object ledger exhausted"
                        );
                        worker.reset_guest_cell(&cell)?;
                        residencies.insert(cell.clone(), residency.clone());
                    }
                    Some(_) => {}
                    None => {
                        ensure!(residencies.len() < 128, "guest residency count exceeded");
                        residencies.insert(cell.clone(), residency.clone());
                    }
                }
                // Admission must not park the executor driving another event's
                // critical section; schedule() starts the connector off V8.
                let upload = rpc_upload(&invoke, &mut io);
                events.push(Event {
                    invocation: invoke.invocation.clone(),
                    cell,
                    deadline: tokio::time::Instant::now() + Duration::from_secs(35),
                    phase: Phase::Starting(*invoke, None),
                    cancelled: false,
                    rpc_upload: upload,
                });
            }
            Action::Control(Some(Ok(HostControl::RetireObject {
                invocation,
                storage_grant,
                storage_residency,
                object,
                ..
            }))) => {
                ensure!(
                    !shutting_down && events.len() < MAX_ACTIVE,
                    "guest cannot accept retirement"
                );
                ensure!(
                    retired.len() < 65536 && retired.insert(invocation.clone()),
                    "guest replay ledger exhausted or duplicate retirement"
                );
                let cell = format!("{}:{}", object.class, object.id);
                ensure!(
                    resources.iter().any(|resource| resource.owns(&object)),
                    "retirement target is outside configured namespace"
                );
                ensure!(
                    residencies.get(&cell) == Some(&storage_residency),
                    "retirement residency mismatch"
                );
                // The original host supervisor reaps active/uncooperative
                // objects. This message may release only an idle residency.
                ensure!(
                    !events.iter().any(|event| event.cell == cell),
                    "active object requires VM retirement"
                );
                ensure!(
                    retired_residencies.len() < 65536,
                    "retired object ledger exhausted"
                );
                let task = start_channel(cell.clone(), invocation.clone(), storage_grant, true);
                events.push(Event {
                    invocation,
                    cell,
                    deadline: tokio::time::Instant::now() + Duration::from_secs(5),
                    phase: Phase::Retiring {
                        residency: storage_residency,
                        task,
                    },
                    cancelled: false,
                    rpc_upload: None,
                });
            }
            Action::Control(Some(Ok(HostControl::Cancel { invocation, .. }))) => {
                let Some(event) = events
                    .iter_mut()
                    .find(|event| event.invocation == invocation)
                else {
                    ensure!(
                        retired.contains(&invocation),
                        "cancellation names an unknown event"
                    );
                    continue; // A late cancellation cannot resurrect a completed event.
                };
                ensure!(
                    !matches!(event.phase, Phase::Retiring { .. })
                        && !matches!(&event.phase, Phase::Running(running)
                            if matches!(running.response, Response::Retirement { .. })),
                    "object retirement cannot be cancelled as an HTTP event"
                );
                if !event.cancelled {
                    event.cancelled = true;
                    if let Some(upload) = event.rpc_upload.take() {
                        upload.abort();
                        io.requests.remove(&invocation);
                        if let Phase::Starting(invoke, _) = &mut event.phase {
                            invoke.request_stream = false;
                        }
                    }
                    event.deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                    if let Phase::Running(running) = &mut event.phase {
                        // Closing the original consumer wakes its producer's
                        // cancellation watch; cleanup still needs host admission.
                        running.body.take();
                        running.channel.request_cleanup();
                    }
                    event.start_cancellation();
                }
            }
            Action::Control(Some(Ok(HostControl::SignalAbort {
                invocation,
                signal,
                reason,
                ..
            }))) => {
                let Some((declared, held)) = io.signals.get(&invocation) else {
                    ensure!(
                        retired.contains(&invocation)
                            && !events.iter().any(|event| event.invocation == invocation),
                        "signal abort names an unknown or undeclared event"
                    );
                    continue; // A late relay cannot reach a completed event.
                };
                ensure!(declared.contains(&signal), "signal abort was not declared");
                // A declared id this VM already owns as a source is not a mirror;
                // its own source aborts it.
                if let Some(id) = js::parse_request_id(&signal).filter(|id| held.contains(id)) {
                    js::rpc_signal_abort(id, reason);
                }
            }
            Action::Control(Some(Ok(HostControl::ResponsePull {
                invocation,
                sequence,
                ..
            }))) => {
                let event = events
                    .iter_mut()
                    .find(|event| event.invocation == invocation)
                    .context("response credit names an unknown/retired event")?;
                ensure!(!event.cancelled, "response credit after cancellation");
                let Phase::Running(running) = &mut event.phase else {
                    anyhow::bail!("response credit before stream head");
                };
                running
                    .body
                    .as_mut()
                    .context("response credit for buffered event")?
                    .flow
                    .grant(sequence)?;
            }
            Action::Control(Some(Err(error))) => return Err(error),
            Action::Control(None) => anyhow::bail!("host control ended before shutdown drained"),
            Action::Started(index, channel) => {
                let (channel, admitted) = channel?;
                if events[index].cancelled || admitted.is_err() {
                    events[index].cancelled = true;
                    events[index].deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                    channel.request_cleanup();
                    events[index].phase = Phase::Cancelling(Cancelling {
                        channel,
                        running: None,
                        admission: None,
                    });
                    continue;
                }
                admitted?;
                let invocation = events[index].invocation.clone();
                let Phase::Starting(invoke, _) = &mut events[index].phase else {
                    unreachable!()
                };
                let (reply, response) = oneshot::channel();
                let url = std::mem::take(&mut invoke.request.url);
                let method = std::mem::take(&mut invoke.request.method);
                let body = if invoke.request_stream {
                    // At most one credited host chunk is buffered for JS.
                    let (sender, receiver) = mpsc::channel(1);
                    let stream = js::register_body_stream(Box::pin(
                        tokio_stream::wrappers::ReceiverStream::new(receiver),
                    ))
                    .map_err(anyhow::Error::msg)?;
                    let (pump, items) = mpsc::channel(2);
                    io.requests.insert(invocation.clone(), pump);
                    tokio::spawn(js::guest_activity::track(request_pump(
                        invocation.clone(),
                        sender,
                        items,
                        io.outbound.clone(),
                    )));
                    RequestBody::Stream(stream)
                } else {
                    RequestBody::Bytes(std::mem::take(&mut invoke.request.body).into())
                };
                let headers = std::mem::take(&mut invoke.request.headers);
                let (entry, ops, response) = if invoke.queue {
                    // The original queue consumer: the batch is a buffered JSON
                    // body, and a handler exception is still a settlement.
                    let RequestBody::Bytes(batch) = body else {
                        anyhow::bail!("guest queue batch must be buffered");
                    };
                    let (reply, result) = oneshot::channel();
                    let (entry, ops) = match crate::application_guest::decode_queue_batch(&batch) {
                        Ok(batch) => worker.turn_begin(
                            crate::WorkerJob::Queue {
                                queued_at: std::time::Instant::now(),
                                batch,
                                reply,
                            },
                            None,
                        ),
                        Err(error) => {
                            let _ = reply.send(Err(error));
                            (None, Vec::new())
                        }
                    };
                    (entry, ops, Response::Queue(result))
                } else if channel.is_detached() {
                    let service = invoke.service.take();
                    if let Some(service) = service.as_ref().filter(|service| service.rpc.is_some())
                    {
                        let RequestBody::Bytes(args) = body else {
                            anyhow::bail!("guest service RPC arguments must be buffered");
                        };
                        let path = service.rpc.clone().expect("selected RPC");
                        let operation = if service.get {
                            crate::WorkerRpcOperation::Get { path }
                        } else {
                            crate::WorkerRpcOperation::Call {
                                path,
                                args: args.to_vec(),
                            }
                        };
                        let (reply, outcome) = oneshot::channel();
                        let (entry, ops) = worker.turn_begin(
                            crate::WorkerJob::Rpc {
                                entrypoint: service
                                    .entrypoint
                                    .clone()
                                    .unwrap_or_else(|| "default".into()),
                                props: service.props.clone(),
                                operation,
                                invocation_limits: None,
                                reply,
                            },
                            None,
                        );
                        (entry, ops, Response::Rpc(RpcReply::Service(outcome)))
                    } else {
                        let (entry, ops) = worker.turn_begin(
                            crate::WorkerJob::Fetch {
                                queued_at: std::time::Instant::now(),
                                entrypoint: service.and_then(|service| {
                                    service.entrypoint.map(|name| crate::WorkerFetchEntrypoint {
                                        name,
                                        props: service.props,
                                    })
                                }),
                                invocation_limits: None,
                                url,
                                method,
                                body,
                                headers,
                                request_id: Some(js::next_request_id()),
                                tail_report: None,
                                reply,
                            },
                            None,
                        );
                        (entry, ops, Response::Http(response))
                    }
                } else {
                    let object = invoke
                        .object
                        .take()
                        .context("guest object target disappeared")?;
                    let scope = format!("{}:{}", object.class, object.id);
                    if let Some(alarm) = invoke.alarm.take() {
                        let (reply, result) = oneshot::channel();
                        let (entry, ops) = worker.turn_begin_guest_cell(
                            CellJob::Alarm {
                                request_id: Some(js::next_request_id()),
                                scope,
                                scheduled_ms: alarm.scheduled_ms,
                                claim: js::AlarmDispatch::Guest { retry: alarm.retry },
                                reply,
                            },
                            None,
                            channel.clone(),
                        )?;
                        (
                            entry,
                            ops,
                            Response::Alarm {
                                result,
                                counts_against_limit: false,
                            },
                        )
                    } else if let Some(socket) = invoke.socket.take() {
                        let RequestBody::Bytes(message) = body else {
                            anyhow::bail!("guest socket message must be buffered");
                        };
                        let ws_id = socket.ws;
                        let (entry, ops, reply) = match socket.event {
                            celld_applications::invocation_wire::SocketEvent::Close {
                                code,
                                reason,
                                was_clean,
                            } => {
                                let (reply, result) = oneshot::channel();
                                let (entry, ops) = worker.turn_begin_guest_cell(
                                    CellJob::WsClosed {
                                        scope,
                                        ws_id,
                                        facet_opening: None,
                                        code,
                                        reason,
                                        was_clean,
                                        reply,
                                    },
                                    None,
                                    channel.clone(),
                                )?;
                                (entry, ops, SocketReply::Closed(result))
                            }
                            event => {
                                let data = if matches!(
                                    event,
                                    celld_applications::invocation_wire::SocketEvent::Text
                                ) {
                                    js::WsIn::Text(
                                        String::from_utf8(message.to_vec())
                                            .context("WebSocket text message is not UTF-8")?,
                                    )
                                } else {
                                    js::WsIn::Binary(message.to_vec())
                                };
                                let (reply, result) = oneshot::channel();
                                let (entry, ops) = worker.turn_begin_guest_cell(
                                    CellJob::WsMessage {
                                        scope,
                                        ws_id,
                                        facet_opening: None,
                                        data,
                                        started: None,
                                        reply,
                                    },
                                    None,
                                    channel.clone(),
                                )?;
                                (entry, ops, SocketReply::Message(result))
                            }
                        };
                        (entry, ops, Response::Socket(reply))
                    } else if let Some(method) = invoke.rpc.take() {
                        let RequestBody::Bytes(args) = body else {
                            anyhow::bail!("guest RPC arguments must be buffered");
                        };
                        let (reply, outcome) = oneshot::channel();
                        let (entry, ops) = worker.turn_begin_guest_cell(
                            CellJob::Rpc {
                                request_id: Some(js::next_request_id()),
                                scope,
                                name: object.name,
                                method,
                                args: js::RpcData::V8(args),
                                reply,
                            },
                            None,
                            channel.clone(),
                        )?;
                        (entry, ops, Response::Rpc(RpcReply::Object(outcome)))
                    } else {
                        let (entry, ops) = worker.turn_begin_guest_cell(
                            CellJob::Fetch {
                                request_id: Some(js::next_request_id()),
                                scope,
                                name: object.name,
                                url,
                                method,
                                body,
                                headers,
                                reply,
                                order: None,
                            },
                            None,
                            channel.clone(),
                        )?;
                        (entry, ops, Response::Http(response))
                    }
                };
                let finished = finish(channel.clone()).await;
                ensure!(
                    entry.is_some() || ops.is_empty(),
                    "guest entry lost operation ownership"
                );
                events[index].phase = Phase::Running(Running {
                    channel,
                    entry,
                    response,
                    ops: ops.into_iter().map(Operation).collect(),
                    admission: None,
                    delivery: None,
                    poll_requested: false,
                    reply_sent: false,
                    failure: None,
                    turns: 1,
                    cancel_started: false,
                    body: None,
                    sockets: SocketOut::default(),
                });
                if finished.is_err() {
                    lost_ordinary_stream(&mut events[index])?;
                    continue;
                }
                publish(
                    &control,
                    &mut events,
                    Some(index),
                    &mut residencies,
                    &mut retired_residencies,
                    Some((&mut io.sockets, &io.dials)),
                )
                .await?;
            }
            Action::Operation(index, id, result) => {
                let Phase::Running(event) = &mut events[index].phase else {
                    unreachable!()
                };
                ensure!(
                    event.delivery.is_none() && event.admission.is_none(),
                    "overlapping guest event turns"
                );
                event.delivery = Some((id, result));
            }
            Action::Body(index, item) => {
                let invocation = events[index].invocation.clone();
                let Phase::Running(event) = &mut events[index].phase else {
                    unreachable!()
                };
                let body = event.body.as_mut().context("response source disappeared")?;
                let sequence = body.flow.next_sequence();
                let message = match item {
                    Some(Ok(data)) => {
                        ensure!(
                            !data.is_empty() && data.len() <= MAX_CHUNK_BYTES,
                            "response producer chunk exceeds wire bound"
                        );
                        body.flow.chunk(sequence)?;
                        GuestControl::ResponseChunk {
                            version: VERSION,
                            invocation,
                            sequence,
                            data,
                        }
                    }
                    ended => {
                        body.flow.end(sequence)?;
                        GuestControl::ResponseEnd {
                            version: VERSION,
                            invocation,
                            sequence,
                            error: ended.map(|_| "JavaScript response producer failed".into()),
                        }
                    }
                };
                control.send(message).await?;
                publish(
                    &control,
                    &mut events,
                    None,
                    &mut residencies,
                    &mut retired_residencies,
                    Some((&mut io.sockets, &io.dials)),
                )
                .await?;
            }
            Action::Admitted(index, admission) => {
                if admission.is_err() {
                    ensure!(
                        !matches!(&events[index].phase, Phase::Running(running)
                            if matches!(running.response, Response::Retirement { .. })),
                        "guest retirement admission refused"
                    );
                    events[index].cancelled = true;
                    events[index].deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                }
                let cancelled = events[index].cancelled;
                let Phase::Running(running) = &mut events[index].phase else {
                    unreachable!()
                };
                if cancelled && !running.cancel_started {
                    // Joining the old task (including native refusal) precedes
                    // cleanup selection. No cancelled ordinary turn enters V8.
                    running.admission = None;
                    events[index].start_cancellation();
                    continue;
                }
                admission?;
                let Phase::Running(event) = &mut events[index].phase else {
                    unreachable!()
                };
                event.admission = None;
                event.poll_requested = false;
                event.turns += 1;
                ensure!(event.turns <= 65536, "guest operation count exceeded");
                let entry = event.entry.as_mut().context("guest turn has no event")?;
                let delivery = event.delivery.take();
                let delivered = delivery.is_some();
                let ops = match delivery {
                    Some((id, result)) if event.channel.is_detached() => {
                        worker.turn_deliver(entry, id, result)
                    }
                    None if event.channel.is_detached() => worker.turn_poll(entry),
                    Some((id, result)) => worker.turn_deliver_guest_cell(entry, id, result)?,
                    None => worker.turn_poll_guest_cell(entry)?,
                };
                event.ops.extend(ops.into_iter().map(Operation));
                if finish(event.channel.clone()).await.is_err() {
                    lost_ordinary_stream(&mut events[index])?;
                    continue;
                }
                publish(
                    &control,
                    &mut events,
                    delivered.then_some(index),
                    &mut residencies,
                    &mut retired_residencies,
                    Some((&mut io.sockets, &io.dials)),
                )
                .await?;
            }
            Action::RetirementAdmitted(index, channel) => {
                let (channel, admitted) = channel?;
                admitted?;
                let (reply, ack) = oneshot::channel();
                let (entry, ops) =
                    worker.turn_release_guest_cell(&events[index].cell, channel.clone(), reply)?;
                finish(channel.clone()).await?;
                let Phase::Retiring { residency, .. } = &events[index].phase else {
                    unreachable!()
                };
                let residency = residency.clone();
                ensure!(
                    entry.is_some() || ops.is_empty(),
                    "guest release lost operation ownership"
                );
                events[index].phase = Phase::Running(Running {
                    channel,
                    entry,
                    response: Response::Retirement { residency, ack },
                    ops: ops.into_iter().map(Operation).collect(),
                    admission: None,
                    delivery: None,
                    poll_requested: false,
                    reply_sent: false,
                    failure: None,
                    turns: 1,
                    cancel_started: false,
                    body: None,
                    sockets: SocketOut::default(),
                });
                // Original release can settle another object's reactions;
                // retain the existing event-owned mailbox/poll machinery.
                publish(
                    &control,
                    &mut events,
                    None,
                    &mut residencies,
                    &mut retired_residencies,
                    Some((&mut io.sockets, &io.dials)),
                )
                .await?;
            }
            Action::CleanupAdmitted(index, admission) => {
                admission?;
                let Phase::Cancelling(cancelling) = &mut events[index].phase else {
                    unreachable!()
                };
                let channel = cancelling.channel.clone();
                if let Some(mut running) = cancelling.running.take() {
                    if channel.is_detached() {
                        // A stateless fetch keeps original client-disconnect
                        // semantics: waitUntil work and its operations go on.
                        if let Some(entry) = &mut running.entry {
                            let ops = worker.turn_cancel(entry);
                            running.ops.extend(ops.into_iter().map(Operation));
                        }
                    } else {
                        if let Some(entry) = &mut running.entry {
                            let ops = worker.turn_cancel_guest_cell(entry)?;
                            // Original cancellation retired the old resolvers. Drop
                            // their native futures while the cleanup turn is owned.
                            running.ops = ops.into_iter().map(Operation).collect();
                        }
                        running.delivery = None;
                    }
                    running.admission = None;
                    running.poll_requested = false;
                    running.cancel_started = true;
                    finish(channel).await?;
                    events[index].phase = Phase::Running(running);
                    publish(
                        &control,
                        &mut events,
                        Some(index),
                        &mut residencies,
                        &mut retired_residencies,
                        Some((&mut io.sockets, &io.dials)),
                    )
                    .await?;
                } else {
                    // Pre-entry cancellation never constructs or invokes user
                    // code, but still joins its native cleanup admission.
                    finish(channel.clone()).await?;
                    channel.retire();
                    let event = events.remove(index);
                    control
                        .send(GuestControl::Cancelled {
                            version: VERSION,
                            invocation: event.invocation,
                        })
                        .await?;
                }
            }
        }
    }
    Ok(())
}
