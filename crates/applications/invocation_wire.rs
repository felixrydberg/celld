//! Buffered/credited HTTP bodies in both directions, distinct body end and
//! joined event drain, and host-brokered outbound fetch. Every credited
//! direction reuses one ledger. Host positions, routing selectors, proof
//! receipts and network access are never guest fields.

use crate::storage_wire::{self, valid_handle, MAX_MESSAGE_BYTES, VERSION};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

pub const MAX_MEMORY_SAMPLE_BYTES: usize = 4096;
pub const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;
/// Original ordinary forwarding ceiling: `actor::DEFAULT_MAX_REQUEST_BODY_BYTES`
/// plus the existing peer metadata allowance (`main::MAX_PEER_FORWARD_BODY_BYTES`).
/// Large object-call arguments cross in credited chunks under this aggregate bound.
pub const MAX_OBJECT_CALL_BYTES: usize = (1 << 30) + (1 << 20);
/// A queue batch is at most 100 messages of 128 kB, base64 inside JSON.
pub const MAX_QUEUE_BATCH_BYTES: usize = 32 << 20;
pub const MAX_CHUNK_BYTES: usize = 64 * 1024;
/// Live AbortSignals one service RPC may carry, and one abort reason's bytes.
pub const MAX_SERVICE_SIGNALS: usize = 32;
pub const MAX_SIGNAL_REASON_BYTES: usize = 64 * 1024;

/// A signal id is the guest registry's 128-bit identity as 32 lowercase hex.
pub fn valid_signal(value: &str) -> bool {
    value.len() == 32 && value.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// One correlated body stream, with at most one consumer-issued credit. It
/// serves responses, request bodies and brokered fetch replies alike. This
/// ledger is flow control, never resource authority or durability proof.
#[derive(Default)]
pub struct ResponseFlow {
    next: u64,
    credit: bool,
    closed: bool,
}

impl ResponseFlow {
    pub fn next_sequence(&self) -> u64 {
        self.next
    }
    pub fn has_credit(&self) -> bool {
        self.credit
    }
    pub fn is_closed(&self) -> bool {
        self.closed
    }
    pub fn grant(&mut self, sequence: u64) -> Result<()> {
        ensure!(
            !self.closed && !self.credit && sequence == self.next,
            "duplicate, closed or out-of-order response credit"
        );
        self.credit = true;
        Ok(())
    }
    pub fn chunk(&mut self, sequence: u64) -> Result<()> {
        ensure!(
            !self.closed && self.credit && sequence == self.next,
            "response chunk has no matching credit"
        );
        let next = self
            .next
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("response sequence exhausted"))?;
        self.next = next;
        self.credit = false;
        Ok(())
    }
    pub fn end(&mut self, sequence: u64) -> Result<()> {
        ensure!(
            !self.closed && self.credit && sequence == self.next,
            "response end has no matching credit"
        );
        self.credit = false;
        self.closed = true;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpStreamHead {
    pub version: u16,
    pub invocation: String,
    pub status: u16,
    pub headers: Vec<(String, String)>,
}

impl HttpStreamHead {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.version == VERSION && valid_handle(&self.invocation),
            "invalid response stream identity"
        );
        ensure!(
            (200..=599).contains(&self.status),
            "invalid final HTTP status"
        );
        http_fields(&self.headers, &[])
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    #[serde(with = "storage_wire::binary")]
    pub body: Vec<u8>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    #[serde(with = "storage_wire::binary")]
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectTarget {
    pub class: String,
    pub id: String,
    /// Absent for a unique id, as for original `newUniqueId`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl ObjectTarget {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.class.is_empty()
                && !self.class.contains(':')
                && celld_logic::cell::valid_cell_scope(&self.class)
                && valid_handle(&self.id)
                && celld_logic::cell::valid_cell_scope(&format!("{}:{}", self.class, self.id)),
            "invalid guest object target"
        );
        Ok(())
    }
}

/// A guest's call to a Durable Object, sent as `FetchStart.target` in place of
/// network egress. The host takes no resource, grant or route from the guest:
/// it finds the namespace the id authenticates under among the handler's own.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectCall {
    pub object: ObjectTarget,
    /// An RPC method name; the request body is then its structured-clone
    /// arguments and the response body its tagged result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc: Option<String>,
}

impl ObjectCall {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.rpc
                .as_ref()
                .is_none_or(|method| !method.is_empty() && method.len() <= 1024),
            "invalid object RPC method"
        );
        self.object.validate()
    }
}

/// Entrypoint selection and RPC receiver path; argument bytes remain in the
/// credited request body. These fields select behavior, never host authority.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceInvocation {
    pub entrypoint: Option<String>,
    #[serde(with = "storage_wire::binary")]
    pub props: Vec<u8>,
    pub rpc: Option<Vec<String>>,
    pub get: bool,
    /// Live AbortSignals the caller lifted into the RPC arguments. The callee
    /// mirrors exactly these under the caller's ids; the opaque bytes of each
    /// abort follow on the control stream.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub signals: Vec<String>,
    /// Set by the trusted host to the calling transport's random token, never by
    /// a guest. A mirror belongs to the first owner that declares it, so a
    /// receiver that only learned a signal's id cannot abort it elsewhere.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal_owner: Option<String>,
}

impl ServiceInvocation {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.entrypoint
                .as_ref()
                .is_none_or(|name| !name.is_empty() && name.len() <= 1024),
            "invalid service entrypoint"
        );
        ensure!(
            self.props.len() <= 1024 * 1024,
            "service props exceed limit"
        );
        ensure!(
            self.rpc.as_ref().is_none_or(|path| path.len() <= 5120),
            "invalid service RPC path"
        );
        ensure!(
            !self.get || self.rpc.is_some(),
            "service property read requires an RPC path"
        );
        ensure!(
            self.signals.len() <= MAX_SERVICE_SIGNALS
                && self.signals.iter().all(|signal| valid_signal(signal))
                && (self.signals.is_empty() || self.rpc.is_some()),
            "invalid service RPC signals"
        );
        ensure!(
            self.signal_owner.as_deref().is_none_or(valid_handle),
            "invalid service signal owner"
        );
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceCall {
    /// Only a binding alias. The pinned host handler supplies the target.
    pub binding: crate::Name,
    pub invocation: ServiceInvocation,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlarmInvocation {
    pub scheduled_ms: i64,
    pub retry: i64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvokeHttp {
    pub version: u16,
    pub invocation: String,
    pub storage_grant: String,
    /// Opaque VM/object/connection correlation for saved guest read cursors.
    /// It grants no storage access; every operation still needs an event grant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_residency: Option<String>,
    /// Display/dispatch metadata selected by trusted scoped admission. Neither
    /// these fields nor the residency correlation confer storage authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<ObjectTarget>,
    pub request: HttpRequest,
    /// The body arrives as credited `RequestChunk`/`RequestEnd` frames; the
    /// buffered body is then empty.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub request_stream: bool,
    /// An RPC on `object` rather than a fetch; see `ObjectCall::rpc`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc: Option<String>,
    /// An original queue consumer batch rather than a fetch: stateless, and
    /// the body is the JSON batch, buffered or streamed.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub queue: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<ServiceInvocation>,
    /// Host-claimed Durable Object alarm, dispatched through the original JS API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alarm: Option<AlarmInvocation>,
    /// One event of a hibernatable WebSocket the host owns, dispatched through
    /// the original `webSocketMessage`/`webSocketClose` API. A message's bytes
    /// are the buffered request body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socket: Option<SocketInvocation>,
}

/// The host id of a socket in the issuing object's registry and what happened
/// on it. The id names nothing the guest could reach on its own: a frame sent
/// for it is checked against the event's object.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SocketInvocation {
    pub ws: u64,
    pub event: SocketEvent,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SocketEvent {
    Text,
    Binary,
    Close {
        code: u16,
        reason: String,
        was_clean: bool,
    },
}

/// One frame a guest sends on a hibernatable socket of its object. The host
/// proves the event's output, then releases it into the socket's ordered
/// stream. A message above `MAX_BODY_BYTES` is refused where it is sent.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SocketFrame {
    Text {
        data: String,
    },
    Binary {
        #[serde(with = "storage_wire::binary")]
        data: Vec<u8>,
    },
    Close {
        code: u16,
        reason: String,
    },
}

impl SocketFrame {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Text { data } => {
                ensure!(data.len() <= MAX_BODY_BYTES, "WebSocket message exceeds limit")
            }
            Self::Binary { data } => {
                ensure!(data.len() <= MAX_BODY_BYTES, "WebSocket message exceeds limit")
            }
            Self::Close { reason, .. } => {
                ensure!(reason.len() <= 123, "WebSocket close reason exceeds limit")
            }
        }
        Ok(())
    }
}

/// A guest's outbound WebSocket, dialed by the trusted host as a brokered fetch
/// (`FetchStart.dial`): the request is the handshake (a `GET` whose `http(s)`
/// URL is the `ws(s)` twin, headers for the `fetch()` upgrade form). The reply
/// head is the 101 with the negotiated subprotocol, or the ordinary response a
/// server sent instead when `response` is set. Each credited body chunk is one
/// inbound frame (see `DIAL_*`); the guest's frames name the fetch.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SocketDial {
    pub protocols: Vec<String>,
    /// The `fetch()` upgrade form wants a declining server's response back.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub response: bool,
}

impl SocketDial {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.protocols.len() <= 32 && self.protocols.iter().all(|protocol| token(protocol)),
            "invalid WebSocket subprotocols"
        );
        Ok(())
    }
}

/// An inbound frame of a dialed socket crosses as credited chunks of at most
/// `MAX_CHUNK_BYTES`: a continuation flag, then the frame's `WsPull` bytes
/// (tag first), the last chunk of a frame carrying `DIAL_LAST`.
pub const DIAL_MORE: u8 = 1;
pub const DIAL_LAST: u8 = 0;

/// Frames a guest may have sent without a `SocketCredit`, per event.
pub const SOCKET_WINDOW: usize = 4;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpResult {
    pub version: u16,
    pub invocation: String,
    pub response: HttpResponse,
    /// The 101 upgrade of a hibernatable socket: the host id the guest was
    /// given by `SocketAccept`, or, for a handler returning another object's
    /// upgrade, the id the host handed it in `FetchHead`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socket: Option<u64>,
}

impl HttpRequest {
    pub fn validate(&self) -> Result<()> {
        ensure!(token(&self.method), "invalid HTTP method");
        ensure!(
            self.url.len() <= 8192
                && (self.url.starts_with("http://") || self.url.starts_with("https://"))
                && self.url.bytes().all(|byte| byte > 32 && byte != 127),
            "invalid HTTP URL"
        );
        http_fields(&self.headers, &self.body)
    }
}

/// Original fetch redirect modes; the host, not the guest, follows them.
fn redirect(value: &str) -> bool {
    matches!(value, "follow" | "manual" | "error")
}

fn chunk(sequence: u64, data: &[u8]) -> Result<()> {
    ensure!(
        sequence != u64::MAX && !data.is_empty() && data.len() <= MAX_CHUNK_BYTES,
        "invalid body chunk"
    );
    Ok(())
}

fn end(error: &Option<String>) -> Result<()> {
    ensure!(
        error
            .as_ref()
            .is_none_or(|error| !error.is_empty() && error.len() <= 4096),
        "invalid body end"
    );
    Ok(())
}

fn token(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
}

fn http_fields(headers: &[(String, String)], body: &[u8]) -> Result<()> {
    ensure!(body.len() <= MAX_BODY_BYTES, "HTTP body exceeds limit");
    ensure!(headers.len() <= 1024, "HTTP header count exceeds limit");
    let mut total = 0;
    for (name, value) in headers {
        total += name.len() + value.len();
        ensure!(total <= 65536, "HTTP headers exceed limit");
        ensure!(token(name), "invalid HTTP header name");
        ensure!(
            value
                .bytes()
                .all(|byte| byte == b'\t' || (byte >= 32 && byte != 127)),
            "invalid HTTP header value"
        );
    }
    Ok(())
}

impl InvokeHttp {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.version == VERSION,
            "unsupported invocation protocol version"
        );
        ensure!(
            valid_handle(&self.invocation) && valid_handle(&self.storage_grant),
            "invalid invocation capability"
        );
        ensure!(
            self.storage_residency.as_deref().is_none_or(valid_handle),
            "invalid storage residency"
        );
        if let Some(object) = &self.object {
            ensure!(
                self.storage_residency.is_some(),
                "object target requires residency"
            );
            object.validate()?;
        }
        ensure!(
            !self.request_stream || self.request.body.is_empty(),
            "streamed request also carries a buffered body"
        );
        ensure!(
            // A streamed RPC body is assembled by the guest before entry.
            self.rpc.is_none() || self.object.is_some(),
            "RPC requires an object"
        );
        ensure!(
            !self.queue
                || (self.object.is_none()
                    && self.storage_residency.is_none()
                    && self.rpc.is_none()),
            "queue invocation must be stateless"
        );
        if let Some(service) = &self.service {
            ensure!(
                self.object.is_none()
                    && self.storage_residency.is_none()
                    && self.rpc.is_none()
                    && !self.queue
                    && self.alarm.is_none(),
                "service invocation must be stateless"
            );
            service.validate()?;
        }
        if let Some(socket) = &self.socket {
            ensure!(
                self.object.is_some()
                    && self.storage_residency.is_some()
                    && self.rpc.is_none()
                    && !self.queue
                    && !self.request_stream
                    && self.service.is_none()
                    && self.alarm.is_none()
                    && match &socket.event {
                        SocketEvent::Text | SocketEvent::Binary => true,
                        SocketEvent::Close { reason, .. } => {
                            self.request.body.is_empty() && reason.len() <= 4096
                        }
                    },
                "socket invocation requires an exclusive object event"
            );
        }
        if let Some(alarm) = &self.alarm {
            ensure!(
                self.object.is_some()
                    && self.storage_residency.is_some()
                    && self.rpc.is_none()
                    && !self.queue
                    && !self.request_stream
                    && self.service.is_none()
                    && self.request.body.is_empty()
                    && alarm.retry >= 0,
                "alarm invocation requires an exclusive object event"
            );
        }
        self.request.validate()
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let bytes = serde_json::to_vec(self)?;
        ensure!(
            bytes.len() <= MAX_MESSAGE_BYTES,
            "invocation message exceeds limit"
        );
        Ok(bytes)
    }
}

impl HttpResult {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.version == VERSION,
            "unsupported invocation protocol version"
        );
        ensure!(
            valid_handle(&self.invocation),
            "invalid invocation capability"
        );
        ensure!(
            if self.socket.is_some() {
                self.response.status == 101 && self.response.body.is_empty()
            } else {
                (200..=599).contains(&self.response.status)
            },
            "invalid final HTTP status"
        );
        http_fields(&self.response.headers, &self.response.body)
    }

    pub fn decode(bytes: &[u8], invocation: &str) -> Result<Self> {
        ensure!(
            bytes.len() <= MAX_MESSAGE_BYTES,
            "invocation message exceeds limit"
        );
        let result: Self = serde_json::from_slice(bytes)?;
        result.validate()?;
        ensure!(
            valid_handle(invocation) && result.invocation == invocation,
            "result belongs to another invocation"
        );
        Ok(result)
    }
}

/// Internal observations overlap; they are not additive physical charges.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeapMemory {
    pub used_bytes: u64,
    pub physical_bytes: u64,
    pub external_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllocatorMemory {
    pub allocated_bytes: u64,
    pub active_bytes: u64,
    pub resident_bytes: u64,
    pub mapped_bytes: u64,
    pub retained_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LibcMemory {
    pub in_use_bytes: u64,
    pub free_bytes: u64,
    pub mmap_bytes: u64,
    pub arena_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuestMemorySample {
    pub rss_bytes: u64,
    pub available_bytes: Option<u64>,
    pub root: HeapMemory,
    pub total: HeapMemory,
    pub isolates: u32,
    pub busy_isolates: u32,
    pub loading_isolates: u32,
    pub complete: bool,
    pub jemalloc: Option<AllocatorMemory>,
    pub libc_malloc: Option<LibcMemory>,
}

/// Always-pumped control stream, separate from each event's blocking storage.
/// Cancellation is a request; a terminal guest message is not durability proof.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum HostControl {
    IdleProbe {
        version: u16,
        probe: String,
    },
    MemorySample {
        version: u16,
        sample: String,
    },
    InvokeHttp(Box<InvokeHttp>),
    RetireObject {
        version: u16,
        invocation: String,
        storage_grant: String,
        storage_residency: String,
        object: ObjectTarget,
    },
    Cancel {
        version: u16,
        invocation: String,
    },
    ResponsePull {
        version: u16,
        invocation: String,
        sequence: u64,
    },
    /// One credited request-body item for a `request_stream` invocation.
    RequestChunk {
        version: u16,
        invocation: String,
        sequence: u64,
        #[serde(with = "storage_wire::binary")]
        data: Vec<u8>,
    },
    RequestEnd {
        version: u16,
        invocation: String,
        sequence: u64,
        error: Option<String>,
    },
    /// The host performed a brokered fetch; its body follows under credit.
    FetchHead {
        version: u16,
        invocation: String,
        fetch: String,
        status: u16,
        headers: Vec<(String, String)>,
        /// A callee object's accepted hibernatable socket, for a 101. Only an
        /// id the host issued here can come back in this event's `HttpResult`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        socket: Option<u64>,
    },
    /// The host released `count` of an event's `SocketFrame`s.
    SocketCredit {
        version: u16,
        invocation: String,
        count: u32,
    },
    FetchRequestPull {
        version: u16,
        invocation: String,
        fetch: String,
        sequence: u64,
    },
    FetchChunk {
        version: u16,
        invocation: String,
        fetch: String,
        sequence: u64,
        #[serde(with = "storage_wire::binary")]
        data: Vec<u8>,
    },
    /// Body end, or a refusal/failure before or during the reply.
    FetchEnd {
        version: u16,
        invocation: String,
        fetch: String,
        sequence: u64,
        error: Option<String>,
    },
    /// A declared service-RPC signal of this callee event aborted in its caller.
    SignalAbort {
        version: u16,
        invocation: String,
        signal: String,
        #[serde(with = "storage_wire::binary")]
        reason: Vec<u8>,
    },
    Shutdown {
        version: u16,
    },
}

impl HostControl {
    fn validate(&self) -> Result<()> {
        match self {
            Self::IdleProbe { version, probe } => {
                ensure!(
                    *version == VERSION && valid_handle(probe),
                    "invalid guest idle probe"
                );
                Ok(())
            }
            Self::MemorySample { version, sample } => {
                ensure!(
                    *version == VERSION && valid_handle(sample),
                    "invalid memory sample request"
                );
                Ok(())
            }
            Self::FetchRequestPull {
                version,
                invocation,
                fetch,
                sequence,
            } => {
                ensure!(
                    *version == VERSION
                        && valid_handle(invocation)
                        && valid_handle(fetch)
                        && *sequence != u64::MAX,
                    "invalid fetch request credit"
                );
                Ok(())
            }
            Self::InvokeHttp(invoke) => invoke.validate(),
            Self::RetireObject {
                version,
                invocation,
                storage_grant,
                storage_residency,
                object,
            } => {
                ensure!(
                    *version == VERSION
                        && valid_handle(invocation)
                        && valid_handle(storage_grant)
                        && valid_handle(storage_residency),
                    "invalid object retirement"
                );
                object.validate()
            }
            Self::ResponsePull {
                version,
                invocation,
                ..
            }
            | Self::Cancel {
                version,
                invocation,
            } => {
                ensure!(
                    *version == VERSION && valid_handle(invocation),
                    "invalid cancellation"
                );
                Ok(())
            }
            Self::RequestChunk {
                version,
                invocation,
                sequence,
                data,
            } => {
                ensure!(
                    *version == VERSION && valid_handle(invocation),
                    "invalid request chunk"
                );
                chunk(*sequence, data)
            }
            Self::RequestEnd {
                version,
                invocation,
                error,
                ..
            } => {
                ensure!(
                    *version == VERSION && valid_handle(invocation),
                    "invalid request end"
                );
                end(error)
            }
            Self::FetchHead {
                version,
                invocation,
                fetch,
                status,
                headers,
                socket,
            } => {
                ensure!(
                    *version == VERSION && valid_handle(invocation) && valid_handle(fetch),
                    "invalid fetch head"
                );
                ensure!((100..=599).contains(status), "invalid fetch status");
                ensure!(
                    socket.is_none() || *status == 101,
                    "socket on a non-upgrade fetch head"
                );
                http_fields(headers, &[])
            }
            Self::SocketCredit {
                version,
                invocation,
                count,
            } => {
                ensure!(
                    *version == VERSION
                        && valid_handle(invocation)
                        && (1..=SOCKET_WINDOW as u32).contains(count),
                    "invalid socket credit"
                );
                Ok(())
            }
            Self::FetchChunk {
                version,
                invocation,
                fetch,
                sequence,
                data,
            } => {
                ensure!(
                    *version == VERSION && valid_handle(invocation) && valid_handle(fetch),
                    "invalid fetch chunk"
                );
                chunk(*sequence, data)
            }
            Self::FetchEnd {
                version,
                invocation,
                fetch,
                error,
                ..
            } => {
                ensure!(
                    *version == VERSION && valid_handle(invocation) && valid_handle(fetch),
                    "invalid fetch end"
                );
                end(error)
            }
            Self::SignalAbort {
                version,
                invocation,
                signal,
                reason,
            } => {
                ensure!(
                    *version == VERSION
                        && valid_handle(invocation)
                        && valid_signal(signal)
                        && reason.len() <= MAX_SIGNAL_REASON_BYTES,
                    "invalid signal abort"
                );
                Ok(())
            }
            Self::Shutdown { version } => {
                ensure!(
                    *version == VERSION,
                    "unsupported invocation protocol version"
                );
                Ok(())
            }
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let bytes = encode(self)?;
        ensure!(
            !matches!(self, Self::MemorySample { .. } | Self::IdleProbe { .. })
                || bytes.len() <= MAX_MEMORY_SAMPLE_BYTES,
            "bounded control message exceeds limit"
        );
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Self = decode(bytes)?;
        ensure!(
            !matches!(&value, Self::MemorySample { .. } | Self::IdleProbe { .. })
                || bytes.len() <= MAX_MEMORY_SAMPLE_BYTES,
            "bounded control message exceeds limit"
        );
        value.validate()?;
        Ok(value)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum GuestControl {
    IdleStatus {
        version: u16,
        probe: String,
        idle: bool,
    },
    MemorySample {
        version: u16,
        sample: String,
        stats: GuestMemorySample,
    },
    Ready {
        version: u16,
        adapter: crate::Language,
        execution_turns: bool,
        quiescent_terminals: bool,
        cleanup_turns: bool,
        callback_turns: bool,
        response_turns: bool,
        /// Reply readiness does not certify joined event drain.
        #[serde(default)]
        reply_drain: bool,
        #[serde(default)]
        resident_objects: bool,
        #[serde(default)]
        object_retirement: bool,
        #[serde(default)]
        response_streams: bool,
        #[serde(default)]
        memory_diagnostics: bool,
        #[serde(default)]
        idle_lifecycle: bool,
    },
    /// Response bytes are ready for trusted native proof; effects remain live.
    HttpReply(HttpResult),
    HttpStreamHead(HttpStreamHead),
    ResponseChunk {
        version: u16,
        invocation: String,
        sequence: u64,
        #[serde(with = "storage_wire::binary")]
        data: Vec<u8>,
    },
    ResponseEnd {
        version: u16,
        invocation: String,
        sequence: u64,
        /// Bounded adapter error; no proof or host position is accepted.
        error: Option<String>,
    },
    /// Adapter-owned event thread/tasks have joined after a reply.
    Drained {
        version: u16,
        invocation: String,
    },
    ObjectRetired {
        version: u16,
        invocation: String,
    },
    /// Legacy joined buffered result, used only without reply_drain support.
    HttpResult(HttpResult),
    Failed {
        version: u16,
        invocation: String,
        message: String,
    },
    Cancelled {
        version: u16,
        invocation: String,
    },
    /// Credit for one request-body item of a `request_stream` invocation.
    RequestPull {
        version: u16,
        invocation: String,
        sequence: u64,
    },
    /// Ask the trusted host to perform an outbound fetch for this event, or a
    /// Durable Object call when `target` is set. The host applies its egress
    /// policy or namespace grant, and the output gate, before sending it.
    FetchStart {
        version: u16,
        invocation: String,
        fetch: String,
        request: HttpRequest,
        redirect: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<Box<ObjectCall>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        service: Option<Box<ServiceCall>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resource: Option<Box<crate::resource_wire::ResourceCall>>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        request_stream: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_bytes: Option<u64>,
        /// An outbound WebSocket instead of an ordinary fetch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dial: Option<SocketDial>,
    },
    FetchRequestChunk {
        version: u16,
        invocation: String,
        fetch: String,
        sequence: u64,
        #[serde(with = "storage_wire::binary")]
        data: Vec<u8>,
    },
    FetchRequestEnd {
        version: u16,
        invocation: String,
        fetch: String,
        sequence: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    FetchPull {
        version: u16,
        invocation: String,
        fetch: String,
        sequence: u64,
    },
    FetchCancel {
        version: u16,
        invocation: String,
        fetch: String,
    },
    /// A signal this brokered service RPC declared aborted in the caller.
    FetchSignal {
        version: u16,
        invocation: String,
        fetch: String,
        signal: String,
        #[serde(with = "storage_wire::binary")]
        reason: Vec<u8>,
    },
    /// A frame for one hibernatable socket of the event's own object. The
    /// guest sends at most `SOCKET_WINDOW` before the host returns a credit.
    SocketFrame {
        version: u16,
        invocation: String,
        ws: u64,
        /// The dial fetch whose socket this frame is for; `ws` is then unused.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        fetch: Option<String>,
        frame: SocketFrame,
    },
    /// The guest can no longer run (a Python interpreter died). The host stops
    /// leasing this VM and retires it once its events drain. Not tied to an
    /// event, and it grants nothing.
    Condemn { version: u16, reason: String },
}

impl GuestControl {
    /// Body flow and brokered I/O for a live event, not its terminal.
    pub fn is_event_io(&self) -> bool {
        matches!(
            self,
            Self::RequestPull { .. }
                | Self::FetchStart { .. }
                | Self::FetchRequestChunk { .. }
                | Self::FetchRequestEnd { .. }
                | Self::FetchPull { .. }
                | Self::FetchCancel { .. }
                | Self::FetchSignal { .. }
                | Self::SocketFrame { .. }
        )
    }

    pub fn invocation(&self) -> Option<&str> {
        match self {
            Self::Ready { .. }
            | Self::Condemn { .. }
            | Self::MemorySample { .. }
            | Self::IdleStatus { .. } => None,
            Self::HttpResult(result) | Self::HttpReply(result) => Some(&result.invocation),
            Self::HttpStreamHead(head) => Some(&head.invocation),
            Self::ResponseChunk { invocation, .. } | Self::ResponseEnd { invocation, .. } => {
                Some(invocation)
            }
            Self::Failed { invocation, .. }
            | Self::Cancelled { invocation, .. }
            | Self::ObjectRetired { invocation, .. }
            | Self::Drained { invocation, .. }
            | Self::RequestPull { invocation, .. }
            | Self::FetchStart { invocation, .. }
            | Self::FetchRequestChunk { invocation, .. }
            | Self::FetchRequestEnd { invocation, .. }
            | Self::FetchPull { invocation, .. }
            | Self::FetchCancel { invocation, .. }
            | Self::FetchSignal { invocation, .. }
            | Self::SocketFrame { invocation, .. } => Some(invocation),
        }
    }

    fn validate(&self) -> Result<()> {
        match self {
            Self::IdleStatus { version, probe, .. } => {
                ensure!(
                    *version == VERSION && valid_handle(probe),
                    "invalid guest idle status"
                );
            }
            Self::MemorySample {
                version, sample, ..
            } => {
                ensure!(
                    *version == VERSION && valid_handle(sample),
                    "invalid memory sample reply"
                );
            }
            Self::FetchRequestChunk {
                version,
                fetch,
                sequence,
                data,
                ..
            } => {
                ensure!(
                    *version == VERSION && valid_handle(fetch),
                    "invalid fetch request chunk"
                );
                chunk(*sequence, data)?;
            }
            Self::FetchRequestEnd {
                version,
                fetch,
                sequence,
                error,
                ..
            } => {
                ensure!(
                    *version == VERSION && valid_handle(fetch) && *sequence != u64::MAX,
                    "invalid fetch request end"
                );
                end(error)?;
            }
            Self::HttpResult(result) | Self::HttpReply(result) => return result.validate(),
            Self::HttpStreamHead(head) => return head.validate(),
            Self::ResponseChunk {
                version,
                sequence,
                data,
                ..
            } => {
                ensure!(
                    *version == VERSION
                        && *sequence != u64::MAX
                        && !data.is_empty()
                        && data.len() <= MAX_CHUNK_BYTES,
                    "invalid response chunk"
                );
            }
            Self::ResponseEnd { version, error, .. } => {
                ensure!(
                    *version == VERSION
                        && error
                            .as_ref()
                            .is_none_or(|error| !error.is_empty() && error.len() <= 4096),
                    "invalid response end"
                );
            }
            Self::Failed {
                version, message, ..
            } => {
                ensure!(
                    !message.is_empty() && message.len() <= 4096,
                    "invalid guest failure"
                );
                ensure!(
                    *version == VERSION,
                    "unsupported invocation protocol version"
                );
            }
            Self::Condemn { version, reason } => {
                ensure!(
                    *version == VERSION && !reason.is_empty() && reason.len() <= 4096,
                    "invalid guest condemnation"
                );
            }
            Self::Ready { version, .. }
            | Self::Cancelled { version, .. }
            | Self::ObjectRetired { version, .. }
            | Self::Drained { version, .. }
            | Self::RequestPull { version, .. } => {
                ensure!(
                    *version == VERSION,
                    "unsupported invocation protocol version"
                );
            }
            Self::FetchStart {
                version,
                fetch,
                request,
                redirect: mode,
                target,
                service,
                resource,
                request_stream,
                request_bytes,
                dial,
                ..
            } => {
                ensure!(
                    *version == VERSION && valid_handle(fetch) && redirect(mode),
                    "invalid fetch request"
                );
                request.validate()?;
                ensure!(
                    usize::from(target.is_some())
                        + usize::from(service.is_some())
                        + usize::from(resource.is_some())
                        + usize::from(dial.is_some())
                        <= 1,
                    "ambiguous broker target"
                );
                if let Some(dial) = dial {
                    dial.validate()?;
                    ensure!(
                        request.method == "GET"
                            && request.body.is_empty()
                            && !request_stream
                            && request_bytes.is_none(),
                        "a WebSocket dial is a bodiless GET"
                    );
                }
                if let Some(service) = service {
                    service.invocation.validate()?;
                    ensure!(
                        service.invocation.signal_owner.is_none(),
                        "guest named a signal owner"
                    );
                }
                if let Some(resource) = resource {
                    resource.validate()?;
                }
                ensure!(
                    !request_stream
                        || (service.is_some()
                            && request.body.is_empty()
                            && request_bytes.is_none()),
                    "invalid service request stream"
                );
                if let Some(length) = request_bytes {
                    ensure!(
                        (target.is_some() || service.is_some() || resource.is_some())
                            && request.body.is_empty()
                            && *length > MAX_BODY_BYTES as u64
                            && *length <= MAX_OBJECT_CALL_BYTES as u64,
                        "invalid object request upload"
                    );
                }
                if let Some(target) = target {
                    target.validate()?;
                }
            }
            Self::FetchPull { version, fetch, .. } | Self::FetchCancel { version, fetch, .. } => {
                ensure!(
                    *version == VERSION && valid_handle(fetch),
                    "invalid fetch flow"
                );
            }
            Self::FetchSignal {
                version,
                fetch,
                signal,
                reason,
                ..
            } => {
                ensure!(
                    *version == VERSION
                        && valid_handle(fetch)
                        && valid_signal(signal)
                        && reason.len() <= MAX_SIGNAL_REASON_BYTES,
                    "invalid fetch signal"
                );
            }
            Self::SocketFrame {
                version,
                frame,
                fetch,
                ..
            } => {
                ensure!(
                    *version == VERSION && fetch.as_deref().is_none_or(valid_handle),
                    "invalid socket frame"
                );
                frame.validate()?;
            }
        }
        if let Some(invocation) = self.invocation() {
            ensure!(valid_handle(invocation), "invalid invocation capability");
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let bytes = encode(self)?;
        ensure!(
            !matches!(self, Self::MemorySample { .. } | Self::IdleStatus { .. })
                || bytes.len() <= MAX_MEMORY_SAMPLE_BYTES,
            "bounded control message exceeds limit"
        );
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Self = decode(bytes)?;
        ensure!(
            !matches!(&value, Self::MemorySample { .. } | Self::IdleStatus { .. })
                || bytes.len() <= MAX_MEMORY_SAMPLE_BYTES,
            "bounded control message exceeds limit"
        );
        value.validate()?;
        Ok(value)
    }
}

/// First frame on an event's dedicated storage stream. The host must match
/// both opaque fields to its pending admission; neither selects a resource.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConnect {
    pub version: u16,
    pub invocation: String,
    pub grant: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cleanup: bool,
    /// `grant` names a facet event session of `invocation`'s live object.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub facet: bool,
}

impl StorageConnect {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.version == VERSION && valid_handle(&self.invocation) && valid_handle(&self.grant),
            "invalid storage connection capability"
        );
        Ok(())
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        encode(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Self = decode(bytes)?;
        value.validate()?;
        Ok(value)
    }
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value)?;
    ensure!(
        bytes.len() <= MAX_MESSAGE_BYTES,
        "invocation message exceeds limit"
    );
    Ok(bytes)
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    ensure!(
        !bytes.is_empty() && bytes.len() <= MAX_MESSAGE_BYTES,
        "invocation message exceeds limit"
    );
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
#[path = "unit_tests/invocation_wire/tests.rs"]
mod tests;
