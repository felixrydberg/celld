//! Synchronous storage subprotocol shared by guests and the trusted host.
//! Framing works over a dedicated vsock byte stream; HTTP invocation/egress
//! streams use separate channels and cannot deadlock a blocking storage call.

use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

pub const VERSION: u16 = 1;
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
// ponytail: fixed host-registry bounds for guest-supplied socket metadata; the
// original celld registry has none, workerd's own are tighter (2 KiB attachment).
pub const MAX_SOCKET_TAGS: usize = 64;
pub const MAX_SOCKET_TAG_BYTES: usize = 1024;
pub const MAX_SOCKET_ATTACHMENT_BYTES: usize = 64 * 1024;
pub const MAX_SOCKET_AUTO_RESPONSE_BYTES: usize = 4096;

/// JSON numbers cannot preserve i64 in JavaScript or IEEE nonfinite values.
/// Decimal integers and exact float bits preserve native values across the
/// host/guest boundary. Bytes remain bytes, including empty BLOBs distinct from NULL.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SqlValue {
    Null {},
    Integer {
        #[serde(with = "decimal")]
        value: i64,
    },
    Real {
        #[serde(with = "float_bits")]
        bits: u64,
    },
    Text {
        value: String,
    },
    Blob {
        #[serde(with = "binary")]
        value: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "format", rename_all = "snake_case", deny_unknown_fields)]
pub enum StoredValue {
    Json {
        value: String,
    },
    V8 {
        #[serde(with = "binary")]
        value: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloneEntry {
    pub key: String,
    #[serde(with = "binary")]
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListOptions {
    pub begin: Option<String>,
    pub end: Option<String>,
    pub start_after: Option<String>,
    pub prefix: Option<String>,
    pub limit: Option<usize>,
    #[serde(default)]
    pub reverse: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionAction {
    Commit,
    Rollback,
    RollbackExplicit,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    /// The host-owned hibernation registry of this exact object's WebSockets.
    /// `ws` is the host id `SocketAccept` returned; a first accept names the
    /// guest-local pair id instead and `accepted` is false. The host answers
    /// from its own registry and checks every id against the issuing object.
    SocketAccept {
        ws: u64,
        accepted: bool,
        tags: Vec<String>,
    },
    SocketList {
        tag: Option<String>,
    },
    SocketAttachment {
        ws: u64,
        #[serde(with = "binary")]
        attachment: Vec<u8>,
    },
    /// `setWebSocketAutoResponse`: no pair unsets, as in the original.
    SocketAutoResponse {
        pair: Option<(String, String)>,
    },
    SocketAutoResponseGet {},
    SocketAutoResponseTimestamp {
        ws: u64,
    },
    /// Native container bookkeeping under this exact live object's turn.
    /// No Docker or network effect executes on the synchronous channel.
    ContainerState {},
    ContainerBeginRun {},
    Get {
        keys: Vec<String>,
    },
    PutJson {
        entries: Vec<(String, serde_json::Value)>,
    },
    PutClone {
        entries: Vec<CloneEntry>,
    },
    Delete {
        keys: Vec<String>,
    },
    DeleteAll {
        delete_alarm: bool,
    },
    List {
        options: ListOptions,
    },
    /// The original synchronous `kv.list()` cursor: one per object, answered
    /// with `Value::Cursor` and then `Value::Entries` of at most one entry.
    SyncListStart {
        options: ListOptions,
    },
    SyncListNext {
        cursor: String,
    },
    SqlStart {
        query: String,
        bindings: Vec<SqlValue>,
    },
    SqlNext {
        cursor: String,
    },
    SqlClose {
        cursor: String,
    },
    SqlIngest {
        input: String,
    },
    SqlMetadata {},
    SqlSize {},
    /// One original D1 request (`storage::D1Request` as JSON) against the
    /// object's own database; the reply is the original `{ok|error}` JSON.
    D1Run {
        request: String,
    },
    /// Continue only this event's already executed, immutable D1 reply.
    D1Next {},
    D1RequestStart {
        bytes: u64,
    },
    D1RequestChunk {
        data: String,
    },
    D1RequestEnd {},
    TurnStart {},
    TurnFinish {
        turn: String,
    },
    /// `origin` names the foreign continuation's grant when another event's
    /// checkpoint drives its block, as original `[origin, driver]` blocks do.
    /// The driver's own turn holds the native gate; only that origin may
    /// borrow the turn while this critical section is held.
    CriticalSectionStart {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin: Option<String>,
    },
    CriticalSectionFinish {
        critical_section: String,
        failed: bool,
    },
    AbortObject {
        reason: String,
    },
    /// Open one event of a facet under this live object (or facet) session.
    /// The host derives the facet scope from this session's scope; the guest
    /// names only the incarnation (`owner`), the facet and its class label.
    FacetOpen {
        owner: String,
        name: String,
        class: String,
    },
    /// End one facet event's session: its image is flushed to the root first.
    FacetClose {
        facet: String,
    },
    /// End a facet incarnation: every event session and its native cell.
    FacetAbort {
        owner: String,
        name: String,
        class: String,
    },
    /// Delete a facet's stored images (and its descendants').
    FacetDelete {
        name: String,
    },
    /// `origin` as for `CriticalSectionStart`: the driver's session owns the
    /// savepoint and gate while that foreign continuation's grant uses it.
    TransactionStart {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin: Option<String>,
    },
    TransactionFinish {
        transaction: String,
        action: TransactionAction,
    },
    GetAlarm {},
    SetAlarm {
        #[serde(with = "decimal")]
        at_ms: i64,
    },
    DeleteAlarm {},
    GetActorName {},
    /// Storage of a foreign continuation running in another event's ordinary
    /// checkpoint. The outer grant remains the origin's authority; the host
    /// validates the live driver's turn in the same VM/native residency.
    InExecutionTurn {
        turn: String,
        operation: Box<Operation>,
    },
}

impl Operation {
    pub fn is_storage_call(&self) -> bool {
        matches!(
            self,
            Self::SocketAccept { .. }
                | Self::SocketList { .. }
                | Self::SocketAttachment { .. }
                | Self::SocketAutoResponse { .. }
                | Self::SocketAutoResponseGet {}
                | Self::SocketAutoResponseTimestamp { .. }
                | Self::ContainerState {}
                | Self::ContainerBeginRun {}
                | Self::Get { .. }
                | Self::PutJson { .. }
                | Self::PutClone { .. }
                | Self::Delete { .. }
                | Self::DeleteAll { .. }
                | Self::List { .. }
                | Self::SyncListStart { .. }
                | Self::SyncListNext { .. }
                | Self::SqlStart { .. }
                | Self::SqlNext { .. }
                | Self::SqlClose { .. }
                | Self::SqlIngest { .. }
                | Self::SqlMetadata {}
                | Self::SqlSize {}
                | Self::D1Run { .. }
                | Self::D1Next {}
                | Self::D1RequestStart { .. }
                | Self::D1RequestChunk { .. }
                | Self::D1RequestEnd {}
                | Self::GetAlarm {}
                | Self::SetAlarm { .. }
                | Self::DeleteAlarm {}
                | Self::GetActorName {}
        )
    }

    fn validate(&self) -> Result<()> {
        match self {
            Self::D1RequestStart { bytes } => ensure!(
                *bytes > 0 && *bytes <= crate::invocation_wire::MAX_OBJECT_CALL_BYTES as u64,
                "D1 request exceeds limit"
            ),
            Self::D1RequestChunk { data } => ensure!(
                !data.is_empty() && data.len() <= crate::invocation_wire::MAX_CHUNK_BYTES,
                "invalid D1 request chunk"
            ),
            Self::InExecutionTurn { turn, operation } => {
                ensure!(valid_handle(turn), "invalid execution turn handle");
                ensure!(
                    operation.is_storage_call(),
                    "execution turn only carries storage calls"
                );
                operation.validate()?;
            }
            Self::SyncListNext { cursor }
            | Self::SqlNext { cursor }
            | Self::SqlClose { cursor } => {
                ensure!(valid_handle(cursor), "invalid cursor handle")
            }
            Self::TransactionFinish { transaction, .. } => {
                ensure!(valid_handle(transaction), "invalid transaction handle")
            }
            Self::TurnFinish { turn } => {
                ensure!(valid_handle(turn), "invalid execution turn handle")
            }
            Self::CriticalSectionStart {
                origin: Some(origin),
            }
            | Self::TransactionStart {
                origin: Some(origin),
            } => {
                ensure!(valid_handle(origin), "invalid delegated origin")
            }
            Self::CriticalSectionFinish {
                critical_section, ..
            } => {
                ensure!(
                    valid_handle(critical_section),
                    "invalid critical section handle"
                )
            }
            Self::AbortObject { reason } => {
                ensure!(reason.len() <= 4096, "object abort reason exceeds limit")
            }
            Self::SocketAccept { tags, .. } => ensure!(
                tags.len() <= MAX_SOCKET_TAGS && tags.iter().all(|tag| tag.len() <= MAX_SOCKET_TAG_BYTES),
                "WebSocket tags exceed limit"
            ),
            Self::SocketList { tag: Some(tag) } => {
                ensure!(tag.len() <= MAX_SOCKET_TAG_BYTES, "WebSocket tag exceeds limit")
            }
            Self::SocketAttachment { attachment, .. } => ensure!(
                attachment.len() <= MAX_SOCKET_ATTACHMENT_BYTES,
                "WebSocket attachment exceeds limit"
            ),
            Self::SocketAutoResponse { pair: Some((request, response)) } => ensure!(
                request.len() <= MAX_SOCKET_AUTO_RESPONSE_BYTES
                    && response.len() <= MAX_SOCKET_AUTO_RESPONSE_BYTES,
                "WebSocket auto-response exceeds limit"
            ),
            Self::FacetOpen { owner, name, class } | Self::FacetAbort { owner, name, class } => {
                ensure!(
                    [owner, name, class].iter().all(|part| part.len() <= 256) && !class.is_empty(),
                    "invalid facet identity"
                )
            }
            Self::FacetClose { facet } => ensure!(valid_handle(facet), "invalid facet handle"),
            Self::FacetDelete { name } => ensure!(name.len() <= 256, "invalid facet name"),
            _ => {}
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u16,
    pub grant: String,
    pub operation: Operation,
}

impl Request {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.version == VERSION,
            "unsupported storage protocol version"
        );
        ensure!(valid_handle(&self.grant), "invalid storage handle");
        self.operation.validate()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() <= MAX_MESSAGE_BYTES,
            "storage message exceeds limit"
        );
        let request: Self = serde_json::from_slice(bytes)?;
        request.validate()?;
        Ok(request)
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        encode(self)
    }
}

pub fn valid_handle(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// One hibernatable socket as the original `getWebSockets` rebuilds it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SocketRow {
    pub ws: u64,
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "optional_binary")]
    pub attachment: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Value {
    /// The host id of an accepted socket.
    Socket { ws: u64 },
    Sockets { sockets: Vec<SocketRow> },
    AutoResponse { pair: Option<(String, String)> },
    AutoResponseTimestamp { millis: Option<f64> },
    ContainerState { running: bool, run: u64 },
    Unit {},
    Entries {
        entries: Vec<(String, StoredValue)>,
    },
    Count {
        count: u64,
    },
    Cursor {
        cursor: Option<String>,
        columns: Vec<String>,
        row: Option<Vec<SqlValue>>,
        rows_written: u64,
        reused_cached_query: bool,
    },
    Row {
        row: Option<Vec<SqlValue>>,
        rows_written: u64,
    },
    Ingest {
        remainder: String,
        rows_written: u64,
        statement_count: u64,
    },
    Metadata {
        #[serde(with = "decimal")]
        last_insert_rowid: i64,
        changes: u64,
    },
    Size {
        bytes: u64,
    },
    D1 {
        reply: String,
        complete: bool,
    },
    Transaction {
        transaction: String,
    },
    Turn {
        turn: String,
    },
    CriticalSection {
        critical_section: String,
    },
    Alarm {
        #[serde(with = "optional_decimal")]
        at_ms: Option<i64>,
    },
    ActorName {
        name: Option<String>,
    },
    Facet {
        scope: String,
        grant: String,
        depth: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum Outcome {
    Ok { value: Value },
    Error { message: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reply {
    pub version: u16,
    pub outcome: Outcome,
}

impl Reply {
    pub fn encode(&self) -> Result<Vec<u8>> {
        ensure!(
            self.version == VERSION,
            "unsupported storage protocol version"
        );
        encode(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() <= MAX_MESSAGE_BYTES,
            "storage message exceeds limit"
        );
        let reply: Self = serde_json::from_slice(bytes)?;
        ensure!(
            reply.version == VERSION,
            "unsupported storage protocol version"
        );
        Ok(reply)
    }
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value)?;
    ensure!(
        bytes.len() <= MAX_MESSAGE_BYTES,
        "storage message exceeds limit"
    );
    Ok(bytes)
}

/// Reject lengths before allocating. EOF is clean only between messages; a
/// partial prefix/body is a transport failure and must retire the connection.
pub fn read_frame(reader: &mut impl Read) -> Result<Option<Vec<u8>>> {
    let mut length = [0; 4];
    loop {
        match reader.read(&mut length[..1]) {
            Ok(0) => return Ok(None),
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }
    reader.read_exact(&mut length[1..])?;
    let length = u32::from_be_bytes(length) as usize;
    ensure!(
        length > 0 && length <= MAX_MESSAGE_BYTES,
        "storage frame exceeds limit"
    );
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes)?;
    Ok(Some(bytes))
}

pub fn write_frame(writer: &mut impl Write, bytes: &[u8]) -> Result<()> {
    ensure!(
        !bytes.is_empty() && bytes.len() <= MAX_MESSAGE_BYTES,
        "storage frame exceeds limit"
    );
    writer.write_all(&(bytes.len() as u32).to_be_bytes())?;
    writer.write_all(bytes)?;
    writer.flush()?;
    Ok(())
}

mod decimal {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &i64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
        let value = String::deserialize(deserializer)?;
        let parsed: i64 = value.parse().map_err(serde::de::Error::custom)?;
        if parsed.to_string() != value {
            return Err(serde::de::Error::custom(
                "integer must be canonical decimal",
            ));
        }
        Ok(parsed)
    }
}

mod optional_decimal {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &Option<i64>, serializer: S) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => serializer.serialize_some(&value.to_string()),
            None => serializer.serialize_none(),
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<i64>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|value| {
                let parsed: i64 = value.parse().map_err(serde::de::Error::custom)?;
                if parsed.to_string() != value {
                    return Err(serde::de::Error::custom(
                        "integer must be canonical decimal",
                    ));
                }
                Ok(parsed)
            })
            .transpose()
    }
}

mod float_bits {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(bits: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{bits:016x}"))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value.len() != 16
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(serde::de::Error::custom("invalid IEEE float bits"));
        }
        u64::from_str_radix(&value, 16).map_err(serde::de::Error::custom)
    }
}

mod optional_binary {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(
        value: &Option<Vec<u8>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => super::binary::serialize(value, serializer),
            None => serializer.serialize_none(),
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|value| super::binary::deserialize(serde::de::value::StringDeserializer::<D::Error>::new(value)))
            .transpose()
    }
}

pub(crate) mod binary {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(value))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value.len() > super::MAX_MESSAGE_BYTES {
            return Err(serde::de::Error::custom(
                "binary value exceeds message limit",
            ));
        }
        STANDARD.decode(value).map_err(serde::de::Error::custom)
    }
}
