//! Firecracker-guest-only WebSocket bridge: dialed sockets and guest registry calls. Moved out of `websocket.rs` so upstream merges stay small.

use super::*;

/// The frame streams of the sockets this guest dialed, by the guest's local id.
/// Each is the credited stream of its dial fetch, one inbound frame to a run
/// of chunks (see `DIAL_*`); `__ws_next` takes one frame per call, so a pump
/// that stops asking stops the host reading the connection.
pub(crate) type DialStream = Arc<tokio::sync::Mutex<HttpChunkStream>>;

pub(super) fn dials() -> &'static std::sync::Mutex<HashMap<u64, DialStream>> {
    static DIALS: OnceLock<std::sync::Mutex<HashMap<u64, DialStream>>> = OnceLock::new();
    DIALS.get_or_init(Default::default)
}

/// The next inbound frame of a dialed socket as `__ws_next` resolves it. A
/// stream that ends or fails without a close frame is an abnormal closure.
pub(crate) async fn next_dial_frame(id: u64, stream: DialStream) -> Vec<u8> {
    use celld_applications::invocation_wire::{DIAL_LAST, MAX_BODY_BYTES};
    let mut stream = stream.lock().await;
    let mut frame = Vec::new();
    let encoded = loop {
        match stream.next().await {
            Some(Ok(chunk)) if !chunk.is_empty() => {
                frame.extend_from_slice(&chunk[1..]);
                if frame.len() > MAX_BODY_BYTES + 1 {
                    break WsPull::Close(1009, "message too big".into(), false).encode();
                }
                if chunk[0] == DIAL_LAST {
                    break frame;
                }
            }
            _ => break WsPull::Close(1006, String::new(), false).encode(),
        }
    };
    if encoded.first() == Some(&WS_PULL_TAG_CLOSE) {
        dials().lock().unwrap().remove(&id);
    }
    encoded
}

/// `new WebSocket(url)` and `fetch(url, { Upgrade })` in guest mode: the host
/// dials. `upgrade` is the `fetch()` form, which also answers a server that
/// declines; both resolve as the original ops do.
/// The handshake is an HTTP request; `ws(s)` is its twin, as in the connector.
pub(super) fn dial_url(url: &str) -> String {
    match url.split_once("://") {
        Some(("ws", rest)) => format!("http://{rest}"),
        Some(("wss", rest)) => format!("https://{rest}"),
        _ => url.to_owned(),
    }
}

/// The subprotocols a `fetch()` upgrade offers, read from its headers.
pub(super) fn upgrade_protocols(headers: &[(String, String)]) -> Vec<String> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("sec-websocket-protocol"))
        .map(|(_, value)| value.split(',').map(|p| p.trim().to_string()).collect())
        .unwrap_or_default()
}

pub(super) fn guest_dial(
    scope: &mut v8::PinScope,
    rv: &mut v8::ReturnValue<v8::Value>,
    id: u64,
    url: &str,
    protocols: Vec<String>,
    headers: Vec<(String, String)>,
    upgrade: bool,
) {
    let brokered = match guest_storage::dial(scope, id, dial_url(url), protocols, headers, upgrade)
    {
        Ok(brokered) => brokered,
        Err(error) => return loader_throw(scope, &error),
    };
    // The socket belongs to the request that opened it, as a Worker's does, and
    // its queue exists before the handshake ends: the pump asks for frames at
    // once, as it does for the original's queue, and waits for the stream.
    ws_track_request_socket(id);
    let (deliver, delivered) = tokio::sync::oneshot::channel::<HttpChunkStream>();
    let queued: HttpChunkStream = Box::pin(futures_util::stream::once(delivered).flat_map(
        |stream| match stream {
            Ok(stream) => stream,
            Err(_) => Box::pin(futures_util::stream::empty()) as HttpChunkStream,
        },
    ));
    dials()
        .lock()
        .unwrap()
        .insert(id, Arc::new(tokio::sync::Mutex::new(queued)));
    let failed = if upgrade {
        "WebSocket upgrade failed"
    } else {
        "WebSocket connection failed"
    };
    let async_id = asyncrt::enqueue(async move {
        let mut response = match brokered.await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                dials().lock().unwrap().remove(&id);
                return Err(format!("{failed}: {error}"));
            }
            Err(_) => {
                dials().lock().unwrap().remove(&id);
                return Err(format!("{failed}: the connection was abandoned"));
            }
        };
        let protocol = response
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("sec-websocket-protocol"))
            .map(|(_, value)| value.clone())
            .unwrap_or_default();
        if response.status == 101 {
            let stream = response
                .stream
                .take()
                .ok_or_else(|| format!("{failed}: the host sent no frame stream"))?;
            let _ = deliver.send(stream);
            return Ok(if upgrade {
                serde_json::json!({"upgraded": true, "protocol": protocol}).to_string()
            } else {
                protocol
            });
        }
        dials().lock().unwrap().remove(&id);
        if !upgrade {
            return Err(format!("{failed}: the server answered {}", response.status));
        }
        let mut body = std::mem::take(&mut response.body);
        if let Some(mut stream) = response.stream.take() {
            while let Some(chunk) = stream.next().await {
                body.extend(chunk?);
            }
        }
        Ok(serde_json::json!({
            "upgraded": false,
            "status": response.status,
            "headers": response.headers,
            "body": body,
        })
        .to_string())
    });
    rv.set(promise_for(scope, async_id));
}

/// A guest object's registry operation, answered by the host. `None` outside
/// guest mode, where the original in-process registry answers.
pub(super) fn guest_registry(
    scope: &mut v8::PinScope,
    cell: Option<&str>,
    operation: celld_applications::storage_wire::Operation,
) -> Option<Result<celld_applications::storage_wire::Value, String>> {
    if !guest_storage::enabled(scope) {
        return None;
    }
    let call = |scope: &mut v8::PinScope| {
        let channel = match cell {
            Some(cell) => guest_storage::channel(scope, cell)?
                .ok_or_else(|| "guest storage is unavailable".to_string())?,
            None => guest_storage::current_channel(scope)?,
        };
        guest_storage::call(scope, &channel, operation)
    };
    Some(call(scope))
}

/// Outbound frame of a guest object, or `false` outside guest mode.
pub(super) fn guest_frame(
    scope: &mut v8::PinScope,
    id: u64,
    frame: celld_applications::invocation_wire::SocketFrame,
) -> bool {
    if !guest_storage::enabled(scope) {
        return false;
    }
    let dial = dials().lock().unwrap().contains_key(&id);
    if let Err(error) = guest_storage::socket_frame(scope, id, dial, frame) {
        loader_throw(scope, &error);
    }
    true
}

/// Sockets that need the host to carry frames in a way no guest bridge exists
/// for yet fail at their first call, not as a silent drop.
pub(super) fn guest_refuses(scope: &mut v8::PinScope, what: &str) -> bool {
    if !guest_storage::enabled(scope) {
        return false;
    }
    loader_throw(
        scope,
        &format!("{what} is not connected in application guests; use state.acceptWebSocket()"),
    );
    true
}

#[cfg(test)]
mod dial_tests {
    use super::*;

    #[test]
    fn dials_use_the_http_twin_and_read_offered_subprotocols() {
        assert_eq!(
            dial_url("wss://peer.invalid/a?b=1"),
            "https://peer.invalid/a?b=1"
        );
        assert_eq!(dial_url("ws://peer.invalid/"), "http://peer.invalid/");
        assert_eq!(dial_url("https://peer.invalid/"), "https://peer.invalid/");
        let headers = vec![
            ("x-key".to_owned(), "v".to_owned()),
            ("Sec-WebSocket-Protocol".to_owned(), "a, b".to_owned()),
        ];
        assert_eq!(upgrade_protocols(&headers), ["a", "b"]);
        assert!(upgrade_protocols(&[]).is_empty());
    }

    /// An inbound frame reaches `__ws_next` whole however many credits carried it.
    #[tokio::test]
    async fn dialed_frames_reassemble_from_flagged_chunks() {
        use celld_applications::invocation_wire::{DIAL_LAST, DIAL_MORE};
        let chunks: Vec<std::result::Result<Vec<u8>, String>> = vec![
            Ok([&[DIAL_LAST][..], &[WS_PULL_TAG_TEXT], b"hi"].concat()),
            Ok([&[DIAL_MORE][..], &[WS_PULL_TAG_BINARY], b"ab"].concat()),
            Ok([&[DIAL_LAST][..], b"cd"].concat()),
        ];
        let stream: DialStream = Arc::new(tokio::sync::Mutex::new(Box::pin(
            futures_util::stream::iter(chunks),
        )));
        let mut frame = next_dial_frame(u64::MAX, stream.clone()).await;
        assert_eq!((frame.remove(0), frame), (WS_PULL_TAG_TEXT, b"hi".to_vec()));
        let mut frame = next_dial_frame(u64::MAX, stream.clone()).await;
        assert_eq!(
            (frame.remove(0), frame),
            (WS_PULL_TAG_BINARY, b"abcd".to_vec())
        );
        // The end of the stream without a close frame is an abnormal closure.
        assert_eq!(
            next_dial_frame(u64::MAX, stream).await[0],
            WS_PULL_TAG_CLOSE
        );
    }
}
