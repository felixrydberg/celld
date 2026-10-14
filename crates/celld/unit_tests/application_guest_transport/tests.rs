use super::*;

#[cfg(test)]
#[derive(Default)]
pub(crate) struct PeerLog {
    pub(crate) ids: Mutex<Vec<u64>>,
    pub(crate) closed: Mutex<Vec<(u64, u16)>>,
}

#[cfg(test)]
pub(crate) fn install_scripted_peer() -> Arc<PeerLog> {
    use crate::js::{OutboundWsOpen, WsOut, WsPull};
    static PEER: std::sync::OnceLock<Arc<PeerLog>> = std::sync::OnceLock::new();
    PEER.get_or_init(|| {
        let (connector, mut dials) = mpsc::unbounded_channel::<crate::js::OutboundWsReq>();
        crate::js::set_outbound_ws_tx(connector);
        let log = Arc::new(PeerLog::default());
        let shared = log.clone();
        crate::asyncrt::spawn(async move {
            while let Some(request) = dials.recv().await {
                shared.ids.lock().unwrap().push(request.id);
                let pull = request.pull.expect("a guest dial polls its socket");
                if request.url.contains("declines") {
                    let _ = request.reply.send(Ok(OutboundWsOpen {
                        protocol: None,
                        declined: Some(crate::js::DeclinedUpgrade {
                            status: 403,
                            headers: vec![("x-peer".into(), "no".into())],
                            body: b"declined".to_vec(),
                        }),
                    }));
                    continue;
                }
                let (output, mut frames) = mpsc::unbounded_channel();
                crate::js::ws_register(request.id, output);
                let (id, shared) = (request.id, shared.clone());
                let _ = pull.send(WsPull::Open("chat".into())).await;
                let _ = request.reply.send(Ok(OutboundWsOpen {
                    protocol: Some("chat".into()),
                    declined: None,
                }));
                crate::asyncrt::spawn(async move {
                    while let Some(out) = frames.recv().await {
                        match out {
                            WsOut::Text(text) => {
                                let _ = pull.send(WsPull::Text(format!("echo:{text}"))).await;
                            }
                            WsOut::Binary(bytes) => {
                                let _ = pull.send(WsPull::Binary(bytes)).await;
                            }
                            WsOut::Close(code, reason) => {
                                shared.closed.lock().unwrap().push((id, code));
                                let _ = pull.send_close(code, reason, true);
                                return;
                            }
                        }
                    }
                });
            }
        });
        log
    })
    .clone()
}

#[cfg(test)]
pub(crate) async fn check_socket_dial(app: &crate::actor::AppHandle) {
    use celld_applications::invocation_wire::{
        HttpResponse, HttpResult, SocketDial, DIAL_LAST, DIAL_MORE,
    };
    let log = install_scripted_peer();
    let first = log.ids.lock().unwrap().len();
    let application = ApplicationId {
        app: "dial".to_owned().try_into().unwrap(),
    };
    let directory = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(directory.path().join("storage.sock")).unwrap();
    let (host, mut guest) = UnixStream::pair().unwrap();
    write_frame(
        &mut guest,
        &GuestControl::Ready {
            version: VERSION,
            adapter: Language::Javascript,
            execution_turns: true,
            quiescent_terminals: true,
            cleanup_turns: true,
            callback_turns: true,
            response_turns: true,
            reply_drain: true,
            resident_objects: false,
            object_retirement: false,
            memory_diagnostics: false,
            idle_lifecycle: false,
            response_streams: true,
        }
        .encode()
        .unwrap(),
    )
    .await
    .unwrap();
    let dispatcher = Dispatcher::connect(application, Language::Javascript, host, listener, None)
        .await
        .unwrap();
    let client = dispatcher.client();
    let call = crate::asyncrt::spawn({
        let (client, app) = (client.clone(), app.clone());
        async move {
            client
                .invoke_stateless(
                    &app,
                    Duration::from_secs(20),
                    HttpRequest {
                        method: "GET".into(),
                        url: "http://dial.invalid/".into(),
                        headers: vec![],
                        body: vec![],
                    },
                    None,
                )
                .await
        }
    });
    async fn next(guest: &mut UnixStream) -> HostControl {
        let frame = timeout(IO_DEADLINE, read_frame(guest))
            .await
            .expect("no host control frame")
            .unwrap()
            .expect("host control ended");
        HostControl::decode(&frame).unwrap()
    }
    let HostControl::InvokeHttp(invoke) = next(&mut guest).await else {
        panic!("not an invocation");
    };
    let id = invoke.invocation;
    let start = |fetch: &str, url: &str, response: bool| GuestControl::FetchStart {
        version: VERSION,
        invocation: id.clone(),
        fetch: fetch.to_owned(),
        request: HttpRequest {
            method: "GET".into(),
            url: url.into(),
            headers: vec![],
            body: vec![],
        },
        redirect: "manual".into(),
        target: None,
        service: None,
        resource: None,
        request_stream: false,
        request_bytes: None,
        dial: Some(SocketDial {
            protocols: vec!["chat".into()],
            response,
        }),
    };
    let fetch = "f".repeat(64);
    write_frame(
        &mut guest,
        &start(&fetch, "https://peer.invalid/socket", false)
            .encode()
            .unwrap(),
    )
    .await
    .unwrap();
    let HostControl::FetchHead {
        status,
        headers,
        socket,
        ..
    } = next(&mut guest).await
    else {
        panic!("not a fetch head");
    };
    assert_eq!(status, 101);
    assert_eq!(socket, None);
    assert!(headers
        .iter()
        .any(|(name, value)| name == "sec-websocket-protocol" && value == "chat"));
    let peer = log.ids.lock().unwrap()[first];
    // The guest's frames name the fetch, not a registry id, and return credit.
    let send = |fetch: &str, frame: SocketFrame| GuestControl::SocketFrame {
        version: VERSION,
        invocation: id.clone(),
        ws: 0,
        fetch: Some(fetch.to_owned()),
        frame,
    };
    write_frame(
        &mut guest,
        &send(
            &fetch,
            SocketFrame::Text {
                data: "ping".into(),
            },
        )
        .encode()
        .unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(
        next(&mut guest).await,
        HostControl::SocketCredit { count: 1, .. }
    ));
    let pull = |sequence| GuestControl::FetchPull {
        version: VERSION,
        invocation: id.clone(),
        fetch: fetch.clone(),
        sequence,
    };
    // One inbound frame per credit; an oversized one arrives in flagged pieces.
    write_frame(&mut guest, &pull(0).encode().unwrap())
        .await
        .unwrap();
    let HostControl::FetchChunk { sequence, data, .. } = next(&mut guest).await else {
        panic!("not a chunk");
    };
    assert_eq!(sequence, 0);
    assert_eq!(data[0], DIAL_LAST);
    assert_eq!(&data[1..], [&[0u8][..], b"echo:ping"].concat());
    write_frame(
        &mut guest,
        &send(
            &fetch,
            SocketFrame::Binary {
                data: vec![9; 150_000],
            },
        )
        .encode()
        .unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(
        next(&mut guest).await,
        HostControl::SocketCredit { count: 1, .. }
    ));
    let mut received = Vec::new();
    for sequence in 1.. {
        write_frame(&mut guest, &pull(sequence).encode().unwrap())
            .await
            .unwrap();
        let HostControl::FetchChunk { data, .. } = next(&mut guest).await else {
            panic!("not a chunk");
        };
        assert!(data.len() <= MAX_CHUNK_BYTES);
        received.extend_from_slice(&data[1..]);
        if data[0] == DIAL_LAST {
            break;
        }
        assert_eq!(data[0], DIAL_MORE);
    }
    assert_eq!(received.len(), 150_001);
    assert_eq!(received[0], 1, "binary tag");
    let sequence = 1 + (150_001usize).div_ceil(MAX_CHUNK_BYTES - 1) as u64;
    // The guest closes; the peer's echo of the close ends the frame stream.
    write_frame(
        &mut guest,
        &send(
            &fetch,
            SocketFrame::Close {
                code: 1000,
                reason: "bye".into(),
            },
        )
        .encode()
        .unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(
        next(&mut guest).await,
        HostControl::SocketCredit { count: 1, .. }
    ));
    write_frame(&mut guest, &pull(sequence).encode().unwrap())
        .await
        .unwrap();
    let HostControl::FetchChunk { data, .. } = next(&mut guest).await else {
        panic!("not a chunk");
    };
    assert_eq!((data[0], data[1]), (DIAL_LAST, 3), "close frame");
    write_frame(&mut guest, &pull(sequence + 1).encode().unwrap())
        .await
        .unwrap();
    assert!(matches!(
        next(&mut guest).await,
        HostControl::FetchEnd { error: None, .. }
    ));

    // A server that declines is an ordinary response, not a socket.
    let declined = "d".repeat(64);
    write_frame(
        &mut guest,
        &start(&declined, "https://declines.invalid/", true)
            .encode()
            .unwrap(),
    )
    .await
    .unwrap();
    let HostControl::FetchHead {
        status, headers, ..
    } = next(&mut guest).await
    else {
        panic!("not a fetch head");
    };
    assert_eq!(status, 403);
    assert!(headers.iter().any(|(name, _)| name == "x-peer"));

    // A dial left open closes with its event (the original ends a Worker's
    // sockets with its request).
    let open = "e".repeat(64);
    write_frame(
        &mut guest,
        &start(&open, "https://peer.invalid/again", false)
            .encode()
            .unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(
        next(&mut guest).await,
        HostControl::FetchHead { status: 101, .. }
    ));
    let again = log.ids.lock().unwrap()[first + 2];
    assert!(crate::js::ws_scope_of(again).is_none());
    write_frame(
        &mut guest,
        &GuestControl::HttpReply(HttpResult {
            version: VERSION,
            invocation: id.clone(),
            response: HttpResponse {
                status: 200,
                headers: vec![],
                body: b"dialed".to_vec(),
            },
            socket: None,
        })
        .encode()
        .unwrap(),
    )
    .await
    .unwrap();
    write_frame(
        &mut guest,
        &GuestControl::Drained {
            version: VERSION,
            invocation: id,
        }
        .encode()
        .unwrap(),
    )
    .await
    .unwrap();
    let response = timeout(IO_DEADLINE, call).await.unwrap().unwrap().unwrap();
    assert_eq!(response.body, b"dialed");
    // Nothing of the event outlives it: both peers' registrations are closed.
    timeout(IO_DEADLINE, async {
        loop {
            let settled = client.0.registry.lock().unwrap().pending.is_empty();
            if settled {
                break;
            }
            crate::asyncrt::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // The first dial closed as the guest asked (1000); the one left open was
    // closed by the host when its event ended (1001).
    timeout(IO_DEADLINE, async {
        while log.closed.lock().unwrap().len() < 2 {
            crate::asyncrt::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a dialed socket outlived its event");
    let closed = log.closed.lock().unwrap().clone();
    assert!(
        closed.contains(&(peer, 1000)) && closed.contains(&(again, 1001)),
        "{closed:?}"
    );
}

#[cfg(test)]
pub(crate) async fn check_socket_frame_boundary() {
    use celld_applications::invocation_wire::{HttpResponse, HttpResult};
    let application = ApplicationId {
        app: "sockets".to_owned().try_into().unwrap(),
    };
    for fault in [
        "credited",
        "no_credit",
        "cancelled",
        "outside_object_event",
        "unissued_upgrade",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let listener = UnixListener::bind(directory.path().join("storage.sock")).unwrap();
        let (host, mut guest) = UnixStream::pair().unwrap();
        write_frame(
            &mut guest,
            &GuestControl::Ready {
                version: VERSION,
                adapter: Language::Javascript,
                execution_turns: true,
                quiescent_terminals: true,
                cleanup_turns: true,
                callback_turns: true,
                response_turns: true,
                reply_drain: true,
                resident_objects: true,
                object_retirement: false,
                memory_diagnostics: false,
                idle_lifecycle: false,
                response_streams: true,
            }
            .encode()
            .unwrap(),
        )
        .await
        .unwrap();
        let dispatcher = Dispatcher::connect(
            application.clone(),
            Language::Javascript,
            host,
            listener,
            None,
        )
        .await
        .unwrap();
        let client = dispatcher.client();
        let retired = dispatcher.retirement();
        let id = "b".repeat(64);
        let (frames, mut forwarded) = mpsc::channel(SOCKET_WINDOW);
        let (response, response_ready) = oneshot::channel();
        let (terminal, _result) = oneshot::channel();
        client.0.registry.lock().unwrap().pending.insert(
            id.clone(),
            Pending {
                execution: None,
                retirement: false,
                grant: "a".repeat(64),
                storage: None,
                response: Some(response),
                response_seen: false,
                terminal: Some(terminal),
                terminal_seen: false,
                connected: true,
                cleanup_connected: false,
                access: None,
                cancelled: fault == "cancelled",
                deadline: None,
                settled: CancellationToken::new(),
                stream: None,
                request: None,
                fetches: HashMap::new(),
                app: None,
                sockets: (fault != "outside_object_event").then_some(frames),
                dials: HashMap::new(),
                issued_sockets: HashMap::new(),
            },
        );
        let frame = |ws| GuestControl::SocketFrame {
            version: VERSION,
            invocation: id.clone(),
            ws,
            fetch: None,
            frame: SocketFrame::Text {
                data: format!("frame {ws}"),
            },
        };
        let count = if fault == "no_credit" {
            SOCKET_WINDOW + 1
        } else {
            SOCKET_WINDOW
        };
        if fault == "unissued_upgrade" {
            write_frame(
                &mut guest,
                &GuestControl::HttpReply(HttpResult {
                    version: VERSION,
                    invocation: id.clone(),
                    response: HttpResponse {
                        status: 101,
                        headers: vec![],
                        body: vec![],
                    },
                    socket: Some(77),
                })
                .encode()
                .unwrap(),
            )
            .await
            .unwrap();
            let reply = timeout(IO_DEADLINE, response_ready).await.unwrap().unwrap();
            assert_eq!(reply.status, 502);
            assert!(reply.websocket.is_none());
            assert!(!retired.is_cancelled(), "a bad upgrade retired the VM");
            continue;
        }
        for ws in 0..count as u64 {
            write_frame(&mut guest, &frame(ws).encode().unwrap())
                .await
                .unwrap();
        }
        match fault {
            "credited" => {
                for ws in 0..SOCKET_WINDOW as u64 {
                    let (forwarded_ws, forwarded_frame) = timeout(IO_DEADLINE, forwarded.recv())
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(
                        matches!(forwarded_ws, SocketTarget::Registry(registry) if registry == ws)
                    );
                    assert_eq!(
                        forwarded_frame,
                        SocketFrame::Text {
                            data: format!("frame {ws}")
                        }
                    );
                }
                assert!(!retired.is_cancelled());
            }
            "cancelled" => {
                crate::asyncrt::sleep(Duration::from_millis(100)).await;
                assert!(
                    forwarded.try_recv().is_err(),
                    "a cancelled event's frame was forwarded"
                );
                assert!(!retired.is_cancelled(), "a late frame retired the VM");
            }
            _ => {
                // Beyond the window, or outside an object event: a protocol fault.
                timeout(IO_DEADLINE, retired.cancelled()).await.unwrap();
            }
        }
    }
}

#[cfg(test)]
pub(crate) async fn check_guest_condemnation() {
    let application = ApplicationId {
        app: "condemn".to_owned().try_into().unwrap(),
    };
    for malformed in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let listener = UnixListener::bind(directory.path().join("storage.sock")).unwrap();
        let (host, mut guest) = UnixStream::pair().unwrap();
        write_frame(
            &mut guest,
            &GuestControl::Ready {
                version: VERSION,
                adapter: Language::Javascript,
                execution_turns: true,
                quiescent_terminals: true,
                cleanup_turns: true,
                callback_turns: true,
                response_turns: true,
                reply_drain: true,
                resident_objects: false,
                object_retirement: false,
                memory_diagnostics: false,
                idle_lifecycle: false,
                response_streams: true,
            }
            .encode()
            .unwrap(),
        )
        .await
        .unwrap();
        let dispatcher = Dispatcher::connect(
            application.clone(),
            Language::Javascript,
            host,
            listener,
            None,
        )
        .await
        .unwrap();
        let client = dispatcher.client();
        assert!(!client.is_condemned());
        let frame = if malformed {
            br#"{"event":"condemn","version":1,"reason":""}"#.to_vec()
        } else {
            GuestControl::Condemn {
                version: VERSION,
                reason: "interpreter died".into(),
            }
            .encode()
            .unwrap()
        };
        write_frame(&mut guest, &frame).await.unwrap();
        let retirement = dispatcher.retirement();
        timeout(IO_DEADLINE, async {
            while !(if malformed {
                retirement.is_cancelled()
            } else {
                client.is_condemned()
            }) {
                crate::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("condemnation was not observed");
        assert_eq!(retirement.is_cancelled(), malformed);
        assert_eq!(client.is_condemned(), !malformed);
    }
}

#[cfg(test)]
pub(crate) async fn check_protocol_boundary() {
    use celld_applications::invocation_wire::{HttpResponse, HttpResult};
    use tokio::io::AsyncWriteExt;
    let application = ApplicationId {
        app: "protocol".to_owned().try_into().unwrap(),
    };
    // The upload adds a declared-size contract to the existing credit ledger.
    let upload = |remaining| {
        let (items, receiver) = mpsc::channel(1);
        (
            RequestUpload {
                flow: ResponseFlow::default(),
                remaining: Some(remaining),
                items,
            },
            receiver,
        )
    };
    let (mut request, mut items) = upload(2);
    assert!(request.push(0, Some(vec![1])).is_err());
    request.flow.grant(0).unwrap();
    assert!(request.push(1, Some(vec![1])).is_err());
    request.push(0, Some(vec![1, 2])).unwrap();
    assert_eq!(items.try_recv().unwrap().unwrap(), Some(vec![1, 2]));
    assert!(request.push(0, Some(vec![1])).is_err());
    request.flow.grant(1).unwrap();
    request.push(1, None).unwrap();
    assert_eq!(items.try_recv().unwrap().unwrap(), None);
    assert!(request.flow.grant(2).is_err());
    let (mut request, _items) = upload(1);
    request.flow.grant(0).unwrap();
    assert!(request.push(0, Some(vec![1, 2])).is_err());
    let (mut request, _items) = upload(1);
    request.flow.grant(0).unwrap();
    assert!(request.push(0, None).is_err());
    for fault in [
        "wrong_grant",
        "unknown_invocation",
        "duplicate_storage",
        "duplicate_terminal",
        "unknown_terminal",
        "unexpected_object_retired",
        "stream_head",
        "stream_chunk",
        "stream_end",
        "credited_no_credit",
        "credited_wrong_sequence",
        "credited_duplicate_chunk",
        "credited_end_without_credit",
        "credited_chunk_after_end",
        "credited_drain_before_end",
        "credited_body_before_head",
        "credited_duplicate_head",
        "partial_hello",
        "oversized_hello",
        "partial_control",
        "cancel_timeout",
        "owner_drop",
        "split_duplicate_reply",
        "split_unknown_reply",
        "split_drain_before_reply",
        "split_legacy_terminal",
        "split_reply_before_storage",
        "split_reply_after_terminal",
        "unadvertised_reply",
        "split_joined",
        "split_cancelled",
        "split_cancel_reply_race",
        "split_failed",
        "split_shutdown",
        "io_request_unstreamed",
        "io_fetch_pull_unknown",
        "io_fetch_malformed",
        "io_fetch_start_retired",
        "io_signal_undeclared",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("storage.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let (host, mut guest) = UnixStream::pair().unwrap();
        write_frame(
            &mut guest,
            &GuestControl::Ready {
                version: VERSION,
                adapter: Language::Javascript,
                execution_turns: true,
                quiescent_terminals: true,
                cleanup_turns: true,
                callback_turns: true,
                response_turns: true,
                reply_drain: fault.starts_with("split") || fault.starts_with("credited"),
                resident_objects: false,
                object_retirement: false,
                memory_diagnostics: false,
                idle_lifecycle: false,
                response_streams: fault.starts_with("credited"),
            }
            .encode()
            .unwrap(),
        )
        .await
        .unwrap();
        let mut dispatcher = Dispatcher::connect(
            application.clone(),
            Language::Javascript,
            host,
            listener,
            None,
        )
        .await
        .unwrap();
        let client = dispatcher.client();
        let retired = dispatcher.retirement();
        let id = "b".repeat(64);
        let grant = "a".repeat(64);
        let (storage, channel) = oneshot::channel();
        let (terminal, mut reply) = oneshot::channel();
        let (response, response_ready) = oneshot::channel();
        let settled = CancellationToken::new();
        let (demand, _demand_ready) = mpsc::channel(1);
        let (chunks, _chunk_ready) = mpsc::channel(1);
        let mut flow = ResponseFlow::default();
        if matches!(
            fault,
            "credited_wrong_sequence" | "credited_duplicate_chunk" | "credited_chunk_after_end"
        ) {
            flow.grant(0).unwrap();
        }
        let (relay, mut relays) = mpsc::unbounded_channel();
        client.0.registry.lock().unwrap().pending.insert(
            id.clone(),
            Pending {
                execution: None,
                retirement: false,
                grant: grant.clone(),
                storage: Some(storage),
                response: Some(response),
                response_seen: fault.starts_with("credited"),
                terminal: Some(terminal),
                terminal_seen: false,
                connected: (fault.starts_with("split")
                    || fault.starts_with("credited")
                    || fault.starts_with("io_")
                    || fault == "unadvertised_reply")
                    && fault != "split_reply_before_storage",
                cleanup_connected: false,
                access: None,
                cancelled: false,
                deadline: None,
                settled: settled.clone(),
                request: None,
                fetches: if fault == "io_signal_undeclared" {
                    HashMap::from([(
                        "f".repeat(64),
                        BrokeredFetch {
                            flow: ResponseFlow::default(),
                            pulls: mpsc::channel(1).0,
                            _stop: CancellationToken::new().drop_guard(),
                            request: None,
                            signals: HashSet::from(["a".repeat(32)]),
                            relay,
                        },
                    )])
                } else {
                    HashMap::new()
                },
                app: None,
                stream: fault.starts_with("credited").then(|| PendingStream {
                    flow,
                    head_seen: fault != "credited_body_before_head",
                    demand,
                    chunks,
                    dropped: CancellationToken::new(),
                }),
                sockets: None,
                dials: HashMap::new(),
                issued_sockets: HashMap::new(),
            },
        );
        if fault == "owner_drop" {
            let task = dispatcher.task.take().unwrap();
            drop(dispatcher);
            assert!(task.await.unwrap().is_err());
            assert!(retired.is_cancelled());
            assert!(channel.await.is_err() && reply.await.is_err());
            assert!(!client.0.registry.lock().unwrap().running);
            continue;
        }
        if fault.starts_with("credited") {
            let chunk = |sequence| GuestControl::ResponseChunk {
                version: VERSION,
                invocation: id.clone(),
                sequence,
                data: vec![0, 255],
            };
            let end = GuestControl::ResponseEnd {
                version: VERSION,
                invocation: id.clone(),
                sequence: 0,
                error: None,
            };
            let frames = match fault {
                "credited_wrong_sequence" => vec![chunk(1)],
                "credited_duplicate_chunk" => vec![chunk(0), chunk(0)],
                "credited_end_without_credit" => vec![end],
                "credited_chunk_after_end" => vec![end, chunk(0)],
                "credited_drain_before_end" => vec![GuestControl::Drained {
                    version: VERSION,
                    invocation: id.clone(),
                }],
                "credited_duplicate_head" => vec![GuestControl::HttpStreamHead(
                    celld_applications::invocation_wire::HttpStreamHead {
                        version: VERSION,
                        invocation: id.clone(),
                        status: 200,
                        headers: vec![],
                    },
                )],
                _ => vec![chunk(0)],
            };
            for frame in frames {
                write_frame(&mut guest, &frame.encode().unwrap())
                    .await
                    .unwrap();
            }
        } else if fault.starts_with("split") || fault == "unadvertised_reply" {
            let message = GuestControl::HttpReply(HttpResult {
                version: VERSION,
                invocation: if fault == "split_unknown_reply" {
                    "c".repeat(64)
                } else {
                    id.clone()
                },
                response: HttpResponse {
                    status: 200,
                    headers: vec![],
                    body: vec![0, 255],
                },
                socket: None,
            })
            .encode()
            .unwrap();
            let drained = GuestControl::Drained {
                version: VERSION,
                invocation: id.clone(),
            }
            .encode()
            .unwrap();
            let positive = matches!(
                fault,
                "split_joined"
                    | "split_cancelled"
                    | "split_cancel_reply_race"
                    | "split_failed"
                    | "split_shutdown"
            );
            if fault == "split_cancel_reply_race" {
                drop(Registration {
                    shared: client.0.clone(),
                    id: id.clone(),
                });
                assert!(matches!(
                    HostControl::decode(&read_frame(&mut guest).await.unwrap().unwrap()).unwrap(),
                    HostControl::Cancel { .. }
                ));
            }
            match fault {
                "split_drain_before_reply" => write_frame(&mut guest, &drained).await.unwrap(),
                "split_legacy_terminal" => write_frame(
                    &mut guest,
                    &GuestControl::HttpResult(HttpResult {
                        version: VERSION,
                        invocation: id.clone(),
                        response: HttpResponse {
                            status: 200,
                            headers: vec![],
                            body: vec![],
                        },
                        socket: None,
                    })
                    .encode()
                    .unwrap(),
                )
                .await
                .unwrap(),
                "split_reply_after_terminal" => {
                    write_frame(
                        &mut guest,
                        &GuestControl::Cancelled {
                            version: VERSION,
                            invocation: id.clone(),
                        }
                        .encode()
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                    write_frame(&mut guest, &message).await.unwrap();
                }
                _ => write_frame(&mut guest, &message).await.unwrap(),
            }
            if positive || fault == "split_duplicate_reply" {
                if fault == "split_cancel_reply_race" {
                    assert!(response_ready.await.is_err());
                } else {
                    assert_eq!(
                        timeout(IO_DEADLINE, response_ready)
                            .await
                            .unwrap()
                            .unwrap()
                            .body,
                        [0, 255]
                    );
                }
                // A reply alone never satisfies the joined witness or retires
                // the routing/access record, including a cancellation race.
                assert!(!settled.is_cancelled());
                assert!(!client.0.registry.lock().unwrap().pending[&id].terminal_seen);
                assert_eq!(client.pending(), 1);
                if fault == "split_duplicate_reply" {
                    write_frame(&mut guest, &message).await.unwrap();
                } else {
                    if fault == "split_shutdown" {
                        let mut shutdown = Box::pin(dispatcher.shutdown());
                        crate::asyncrt::select! {
                            _ = crate::asyncrt::sleep(Duration::from_millis(20)) => {},
                            _ = &mut shutdown => panic!("shutdown treated reply as drain"),
                        }
                        assert!(!client.0.registry.lock().unwrap().running);
                        assert_eq!(client.pending(), 1);
                        assert!(!client.0.stop.is_cancelled() && !settled.is_cancelled());
                        write_frame(&mut guest, &drained).await.unwrap();
                        timeout(IO_DEADLINE, settled.cancelled()).await.unwrap();
                        assert!(matches!(reply.await.unwrap(), GuestControl::Drained { .. }));
                        drop(Registration {
                            shared: client.0.clone(),
                            id: id.clone(),
                        });
                        let (shutdown, ()) = tokio::join!(shutdown, async {
                            assert!(matches!(
                                HostControl::decode(
                                    &read_frame(&mut guest).await.unwrap().unwrap()
                                )
                                .unwrap(),
                                HostControl::Shutdown { .. }
                            ));
                            guest.shutdown().await.unwrap();
                        });
                        shutdown.unwrap();
                        assert_eq!(client.pending(), 0);
                        assert!(retired.is_cancelled());
                        continue;
                    }
                    if fault == "split_cancelled" {
                        assert!(matches!(
                            reply.try_recv(),
                            Err(oneshot::error::TryRecvError::Empty)
                        ));
                        drop(Registration {
                            shared: client.0.clone(),
                            id: id.clone(),
                        });
                        assert!(matches!(
                            HostControl::decode(&read_frame(&mut guest).await.unwrap().unwrap())
                                .unwrap(),
                            HostControl::Cancel { .. }
                        ));
                    }
                    if fault == "split_failed" {
                        write_frame(
                            &mut guest,
                            &GuestControl::Failed {
                                version: VERSION,
                                invocation: id.clone(),
                                message: "background failed".into(),
                            }
                            .encode()
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                    } else {
                        write_frame(&mut guest, &drained).await.unwrap();
                    }
                    timeout(IO_DEADLINE, settled.cancelled()).await.unwrap();
                    if fault == "split_joined" || fault == "split_failed" {
                        assert!(matches!(
                            reply.await.unwrap(),
                            GuestControl::Drained { .. } | GuestControl::Failed { .. }
                        ));
                        drop(Registration {
                            shared: client.0.clone(),
                            id: id.clone(),
                        });
                    } else {
                        assert!(reply.await.is_err());
                    }
                    assert_eq!(client.pending(), 0);
                    assert!(client.0.registry.lock().unwrap().retired.contains(&id));
                    let (shutdown, ()) = tokio::join!(dispatcher.shutdown(), async {
                        assert!(matches!(
                            HostControl::decode(&read_frame(&mut guest).await.unwrap().unwrap())
                                .unwrap(),
                            HostControl::Shutdown { .. }
                        ));
                        guest.shutdown().await.unwrap();
                    });
                    shutdown.unwrap();
                    assert!(retired.is_cancelled());
                    continue;
                }
            }
        } else if fault.starts_with("stream_") {
            let message = match fault {
                "stream_head" => GuestControl::HttpStreamHead(
                    celld_applications::invocation_wire::HttpStreamHead {
                        version: VERSION,
                        invocation: id.clone(),
                        status: 200,
                        headers: vec![],
                    },
                ),
                "stream_chunk" => GuestControl::ResponseChunk {
                    version: VERSION,
                    invocation: id.clone(),
                    sequence: 0,
                    data: vec![0, 255],
                },
                "stream_end" => GuestControl::ResponseEnd {
                    version: VERSION,
                    invocation: id.clone(),
                    sequence: 0,
                    error: None,
                },
                _ => unreachable!(),
            };
            write_frame(&mut guest, &message.encode().unwrap())
                .await
                .unwrap();
        } else if fault == "unexpected_object_retired" {
            write_frame(
                &mut guest,
                &GuestControl::ObjectRetired {
                    version: VERSION,
                    invocation: id.clone(),
                }
                .encode()
                .unwrap(),
            )
            .await
            .unwrap();
        } else if fault == "io_fetch_start_retired" {
            // Late credit and cancel frames from a retired event are harmless;
            // a new fetch for it is not.
            let late = "c".repeat(64);
            client
                .0
                .registry
                .lock()
                .unwrap()
                .retired
                .insert(late.clone());
            let fetch = "f".repeat(64);
            for frame in [
                GuestControl::RequestPull {
                    version: VERSION,
                    invocation: late.clone(),
                    sequence: 0,
                },
                GuestControl::FetchPull {
                    version: VERSION,
                    invocation: late.clone(),
                    fetch: fetch.clone(),
                    sequence: 0,
                },
                GuestControl::FetchCancel {
                    version: VERSION,
                    invocation: late.clone(),
                    fetch: fetch.clone(),
                },
                GuestControl::HttpResult(HttpResult {
                    version: VERSION,
                    invocation: id.clone(),
                    response: HttpResponse {
                        status: 200,
                        headers: vec![],
                        body: vec![],
                    },
                    socket: None,
                }),
            ] {
                write_frame(&mut guest, &frame.encode().unwrap())
                    .await
                    .unwrap();
            }
            timeout(IO_DEADLINE, &mut reply)
                .await
                .expect("late event I/O retired the dispatcher")
                .unwrap();
            let start = GuestControl::FetchStart {
                version: VERSION,
                invocation: late,
                fetch,
                request: celld_applications::invocation_wire::HttpRequest {
                    method: "GET".into(),
                    url: "http://127.0.0.1:1/".into(),
                    headers: vec![],
                    body: vec![],
                },
                redirect: "follow".into(),
                target: None,
                service: None,
                resource: None,
                request_stream: false,
                request_bytes: None,
                dial: None,
            };
            write_frame(&mut guest, &start.encode().unwrap())
                .await
                .unwrap();
        } else if fault.starts_with("io_") {
            // Event I/O must name a live stream and stay within its credit;
            // a malformed brokered request never reaches the network.
            let frame = match fault {
                "io_request_unstreamed" => GuestControl::RequestPull {
                    version: VERSION,
                    invocation: id.clone(),
                    sequence: 0,
                }
                .encode()
                .unwrap(),
                "io_fetch_pull_unknown" => GuestControl::FetchPull {
                    version: VERSION,
                    invocation: id.clone(),
                    fetch: "f".repeat(64),
                    sequence: 0,
                }
                .encode()
                .unwrap(),
                "io_signal_undeclared" => {
                    // A declared abort is relayed to the callee; any other id is
                    // a protocol violation that retires the guest.
                    let signal = |signal: String| {
                        GuestControl::FetchSignal {
                            version: VERSION,
                            invocation: id.clone(),
                            fetch: "f".repeat(64),
                            signal,
                            reason: vec![1, 2],
                        }
                        .encode()
                        .unwrap()
                    };
                    write_frame(&mut guest, &signal("a".repeat(32))).await.unwrap();
                    assert_eq!(
                        timeout(IO_DEADLINE, relays.recv()).await.unwrap().unwrap(),
                        ("a".repeat(32), vec![1, 2])
                    );
                    signal("d".repeat(32))
                }
                _ => serde_json::to_vec(&serde_json::json!({
                    "event": "fetch_start", "version": VERSION, "invocation": id,
                    "fetch": "f".repeat(64), "redirect": "follow",
                    "request": {"method": "GET", "url": "file:///etc/passwd", "headers": [], "body": ""}
                }))
                .unwrap(),
            };
            write_frame(&mut guest, &frame).await.unwrap();
        } else if fault == "partial_control" {
            guest.write_all(&[0]).await.unwrap();
        } else if fault == "cancel_timeout" {
            drop(Registration {
                shared: client.0.clone(),
                id: id.clone(),
            });
            let cancel = timeout(IO_DEADLINE, read_frame(&mut guest))
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(
                HostControl::decode(&cancel).unwrap(),
                HostControl::Cancel {
                    version: VERSION,
                    invocation: id
                }
            );
        } else if fault.ends_with("terminal") {
            let result = GuestControl::HttpResult(HttpResult {
                version: VERSION,
                invocation: if fault == "unknown_terminal" {
                    "c".repeat(64)
                } else {
                    id
                },
                response: HttpResponse {
                    status: 200,
                    headers: vec![],
                    body: vec![0, 255],
                },
                socket: None,
            })
            .encode()
            .unwrap();
            write_frame(&mut guest, &result).await.unwrap();
            if fault == "duplicate_terminal" {
                write_frame(&mut guest, &result).await.unwrap();
            }
        } else {
            let mut socket = UnixStream::connect(&path).await.unwrap();
            if fault == "partial_hello" {
                socket.write_all(&[0, 0, 0, 4, b'{']).await.unwrap();
                socket.shutdown().await.unwrap();
            } else if fault == "oversized_hello" {
                socket.write_u32(513).await.unwrap();
            } else {
                let hello = StorageConnect {
                    version: VERSION,
                    invocation: if fault == "unknown_invocation" {
                        "c".repeat(64)
                    } else {
                        id
                    },
                    grant: if fault == "wrong_grant" {
                        "d".repeat(64)
                    } else {
                        grant
                    },
                    cleanup: false,
                    facet: false,
                }
                .encode()
                .unwrap();
                write_frame(&mut socket, &hello).await.unwrap();
                if fault == "duplicate_storage" {
                    let accepted = timeout(IO_DEADLINE, channel).await.unwrap().unwrap();
                    let mut duplicate = UnixStream::connect(&path).await.unwrap();
                    write_frame(&mut duplicate, &hello).await.unwrap();
                    drop(accepted);
                }
            }
        }
        timeout(IO_DEADLINE * 2, retired.cancelled())
            .await
            .expect("bad guest protocol did not retire the dispatcher");
        assert!(
            dispatcher.join().await.is_err(),
            "fault {fault} was accepted"
        );
        assert_eq!(client.pending(), 0);
    }
    for (
        adapter,
        execution_turns,
        quiescent_terminals,
        cleanup_turns,
        callback_turns,
        response_turns,
        response_streams,
    ) in [
        (Language::Javascript, false, true, true, true, true, false),
        (Language::Javascript, true, false, true, true, true, false),
        (Language::Javascript, true, true, false, true, true, false),
        (Language::Javascript, true, true, true, false, true, false),
        (Language::Javascript, true, true, true, true, false, false),
        (Language::Javascript, true, true, true, true, true, true),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let (host, mut guest) = UnixStream::pair().unwrap();
        write_frame(
            &mut guest,
            &GuestControl::Ready {
                version: VERSION,
                adapter,
                execution_turns,
                quiescent_terminals,
                cleanup_turns,
                callback_turns,
                response_turns,
                reply_drain: false,
                resident_objects: false,
                object_retirement: false,
                response_streams,
                memory_diagnostics: false,
                idle_lifecycle: false,
            }
            .encode()
            .unwrap(),
        )
        .await
        .unwrap();
        assert!(Dispatcher::connect(
            application.clone(),
            Language::Javascript,
            host,
            UnixListener::bind(directory.path().join("storage.sock")).unwrap(),
            None
        )
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("readiness mismatch"));
    }
    // A guest's object call names no resource: only an id authenticating under
    // a bound namespace, in a declared class, is served.
    {
        use celld_applications::{ResourceKind, ResourceRef};
        let directory = tempfile::tempdir().unwrap();
        let (host, mut guest) = UnixStream::pair().unwrap();
        write_frame(
            &mut guest,
            &GuestControl::Ready {
                version: VERSION,
                adapter: Language::Javascript,
                execution_turns: true,
                quiescent_terminals: true,
                cleanup_turns: true,
                callback_turns: true,
                response_turns: true,
                reply_drain: false,
                resident_objects: false,
                object_retirement: false,
                memory_diagnostics: false,
                idle_lifecycle: false,
                response_streams: false,
            }
            .encode()
            .unwrap(),
        )
        .await
        .unwrap();
        let dispatcher = Dispatcher::connect(
            application.clone(),
            Language::Javascript,
            host,
            UnixListener::bind(directory.path().join("storage.sock")).unwrap(),
            None,
        )
        .await
        .unwrap();
        let client = dispatcher.client();
        let address = |tenant: &ApplicationId, id: &str| ResourceAddress {
            application: tenant.clone(),
            resource: ResourceRef {
                kind: ResourceKind::DurableObject,
                id: id.to_owned().try_into().unwrap(),
            },
        };
        let elsewhere = ApplicationId {
            app: "elsewhere".to_owned().try_into().unwrap(),
        };
        let target = |resource: &ResourceAddress, class: &str| {
            let scope = resource.cell_scope(class, "a").unwrap();
            ObjectTarget {
                class: class.into(),
                id: scope.split_once(':').unwrap().1.into(),
                name: Some("a".into()),
            }
        };
        let (counter, sibling) = (
            address(&application, "counter"),
            address(&application, "sibling"),
        );
        let database = ResourceAddress {
            resource: ResourceRef {
                kind: ResourceKind::D1,
                id: "db".to_owned().try_into().unwrap(),
            },
            ..counter.clone()
        };
        let cache = |application: &ApplicationId, id: &str| ResourceAddress {
            application: application.clone(),
            resource: ResourceRef {
                kind: ResourceKind::Kv,
                id: id.to_owned().try_into().unwrap(),
            },
        };
        let jobs_of = |application: &ApplicationId, id: &str| ResourceAddress {
            application: application.clone(),
            resource: ResourceRef {
                kind: ResourceKind::Queue,
                id: id.to_owned().try_into().unwrap(),
            },
        };
        let object = target(&counter, "Counter");
        let workflow = ResourceAddress {
            resource: ResourceRef {
                kind: ResourceKind::Workflow,
                id: "flow".to_owned().try_into().unwrap(),
            },
            ..counter.clone()
        };
        let workflow_class = crate::deploy::workflow_class("flow");
        assert!(
            client.namespace(&object).is_err(),
            "unbound VM served a call"
        );
        client
            .bind(
                vec![
                    (counter.clone(), "Counter".to_owned()),
                    (sibling.clone(), "Alternate".to_owned()),
                    (database.clone(), crate::deploy::D1_CLASS.to_owned()),
                    (workflow.clone(), workflow_class.clone()),
                    (
                        cache(&application, "cache"),
                        celld_logic::kv::RESERVED_CLASS.to_owned(),
                    ),
                    (
                        jobs_of(&application, "jobs"),
                        celld_logic::queue::RESERVED_CLASS.to_owned(),
                    ),
                ],
                Duration::from_secs(1),
            )
            .unwrap();
        assert!(client.bind(vec![], Duration::ZERO).is_err());
        let workflow_scope = workflow
            .cell_scope(&workflow_class, "flow/instance")
            .unwrap();
        let native_alarm = ObjectTarget {
            class: workflow_class.clone(),
            id: workflow_scope.split_once(':').unwrap().1.into(),
            name: None,
        };
        assert!(client.alarm_namespace(&native_alarm).is_ok());
        assert!(
            client.namespace(&native_alarm).is_err(),
            "guest Workflow call omitted instance name"
        );
        let foreign = ResourceAddress {
            application: elsewhere.clone(),
            ..workflow.clone()
        };
        assert!(client
            .alarm_namespace(&target(&foreign, &workflow_class))
            .is_err());
        assert!(client
            .alarm_namespace(&target(&workflow, "__Workflow.other"))
            .is_err());
        assert!(client
            .alarm_namespace(&target(&database, crate::deploy::D1_CLASS))
            .is_err());
        let services: celld_applications::Handler = serde_json::from_value(serde_json::json!({
            "id":"caller", "language":"javascript", "entrypoint":"main.mjs", "artifact_sha256":"a".repeat(64), "runtime_image_sha256":"b".repeat(64),
            "bindings":{"SVC":{"kind":"handler","handler":"same"},"NAMED":{"kind":"handler","handler":"same"},"BLOBS":{"kind":"resource","resource":{"kind":"r2","id":"bucket"}},"SITE":{"kind":"resource","resource":{"kind":"static_assets","id":"site"}}},
            "worker_config":{"services":[{"binding":"NAMED","service":"same","entrypoint":"Named"}]}
        })).unwrap();
        let deployment: celld_applications::Name = "pinned-old".to_owned().try_into().unwrap();
        client.bind_services(deployment.clone(), &services).unwrap();
        let resource_call = |binding: &str| celld_applications::resource_wire::ResourceCall {
            binding: binding.to_owned().try_into().unwrap(),
            operation: celld_applications::resource_wire::ResourceOperation::R2 {
                operation: celld_applications::resource_wire::R2Operation::Head {
                    key: "record".into(),
                },
            },
        };
        let bound = client.resource_target(&resource_call("BLOBS")).unwrap();
        assert_eq!(bound.application, application);
        assert_eq!(bound.resource.id.as_str(), "bucket");
        assert_eq!(bound.resource.kind, ResourceKind::R2);
        assert!(
            client.resource_target(&resource_call("bucket")).is_err(),
            "resource id bypassed immutable alias"
        );
        assert!(
            client.resource_target(&resource_call("SVC")).is_err(),
            "service alias granted a bucket"
        );
        assert!(client.resource_target(&resource_call("UNBOUND")).is_err());
        let assets_call = |binding: &str| celld_applications::resource_wire::ResourceCall {
            binding: binding.to_owned().try_into().unwrap(),
            operation: celld_applications::resource_wire::ResourceOperation::Assets {},
        };
        assert_eq!(
            client
                .resource_target(&assets_call("SITE"))
                .unwrap()
                .resource
                .kind,
            ResourceKind::StaticAssets
        );
        assert!(
            client.resource_target(&assets_call("BLOBS")).is_err(),
            "bucket served assets"
        );
        assert!(
            client.resource_target(&resource_call("SITE")).is_err(),
            "assets served R2"
        );
        assert!(client.resource_target(&assets_call("SVC")).is_err());
        assert_eq!(
            client.0.handler.get().map(|name| name.as_str()),
            Some("caller")
        );
        let call = |binding: &str, entrypoint: Option<&str>| {
            celld_applications::invocation_wire::ServiceCall {
                binding: binding.to_owned().try_into().unwrap(),
                invocation: celld_applications::invocation_wire::ServiceInvocation {
                    entrypoint: entrypoint.map(Into::into),
                    props: vec![],
                    rpc: None,
                    get: false,
                    signals: vec![],
                    signal_owner: None,
                },
            }
        };
        assert_eq!(
            client.service_target(&call("SVC", None)).unwrap(),
            (deployment.clone(), "same".to_owned().try_into().unwrap())
        );
        assert!(client.service_target(&call("same", None)).is_err());
        assert!(client.service_target(&call("UNBOUND", None)).is_err());
        assert!(client.service_target(&call("SVC", Some("Named"))).is_err());
        assert!(client.service_target(&call("NAMED", None)).is_err());
        assert!(client.service_target(&call("NAMED", Some("Named"))).is_ok());
        assert!(client.bind_services(deployment, &services).is_err());
        assert_eq!(client.namespace(&object).unwrap().0, counter);
        assert_eq!(
            client.namespace(&target(&sibling, "Alternate")).unwrap().0,
            sibling
        );
        let unique = ObjectTarget {
            name: None,
            ..object.clone()
        };
        assert_eq!(client.namespace(&unique).unwrap().0, counter);
        // A D1 resource serves the internal database class, and only it.
        let d1 = crate::deploy::D1_CLASS;
        assert_eq!(
            client.namespace(&target(&database, d1)).unwrap().0,
            database
        );
        for refused in [
            target(&sibling, "Counter"),
            target(&counter, "Alternate"),
            target(&address(&application, "unbound"), "Counter"),
            target(&address(&elsewhere, "counter"), "Counter"),
            target(&counter, "Undeclared"),
            target(&counter, d1),
            target(&database, "Counter"),
            target(&address(&elsewhere, "db"), d1),
            ObjectTarget {
                name: Some("b".into()),
                ..object.clone()
            },
            ObjectTarget {
                id: "0".repeat(64),
                ..object
            },
        ] {
            assert!(
                client.namespace(&refused).is_err(),
                "{refused:?} was served"
            );
        }
        // A KV resource serves exactly its one shard, under its own application.
        let kv = celld_logic::kv::RESERVED_CLASS;
        let shard = |resource: &ResourceAddress, name: &str| {
            let scope = resource.cell_scope(kv, name).unwrap();
            ObjectTarget {
                class: kv.into(),
                id: scope.split_once(':').unwrap().1.into(),
                name: Some(name.into()),
            }
        };
        let name = celld_logic::kv::cell_name("namespace", 0);
        let (mine, theirs) = (cache(&application, "cache"), cache(&elsewhere, "cache"));
        let canonical = shard(&mine, &name);
        assert_eq!(client.namespace(&canonical).unwrap().0, mine);
        let unnamed = ObjectTarget {
            name: None,
            ..canonical.clone()
        };
        assert_eq!(client.namespace(&unnamed).unwrap().0, mine);
        assert_ne!(canonical.id, shard(&theirs, &name).id);
        for refused in [
            shard(&theirs, &name),
            shard(&cache(&application, "withdrawn"), &name),
            shard(&mine, &celld_logic::kv::cell_name("namespace", 1)),
            ObjectTarget {
                name: Some(celld_logic::kv::cell_name("namespace", 1)),
                ..canonical.clone()
            },
            ObjectTarget {
                id: "0".repeat(64),
                ..canonical.clone()
            },
            ObjectTarget {
                class: "Counter".into(),
                ..canonical.clone()
            },
            target(&counter, kv),
        ] {
            assert!(
                client.namespace(&refused).is_err(),
                "{refused:?} was served"
            );
        }
        let kv_call = |resource: &ResourceAddress, object: &ObjectTarget, rpc: Option<&str>| {
            authorize_kv(
                resource,
                &ObjectCall {
                    object: object.clone(),
                    rpc: rpc.map(str::to_owned),
                },
            )
        };
        assert_eq!(
            kv_call(&mine, &canonical, Some("__kvGet")).unwrap(),
            "__kvGet"
        );
        assert!(kv_call(&theirs, &canonical, Some("__kvGet")).is_err());
        assert!(kv_call(&database, &canonical, Some("__kvGet")).is_err());
        for method in [
            None,
            Some("fetch"),
            Some("__kvSweep"),
            Some("__kvMetrics"),
            Some("alarm"),
        ] {
            assert!(kv_call(&mine, &canonical, method).is_err(), "{method:?}");
        }
        // A queue resource serves exactly the queue it names, under its own
        // application, and only the producer's two methods.
        let queue = celld_logic::queue::RESERVED_CLASS;
        let queue_cell = |resource: &ResourceAddress, name: &str| {
            let scope = resource.cell_scope(queue, name).unwrap();
            ObjectTarget {
                class: queue.into(),
                id: scope.split_once(':').unwrap().1.into(),
                name: Some(name.into()),
            }
        };
        let (jobs, others) = (jobs_of(&application, "jobs"), jobs_of(&elsewhere, "jobs"));
        let canonical = queue_cell(&jobs, "jobs");
        assert_eq!(client.namespace(&canonical).unwrap().0, jobs);
        let unnamed = ObjectTarget {
            name: None,
            ..canonical.clone()
        };
        assert_eq!(client.namespace(&unnamed).unwrap().0, jobs);
        assert_ne!(canonical.id, queue_cell(&others, "jobs").id);
        for refused in [
            queue_cell(&others, "jobs"),
            queue_cell(&jobs_of(&application, "withdrawn"), "withdrawn"),
            queue_cell(&jobs, "other"),
            ObjectTarget {
                name: Some("other".into()),
                ..canonical.clone()
            },
            ObjectTarget {
                id: "0".repeat(64),
                ..canonical.clone()
            },
            ObjectTarget {
                class: "Counter".into(),
                ..canonical.clone()
            },
            target(&counter, queue),
        ] {
            assert!(
                client.namespace(&refused).is_err(),
                "{refused:?} was served"
            );
        }
        let queue_call = |resource: &ResourceAddress, object: &ObjectTarget, rpc: Option<&str>| {
            authorize_queue(
                resource,
                &ObjectCall {
                    object: object.clone(),
                    rpc: rpc.map(str::to_owned),
                },
            )
        };
        for method in ["__queueSend", "__queueMetrics"] {
            assert_eq!(queue_call(&jobs, &canonical, Some(method)).unwrap(), method);
        }
        assert!(queue_call(&others, &canonical, Some("__queueSend")).is_err());
        assert!(queue_call(&database, &canonical, Some("__queueSend")).is_err());
        for method in [
            None,
            Some("fetch"),
            Some("alarm"),
            Some("__queueDlqAcceptBatch"),
            Some("__kvGet"),
        ] {
            assert!(queue_call(&jobs, &canonical, method).is_err(), "{method:?}");
        }
        eprintln!("guest object calls: unbound VM, unbound namespace, another application's id, undeclared class, forged id and mismatched name refused; named and unique ids of a bound namespace resolve to it; a D1 resource serves only the internal database class and a Durable Object resource cannot; a KV resource serves only its canonical shard under its own application, and only its four client methods; a queue resource serves only the queue it names under its own application, and only its two producer methods");
    }
    // A fetch's FetchEnd waits on a full outbound queue with its stop guard
    // held: retiring the entry earlier would cancel the task and lose the end.
    {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", origin.local_addr().unwrap());
        crate::asyncrt::spawn(async move {
            let (mut socket, _) = origin.accept().await.unwrap();
            let _ = socket.read(&mut [0; 1024]).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok")
                .await
                .unwrap();
        });
        let (outbound, mut queue) = mpsc::channel(1);
        let client = Client(Arc::new(Shared {
            execution_gate: std::sync::OnceLock::new(),
            idle_lifecycle: false,
            application: application.clone(),
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
            owner_pid: None,
            reaped: CancellationToken::new(),
            reply_drain: true,
            resident_objects: false,
            object_retirement: false,
            response_streams: true,
            memory_diagnostics: false,
            drained: Notify::new(),
            object_stops: tokio::sync::Mutex::new(()),
            bound: std::sync::OnceLock::new(),
            services: std::sync::OnceLock::new(),
            resources: std::sync::OnceLock::new(),
            service_routes: std::sync::OnceLock::new(),
            signal_owner: "e".repeat(64),
            handler: std::sync::OnceLock::new(),
            containers: std::sync::OnceLock::new(),
        }));
        let (id, fetch) = ("b".repeat(64), "f".repeat(64));
        let (pulls, credits) = mpsc::channel(1);
        let stop = CancellationToken::new();
        client.0.registry.lock().unwrap().pending.insert(
            id.clone(),
            Pending {
                execution: None,
                retirement: false,
                grant: "a".repeat(64),
                storage: None,
                response: None,
                response_seen: false,
                terminal: None,
                terminal_seen: false,
                connected: true,
                cleanup_connected: false,
                access: None,
                cancelled: false,
                deadline: None,
                settled: CancellationToken::new(),
                request: None,
                fetches: HashMap::from([(
                    fetch.clone(),
                    BrokeredFetch {
                        flow: ResponseFlow::default(),
                        pulls: pulls.clone(),
                        _stop: stop.clone().drop_guard(),
                        request: None,
                        signals: HashSet::new(),
                        relay: mpsc::unbounded_channel().0,
                    },
                )]),
                app: None,
                stream: None,
                sockets: None,
                dials: HashMap::new(),
                issued_sockets: HashMap::new(),
            },
        );
        // The check polls `is_finished`, which the domain task handle omits.
        #[allow(clippy::disallowed_methods)]
        // The check polls `is_finished`, which the domain task handle omits.
        #[allow(clippy::disallowed_methods)]
        let broker = tokio::spawn(broker_fetch(
            client.clone(),
            id.clone(),
            fetch.clone(),
            HttpRequest {
                method: "GET".into(),
                url,
                headers: vec![],
                body: vec![],
            },
            "follow".into(),
            None,
            None,
            None,
            None,
            None,
            credits,
            stop,
            None,
            mpsc::unbounded_channel().1,
            None,
            None,
        ));
        let grant = |sequence| {
            let mut registry = client.0.registry.lock().unwrap();
            let state = registry.pending.get_mut(&id).unwrap();
            let state = state.fetches.get_mut(&fetch).unwrap();
            state.flow.grant(sequence).unwrap();
            let pulls = pulls.clone();
            async move { pulls.send(sequence).await.unwrap() }
        };
        async fn next(queue: &mut mpsc::Receiver<Outbound>) -> HostControl {
            let frame = timeout(IO_DEADLINE, queue.recv()).await;
            match frame.unwrap().unwrap() {
                Outbound::Frame(bytes) => HostControl::decode(&bytes).unwrap(),
                Outbound::Fence(_) => panic!("unexpected writer fence"),
            }
        }
        assert!(matches!(
            next(&mut queue).await,
            HostControl::FetchHead { .. }
        ));
        grant(0).await;
        // The chunk fills the one-slot queue, so the credited end must wait.
        timeout(IO_DEADLINE, async {
            while client.0.outbound.capacity() != 0 {
                crate::asyncrt::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        grant(1).await;
        crate::asyncrt::sleep(Duration::from_millis(20)).await;
        assert!(
            !broker.is_finished(),
            "fetch task stopped with FetchEnd unsent"
        );
        assert!(
            client.0.registry.lock().unwrap().pending[&id]
                .fetches
                .contains_key(&fetch),
            "fetch retired before FetchEnd was queued"
        );
        assert!(matches!(
            next(&mut queue).await,
            HostControl::FetchChunk { .. }
        ));
        assert!(matches!(
            next(&mut queue).await,
            HostControl::FetchEnd {
                sequence: 1,
                error: None,
                ..
            }
        ));
        timeout(IO_DEADLINE, broker).await.unwrap().unwrap();
        assert!(client.0.registry.lock().unwrap().pending[&id]
            .fetches
            .is_empty());
    }
    eprintln!("reply/drain protocol: reply alone retained active routing and joined witness; normal drain, joined failure and cancellation/reply races settled separately; duplicate, reordered, unknown, unconnected and unadvertised messages retired transport");
    eprintln!("response stream admission: streams without joined drain refused before production connection; streamed head/chunk/end frames retired buffered transport without delivery; incomplete JavaScript lifecycle readiness remains refused");
    eprintln!("credited stream protocol: absent credit, wrong sequence, duplicate chunk/head, end without credit, chunk after end, body before head and drain before end retired transport without output");
}
