use super::*;
use celld_applications::{ApplicationId, ResourceAddress, ResourceKind, ResourceRef};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn durable_guest_alarm_claim_and_settlement() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("alarm.sqlite");
    let scope = format!("AlarmCounter:{}", "a".repeat(64));
    let cells = Cells::default();
    let _installed = cells.install();
    let open =
        || storage::open_at_epoch(&scope, path.to_str().unwrap(), 7, false, None, false).unwrap();
    open();
    let now = crate::ownership_store::now_ms() as i64;
    let due = now - 10;
    storage::set_alarm(&scope, due).unwrap();
    storage::close(&scope);
    open();
    assert_eq!(
        storage::get_alarm(&scope),
        Some(due),
        "committed alarm lost on reopen"
    );
    let mut session = StorageSession::new(&cells, scope.clone(), 7).unwrap();
    let event = session.begin_event(&cells).unwrap();
    assert!(session
        .claim_alarm(&cells, now + 60_000)
        .unwrap_err()
        .to_string()
        .contains("before its deadline"));
    assert!(session
        .claim_alarm(&cells, due - 1)
        .unwrap_err()
        .to_string()
        .contains("stale"));
    session.claim_alarm(&cells, due).unwrap();
    assert_eq!(session.alarm().unwrap().retry, 0);
    assert_eq!(
        storage::get_alarm(&scope),
        None,
        "firing alarm must be hidden"
    );
    let mut duplicate = StorageSession::new(&cells, scope.clone(), 7).unwrap();
    assert!(duplicate.claim_alarm(&cells, due).is_err());
    let (output, refused) = session
        .finish_event_with_alarm(&cells, &event, Some((true, false)))
        .unwrap();
    assert!(refused.is_none());
    assert!(
        output.position().is_some(),
        "alarm deletion escaped output proof"
    );
    storage::close(&scope);
    open();
    assert_eq!(
        storage::get_alarm(&scope),
        None,
        "consumed alarm returned on reopen"
    );

    // A failed handler's durable retry shares the event's output position.
    storage::set_alarm(&scope, due).unwrap();
    let mut failed = StorageSession::new(&cells, scope.clone(), 7).unwrap();
    let event = failed.begin_event(&cells).unwrap();
    failed.claim_alarm(&cells, due).unwrap();
    let (output, refused) = failed
        .finish_event_with_alarm(&cells, &event, Some((false, true)))
        .unwrap();
    assert!(refused.is_none() && output.position().is_some());
    let (retry_at, retry) = storage::due_alarm_entry(&scope, i64::MAX).unwrap();
    assert!(retry_at > now && retry == 1);
    storage::close(&scope);
    open();
    assert_eq!(
        storage::due_alarm_entry(&scope, i64::MAX),
        Some((retry_at, retry))
    );
    let (_, _, retries, counted_retries) =
        storage::persisted_alarm(path.to_str().unwrap(), &scope, None).unwrap();
    assert_eq!((retries, counted_retries), (1, 1));

    // Infrastructure failure still retries, without spending the original
    // separately persisted counted-retry limit.
    storage::set_alarm(&scope, due).unwrap();
    let mut uncounted = StorageSession::new(&cells, scope.clone(), 7).unwrap();
    let event = uncounted.begin_event(&cells).unwrap();
    uncounted.claim_alarm(&cells, due).unwrap();
    let (output, refused) = uncounted
        .finish_event_with_alarm(&cells, &event, Some((false, false)))
        .unwrap();
    assert!(refused.is_none() && output.position().is_some());
    storage::close(&scope);
    open();
    let (_, _, retries, counted_retries) =
        storage::persisted_alarm(path.to_str().unwrap(), &scope, None).unwrap();
    assert_eq!((retries, counted_retries), (1, 0));

    // An explicit re-arm wins even if the claimed handler fails.
    storage::set_alarm(&scope, due).unwrap();
    let mut rearmed = StorageSession::new(&cells, scope.clone(), 7).unwrap();
    let event = rearmed.begin_event(&cells).unwrap();
    rearmed.claim_alarm(&cells, due).unwrap();
    storage::set_alarm(&scope, now + 60_000).unwrap();
    rearmed
        .finish_event_with_alarm(&cells, &event, Some((false, true)))
        .unwrap();
    assert_eq!(storage::get_alarm(&scope), Some(now + 60_000));

    // Rolled-back installation cannot be claimed or appear after restart.
    storage::delete_alarm(&scope).unwrap();
    let mut rolled_back = StorageSession::new(&cells, scope.clone(), 7).unwrap();
    let begin = request(&rolled_back, Operation::TransactionStart { origin: None });
    let Value::Transaction { transaction } = success(rolled_back.execute(&cells, true, begin))
    else {
        panic!("missing savepoint")
    };
    let set = request(&rolled_back, Operation::SetAlarm { at_ms: due });
    success(rolled_back.execute(&cells, true, set));
    let rollback = request(
        &rolled_back,
        Operation::TransactionFinish {
            transaction,
            action: wire::TransactionAction::Rollback,
        },
    );
    success(rolled_back.execute(&cells, true, rollback));
    assert!(rolled_back.claim_alarm(&cells, due).is_err());
    storage::close(&scope);
    open();
    assert_eq!(storage::get_alarm(&scope), None);

    // A stale connection cannot settle a successor's alarm at the same epoch.
    storage::set_alarm(&scope, due).unwrap();
    let mut stale = StorageSession::new(&cells, scope.clone(), 7).unwrap();
    let event = stale.begin_event(&cells).unwrap();
    stale.claim_alarm(&cells, due).unwrap();
    storage::close(&scope);
    open();
    storage::set_alarm(&scope, now + 60_000).unwrap();
    assert!(stale
        .finish_event_with_alarm(&cells, &event, Some((true, false)))
        .is_err());
    assert_eq!(storage::get_alarm(&scope), Some(now + 60_000));
    storage::close(&scope);
}

/// The host registry owns a guest object's hibernatable sockets: ids are
/// the host's, every call is scoped to the issuing object, auto-responses
/// are the shell's, and what no upgrade claimed is released at settlement.
#[test]
fn guest_sockets_belong_to_the_host_registry_and_their_object() {
    let directory = tempfile::tempdir().unwrap();
    let cells = Cells::default();
    let _installed = cells.install();
    let open = |name: &str| {
        let scope = format!("Room:{}", name.repeat(64));
        let path = directory.path().join(format!("{name}.sqlite"));
        storage::open_at_epoch(&scope, path.to_str().unwrap(), 7, false, None, false).unwrap();
        scope
    };
    let (one, two) = (open("a"), open("b"));
    let mut a = StorageSession::new(&cells, one.clone(), 7).unwrap();
    let mut b = StorageSession::new(&cells, two.clone(), 7).unwrap();
    let run = |session: &mut StorageSession, operation| {
        let request = request(session, operation);
        session.execute(&cells, true, request).outcome
    };
    let ok = |outcome: Outcome| match outcome {
        Outcome::Ok { value } => value,
        Outcome::Error { message } => panic!("socket operation failed: {message}"),
    };
    let socket = |value: Value| match value {
        Value::Socket { ws } => ws,
        other => panic!("not a socket: {other:?}"),
    };
    let rows = |value: Value| match value {
        Value::Sockets { sockets } => sockets,
        other => panic!("not a list: {other:?}"),
    };

    // The guest names a pair id only it has seen; the host answers with its own.
    let first = socket(ok(run(
        &mut a,
        Operation::SocketAccept {
            ws: 1,
            accepted: false,
            tags: vec!["room".into(), "user:7".into()],
        },
    )));
    let foreign = socket(ok(run(
        &mut b,
        Operation::SocketAccept {
            ws: 1,
            accepted: false,
            tags: vec!["room".into()],
        },
    )));
    assert_ne!(first, foreign, "two objects shared a pair id");
    let listed = |session: &mut StorageSession, tag: Option<&str>| {
        rows(ok(run(
            session,
            Operation::SocketList {
                tag: tag.map(Into::into),
            },
        )))
    };
    assert_eq!(
        listed(&mut a, None)
            .iter()
            .map(|row| row.ws)
            .collect::<Vec<_>>(),
        [first]
    );
    assert_eq!(listed(&mut a, Some("user:7")).len(), 1);
    assert!(listed(&mut a, Some("other")).is_empty());
    assert_eq!(
        listed(&mut b, None)
            .iter()
            .map(|row| row.ws)
            .collect::<Vec<_>>(),
        [foreign]
    );

    // Metadata of another object's socket is refused, not read or written.
    for operation in [
        Operation::SocketAttachment {
            ws: foreign,
            attachment: vec![1],
        },
        Operation::SocketAutoResponseTimestamp { ws: foreign },
        Operation::SocketAccept {
            ws: foreign,
            accepted: true,
            tags: vec!["stolen".into()],
        },
    ] {
        assert!(matches!(run(&mut a, operation), Outcome::Error { .. }));
    }
    assert_eq!(listed(&mut b, None)[0].tags, ["room"]);
    assert!(listed(&mut b, None)[0].attachment.is_none());

    ok(run(
        &mut a,
        Operation::SocketAttachment {
            ws: first,
            attachment: vec![0, 255, 7],
        },
    ));
    assert_eq!(listed(&mut a, None)[0].attachment, Some(vec![0, 255, 7]));
    // Re-accepting a socket this object holds only changes its tags.
    assert_eq!(
        socket(ok(run(
            &mut a,
            Operation::SocketAccept {
                ws: first,
                accepted: true,
                tags: vec!["moved".into()],
            },
        ))),
        first
    );
    assert_eq!(listed(&mut a, Some("moved")).len(), 1);

    // The auto-response pair is the shell's: a match answers without the
    // guest and stamps the socket; another object keeps its own pair.
    let pair = ("ping".to_string(), "pong".to_string());
    ok(run(
        &mut a,
        Operation::SocketAutoResponse {
            pair: Some(pair.clone()),
        },
    ));
    assert!(matches!(
        ok(run(&mut a, Operation::SocketAutoResponseGet {})),
        Value::AutoResponse { pair: Some(found) } if found == pair
    ));
    assert!(matches!(
        ok(run(&mut b, Operation::SocketAutoResponseGet {})),
        Value::AutoResponse { pair: None }
    ));
    assert!(matches!(
        ok(run(
            &mut a,
            Operation::SocketAutoResponseTimestamp { ws: first }
        )),
        Value::AutoResponseTimestamp { millis: None }
    ));
    assert_eq!(
        crate::js::ws_auto_response(&one, first, "ping").as_deref(),
        Some("pong")
    );
    assert!(crate::js::ws_auto_response(&one, first, "other").is_none());
    assert!(crate::js::ws_auto_response(&two, foreign, "ping").is_none());
    assert!(matches!(
        ok(run(
            &mut a,
            Operation::SocketAutoResponseTimestamp { ws: first }
        )),
        Value::AutoResponseTimestamp { millis: Some(_) }
    ));
    ok(run(&mut a, Operation::SocketAutoResponse { pair: None }));
    assert!(crate::js::ws_auto_response(&one, first, "ping").is_none());

    // One upgrade takes a socket once; whatever no upgrade took is released
    // when its event settles, and a claimed socket survives that.
    assert_eq!(crate::js::ws_claim_guest(first).unwrap(), one);
    assert!(crate::js::ws_claim_guest(first).is_err(), "claimed twice");
    a.revoke(&cells).unwrap();
    b.revoke(&cells).unwrap();
    assert_eq!(crate::js::ws_scope_of(first).as_deref(), Some(one.as_str()));
    assert!(
        crate::js::ws_scope_of(foreign).is_none(),
        "unclaimed socket outlived its event"
    );
    crate::js::ws_unregister(first);
    storage::close(&one);
    storage::close(&two);
}

pub(crate) async fn check_guest_alarm_cancellation_closes_only_its_native_claim() {
    use crate::runtime::{RuntimeManager, RuntimeOptions};
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;
    crate::js::Engine::init();
    let directory = tempfile::tempdir().unwrap();
    let address = address("alarm-cancellation");
    let scope = address.cell_scope("Counter", "named-alarm").unwrap();
    let runtime = RuntimeManager::start(
        check_generation(&address, 1),
        RuntimeOptions {
            data_dir: directory.path().join("data"),
            replication: None,
            wake: None,
            alarm_observer: Arc::new(|_, _| {}),
            node: "host".into(),
            region: "local".into(),
            bucket: None,
        },
    )
    .unwrap();
    runtime
        .restore_cell(
            &scope,
            &celld_logic::RestoreSpec {
                epoch: 7,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            },
        )
        .await
        .unwrap();
    runtime.start_cell(scope.clone(), 7, true).await.unwrap();
    runtime.publish_cell(&scope, 7).unwrap();
    let due = crate::ownership_store::now_ms() as i64 - 10;
    let admission = runtime.admit_storage_request(&scope, 7).unwrap();
    runtime
        .admitted_storage_turn(&admission, |_| storage::set_alarm(&scope, due).unwrap())
        .await
        .unwrap();

    let already_cancelled = CancellationToken::new();
    already_cancelled.cancel();
    assert!(runtime
        .begin_guest_storage_event(&admission, None, Some((due, already_cancelled)))
        .await
        .is_err());
    assert!(runtime
        .admitted_storage_turn(&admission, |_| storage::active_alarm_scheduled_time(&scope))
        .await
        .unwrap()
        .is_none());

    let mut holder = runtime
        .admitted_storage_turn(&admission, |cells| {
            let mut session = StorageSession::new(cells, scope.clone(), 7).unwrap();
            let start = request(&session, Operation::TransactionStart { origin: None });
            success(session.execute(cells, true, start));
            session
        })
        .await
        .unwrap();
    let queued_cancel = CancellationToken::new();
    let mut queued = Box::pin(runtime.begin_guest_storage_event(
        &admission,
        None,
        Some((due, queued_cancel.clone())),
    ));
    assert!(
        timeout(Duration::from_millis(50), &mut queued)
            .await
            .is_err(),
        "alarm bypassed a foreign transaction"
    );
    queued_cancel.cancel();
    assert!(timeout(Duration::from_secs(1), queued)
        .await
        .unwrap()
        .is_err());
    runtime
        .admitted_storage_turn(&admission, |cells| {
            assert!(
                storage::active_alarm_scheduled_time(&scope).is_none(),
                "cancelled queued alarm acquired an orphan claim"
            );
            holder.revoke(cells).unwrap();
        })
        .await
        .unwrap();

    let running_cancel = CancellationToken::new();
    let (mut session, event) = runtime
        .begin_guest_storage_event(&admission, None, Some((due, running_cancel.clone())))
        .await
        .unwrap();
    assert!(session.alarm().is_some());
    running_cancel.cancel();
    let (output, refused) = runtime
        .finish_guest_storage_event(&admission, &mut session, &event, Some((false, false)))
        .await
        .unwrap();
    assert!(
        refused.is_none() && output.position().is_some(),
        "cancelled alarm retry escaped native proof"
    );
    let retry = runtime
        .with_alarm_snapshot(&scope, |alarm| alarm.at_ms())
        .unwrap();
    assert!(
        retry > due,
        "final retry was not reported before completion"
    );
    runtime
        .admitted_storage_turn(&admission, |_| {
            assert!(
                storage::active_alarm_scheduled_time(&scope).is_none(),
                "cancelled running alarm kept its claim"
            );
            storage::set_alarm(&scope, due).unwrap();
        })
        .await
        .unwrap();
    let (mut successor, event) = runtime
        .begin_guest_storage_event(&admission, None, Some((due, CancellationToken::new())))
        .await
        .unwrap();
    runtime
        .finish_guest_storage_event(&admission, &mut successor, &event, Some((true, false)))
        .await
        .unwrap();
    assert_eq!(
        runtime.alarm(&scope),
        None,
        "successor deletion was not reported before completion"
    );
    drop(admission);
}

/// Multiple event channels from one guest share the actual native
/// connection and VM residency. Socket readers never own Cells: one host
/// thread serializes validation and effects, just as the runtime lock does.
pub(crate) fn shared_v8_host(
    scope: String,
    streams: Vec<std::os::unix::net::UnixStream>,
    ready: std::sync::mpsc::Sender<Vec<String>>,
) -> (Vec<(String, storage::StoredValue)>, usize, usize) {
    let count = streams.len();
    let (send, receive) = std::sync::mpsc::channel();
    for (index, stream) in streams.into_iter().enumerate() {
        send.send((index, stream)).unwrap();
    }
    drop(send);
    shared_v8_host_incoming(scope, count, receive, ready, None, None)
}

pub(crate) struct CancellationVmWitness {
    pub entered: std::sync::mpsc::Sender<()>,
    pub cancelled: Arc<AtomicBool>,
    pub joined: Arc<AtomicBool>,
    pub pre_entry: bool,
    pub cleanup_waiting: std::sync::mpsc::Sender<()>,
    pub allow_cleanup: Arc<AtomicBool>,
}

/// Cancellation bridge fixture. Both streams name one native event;
/// observing cleanup closes ordinary effects without releasing its holds.
pub(crate) fn cancelled_v8_host(
    scope: String,
    ordinary: std::os::unix::net::UnixStream,
    cleanup: std::os::unix::net::UnixStream,
    ready: std::sync::mpsc::Sender<String>,
    snapshots: std::sync::mpsc::Receiver<std::sync::mpsc::Sender<(usize, usize, bool, bool)>>,
    vm: Option<CancellationVmWitness>,
) -> Vec<(String, storage::StoredValue)> {
    use celld_applications::invocation_wire::StorageConnect;
    enum Frame {
        CleanupConnected(StorageConnect),
        Request(bool, Request, std::sync::mpsc::Sender<Reply>),
        Closed(bool),
    }
    let directory = tempfile::tempdir().unwrap();
    let cells = Cells::default();
    let _installed = cells.install();
    storage::open_at_epoch(
        &scope,
        directory.path().join("host.sqlite").to_str().unwrap(),
        7,
        false,
        None,
        false,
    )
    .unwrap();
    storage::set_actor_name(&scope, "named-object").unwrap();
    let mut session = StorageSession::new(&cells, scope.clone(), 7).unwrap();
    let mut successor = StorageSession::new(&cells, scope.clone(), 7).unwrap();
    let pre_entry = vm.as_ref().is_some_and(|vm| vm.pre_entry);
    let mut blocker = pre_entry.then(|| {
        let mut blocker = StorageSession::new(&cells, scope.clone(), 7).unwrap();
        let Value::Turn { turn } =
            success(blocker.execute(&cells, true, request(&blocker, Operation::TurnStart {})))
        else {
            panic!("blocker turn missing")
        };
        let Value::CriticalSection { critical_section } = success(blocker.execute(
            &cells,
            true,
            request(&blocker, Operation::CriticalSectionStart { origin: None }),
        )) else {
            panic!("blocker critical hold missing")
        };
        success(blocker.execute(
            &cells,
            true,
            request(&blocker, Operation::TurnFinish { turn }),
        ));
        (blocker, critical_section)
    });
    ready.send(session.grant().into()).unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let mut readers = Vec::new();
    for (is_cleanup, mut stream) in [(false, ordinary), (true, cleanup)] {
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let send = send.clone();
        readers.push(std::thread::spawn(move || {
            if is_cleanup {
                let hello =
                    StorageConnect::decode(&wire::read_frame(&mut stream).unwrap().unwrap())
                        .unwrap();
                send.send(Frame::CleanupConnected(hello)).unwrap();
            }
            while let Some(frame) = wire::read_frame(&mut stream).unwrap() {
                let request = Request::decode(&frame).unwrap();
                let (reply, result) = std::sync::mpsc::channel();
                send.send(Frame::Request(is_cleanup, request, reply))
                    .unwrap();
                let reply = result.recv_timeout(Duration::from_secs(5)).unwrap();
                wire::write_frame(&mut stream, &Reply::encode(&reply).unwrap()).unwrap();
            }
            send.send(Frame::Closed(is_cleanup)).unwrap();
        }));
    }
    drop(send);
    let mut cleanup_connected = false;
    let mut closed = std::collections::HashSet::new();
    let mut queued = None;
    let mut witnessed = false;
    let mut entered = false;
    let mut waiting: Vec<(
        bool,
        Request,
        std::sync::mpsc::Sender<Reply>,
        crate::js::InputGateWait,
    )> = Vec::new();
    let mut user_effects = 0;
    let deadline = Instant::now() + Duration::from_secs(15);
    while closed.len() != 2
        || vm
            .as_ref()
            .is_some_and(|vm| !vm.joined.load(Ordering::Acquire))
    {
        assert!(Instant::now() < deadline, "cancel fixture did not join");
        if blocker.is_some() && vm.as_ref().unwrap().allow_cleanup.load(Ordering::Acquire) {
            let (mut blocker, critical_section) = blocker.take().unwrap();
            let Value::Turn { turn } =
                success(blocker.execute(&cells, true, request(&blocker, Operation::TurnStart {})))
            else {
                panic!("blocker release turn missing")
            };
            success(blocker.execute(
                &cells,
                true,
                request(
                    &blocker,
                    Operation::CriticalSectionFinish {
                        critical_section,
                        failed: false,
                    },
                ),
            ));
            success(blocker.execute(
                &cells,
                true,
                request(&blocker, Operation::TurnFinish { turn }),
            ));
            blocker.revoke(&cells).unwrap();
        }
        if let Ok(snapshot) = snapshots.try_recv() {
            assert!(cleanup_connected && !witnessed && session.turn.is_none());
            if !pre_entry {
                let request = request(&successor, Operation::TurnStart {});
                let StorageAttempt::Wait { gate, .. } =
                    successor.try_execute(&cells, true, request)
                else {
                    panic!("guest cancellation released native input hold before joined settlement")
                };
                queued = Some(gate);
                assert!(matches!(
                    queued.as_mut().unwrap().try_recv(),
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty)
                ));
            }
            witnessed = true;
            snapshot
                .send((
                    session.transactions.len(),
                    session.critical_sections.len(),
                    storage::sync_sample(&scope)
                        .unwrap()
                        .unwrap()
                        .in_transaction,
                    !pre_entry,
                ))
                .unwrap();
        }
        let mut incoming = Vec::new();
        match receive.recv_timeout(Duration::from_millis(1)) {
            Ok(Frame::CleanupConnected(hello)) => {
                assert!(!cleanup_connected && hello.cleanup);
                assert_eq!(hello.invocation, "f".repeat(64));
                assert_eq!(hello.grant, session.grant());
                cleanup_connected = true;
            }
            Ok(Frame::Request(cleanup, request, reply)) => {
                incoming.push((cleanup, request, reply));
            }
            Ok(Frame::Closed(cleanup)) => {
                assert!(closed.insert(cleanup));
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) if closed.len() == 2 => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("cancel fixture transport ended: {error}"),
        }
        for index in (0..waiting.len()).rev() {
            let (cleanup, _, _, gate) = &mut waiting[index];
            if !*cleanup && vm.as_ref().unwrap().cancelled.load(Ordering::Acquire) {
                let (_, _, reply, _) = waiting.swap_remove(index);
                reply
                    .send(Reply {
                        version: wire::VERSION,
                        outcome: Outcome::Error {
                            message: "host cancelled queued ordinary admission".into(),
                        },
                    })
                    .unwrap();
                continue;
            }
            match gate.try_recv() {
                Ok(Ok(())) => {
                    let (cleanup, request, reply, _) = waiting.swap_remove(index);
                    incoming.push((cleanup, request, reply));
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
                result => panic!("cancel fixture queued gate refused: {result:?}"),
            }
        }
        for (cleanup, request, reply) in incoming {
            let finish = matches!(request.operation, Operation::TurnFinish { .. });
            if !cleanup
                && !matches!(
                    request.operation,
                    Operation::TurnStart {} | Operation::TurnFinish { .. }
                )
            {
                user_effects += 1;
            }
            let attempt = if cleanup {
                assert!(cleanup_connected);
                session.try_cleanup_turn(&cells, true, request)
            } else {
                session.try_execute(
                    &cells,
                    !cleanup_connected
                        && vm
                            .as_ref()
                            .is_none_or(|vm| !vm.cancelled.load(Ordering::Acquire)),
                    request,
                )
            };
            let result = match attempt {
                StorageAttempt::Reply(result) => result,
                StorageAttempt::Wait { gate, request } => {
                    assert!(pre_entry && matches!(request.operation, Operation::TurnStart {}));
                    waiting.push((cleanup, request, reply, gate));
                    let vm = vm.as_ref().unwrap();
                    if cleanup {
                        vm.cleanup_waiting.send(()).unwrap();
                    } else {
                        assert!(!entered);
                        vm.entered.send(()).unwrap();
                        entered = true;
                    }
                    continue;
                }
            };
            let armed = !cleanup
                && finish
                && !entered
                && !session.transactions.is_empty()
                && !session.critical_sections.is_empty()
                && matches!(result.outcome, Outcome::Ok { .. });
            reply.send(result).unwrap();
            if armed {
                if let Some(vm) = &vm {
                    vm.entered.send(()).unwrap();
                    entered = true;
                }
            }
        }
    }
    assert!(witnessed && cleanup_connected && session.turn.is_none());
    assert!(waiting.is_empty() && blocker.is_none());
    if pre_entry {
        assert_eq!(
            user_effects, 0,
            "cancelled queued turn constructed or invoked user code"
        );
        assert!(session.transactions.is_empty() && session.critical_sections.is_empty());
    } else {
        assert!(!session.transactions.is_empty() && !session.critical_sections.is_empty());
    }
    session.revoke(&cells).unwrap();
    if !pre_entry {
        assert!(
            matches!(queued.as_mut().unwrap().try_recv(), Ok(Err(reason))
            if reason == CRITICAL_SECTION_FAILED)
        );
    }
    assert!(session.transactions.is_empty() && session.critical_sections.is_empty());
    successor.revoke(&cells).unwrap();
    let mut fresh = StorageSession::new(&cells, scope.clone(), 7).unwrap();
    let start = request(&fresh, Operation::TurnStart {});
    let StorageAttempt::Reply(Reply {
        outcome: Outcome::Ok {
            value: Value::Turn { turn },
        },
        ..
    }) = fresh.try_execute(&cells, true, start)
    else {
        panic!("fresh native event refused after joined cancellation rollback")
    };
    let finish = request(&fresh, Operation::TurnFinish { turn });
    assert!(matches!(
        fresh.try_execute(&cells, true, finish),
        StorageAttempt::Reply(Reply {
            outcome: Outcome::Ok {
                value: Value::Unit {}
            },
            ..
        })
    ));
    fresh.revoke(&cells).unwrap();
    for reader in readers {
        reader.join().unwrap();
    }
    assert!(
        !storage::sync_sample(&scope)
            .unwrap()
            .unwrap()
            .in_transaction
    );
    assert_eq!(storage::sql_cursor_count(&scope), 0);
    let values =
        storage::list_stored_with_options(&scope, None, None, None, None, None, false).unwrap();
    storage::close(&scope);
    values
}

enum RetirementWitness {
    Snapshot(std::sync::mpsc::Sender<usize>),
    Close(std::sync::mpsc::Sender<(usize, usize)>),
}

fn shared_v8_host_incoming(
    scope: String,
    count: usize,
    streams: std::sync::mpsc::Receiver<(usize, std::os::unix::net::UnixStream)>,
    ready: std::sync::mpsc::Sender<Vec<String>>,
    slow_entered: Option<std::sync::mpsc::Sender<()>>,
    retirement: Option<std::sync::mpsc::Receiver<RetirementWitness>>,
) -> (Vec<(String, storage::StoredValue)>, usize, usize) {
    enum Frame {
        Request(usize, Request, std::sync::mpsc::Sender<Reply>),
        Closed(usize),
    }
    let directory = tempfile::tempdir().unwrap();
    let cells = Cells::default();
    let _installed = cells.install();
    storage::open_at_epoch(
        &scope,
        directory.path().join("host.sqlite").to_str().unwrap(),
        7,
        false,
        None,
        false,
    )
    .unwrap();
    storage::set_actor_name(&scope, "named-object").unwrap();
    let mut sessions: Vec<_> = (0..count)
        .map(|_| StorageSession::new(&cells, scope.clone(), 7).unwrap())
        .collect();
    let resident =
        Arc::new(CursorResidency::new(&sessions[0], Arc::new(AtomicBool::new(true))).unwrap());
    let successor = retirement.as_ref().map(|_| {
        assert_eq!(count, 8);
        Arc::new(CursorResidency::new(&sessions[7], Arc::new(AtomicBool::new(true))).unwrap())
    });
    for (index, session) in sessions.iter_mut().enumerate() {
        session.cursor_residency = Some(match &successor {
            Some(successor) if index == 7 => successor.clone(),
            _ => resident.clone(),
        });
    }
    let (send, receive) = std::sync::mpsc::channel();
    let retirement_enabled = retirement.is_some();
    let readers = std::thread::spawn(move || {
        let mut readers = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for (index, mut stream) in streams {
            assert!(index < count && seen.insert(index));
            stream
                .set_read_timeout(Some(Duration::from_secs(
                    if retirement_enabled && index == 6 {
                        10
                    } else {
                        5
                    },
                )))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let send = send.clone();
            readers.push(std::thread::spawn(move || {
                while let Some(frame) = wire::read_frame(&mut stream).unwrap() {
                    let request = Request::decode(&frame).unwrap();
                    let pending = format!("event {index}: {:?}", request.operation);
                    let (reply, result) = std::sync::mpsc::channel();
                    send.send(Frame::Request(index, request, reply)).unwrap();
                    let reply: Reply = result
                        .recv_timeout(Duration::from_secs(5))
                        .unwrap_or_else(|error| panic!("native reply {pending}: {error}"));
                    wire::write_frame(&mut stream, &reply.encode().unwrap()).unwrap();
                }
                send.send(Frame::Closed(index)).unwrap();
            }));
        }
        assert_eq!(seen.len(), count);
        drop(send);
        for reader in readers {
            reader.join().unwrap();
        }
    });
    ready
        .send(
            sessions
                .iter()
                .map(|session| session.grant().to_owned())
                .collect(),
        )
        .unwrap();
    let mut borrowed_calls = 0;
    let mut queued_turns = 0;
    let mut waiting = Vec::new();
    let mut closed = std::collections::HashSet::new();
    let mut close_requested = None;
    let mut cursors_retired = false;
    let mut hold_released = false;
    loop {
        if let Some(retirement) = &retirement {
            if let Ok(witness) = retirement.try_recv() {
                match witness {
                    RetirementWitness::Snapshot(reply) => {
                        reply.send(storage::sql_cursor_count(&scope)).unwrap();
                    }
                    RetirementWitness::Close(reply) => {
                        assert!(!cursors_retired && close_requested.is_none());
                        close_requested = Some(reply);
                    }
                }
            }
            if closed.contains(&6) {
                if let Some(reply) = close_requested.take() {
                    let before = storage::sql_cursor_count(&scope);
                    assert_eq!(before, 1, "saved read disappeared before object retirement");
                    resident.retired.store(true, Ordering::Release);
                    resident.close(&cells);
                    let after = storage::sql_cursor_count(&scope);
                    assert_eq!(after, 0);
                    cursors_retired = true;
                    reply.send((before, after)).unwrap();
                }
            }
        }
        let incoming = match receive.recv_timeout(Duration::from_millis(1)) {
            Ok(Frame::Request(index, request, reply)) => Some((index, request, reply)),
            Ok(Frame::Closed(index)) => {
                assert!(closed.insert(index));
                let session = &mut sessions[index];
                assert!(session.turn.is_none() && session.turn_lease.is_none());
                assert!(session.transactions.is_empty() && session.critical_sections.is_empty());
                // Match the owned production event boundary: revoke this
                // grant at EOF, while VM-resident read cursors survive.
                session.revoke(&cells).unwrap();
                None
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                assert!(waiting.is_empty());
                break;
            }
        };
        let mut ready = Vec::new();
        if let Some(incoming) = incoming {
            ready.push(incoming);
        }
        for index in (0..waiting.len()).rev() {
            let (_, _, _, gate): &mut (
                usize,
                Request,
                std::sync::mpsc::Sender<Reply>,
                crate::js::InputGateWait,
            ) = &mut waiting[index];
            match gate.try_recv() {
                Ok(Ok(())) => {
                    let (index, request, reply, _) = waiting.swap_remove(index);
                    ready.push((index, request, reply));
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
                outcome => {
                    let (_, _, reply, _) = waiting.swap_remove(index);
                    reply
                        .send(Reply {
                            version: wire::VERSION,
                            outcome: Outcome::Error {
                                message: format!(
                                    "native gate refused queued fixture turn: {outcome:?}"
                                ),
                            },
                        })
                        .unwrap();
                }
            }
        }
        for (index, request, reply) in ready {
            assert!(!closed.contains(&index));
            assert_eq!(request.grant, sessions[index].grant());
            if retirement.is_some() {
                eprintln!("JS native fixture event {index}: {:?}", request.operation);
            }
            let entered = matches!(&request.operation, Operation::PutClone { entries }
                    if entries.iter().any(|entry| entry.key == "slow-entered"));
            let releases = matches!(&request.operation, Operation::PutClone { entries }
                    if entries.iter().any(|entry| entry.key == "hold-released"));
            if matches!(request.operation, Operation::InExecutionTurn { .. }) {
                if count == 5 {
                    // T02's early-reply and failed origins are events 0/2;
                    // their resolving peer drivers are events 1/3.
                    assert!(
                        matches!(index, 0 | 2),
                        "foreign reaction borrowed the driver's grant"
                    );
                } else if count == 6 {
                    // T03's foreign block origin is event 0 (driver 1);
                    // event 3's continuations run in event 4's transaction.
                    assert!(
                        matches!(index, 0 | 3),
                        "foreign reaction borrowed the driver's grant"
                    );
                } else {
                    assert_eq!(index, 0, "foreign reaction borrowed the driver's grant");
                }
                assert!(sessions[index].turn.is_none());
                borrowed_calls += 1;
            }
            let attempt = if retirement.is_some() && index == 6 {
                sessions[index].try_cleanup_turn(&cells, true, request)
            } else {
                sessions[index].try_execute(&cells, true, request)
            };
            match attempt {
                StorageAttempt::Reply(result) => {
                    hold_released |= releases && matches!(result.outcome, Outcome::Ok { .. });
                    // The increment (event 2 of the retirement fixture) queues
                    // behind the hold's native gate.
                    assert!(
                        retirement.is_none() || index != 2 || hold_released,
                        "increment ran before the hold released the native gate"
                    );
                    if entered && matches!(result.outcome, Outcome::Ok { .. }) {
                        if let Some(observed) = &slow_entered {
                            observed.send(()).unwrap();
                        }
                    }
                    reply.send(result).unwrap();
                }
                StorageAttempt::Wait { gate, request } => {
                    assert!(matches!(request.operation, Operation::TurnStart {}));
                    queued_turns += 1;
                    waiting.push((index, request, reply, gate));
                }
            }
        }
    }
    readers.join().unwrap();
    assert_eq!(closed.len(), count);
    assert!(retirement.is_none() || cursors_retired);
    assert!(resident.turns.lock().unwrap().is_empty());
    assert!(
        !storage::sync_sample(&scope)
            .unwrap()
            .unwrap()
            .in_transaction
    );
    assert_eq!(storage::sql_cursor_count(&scope), 0);
    resident.close(&cells);
    if let Some(successor) = successor {
        successor.close(&cells);
    }
    let values =
        storage::list_stored_with_options(&scope, None, None, None, None, None, false).unwrap();
    storage::close(&scope);
    (values, borrowed_calls, queued_turns)
}

// Test supervisor owns the jailer/VMM child directly: no daemonization or
// sudo wrapper that could survive teardown. Drop also covers failed asserts.
struct Guest {
    child: Child,
    console: PathBuf,
    vm: bool,
    marker: &'static str,
}

impl Drop for Guest {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Guest {
    fn log(&self) -> String {
        let bytes = std::fs::read(&self.console).unwrap_or_default();
        String::from_utf8_lossy(&bytes[bytes.len().saturating_sub(32768)..]).into_owned()
    }

    fn assert_running(&mut self) {
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "guest exited: {}",
            self.log()
        );
    }

    fn finish(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if self.vm && self.log().contains(self.marker) {
                self.child.kill().unwrap();
                self.child.wait().unwrap();
                return;
            }
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(
                    !self.vm && status.success(),
                    "guest check failed: {}",
                    self.log()
                );
                return;
            }
            assert!(
                Instant::now() < deadline,
                "guest completion timed out: {}",
                self.log()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

fn digest(path: &Path) -> String {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let mut file = std::fs::File::open(path).unwrap();
    let mut hash = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let size = file.read(&mut buffer).unwrap();
        if size == 0 {
            break;
        }
        hash.update(&buffer[..size]);
    }
    format!("{:x}", hash.finalize())
}

// Selectively follows the reference Firecracker API client, using existing
// Hyper rather than a hand-written HTTP parser. Bounds both body and time.
async fn vm_put(api: &Path, path: &str, body: serde_json::Value) {
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full, Limited};
    use hyper_util::rt::TokioIo;
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        let stream = tokio::net::UnixStream::connect(api).await?;
        let request = hyper::Request::builder()
            .method("PUT")
            .uri(path)
            .header("host", "firecracker")
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(serde_json::to_vec(&body)?)))?;
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
        let connection = tokio::spawn(connection);
        let result: Result<()> = async {
            let response = sender.send_request(request).await?;
            let status = response.status();
            let body = Limited::new(response.into_body(), 65536)
                .collect()
                .await
                .map_err(|error| anyhow::anyhow!("Firecracker API body: {error}"))?
                .to_bytes();
            ensure!(
                status == 204 || status == 200,
                "Firecracker {path}: {status}: {}",
                String::from_utf8_lossy(&body)
            );
            Ok(())
        }
        .await;
        connection.abort();
        result
    })
    .await
    .unwrap();
    result.unwrap();
}

async fn guest(directory: &Path, mode: &str) -> (Guest, std::os::unix::net::UnixListener) {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixListener;
    assert_eq!(mode, "javascript");
    let image =
        PathBuf::from(std::env::var_os("CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE").unwrap());
    let console = directory.join("guest.log");
    let log = std::fs::File::create(&console).unwrap();
    let marker = "CELLD_APPLICATION_JAVASCRIPT_VM_PASS";
    {
        assert!(
            image.is_absolute(),
            "test VM image must be an absolute path"
        );
        assert_eq!(
            digest(&image),
            std::fs::read_to_string(image.with_extension("sha256"))
                .unwrap()
                .trim(),
            "test image checksum changed"
        );
        let kernel =
            PathBuf::from(std::env::var_os("CELLD_APPLICATION_JAVASCRIPT_TEST_KERNEL").unwrap());
        assert!(kernel.is_absolute());
        assert_eq!(
            digest(&kernel),
            std::env::var("CELLD_APPLICATION_JAVASCRIPT_TEST_KERNEL_SHA256").unwrap(),
            "test kernel checksum changed"
        );
        let base = directory.join("jails");
        let root = base.join("firecracker/storage-check/root");
        std::fs::create_dir_all(&root).unwrap();
        for (source, name) in [(&image, "rootfs.ext4"), (&kernel, "vmlinux")] {
            let target = root.join(name);
            std::fs::copy(source, &target).unwrap();
            std::fs::set_permissions(target, std::fs::Permissions::from_mode(0o444)).unwrap();
        }
        let socket = root.join(if mode == "agent" || mode == "javascript" {
            "vsock_5002"
        } else {
            "vsock_5001"
        });
        let listener = UnixListener::bind(&socket).unwrap();
        std::os::unix::fs::chown(&socket, Some(1000), Some(1000)).unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let child = Command::new("/usr/local/bin/jailer")
            .args([
                "--id",
                "storage-check",
                "--uid",
                "1000",
                "--gid",
                "1000",
                "--exec-file",
                "/usr/local/bin/firecracker",
                "--chroot-base-dir",
            ])
            .arg(&base)
            .args(["--", "--api-sock", "/api.sock"])
            .env_clear()
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let mut guest = Guest {
            child,
            console,
            vm: true,
            marker,
        };
        let api = root.join("api.sock");
        let deadline = Instant::now() + Duration::from_secs(10);
        while !api.exists() {
            guest.assert_running();
            assert!(
                Instant::now() < deadline,
                "jailer API did not start: {}",
                guest.log()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        use serde_json::json;
        vm_put(
            &api,
            "/machine-config",
            json!({"vcpu_count":1,"mem_size_mib":512,"smt":false}),
        )
        .await;
        let boot_args = format!("console=ttyS0 reboot=k panic=1 pci=off root=/dev/vda init=/sbin/init ro devtmpfs.mount=1 celld.check={mode}");
        vm_put(
            &api,
            "/boot-source",
            json!({"kernel_image_path":"/vmlinux","boot_args":boot_args}),
        )
        .await;
        vm_put(&api, "/drives/rootfs", json!({"drive_id":"rootfs","path_on_host":"/rootfs.ext4","is_root_device":true,"is_read_only":true})).await;
        vm_put(&api, "/entropy", json!({})).await;
        vm_put(&api, "/vsock", json!({"guest_cid":3,"uds_path":"/vsock"})).await;
        vm_put(&api, "/actions", json!({"action_type":"InstanceStart"})).await;
        (guest, listener)
    }
}

async fn connected_mode_guest(
    directory: &Path,
    mode: &str,
) -> (Guest, std::os::unix::net::UnixStream) {
    let (mut guest, listener) = guest(directory, mode).await;
    let deadline = Instant::now() + Duration::from_secs(15);
    let (socket, _) = loop {
        match listener.accept() {
            Ok(socket) => break socket,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < deadline,
                    "guest did not connect: {}",
                    guest.log()
                );
                guest.assert_running();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => panic!("storage listener failed: {error}"),
        }
    };
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    (guest, socket)
}

fn address(app: &str) -> ResourceAddress {
    ResourceAddress {
        application: ApplicationId {
            app: app.to_owned().try_into().unwrap(),
        },
        resource: ResourceRef {
            kind: ResourceKind::DurableObject,
            id: "counter".to_owned().try_into().unwrap(),
        },
    }
}

fn scope(app: &str) -> String {
    address(app).cell_scope("Counter", "named-object").unwrap()
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires explicit JavaScript image, Linux KVM and jailer"]
async fn javascript_vm_idle_retirement() {
    crate::asyncrt::bind_test_runtime();
    assert!(std::env::var_os("CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE").is_some());
    check_javascript_vm().await;
}

pub(crate) async fn check_javascript_vm() {
    if std::env::var_os("CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE").is_none() {
        eprintln!("original V8 real-VM check skipped: no CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE; retained V8 checks are local only");
        return;
    }
    for mode in ["async", "never", "throw"] {
        check_javascript_vm_retirement_case(mode).await;
    }
    check_javascript_vm_quiescence().await;
    check_javascript_vm_callbacks().await;
}

async fn check_javascript_vm_quiescence() {
    use celld_applications::invocation_wire::{
        GuestControl, HostControl, HttpRequest, InvokeHttp, ObjectTarget, StorageConnect,
    };
    let directory = tempfile::tempdir().unwrap();
    let (mut guest, mut control) = connected_mode_guest(directory.path(), "javascript").await;
    let root = directory
        .path()
        .join("jails/firecracker/storage-check/root");
    let listener = std::os::unix::net::UnixListener::bind(root.join("vsock_5001")).unwrap();
    std::os::unix::fs::chown(root.join("vsock_5001"), Some(1000), Some(1000)).unwrap();
    std::fs::set_permissions(
        root.join("vsock_5001"),
        std::os::unix::fs::PermissionsExt::from_mode(0o600),
    )
    .unwrap();
    listener.set_nonblocking(true).unwrap();
    assert!(matches!(
        GuestControl::decode(&wire::read_frame(&mut control).unwrap().unwrap()).unwrap(),
        GuestControl::Ready {
            adapter: celld_applications::Language::Javascript,
            ..
        }
    ));
    let cell = address("javascript-vm")
        .cell_scope("QuiescenceCounter", "named-object")
        .unwrap();
    let host_cell = cell.clone();
    let (send, grants) = std::sync::mpsc::channel();
    let (streams, incoming) = std::sync::mpsc::channel();
    let host = std::thread::spawn(move || {
        shared_v8_host_incoming(host_cell, 5, incoming, send, None, None)
    });
    let grants = grants.recv_timeout(Duration::from_secs(5)).unwrap();
    let connect_grants = grants.clone();
    let acceptor = std::thread::spawn(move || {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..5 {
            let deadline = Instant::now() + Duration::from_secs(20);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "quiescence guest did not connect storage"
                        );
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("quiescence storage accept: {error}"),
                }
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let connect =
                StorageConnect::decode(&wire::read_frame(&mut stream).unwrap().unwrap()).unwrap();
            let index = connect_grants
                .iter()
                .position(|grant| grant == &connect.grant)
                .unwrap();
            assert!(seen.insert(index) && !connect.cleanup);
            assert_eq!(connect.invocation, format!("{:064x}", index + 1));
            streams.send((index, stream)).unwrap();
        }
    });
    let invoke = |control: &mut std::os::unix::net::UnixStream, index: usize, path: &str| {
        wire::write_frame(
            control,
            &HostControl::InvokeHttp(Box::new(InvokeHttp {
                request_stream: false,
                queue: false,
                alarm: None,
                service: None,
                rpc: None,
                version: wire::VERSION,
                invocation: format!("{:064x}", index + 1),
                storage_grant: grants[index].clone(),
                storage_residency: Some("c".repeat(64)),
                object: Some(ObjectTarget {
                    class: "QuiescenceCounter".into(),
                    id: cell.split_once(':').unwrap().1.into(),
                    name: Some("named-object".into()),
                }),
                request: HttpRequest {
                    method: "GET".into(),
                    url: format!("http://check{path}"),
                    headers: vec![],
                    body: vec![],
                },
                socket: None,
            }))
            .encode()
            .unwrap(),
        )
        .unwrap();
    };
    let receive = |control: &mut std::os::unix::net::UnixStream| {
        GuestControl::decode(
            &wire::read_frame(control)
                .unwrap_or_else(|error| panic!("quiescence control: {error}; {}", guest.log()))
                .unwrap_or_else(|| panic!("quiescence guest ended: {}", guest.log())),
        )
        .unwrap()
    };
    let reply = |message: GuestControl, index: usize, body: &[u8]| {
        let GuestControl::HttpReply(result) = message else {
            panic!("guest retired background before peer progress: {message:?}")
        };
        assert_eq!(result.invocation, format!("{:064x}", index + 1));
        assert_eq!(result.response.body, body);
    };
    let drained = |message, index| {
        assert_eq!(
            message,
            GuestControl::Drained {
                version: wire::VERSION,
                invocation: format!("{:064x}", index + 1)
            }
        )
    };
    invoke(&mut control, 0, "/quiescence-wait");
    reply(receive(&mut control), 0, b"early");
    invoke(&mut control, 1, "/quiescence-resolve");
    reply(receive(&mut control), 1, b"peer");
    drained(receive(&mut control), 1);
    drained(receive(&mut control), 0);
    assert!(guest
        .log()
        .contains("CELLD_JS_QUIESCENCE_NESTED /quiescence-wait"));
    invoke(&mut control, 2, "/quiescence-fail");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !guest
        .log()
        .contains("CELLD_JS_QUIESCENCE_WAITING /quiescence-fail")
    {
        assert!(Instant::now() < deadline, "{}", guest.log());
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    invoke(&mut control, 3, "/quiescence-resolve");
    reply(receive(&mut control), 3, b"peer");
    drained(receive(&mut control), 3);
    let failed = receive(&mut control);
    assert!(
        matches!(failed, GuestControl::Failed { invocation, message, .. } if invocation == format!("{:064x}", 3) && message.contains("quiescence handler failure"))
    );
    assert!(guest
        .log()
        .contains("CELLD_JS_QUIESCENCE_NESTED /quiescence-fail"));
    invoke(&mut control, 4, "/quiescence-inspect");
    let GuestControl::HttpReply(result) = receive(&mut control) else {
        panic!("inspection missing")
    };
    assert_eq!(result.invocation, format!("{:064x}", 5));
    let result: serde_json::Value = serde_json::from_slice(&result.response.body).unwrap();
    assert_eq!(
        result,
        serde_json::json!({"prefix":"/quiescence-fail", "background":"/quiescence-fail", "nested":"/quiescence-fail"})
    );
    drained(receive(&mut control), 4);
    acceptor.join().unwrap();
    let (values, borrowed, _) = host.join().unwrap();
    assert!(borrowed > 0, "fixture did not run a foreign continuation");
    for key in [
        "quiescence-prefix",
        "quiescence-background",
        "quiescence-nested",
    ] {
        assert!(values.iter().any(|(stored, _)| stored == key));
    }
    wire::write_frame(
        &mut control,
        &HostControl::Shutdown {
            version: wire::VERSION,
        }
        .encode()
        .unwrap(),
    )
    .unwrap();
    assert!(wire::read_frame(&mut control).unwrap().is_none());
    let deadline = Instant::now() + Duration::from_secs(5);
    while !guest.log().contains(guest.marker) {
        guest.assert_running();
        assert!(Instant::now() < deadline, "{}", guest.log());
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    guest.finish();
    eprintln!("original V8 real Firecracker quiescence: overridden allSettled did not retire early-reply or failed events; peer resolved raw background promise in a foreign checkpoint; nested waitUntil persisted before matched Drained/Failed; all native grants/cursors/turns settled and VMM reaped; fixture, not production Actor/independent-host qualification");
}

/// T03: interleaved events on one object in a real VM. A's block runs in
/// B's checkpoint and is held by B; C queues behind it. E's continuations
/// run inside D's transaction and must be refused without parking.
async fn check_javascript_vm_callbacks() {
    use celld_applications::invocation_wire::{
        GuestControl, HostControl, HttpRequest, InvokeHttp, ObjectTarget, StorageConnect,
    };
    const EVENTS: usize = 6;
    let directory = tempfile::tempdir().unwrap();
    let (mut guest, mut control) = connected_mode_guest(directory.path(), "javascript").await;
    let root = directory
        .path()
        .join("jails/firecracker/storage-check/root");
    let listener = std::os::unix::net::UnixListener::bind(root.join("vsock_5001")).unwrap();
    std::os::unix::fs::chown(root.join("vsock_5001"), Some(1000), Some(1000)).unwrap();
    std::fs::set_permissions(
        root.join("vsock_5001"),
        std::os::unix::fs::PermissionsExt::from_mode(0o600),
    )
    .unwrap();
    listener.set_nonblocking(true).unwrap();
    assert!(matches!(
        GuestControl::decode(&wire::read_frame(&mut control).unwrap().unwrap()).unwrap(),
        GuestControl::Ready {
            adapter: celld_applications::Language::Javascript,
            callback_turns: true,
            ..
        }
    ));
    let cell = address("javascript-vm")
        .cell_scope("CallbackCounter", "named-object")
        .unwrap();
    let host_cell = cell.clone();
    let (send, grants) = std::sync::mpsc::channel();
    let (streams, incoming) = std::sync::mpsc::channel();
    let host = std::thread::spawn(move || {
        shared_v8_host_incoming(host_cell, EVENTS, incoming, send, None, None)
    });
    let grants = grants.recv_timeout(Duration::from_secs(5)).unwrap();
    let connect_grants = grants.clone();
    let acceptor = std::thread::spawn(move || {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..EVENTS {
            let deadline = Instant::now() + Duration::from_secs(20);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "callback guest did not connect storage"
                        );
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("callback storage accept: {error}"),
                }
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let connect =
                StorageConnect::decode(&wire::read_frame(&mut stream).unwrap().unwrap()).unwrap();
            let index = connect_grants
                .iter()
                .position(|grant| grant == &connect.grant)
                .unwrap();
            assert!(seen.insert(index) && !connect.cleanup);
            assert_eq!(connect.invocation, format!("{:064x}", index + 1));
            streams.send((index, stream)).unwrap();
        }
    });
    let invoke = |control: &mut std::os::unix::net::UnixStream, index: usize, path: &str| {
        wire::write_frame(
            control,
            &HostControl::InvokeHttp(Box::new(InvokeHttp {
                request_stream: false,
                queue: false,
                alarm: None,
                service: None,
                rpc: None,
                version: wire::VERSION,
                invocation: format!("{:064x}", index + 1),
                storage_grant: grants[index].clone(),
                storage_residency: Some("d".repeat(64)),
                object: Some(ObjectTarget {
                    class: "CallbackCounter".into(),
                    id: cell.split_once(':').unwrap().1.into(),
                    name: Some("named-object".into()),
                }),
                request: HttpRequest {
                    method: "GET".into(),
                    url: format!("http://check{path}"),
                    headers: vec![],
                    body: vec![],
                },
                socket: None,
            }))
            .encode()
            .unwrap(),
        )
        .unwrap();
    };
    let logged = |guest: &mut Guest, marker: &str| {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !guest.log().contains(marker) {
            guest.assert_running();
            assert!(Instant::now() < deadline, "{marker}: {}", guest.log());
            std::thread::sleep(Duration::from_millis(5));
        }
    };
    // A parked guest stops answering; the control read deadline fails it.
    let collect = |control: &mut std::os::unix::net::UnixStream,
                   guest: &Guest,
                   indexes: &[usize]| {
        let mut replies = std::collections::HashMap::new();
        let mut drained = std::collections::HashSet::new();
        while drained.len() < indexes.len() {
            let message = GuestControl::decode(
                &wire::read_frame(control)
                    .unwrap_or_else(|error| panic!("callback control: {error}; {}", guest.log()))
                    .unwrap_or_else(|| panic!("callback guest ended: {}", guest.log())),
            )
            .unwrap();
            let index = |invocation: &str| {
                let index = usize::from_str_radix(invocation, 16).unwrap() - 1;
                assert!(indexes.contains(&index), "unexpected event {index}");
                index
            };
            match message {
                GuestControl::HttpReply(result) => {
                    let index = index(&result.invocation);
                    assert!(
                        !drained.contains(&index),
                        "event {index} drained before reply"
                    );
                    assert!(replies.insert(index, result.response.body).is_none());
                }
                GuestControl::Drained { invocation, .. } => {
                    let index = index(&invocation);
                    assert!(replies.contains_key(&index) && drained.insert(index));
                }
                message => panic!("unexpected callback terminal: {message:?}"),
            }
        }
        replies
    };
    invoke(&mut control, 0, "/callback-wait");
    logged(&mut guest, "CELLD_JS_CALLBACK_WAITING");
    invoke(&mut control, 1, "/callback-drive");
    logged(&mut guest, "CELLD_JS_CALLBACK_CRITICAL_ENTERED");
    assert!(!guest.log().contains("CELLD_JS_CALLBACK_CRITICAL_DONE"));
    invoke(&mut control, 2, "/callback-ping");
    let replies = collect(&mut control, &guest, &[0, 1, 2]);
    assert_eq!(replies[&0], b"A:A");
    assert_eq!(replies[&1], b"B");
    assert_eq!(
        replies[&2], b"A",
        "C entered before A's foreign-driven block released its gate"
    );
    invoke(&mut control, 3, "/callback-tx-wait");
    logged(&mut guest, "CELLD_JS_CALLBACK_TX_WAITING");
    invoke(&mut control, 4, "/callback-tx-hold");
    let replies = collect(&mut control, &guest, &[3, 4]);
    assert_eq!(replies[&4], b"rolled-back:callback rollback");
    let isolation: serde_json::Value = serde_json::from_slice(&replies[&3]).unwrap();
    for key in ["foreign", "late"] {
        assert!(
            isolation[key]
                .as_str()
                .unwrap()
                .contains("transaction or critical section"),
            "{key} continuation entered another event's transaction: {isolation}"
        );
    }
    assert_eq!(isolation["own"], "undefined");
    invoke(&mut control, 5, "/callback-inspect");
    let replies = collect(&mut control, &guest, &[5]);
    let inspected: serde_json::Value = serde_json::from_slice(&replies[&5]).unwrap();
    assert_eq!(
        inspected,
        serde_json::json!({"critical":"A","transaction":"A","rows":1,"driver":1,"foreign":"own-turn","uncommitted":"undefined"})
    );
    acceptor.join().unwrap();
    let (values, borrowed, queued) = host.join().unwrap();
    assert!(borrowed > 0, "fixture did not run a foreign continuation");
    assert!(queued > 0, "native gate did not queue an interleaved event");
    for key in [
        "callback-critical",
        "callback-transaction",
        "callback-driver",
        "callback-tx-foreign",
    ] {
        assert!(values.iter().any(|(stored, _)| stored == key));
    }
    assert!(!values
        .iter()
        .any(|(stored, _)| stored == "callback-tx-uncommitted"));
    wire::write_frame(
        &mut control,
        &HostControl::Shutdown {
            version: wire::VERSION,
        }
        .encode()
        .unwrap(),
    )
    .unwrap();
    assert!(wire::read_frame(&mut control).unwrap().is_none());
    let deadline = Instant::now() + Duration::from_secs(5);
    while !guest.log().contains(guest.marker) {
        guest.assert_running();
        assert!(Instant::now() < deadline, "{}", guest.log());
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    guest.finish();
    eprintln!("original V8 real Firecracker callbacks: A's transactionSync and its block with a multi-turn async transaction, started in B's checkpoint, were held by B's native turn while A's grant wrote through B's delegated turn; C queued behind it and read A's committed write; E's foreign continuations inside D's transaction were refused without parking the isolate, E resumed in its own turn after rollback and its write survived; all native grants/cursors/turns settled and VMM reaped; fixture, not production Actor/independent-host qualification");
}

async fn check_javascript_vm_retirement_case(mode: &str) {
    use celld_applications::invocation_wire::{
        GuestControl, HostControl, HttpRequest, InvokeHttp, ObjectTarget, StorageConnect,
    };
    let directory = tempfile::tempdir().unwrap();
    let (mut guest, mut control) = connected_mode_guest(directory.path(), "javascript").await;
    let root = directory
        .path()
        .join("jails/firecracker/storage-check/root");
    let socket = root.join("vsock_5001");
    let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    std::os::unix::fs::chown(&socket, Some(1000), Some(1000)).unwrap();
    std::fs::set_permissions(&socket, std::os::unix::fs::PermissionsExt::from_mode(0o600)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let ready = wire::read_frame(&mut control).unwrap().unwrap();
    assert!(matches!(
        GuestControl::decode(&ready).unwrap(),
        GuestControl::Ready {
            adapter: celld_applications::Language::Javascript,
            execution_turns: true,
            cleanup_turns: true,
            callback_turns: true,
            response_turns: true,
            quiescent_terminals: true,
            reply_drain: true,
            resident_objects: true,
            object_retirement: true,
            response_streams: true,
            ..
        }
    ));
    let resource = address("javascript-vm");
    let cell = resource.cell_scope("Counter", "named-object").unwrap();
    let host_cell = cell.clone();
    let (send, grants) = std::sync::mpsc::channel();
    let (streams, incoming) = std::sync::mpsc::channel();
    let (observed, slow_entered) = std::sync::mpsc::channel();
    let (retire_cursors, cursor_retirement) = std::sync::mpsc::channel();
    let host = std::thread::spawn(move || {
        shared_v8_host_incoming(
            host_cell,
            8,
            incoming,
            send,
            Some(observed),
            Some(cursor_retirement),
        )
    });
    let grants = grants.recv_timeout(Duration::from_secs(5)).unwrap();
    let connect_grants = grants.clone();
    let unsuccessful = mode != "async";
    let acceptor = std::thread::spawn(move || {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..if unsuccessful {
            7
        } else {
            connect_grants.len()
        } {
            let deadline = Instant::now() + Duration::from_secs(20);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "JS guest did not connect storage"
                        );
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("JS storage accept: {error}"),
                }
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let connect =
                StorageConnect::decode(&wire::read_frame(&mut stream).unwrap().unwrap()).unwrap();
            let index = connect_grants
                .iter()
                .position(|grant| grant == &connect.grant)
                .unwrap();
            assert!(seen.insert(index));
            assert_eq!(connect.invocation, format!("{:064x}", index + 1));
            assert!(!connect.cleanup);
            streams.send((index, stream)).unwrap();
        }
        streams
    });
    let message = |index: usize, path: &str| {
        HostControl::InvokeHttp(Box::new(InvokeHttp {
            request_stream: false,
            queue: false,
            alarm: None,
            service: None,
            rpc: None,
            version: wire::VERSION,
            invocation: format!("{:064x}", index + 1),
            storage_grant: grants[index].clone(),
            storage_residency: Some(if index == 7 { "d" } else { "c" }.repeat(64)),
            object: Some(ObjectTarget {
                class: "Counter".into(),
                id: cell.split_once(':').unwrap().1.into(),
                name: Some("named-object".into()),
            }),
            request: HttpRequest {
                method: "GET".into(),
                url: format!("http://check{path}"),
                headers: vec![],
                body: vec![],
            },
            socket: None,
        }))
        .encode()
        .unwrap()
    };
    let receive = |control: &mut std::os::unix::net::UnixStream, guest: &Guest| {
        let frame = wire::read_frame(control)
            .unwrap_or_else(|error| panic!("JS control: {error}; {}", guest.log()))
            .unwrap_or_else(|| panic!("JS guest ended early: {}", guest.log()));
        let result = GuestControl::decode(&frame).unwrap();
        assert!(
            !matches!(result, GuestControl::Failed { .. }),
            "JS guest failed: {result:?}; {}",
            guest.log()
        );
        result
    };
    let counter = |result: GuestControl, index: usize, count: usize| {
        let GuestControl::HttpReply(result) = result else {
            panic!("expected JS counter reply: {result:?}")
        };
        assert_eq!(result.invocation, format!("{:064x}", index + 1));
        assert_eq!(result.response.status, 200);
        let body: serde_json::Value = serde_json::from_slice(&result.response.body).unwrap();
        assert_eq!(
            body,
            serde_json::json!({"count":count,"row":count*11,"constructed":1,"name":"named-object","language":"original-v8"})
        );
    };
    let drained = |result: GuestControl, index: usize| {
        assert_eq!(
            result,
            GuestControl::Drained {
                version: wire::VERSION,
                invocation: format!("{:064x}", index + 1)
            }
        );
    };
    wire::write_frame(&mut control, &message(0, "/increment")).unwrap();
    counter(receive(&mut control, &guest), 0, 1);
    drained(receive(&mut control, &guest), 0);

    // Response is available while this event's original critical callback
    // still holds the native gate. B must queue, without parking the V8
    // executor that drives A's timer and releases it.
    wire::write_frame(&mut control, &message(1, "/hold")).unwrap();
    let GuestControl::HttpReply(hold) = receive(&mut control, &guest) else {
        panic!("hold did not reply")
    };
    assert_eq!(hold.invocation, format!("{:064x}", 2));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&hold.response.body).unwrap(),
        serde_json::json!({"phase":"holding"})
    );
    wire::write_frame(&mut control, &message(2, "/increment")).unwrap();
    // The hold's Drained needs one more guest turn after the gate release;
    // that races the queued increment, so its reply may precede it.
    let frames: Vec<_> = (0..3).map(|_| receive(&mut control, &guest)).collect();
    let at = |index: usize| {
        let want = GuestControl::Drained {
            version: wire::VERSION,
            invocation: format!("{:064x}", index + 1),
        };
        frames
            .iter()
            .position(|frame| *frame == want)
            .unwrap_or_else(|| panic!("missing {want:?} in {frames:?}"))
    };
    at(1);
    let own_drained = at(2);
    let replied = frames
        .iter()
        .position(|frame| matches!(frame, GuestControl::HttpReply(_)))
        .unwrap_or_else(|| panic!("increment did not reply: {frames:?}"));
    assert!(replied < own_drained);
    counter(frames.into_iter().nth(replied).unwrap(), 2, 2);

    // Native storage witnesses that the slow foreground handler entered.
    // Its ordinary suspension yields admission to the next invocation,
    // whose reply must arrive first in this same Worker/object instance.
    wire::write_frame(&mut control, &message(3, "/slow")).unwrap();
    slow_entered.recv_timeout(Duration::from_secs(5)).unwrap();
    wire::write_frame(&mut control, &message(4, "/increment")).unwrap();
    counter(receive(&mut control, &guest), 4, 3);
    drained(receive(&mut control, &guest), 4);
    let GuestControl::HttpReply(slow) = receive(&mut control, &guest) else {
        panic!("slow did not reply")
    };
    assert_eq!(slow.invocation, format!("{:064x}", 4));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&slow.response.body).unwrap(),
        serde_json::json!({"phase":"slow"})
    );
    drained(receive(&mut control, &guest), 3);

    // A saved native read outlives its event. The guest releases only the
    // original object registries, then the trusted host closes that
    // residency's cursor table after the matched retirement terminal.
    wire::write_frame(
        &mut control,
        &message(
            5,
            match mode {
                "async" => "/retain",
                "never" => "/retain-never",
                "throw" => "/retain-throw",
                _ => unreachable!(),
            },
        ),
    )
    .unwrap();
    let GuestControl::HttpReply(retained) = receive(&mut control, &guest) else {
        panic!("retain did not reply")
    };
    assert_eq!(retained.invocation, format!("{:064x}", 6));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&retained.response.body).unwrap(),
        serde_json::json!({"phase":"retained","row":11})
    );
    drained(receive(&mut control, &guest), 5);
    let retirement = HostControl::RetireObject {
        version: wire::VERSION,
        invocation: format!("{:064x}", 7),
        storage_grant: grants[6].clone(),
        storage_residency: "c".repeat(64),
        object: ObjectTarget {
            class: "Counter".into(),
            id: cell.split_once(':').unwrap().1.into(),
            name: Some("named-object".into()),
        },
    };
    let retirement_started = Instant::now();
    wire::write_frame(&mut control, &retirement.encode().unwrap()).unwrap();
    if unsuccessful {
        assert!(
            wire::read_frame(&mut control).unwrap().is_none(),
            "unsuccessful release emitted a terminal: {}",
            guest.log()
        );
        if mode == "never" {
            assert!(retirement_started.elapsed() >= Duration::from_secs(4));
            assert!(
                guest.log().contains("CELLD_JS_RELEASE_PENDING_WITHOUT_IO"),
                "{}",
                guest.log()
            );
        }
        // PID 1 exit can truncate serial stderr; prove refusal and native reaping below.
        assert!(!guest.log().contains(guest.marker));
        let (snapshot, result) = std::sync::mpsc::channel();
        retire_cursors
            .send(RetirementWitness::Snapshot(snapshot))
            .unwrap();
        assert_eq!(
            result.recv_timeout(Duration::from_secs(5)).unwrap(),
            1,
            "native saved cursor was closed before trusted VM reaping"
        );
        if guest.child.try_wait().unwrap().is_none() {
            guest.child.kill().unwrap();
        }
        guest.child.wait().unwrap();
        assert!(guest.child.try_wait().unwrap().is_some());
        let (close, result) = std::sync::mpsc::channel();
        retire_cursors
            .send(RetirementWitness::Close(close))
            .unwrap();
        assert_eq!(result.recv_timeout(Duration::from_secs(5)).unwrap(), (1, 0));
        // Feed a fresh native successor only after physical reaping and
        // trusted residency closure. This is not another guest invocation.
        let streams = acceptor.join().unwrap();
        let (mut fresh, host_stream) = std::os::unix::net::UnixStream::pair().unwrap();
        fresh
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        streams.send((7, host_stream)).unwrap();
        drop(streams);
        let call = |stream: &mut std::os::unix::net::UnixStream, operation| {
            wire::write_frame(
                stream,
                &Request {
                    version: wire::VERSION,
                    grant: grants[7].clone(),
                    operation,
                }
                .encode()
                .unwrap(),
            )
            .unwrap();
            success(Reply::decode(&wire::read_frame(stream).unwrap().unwrap()).unwrap())
        };
        let Value::Turn { turn } = call(&mut fresh, Operation::TurnStart {}) else {
            panic!("fresh turn missing")
        };
        assert_eq!(
            call(&mut fresh, Operation::TurnFinish { turn }),
            Value::Unit {}
        );
        drop(fresh);
        let (values, _, queued_turns) = host.join().unwrap();
        assert!(queued_turns > 0);
        assert!(values.iter().any(|(key, _)| key == "count"));
        assert!(!values.iter().any(|(key, _)| key == "forbidden"));
        eprintln!("original V8 real Firecracker unsuccessful retirement ({mode}): no ObjectRetired; raw pending release without I/O waited for lifecycle deadline, or rejection failed control; native saved cursor remained 1 before physical VMM kill/wait, trusted closure changed 1->0 only after reaping, fresh native successor admitted; prefix retained; fixture, no production Actor/output proof or independent-host qualification");
        return;
    }
    assert_eq!(
        receive(&mut control, &guest),
        GuestControl::ObjectRetired {
            version: wire::VERSION,
            invocation: format!("{:064x}", 7),
        }
    );
    let (closed, result) = std::sync::mpsc::channel();
    assert!(
        guest.log().contains("CELLD_JS_RELEASE_DONE true"),
        "{}",
        guest.log()
    );
    assert!(
        guest.log().contains("CELLD_JS_RELEASE_LATE_DONE"),
        "{}",
        guest.log()
    );
    retire_cursors
        .send(RetirementWitness::Close(closed))
        .unwrap();
    assert_eq!(result.recv_timeout(Duration::from_secs(5)).unwrap(), (1, 0));
    wire::write_frame(&mut control, &message(7, "/inspect")).unwrap();
    let GuestControl::HttpReply(inspected) = receive(&mut control, &guest) else {
        panic!("fresh object did not reply")
    };
    assert_eq!(inspected.invocation, format!("{:064x}", 8));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&inspected.response.body).unwrap(),
        serde_json::json!({"phase":"inspected","count":0,"constructed":2,"persisted":"3",
                "rows":[11,22,33],"name":"named-object",
                "releaseDone":true,"releaseLateDone":true,"releaseDenied":true})
    );
    drained(receive(&mut control, &guest), 7);
    acceptor.join().unwrap();
    let (values, _, queued_turns) = host.join().unwrap();
    assert!(
        queued_turns > 0,
        "fixture never witnessed native gate queuing"
    );
    for key in [
        "count",
        "background",
        "hold-entered",
        "hold-released",
        "slow-entered",
        "response-background",
    ] {
        assert!(matches!(
            values.iter().find(|(stored, _)| stored == key),
            Some((_, storage::StoredValue::V8(_)))
        ));
    }
    wire::write_frame(
        &mut control,
        &HostControl::Shutdown {
            version: wire::VERSION,
        }
        .encode()
        .unwrap(),
    )
    .unwrap();
    assert!(wire::read_frame(&mut control).unwrap().is_none());
    let deadline = Instant::now() + Duration::from_secs(5);
    while !guest.log().contains(guest.marker) {
        guest.assert_running();
        assert!(
            Instant::now() < deadline,
            "JS VM pass marker missing: {}",
            guest.log()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    guest.finish();
    for count in 1..=3 {
        assert!(guest
            .log()
            .contains(&format!("CELLD_JS_RESPONSE_BACKGROUND {count}")));
    }
    eprintln!("original V8 real Firecracker response getters: original response headers getter executed native SQL under owner turn; asynchronous getter waitUntil writes completed before each Drained, while reply remained separately available; all three counter events retained; no guest proof or production Actor/output/stream qualification");
    eprintln!("original V8 real Firecracker: seven HTTP events and idle object retirement over genuine vsock; retained native critical queuing, reply/drain and foreground overtaking checks; original object release reset memory/name registries, constructor reran with persisted BigInt/SQL/name in the same VM under a new residency; saved read closed 1->0 by host after matched ObjectRetired; per-event grants retired and no native cursor/transaction/turn leak; incomplete lifecycle remains refused by production; trusted native fixture, no Actor/output proof or independent-host qualification");
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires explicit JavaScript image, Linux KVM and jailer"]
async fn javascript_vm_cancellation() {
    crate::asyncrt::bind_test_runtime();
    assert!(std::env::var_os("CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE").is_some());
    check_javascript_vm_cancellation().await;
}

pub(crate) async fn check_javascript_vm_cancellation() {
    if std::env::var_os("CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE").is_none() {
        eprintln!("original V8 cancellation VM check skipped: no JavaScript image; cancellation bridge checks remain local");
        return;
    }
    for mode in ["ready", "queued", "async", "background", "never"] {
        check_javascript_vm_cancellation_case(mode).await;
    }
    check_javascript_vm_host_cancellation().await;
}

pub(crate) async fn check_javascript_vm_stream() {
    use celld_applications::invocation_wire::{
        GuestControl, HostControl, HttpRequest, InvokeHttp, ObjectTarget, ResponseFlow,
        StorageConnect, MAX_BODY_BYTES, MAX_CHUNK_BYTES,
    };
    if std::env::var_os("CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE").is_none() {
        eprintln!("original V8 stream VM check skipped: no JavaScript image; host streaming proof remains required");
        return;
    }
    for mode in ["large", "error"] {
        let directory = tempfile::tempdir().unwrap();
        let (mut guest, mut control) = connected_mode_guest(directory.path(), "javascript").await;
        let socket = directory
            .path()
            .join("jails/firecracker/storage-check/root/vsock_5001");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        std::os::unix::fs::chown(&socket, Some(1000), Some(1000)).unwrap();
        std::fs::set_permissions(&socket, std::os::unix::fs::PermissionsExt::from_mode(0o600))
            .unwrap();
        listener.set_nonblocking(true).unwrap();
        assert!(matches!(
            GuestControl::decode(&wire::read_frame(&mut control).unwrap().unwrap()).unwrap(),
            GuestControl::Ready {
                response_streams: true,
                quiescent_terminals: true,
                ..
            }
        ));
        let resource = address("javascript-vm");
        let cell = resource
            .cell_scope("StreamCounter", "named-object")
            .unwrap();
        let (streams, incoming) = std::sync::mpsc::channel();
        let (ready, grants) = std::sync::mpsc::channel();
        let host_cell = cell.clone();
        let host = std::thread::spawn(move || {
            shared_v8_host_incoming(host_cell, 3, incoming, ready, None, None)
        });
        let grants = grants.recv_timeout(Duration::from_secs(5)).unwrap();
        let accepted_grants = grants.clone();
        let acceptor = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(35);
            let mut seen = std::collections::HashSet::new();
            while seen.len() < 3 {
                let (mut stream, _) = match listener.accept() {
                    Ok(accepted) => accepted,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline);
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(error) => panic!("stream storage accept: {error}"),
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let hello =
                    StorageConnect::decode(&wire::read_frame(&mut stream).unwrap().unwrap())
                        .unwrap();
                let index = accepted_grants
                    .iter()
                    .position(|grant| grant == &hello.grant)
                    .unwrap();
                assert!(seen.insert(index) && !hello.cleanup);
                assert_eq!(hello.invocation, format!("{:064x}", index + 1));
                streams.send((index, stream)).unwrap();
            }
        });
        let invoke = |index: usize, path: &str| {
            HostControl::InvokeHttp(Box::new(InvokeHttp {
                request_stream: false,
                queue: false,
                alarm: None,
                service: None,
                rpc: None,
                version: wire::VERSION,
                invocation: format!("{:064x}", index + 1),
                storage_grant: grants[index].clone(),
                storage_residency: Some("e".repeat(64)),
                object: Some(ObjectTarget {
                    class: "StreamCounter".into(),
                    id: cell.split_once(':').unwrap().1.into(),
                    name: Some("named-object".into()),
                }),
                request: HttpRequest {
                    method: "GET".into(),
                    url: format!("http://check{path}"),
                    headers: vec![],
                    body: vec![],
                },
                socket: None,
            }))
            .encode()
            .unwrap()
        };
        let receive = |control: &mut std::os::unix::net::UnixStream, guest: &Guest| {
            let bytes = wire::read_frame(control)
                .unwrap_or_else(|error| panic!("stream control: {error}; {}", guest.log()))
                .unwrap_or_else(|| panic!("stream guest ended: {}", guest.log()));
            GuestControl::decode(&bytes).unwrap()
        };
        wire::write_frame(&mut control, &invoke(0, &format!("/{mode}"))).unwrap();
        let GuestControl::HttpStreamHead(head) = receive(&mut control, &guest) else {
            panic!("missing stream head")
        };
        assert_eq!(head.invocation, format!("{:064x}", 1));
        assert_eq!(head.status, 201);
        assert_eq!(
            head.headers,
            vec![
                ("set-cookie".into(), "one".into()),
                ("set-cookie".into(), "two".into())
            ]
        );
        // No credit means no chunk or early Drained, while another event
        // can enter this same original object/VM/native SQLite connection.
        wire::write_frame(&mut control, &invoke(1, "/ping")).unwrap();
        let GuestControl::HttpReply(peer) = receive(&mut control, &guest) else {
            panic!("peer did not reply while stream paused")
        };
        assert_eq!(peer.invocation, format!("{:064x}", 2));
        assert_eq!(peer.response.body, b"peer");
        assert_eq!(
            receive(&mut control, &guest),
            GuestControl::Drained {
                version: wire::VERSION,
                invocation: format!("{:064x}", 2)
            }
        );
        control
            .set_read_timeout(Some(Duration::from_millis(150)))
            .unwrap();
        assert!(matches!(
            wire::read_frame(&mut control)
                .unwrap_err()
                .downcast_ref::<std::io::Error>()
                .map(|error| error.kind()),
            Some(std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)
        ));
        control
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut flow = ResponseFlow::default();
        let mut bytes = 0;
        loop {
            let sequence = flow.next_sequence();
            flow.grant(sequence).unwrap();
            wire::write_frame(
                &mut control,
                &HostControl::ResponsePull {
                    version: wire::VERSION,
                    invocation: head.invocation.clone(),
                    sequence,
                }
                .encode()
                .unwrap(),
            )
            .unwrap();
            match receive(&mut control, &guest) {
                GuestControl::ResponseChunk {
                    invocation,
                    sequence: received,
                    data,
                    ..
                } => {
                    assert_eq!(invocation, head.invocation);
                    flow.chunk(received).unwrap();
                    assert_eq!(data.len(), MAX_CHUNK_BYTES);
                    assert!(data
                        .iter()
                        .all(|byte| *byte == ((sequence + 1) & 255) as u8));
                    bytes += data.len();
                }
                GuestControl::ResponseEnd {
                    invocation,
                    sequence: received,
                    error,
                    ..
                } => {
                    assert_eq!(invocation, head.invocation);
                    flow.end(received).unwrap();
                    assert_eq!(
                        error,
                        (mode == "error").then(|| "JavaScript response producer failed".into())
                    );
                    break;
                }
                event => panic!("unexpected stream event: {event:?}"),
            }
        }
        let count = if mode == "large" { 257 } else { 3 };
        assert_eq!(flow.next_sequence(), count);
        assert_eq!(bytes, count as usize * MAX_CHUNK_BYTES);
        if mode == "large" {
            assert!(bytes > MAX_BODY_BYTES);
            assert!(bytes > wire::MAX_MESSAGE_BYTES);
        }
        assert_eq!(
            receive(&mut control, &guest),
            GuestControl::Drained {
                version: wire::VERSION,
                invocation: head.invocation.clone()
            }
        );
        wire::write_frame(&mut control, &invoke(2, "/inspect")).unwrap();
        let GuestControl::HttpReply(inspect) = receive(&mut control, &guest) else {
            panic!("inspect did not reply")
        };
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&inspect.response.body).unwrap(),
            serde_json::json!({"produced":count,"background":true,"rows":count})
        );
        assert_eq!(
            receive(&mut control, &guest),
            GuestControl::Drained {
                version: wire::VERSION,
                invocation: format!("{:064x}", 3)
            }
        );
        acceptor.join().unwrap();
        let (values, _, _) = host.join().unwrap();
        for key in ["stream-produced", "stream-peer", "stream-background"] {
            assert!(values.iter().any(|(stored, _)| stored == key));
        }
        wire::write_frame(
            &mut control,
            &HostControl::Shutdown {
                version: wire::VERSION,
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
        assert!(wire::read_frame(&mut control).unwrap().is_none());
        guest.finish();
        eprintln!("original V8 real Firecracker response stream ({mode}): {bytes} bytes in {count} bounded credited chunks; duplicate Set-Cookie retained; no chunk or drain without credit, peer completed while consumer paused; original producer callbacks wrote matching native SQL/clone state, generic error withheld private diagnostic, body end preceded joined drain and native state inspection; VMM reaped; native fixture, not host Actor/per-chunk proof/request-stream/independent-host qualification");
    }
}

/// T04: the original host Actor, runtime and production guest transport
/// against a real JavaScript VM.
async fn check_javascript_vm_host_cancellation() {
    use crate::application_guest_transport::Dispatcher;
    use celld_applications::invocation_wire::HttpRequest;
    let resource = address("javascript-vm");
    let cell = resource.cell_scope("Counter", "named-object").unwrap();
    let state = tempfile::tempdir().unwrap();
    let (app, runtime, running) = actor_bridge(state.path(), &resource).await;
    async fn boot(directory: &Path, resource: &ResourceAddress) -> (Guest, Dispatcher) {
        let (guest, control) = connected_mode_guest(directory, "javascript").await;
        let socket = directory.join("jails/firecracker/storage-check/root/vsock_5001");
        let storage = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        std::os::unix::fs::chown(&socket, Some(1000), Some(1000)).unwrap();
        std::fs::set_permissions(&socket, std::os::unix::fs::PermissionsExt::from_mode(0o600))
            .unwrap();
        storage.set_nonblocking(true).unwrap();
        control.set_nonblocking(true).unwrap();
        let dispatcher = Dispatcher::connect(
            resource.application.clone(),
            celld_applications::Language::Javascript,
            tokio::net::UnixStream::from_std(control).unwrap(),
            tokio::net::UnixListener::from_std(storage).unwrap(),
            Some(guest.child.id()),
        )
        .await
        .unwrap();
        (guest, dispatcher)
    }
    let request = |path: &str| HttpRequest {
        method: "GET".into(),
        url: format!("http://check{path}"),
        headers: vec![],
        body: vec![],
    };
    let console = |guest: &Guest| {
        String::from_utf8_lossy(&std::fs::read(&guest.console).unwrap_or_default()).into_owned()
    };
    // The host Actor runs on this runtime: never block it while waiting.
    let logged = |guest: &Guest, marker: &str, count: usize| {
        let (log, marker) = (guest.console.clone(), marker.to_owned());
        async move {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let text =
                    String::from_utf8_lossy(&std::fs::read(&log).unwrap_or_default()).into_owned();
                if text.matches(marker.as_str()).count() >= count {
                    return;
                }
                assert!(Instant::now() < deadline, "{marker}: {text}");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    };
    let idle = |guest: &Guest| {
        let log = guest.console.clone();
        let app = app.clone();
        async move {
            let settled = tokio::time::timeout(Duration::from_secs(10), async {
                while app.public_in_flight() != 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await;
            assert!(
                settled.is_ok(),
                "guest event did not settle: {}",
                String::from_utf8_lossy(&std::fs::read(log).unwrap_or_default())
            );
        }
    };
    // (stored, open native transaction), sampled under the original turn.
    let sample = |key: String| {
        let (app, runtime, cell) = (app.clone(), runtime.clone(), cell.clone());
        async move {
            let epoch = app.resident_epoch(cell.clone()).await.unwrap();
            let admission = runtime.admit_storage_request(&cell, epoch).unwrap();
            runtime
                .admitted_storage_turn(&admission, |_| {
                    (
                        storage::get_stored(&cell, &key).unwrap().is_some(),
                        storage::sync_sample(&cell).unwrap().unwrap().in_transaction,
                    )
                })
                .await
                .unwrap()
        }
    };
    let settled = |mode: &'static str| {
        let sample = &sample;
        async move {
            assert_eq!(
                sample(format!("acknowledged:{mode}")).await,
                (true, false),
                "{mode} lost its acknowledged prefix or kept a transaction"
            );
            assert!(
                !sample("uncommitted".into()).await.0,
                "{mode} kept its uncommitted suffix"
            );
        }
    };
    let directory = tempfile::tempdir().unwrap();
    let (mut guest, dispatcher) = boot(directory.path(), &resource).await;
    let client = dispatcher.client();
    let invoke = |path: &str, deadline: Duration| {
        let (client, app, resource) = (client.clone(), app.clone(), resource.clone());
        let request = request(path);
        async move {
            client
                .invoke(
                    app,
                    resource,
                    "Counter".into(),
                    "named-object".into(),
                    deadline,
                    request,
                )
                .await
        }
    };

    // A fresh event progresses through the original Actor and native gate.
    let fresh = invoke("/increment", Duration::from_secs(5)).await.unwrap();
    let body: serde_json::Value =
        serde_json::from_slice(&fresh.result.as_ref().unwrap().body).unwrap();
    drop(fresh);
    assert_eq!(
        (body["count"].as_u64(), body["constructed"].as_u64()),
        (Some(1), Some(1))
    );
    idle(&guest).await;

    // A failed constructor fails only its own event; the next constructs.
    for expected in ["fixture constructor failure", "constructed"] {
        let completion = client
            .invoke(
                app.clone(),
                resource.clone(),
                "Alternate".into(),
                "named-object".into(),
                Duration::from_secs(5),
                request("/"),
            )
            .await
            .unwrap();
        match &completion.result {
            Ok(response) => assert_eq!(response.body, expected.as_bytes()),
            Err(error) => assert!(format!("{error:#}").contains(expected), "{error:#}"),
        }
        drop(completion);
        idle(&guest).await;
    }

    // Active cancellation and thrown cleanup: dropping the frontend sends
    // Cancel; the guest's joined terminal precedes host rollback/proof.
    let ready = r#"CELLD_JS_CANCEL_READY_CLEANUP {"aborted":true,"runs":1,"denied":true}"#;
    for (index, mode) in ["/cancel-native", "/cancel-throw"].into_iter().enumerate() {
        let mut active = tokio::spawn(invoke(mode, Duration::from_secs(10)));
        crate::asyncrt::select! {
            _ = logged(&guest, &format!("CELLD_JS_CANCEL_WAITING {mode}"), 1) => {}
            ended = &mut active => panic!(
                "{mode} ended before waiting: {:?}; {}",
                ended.unwrap().map(|done| done.result.err().map(|error| format!("{error:#}"))),
                console(&guest)
            ),
        }
        active.abort();
        let _ = active.await;
        idle(&guest).await;
        logged(&guest, ready, index + 1).await;
        settled(mode).await;
    }

    // Deadline: the original invocation budget cancels a waiting handler;
    // nested late cleanup joins before the host settles.
    let late = "CELLD_JS_CANCEL_LATE_DONE 2";
    let expired = invoke("/cancel-async", Duration::from_millis(400))
        .await
        .unwrap();
    // Settlement may replace the deadline with its own refusal of the
    // reset object's output; either way the invocation fails.
    let failure = format!("{:#}", expired.result.as_ref().err().unwrap());
    assert!(
        failure.contains("deadline") || failure.contains("critical section failed"),
        "{failure}"
    );
    drop(expired);
    idle(&guest).await;
    assert_eq!(console(&guest).matches(late).count(), 1, "{}", guest.log());
    settled("/cancel-async").await;

    // Post-reply: the delivered reply does not end cancellation authority
    // over background work; the original request pin cancels it.
    let request_id = crate::js::next_request_id();
    let replied = client
        .invoke_request(
            app.clone(),
            resource.clone(),
            "Counter".into(),
            "named-object".into(),
            Some(request_id),
            Duration::from_secs(10),
            request("/cancel-late"),
            None,
        )
        .await
        .unwrap();
    match &replied.result {
        Ok(response) => assert_eq!(response.body, b"late"),
        Err(error) => panic!("post-reply event failed: {error:#}; {}", console(&guest)),
    }
    logged(&guest, "CELLD_JS_CANCEL_WAITING /cancel-late", 1).await;
    assert!(app.drain_pins.cancel_request(request_id, "check").is_some());
    drop(replied);
    idle(&guest).await;
    logged(&guest, ready, 4).await;
    settled("/cancel-late").await;

    // Queued: an event waiting behind another event's held block is
    // cancelled at host input admission and never reaches the guest.
    // The holder's own reply also waits for its block, so observe the
    // block from inside before queueing the next event.
    let hold = tokio::spawn(invoke("/hold", Duration::from_secs(5)));
    logged(&guest, "CELLD_JS_HOLD_ENTERED", 1).await;
    let queued = tokio::spawn(invoke("/cancel-native", Duration::from_secs(10)));
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!queued.is_finished());
    queued.abort();
    let _ = queued.await;
    let hold = hold.await.unwrap().unwrap();
    assert_eq!(
        hold.result.as_ref().unwrap().body,
        br#"{"phase":"holding"}"#
    );
    drop(hold);
    idle(&guest).await;
    assert_eq!(
        console(&guest)
            .matches("CELLD_JS_CANCEL_WAITING /cancel-native")
            .count(),
        1,
        "queued cancellation entered the guest"
    );
    assert_eq!(sample("hold-released".into()).await, (true, false));

    // Non-cooperation: no terminal within the grace retires the transport;
    // the native transaction stays held until the VM is physically reaped.
    let never = tokio::spawn(invoke("/cancel-never", Duration::from_secs(10)));
    logged(&guest, "CELLD_JS_CANCEL_WAITING /cancel-never", 1).await;
    never.abort();
    let _ = never.await;
    logged(&guest, "CELLD_JS_CANCEL_PENDING_WITHOUT_IO", 1).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(invoke("/inspect", Duration::from_secs(1))
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("guest transport retired"));
    assert_eq!(app.public_in_flight(), 1, "host settled before reaping");
    assert!(
        sample("uncommitted".into()).await.1,
        "native hold released before reap"
    );
    // The supervisor's order: kill/wait, confirm reaped, close VM reads.
    let _ = guest.child.kill();
    guest.child.wait().unwrap();
    client.confirm_fixture_reaped(&mut guest.child).unwrap();
    client.close_read_cursors().await;
    idle(&guest).await;
    settled("/cancel-never").await;
    drop(dispatcher);

    // Object abort: the host latches it, retires the VM and keeps native
    // holds until the VMM is reaped; the acknowledged prefix survives.
    let abort_directory = tempfile::tempdir().unwrap();
    let (mut aborting, dispatcher) = boot(abort_directory.path(), &resource).await;
    let client = dispatcher.client();
    let aborted = {
        let (client, app, resource) = (client.clone(), app.clone(), resource.clone());
        tokio::spawn(async move {
            client
                .invoke(
                    app,
                    resource,
                    "Counter".into(),
                    "named-object".into(),
                    Duration::from_secs(10),
                    request("/abort"),
                )
                .await
        })
    };
    logged(&aborting, "CELLD_JS_ABORTING", 1).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while !dispatcher.retirement().is_cancelled() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("object abort did not retire the transport");
    assert_eq!(
        app.public_in_flight(),
        1,
        "host settled an abort before reaping"
    );
    let _ = aborting.child.kill();
    aborting.child.wait().unwrap();
    client.confirm_fixture_reaped(&mut aborting.child).unwrap();
    client.close_read_cursors().await;
    let aborted = aborted.await.unwrap().unwrap();
    assert!(aborted.result.is_err());
    drop(aborted);
    idle(&aborting).await;
    assert_eq!(sample("abort-prefix".into()).await, (true, false));
    drop(dispatcher);

    // The successor VM admits a fresh event; no stale effect survives.
    let successor_directory = tempfile::tempdir().unwrap();
    let (successor, dispatcher) = boot(successor_directory.path(), &resource).await;
    let client = dispatcher.client();
    let inspected = client
        .invoke(
            app.clone(),
            resource.clone(),
            "Counter".into(),
            "named-object".into(),
            Duration::from_secs(5),
            request("/inspect"),
        )
        .await
        .unwrap();
    let body: serde_json::Value =
        serde_json::from_slice(&inspected.result.as_ref().unwrap().body).unwrap();
    drop(inspected);
    assert_eq!(
        (body["persisted"].as_str(), body["constructed"].as_u64()),
        (Some("1"), Some(1))
    );
    idle(&successor).await;
    assert_eq!(sample("uncommitted".into()).await, (false, false));
    dispatcher.shutdown().await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut successor = successor;
    while !successor.log().contains(successor.marker) {
        successor.assert_running();
        assert!(Instant::now() < deadline, "{}", successor.log());
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    successor.finish();
    client.confirm_fixture_reaped(&mut successor.child).unwrap();
    client.close_read_cursors().await;
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    eprintln!("original V8 real Firecracker host cancellation: original Actor/runtime/transport drove a real JS VM; a failed constructor failed only its event; active and thrown-cleanup frontend drops, invocation deadline with nested late cleanup, post-reply request cancellation and queued input-admission cancellation each kept the acknowledged prefix and rolled back the uncommitted suffix; non-cooperative cleanup retired the transport, kept the native transaction until the VMM was killed/reaped, then settled; object abort was latched by the host, retired the VM and settled only after reap with its prefix kept; successor VM admitted a fresh event with no stale effect");
}

async fn check_javascript_vm_cancellation_case(mode: &str) {
    let pre_entry = mode == "queued";
    let never = mode == "never";
    use celld_applications::invocation_wire::{
        GuestControl, HostControl, HttpRequest, InvokeHttp, ObjectTarget, StorageConnect,
    };
    use std::os::unix::net::{UnixListener, UnixStream};
    let directory = tempfile::tempdir().unwrap();
    let (mut guest, mut control) = connected_mode_guest(directory.path(), "javascript").await;
    control
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let socket = directory
        .path()
        .join("jails/firecracker/storage-check/root/vsock_5001");
    let listener = UnixListener::bind(&socket).unwrap();
    std::os::unix::fs::chown(&socket, Some(1000), Some(1000)).unwrap();
    std::fs::set_permissions(&socket, std::os::unix::fs::PermissionsExt::from_mode(0o600)).unwrap();
    listener.set_nonblocking(true).unwrap();
    assert!(matches!(
        GuestControl::decode(&wire::read_frame(&mut control).unwrap().unwrap()).unwrap(),
        GuestControl::Ready {
            adapter: celld_applications::Language::Javascript,
            quiescent_terminals: true,
            ..
        }
    ));
    let resource = address("javascript-vm");
    let cell = resource.cell_scope("Counter", "named-object").unwrap();
    let (ordinary_proxy, ordinary) = UnixStream::pair().unwrap();
    let (cleanup_proxy, cleanup) = UnixStream::pair().unwrap();
    let (ready, grants) = std::sync::mpsc::channel();
    let (snapshots, requested) = std::sync::mpsc::channel();
    let (entered, armed) = std::sync::mpsc::channel();
    let cancelled = Arc::new(AtomicBool::new(false));
    let joined = Arc::new(AtomicBool::new(false));
    let (cleanup_waiting, pending_cleanup) = std::sync::mpsc::channel();
    let allow_cleanup = Arc::new(AtomicBool::new(false));
    let host_allow_cleanup = allow_cleanup.clone();
    let host_cancelled = cancelled.clone();
    let host_joined = joined.clone();
    let host = std::thread::spawn(move || {
        cancelled_v8_host(
            cell,
            ordinary,
            cleanup,
            ready,
            requested,
            Some(CancellationVmWitness {
                entered,
                cancelled: host_cancelled,
                joined: host_joined,
                pre_entry,
                cleanup_waiting,
                allow_cleanup: host_allow_cleanup,
            }),
        )
    });
    let grant = grants.recv_timeout(Duration::from_secs(5)).unwrap();
    let wire_grant = grant.clone();
    let acceptor = std::thread::spawn(move || {
        let mut ordinary = Some(ordinary_proxy);
        let mut cleanup = Some(cleanup_proxy);
        let mut relays = Vec::new();
        for _ in 0..2 {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "cancellation storage stream missing"
                        );
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("cancellation storage accept: {error}"),
                }
            };
            socket
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let hello =
                StorageConnect::decode(&wire::read_frame(&mut socket).unwrap().unwrap()).unwrap();
            assert_eq!(hello.invocation, "f".repeat(64));
            assert_eq!(hello.grant, wire_grant);
            let mut proxy = if hello.cleanup {
                let mut proxy = cleanup.take().expect("duplicate cleanup stream");
                wire::write_frame(&mut proxy, &hello.encode().unwrap()).unwrap();
                proxy
            } else {
                ordinary.take().expect("duplicate effect stream")
            };
            proxy
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            proxy
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            // The native fixture receives unchanged request/reply frames.
            // Only the original ordinary hello is consumed here; cleanup
            // hello remains validated by the same-event host fixture.
            let mut from_guest = socket.try_clone().unwrap();
            let mut to_native = proxy.try_clone().unwrap();
            relays.push(std::thread::spawn(move || {
                let incoming = std::thread::spawn(move || {
                    std::io::copy(&mut from_guest, &mut to_native).unwrap();
                    to_native.shutdown(std::net::Shutdown::Write).unwrap();
                });
                std::io::copy(&mut proxy, &mut socket).unwrap();
                let _ = socket.shutdown(std::net::Shutdown::Write);
                incoming.join().unwrap();
            }));
        }
        for relay in relays {
            relay.join().unwrap();
        }
    });
    wire::write_frame(
        &mut control,
        &HostControl::InvokeHttp(Box::new(InvokeHttp {
            request_stream: false,
            queue: false,
            alarm: None,
            service: None,
            rpc: None,
            version: wire::VERSION,
            invocation: "f".repeat(64),
            storage_grant: grant,
            storage_residency: Some("c".repeat(64)),
            object: Some(ObjectTarget {
                class: "Counter".into(),
                id: resource
                    .cell_scope("Counter", "named-object")
                    .unwrap()
                    .split_once(':')
                    .unwrap()
                    .1
                    .into(),
                name: Some("named-object".into()),
            }),
            request: HttpRequest {
                method: "GET".into(),
                url: format!(
                    "http://check/{}",
                    match mode {
                        "ready" | "queued" => "cancel-native",
                        "async" => "cancel-async",
                        "background" => "cancel-background",
                        "never" => "cancel-never",
                        _ => unreachable!(),
                    }
                ),
                headers: vec![],
                body: vec![],
            },
            socket: None,
        }))
        .encode()
        .unwrap(),
    )
    .unwrap();
    armed.recv_timeout(Duration::from_secs(5)).unwrap();
    if mode == "background" {
        let GuestControl::HttpReply(response) =
            GuestControl::decode(&wire::read_frame(&mut control).unwrap().unwrap()).unwrap()
        else {
            panic!("background did not publish distinct HTTP reply")
        };
        assert_eq!(response.invocation, "f".repeat(64));
        assert_eq!(response.response.body, b"background");
    }
    cancelled.store(true, Ordering::Release);
    let cancellation_started = Instant::now();
    let cancel = HostControl::Cancel {
        version: wire::VERSION,
        invocation: "f".repeat(64),
    }
    .encode()
    .unwrap();
    wire::write_frame(&mut control, &cancel).unwrap();
    wire::write_frame(&mut control, &cancel).unwrap(); // duplicate is idempotent
    if pre_entry {
        pending_cleanup
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        // The guest joined refused ordinary admission and requested cleanup,
        // but cannot execute V8 or publish a terminal until the host grants it.
        assert!(!joined.load(Ordering::Acquire));
        allow_cleanup.store(true, Ordering::Release);
    }
    let terminal = wire::read_frame(&mut control).unwrap();
    if never {
        assert!(
            terminal.is_none(),
            "non-cooperative cleanup emitted a terminal: {terminal:?}"
        );
        assert!(cancellation_started.elapsed() >= Duration::from_secs(4));
        assert!(
            guest.log().contains("CELLD_JS_CANCEL_PENDING_WITHOUT_IO"),
            "{}",
            guest.log()
        );
        assert!(!guest.log().contains(guest.marker));
    } else {
        assert_eq!(
            GuestControl::decode(
                &terminal.unwrap_or_else(|| panic!("cancellation VM ended: {}", guest.log()))
            )
            .unwrap(),
            GuestControl::Cancelled {
                version: wire::VERSION,
                invocation: "f".repeat(64),
            },
            "{}",
            guest.log()
        );
        if mode == "async" || mode == "background" {
            assert!(
                guest.log().contains("CELLD_JS_CANCEL_LATE_DONE 2"),
                "terminal preceded asynchronous cleanup: {}",
                guest.log()
            );
        }
    }
    let (snapshot, result) = std::sync::mpsc::channel();
    snapshots.send(snapshot).unwrap();
    assert_eq!(
        result.recv_timeout(Duration::from_secs(5)).unwrap(),
        if pre_entry {
            (0, 0, false, false)
        } else {
            (1, 1, true, true)
        }
    );
    if never {
        // EOF is not joined success. Physically reap the directly owned
        // VMM before trusted native rollback can release the retained hold.
        if guest.child.try_wait().unwrap().is_none() {
            guest.child.kill().unwrap();
        }
        guest.child.wait().unwrap();
        assert!(guest.child.try_wait().unwrap().is_some());
    }
    joined.store(true, Ordering::Release);
    acceptor.join().unwrap();
    let values = host.join().unwrap();
    assert_eq!(
        values.iter().any(|(key, _)| key == "acknowledged"),
        !pre_entry
    );
    assert!(!values
        .iter()
        .any(|(key, _)| key == "uncommitted" || key == "forbidden"));
    if never {
        eprintln!("original V8 real Firecracker non-cooperative cancellation: pending finally without native I/O emitted no Cancelled; guest cleanup deadline ended control, native transaction/critical/input hold remained, direct VMM child killed/waited before trusted rollback, old queued event refused and fresh event admitted; prefix retained, uncommitted/forbidden absent");
        return;
    }
    wire::write_frame(&mut control, &cancel).unwrap(); // late cancellation cannot revive it
    wire::write_frame(
        &mut control,
        &HostControl::Shutdown {
            version: wire::VERSION,
        }
        .encode()
        .unwrap(),
    )
    .unwrap();
    assert!(wire::read_frame(&mut control).unwrap().is_none());
    let deadline = Instant::now() + Duration::from_secs(5);
    while !guest.log().contains(guest.marker) {
        guest.assert_running();
        assert!(
            Instant::now() < deadline,
            "cancellation VM pass marker missing: {}",
            guest.log()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        guest.log().contains(
            "CELLD_JS_CANCEL_READY_CLEANUP {\"aborted\":true,\"runs\":1,\"denied\":true}"
        ),
        !pre_entry,
        "{}",
        guest.log()
    );
    guest.finish();
    if pre_entry {
        eprintln!("original V8 real Firecracker queued cancellation: native ordinary admission queued behind another event's original critical hold; host refusal joined; separate same-grant cleanup queued until host gracefully released blocker; correlated Cancelled with zero constructor/handler/storage effects, zero native transaction/critical claims; duplicate/late cancel harmless; streams joined, fresh native event admitted, supervisor reaped");
    } else if mode == "async" || mode == "background" {
        eprintln!("original V8 real Firecracker asynchronous cancellation ({mode}): original signal/finally joined timer and nested late waitUntil callbacks despite overridden allSettled; matched Cancelled followed late callback completion, native claims stayed host-owned until trusted settlement; distinct pre-cancel HTTP reply for background case; duplicate/late cancel harmless and supervisor reaped; fixture, not full callback/facet/Actor/independent-host qualification");
    } else {
        eprintln!("original V8 real Firecracker active cancellation: genuine vsock same-grant cleanup; original request signal/finally ran once with denied effects; duplicate/late cancel harmless; correlated Cancelled arrived while original native transaction/critical/input hold still live; trusted host rollback after matched terminal and both stream EOF refused queued old code and admitted fresh native event; prefix key retained and uncommitted/forbidden absent; supervisor reaped; synchronous cleanup fixture, not full async/callback/Actor/output-proof/independent-host qualification");
    }
}

fn request(session: &StorageSession, operation: Operation) -> Request {
    Request {
        version: wire::VERSION,
        grant: session.grant().into(),
        operation,
    }
}

fn success(reply: Reply) -> Value {
    match reply.outcome {
        Outcome::Ok { value } => value,
        Outcome::Error { message } => panic!("storage operation failed: {message}"),
    }
}

// Policy regression over the actual decision core with controlled proof
// outcomes. This deliberately does not claim fleet/provider qualification.
fn check_output_core(writer: &PreparedOutput, reader: &PreparedOutput) {
    use celld_logic::{
        on_event, CasOutcome, Config, Effect, Event, Failure, OwnerRecord, OwnershipOnEvict,
        ProofSource, Route, State,
    };
    fn resident(scope: &str, epoch: u64) -> State {
        let mut state = State::new(
            "host",
            Config {
                max_resident: 2,
                max_activations: 2,
                max_evictions: 1,
                max_releases: 1,
                max_outbound_websockets: 2,
                ownership_on_evict: OwnershipOnEvict::Sticky,
                require_node_lease: false,
                peer_protocol: crate::peer_auth::PROTOCOL_VERSION,
                operation_deadline_ms: None,
                owner_log_recovery_backoff_ms: 1,
                owner_log_recovery_attempts: 1,
                alarm_resident_ms: 0,
                idle_evict_ms: None,
                pressure: Default::default(),
            },
        );
        let mut effects = std::collections::VecDeque::from(on_event(
            &mut state,
            Event::Request {
                request: 10,
                cell: scope.into(),
            },
        ));
        let mut complete = false;
        while let Some(effect) = effects.pop_front() {
            let event = match effect {
                Effect::ReadOwner { op, .. } => Event::OwnerRead {
                    op,
                    now_ms: 0,
                    result: Ok(Some(OwnerRecord {
                        node: None,
                        epoch: epoch - 1,
                        etag: "prior".into(),
                    })),
                },
                Effect::ReadCapacityPeers { op, .. } => Event::CapacityPeersRead {
                    op,
                    now_ms: 0,
                    result: Ok(vec![]),
                },
                Effect::CasOwner {
                    op,
                    epoch: selected,
                    ..
                } => {
                    assert_eq!(selected, epoch);
                    Event::OwnerCasCompleted {
                        op,
                        result: Ok(CasOutcome::Applied),
                    }
                }
                Effect::Restore { op, .. } => Event::RestoreCompleted {
                    op,
                    result: Ok(celld_logic::RestoreOutcome {
                        restored: false,
                        alarm: None,
                    }),
                },
                Effect::StartRuntime { op, .. } => Event::RuntimeStarted {
                    op,
                    isolate: None,
                    generation: 1,
                    result: Ok(()),
                },
                Effect::Publish { op, .. } => Event::Published { op, result: Ok(()) },
                Effect::Complete {
                    request: 10,
                    result: Ok(Route::Local),
                } => {
                    complete = true;
                    continue;
                }
                other => panic!("unexpected core activation effect: {other:?}"),
            };
            effects.extend(on_event(&mut state, event));
        }
        assert!(complete);
        assert!(on_event(
            &mut state,
            Event::Request {
                request: 11,
                cell: scope.into()
            }
        )
        .iter()
        .any(|effect| matches!(
            effect,
            Effect::Complete {
                request: 11,
                result: Ok(Route::Local)
            }
        )));
        state
    }
    fn output(state: &mut State, request: u64, ticket: crate::actor::GateTicket) -> Vec<Effect> {
        on_event(
            state,
            Event::OutputAt {
                request,
                channel: ticket.channel,
                position: ticket.position,
                observed: ticket.observed,
                epoch: ticket.epoch,
                now_mono_ms: 0,
            },
        )
    }
    let epoch = writer.ticket.epoch.unwrap();
    let position = writer.ticket.position.unwrap();
    assert_eq!(reader.scope, writer.scope);
    assert_eq!(reader.ticket.position, None);
    assert_eq!(reader.ticket.observed, Some(position));
    for scenario in 0..3 {
        let mut state = resident(&writer.scope, epoch);
        let writing = output(&mut state, 10, writer.ticket);
        let op = writing
            .iter()
            .find_map(|effect| match effect {
                Effect::AwaitDurable {
                    op,
                    position: requested,
                    ..
                } => {
                    assert_eq!(*requested, position);
                    Some(*op)
                }
                _ => None,
            })
            .expect("writer did not open a proof barrier");
        assert!(!writing
            .iter()
            .any(|effect| matches!(effect, Effect::Release { .. })));
        let reading = output(&mut state, 11, reader.ticket);
        assert!(
            !reading
                .iter()
                .any(|effect| matches!(effect, Effect::Release { .. })),
            "reader escaped unproved state"
        );
        let mut effects = on_event(
            &mut state,
            Event::DurableReached {
                op,
                result: if scenario == 0 {
                    Err(Failure::Ambiguous)
                } else {
                    Ok(position)
                },
                source: if scenario == 2 {
                    ProofSource::Fleet
                } else {
                    ProofSource::Bucket
                },
            },
        );
        if scenario == 1 {
            assert!(!effects
                .iter()
                .any(|effect| matches!(effect, Effect::Release { result: Ok(()), .. })));
            let verify = effects
                .iter()
                .find_map(|effect| match effect {
                    Effect::VerifyOwnership { op, .. } => Some(*op),
                    _ => None,
                })
                .unwrap();
            effects.extend(on_event(
                &mut state,
                Event::OwnershipVerified {
                    op: verify,
                    result: Err(Failure::Definite),
                },
            ));
        }
        for request in [10, 11] {
            assert!(effects.iter().any(|effect| matches!(effect, Effect::Release { request: released, result, .. } if *released == request && result.is_ok() == (scenario == 2))), "wrong output verdict for {request}: {effects:?}");
        }
    }
    // A node that lost its lease fails every held output, and a proof that
    // arrives late (a delayed follower or bucket reply) releases nothing.
    let mut fenced = resident(&writer.scope, epoch);
    let op = output(&mut fenced, 10, writer.ticket)
        .iter()
        .find_map(|effect| match effect {
            Effect::AwaitDurable { op, .. } => Some(*op),
            _ => None,
        })
        .expect("writer did not open a proof barrier");
    output(&mut fenced, 11, reader.ticket);
    let mut effects = on_event(&mut fenced, Event::NodeFenced);
    effects.extend(on_event(
        &mut fenced,
        Event::DurableReached {
            op,
            result: Ok(position),
            source: ProofSource::Fleet,
        },
    ));
    effects.extend(output(&mut fenced, 12, writer.ticket));
    for request in [10, 11, 12] {
        assert!(effects.iter().any(|effect| matches!(effect, Effect::Release { request: released, result: Err(_), .. } if *released == request)), "fenced node did not fail {request}: {effects:?}");
    }
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::Release { result: Ok(()), .. })),
        "fenced node released unproved output: {effects:?}"
    );
    let mut successor = resident(&writer.scope, epoch + 1);
    let stale = output(&mut successor, 10, writer.ticket);
    assert!(!stale.iter().any(|effect| matches!(
        effect,
        Effect::AwaitDurable { .. } | Effect::Release { result: Ok(()), .. }
    )));
    assert!(stale
        .iter()
        .any(|effect| matches!(effect, Effect::Release { result: Err(_), .. })));
}

fn check_facet_output(cells: &Cells, root: &mut StorageSession, facet: &str) {
    let names = ["child".to_string()];
    {
        let _installed = cells.install();
        let parent = storage::storage_identity(&root.scope).unwrap().unwrap();
        let (path, restored) = crate::js::facet_file_beside_root(&parent, &names);
        storage::open_embedded(facet, &parent, "child", &path, restored, false).unwrap();
    }
    let mut child = StorageSession::new(cells, facet.into(), root.epoch).unwrap();
    let event = child.begin_event(cells).unwrap();
    let put = request(
        &child,
        Operation::PutJson {
            entries: vec![("value".into(), serde_json::json!(1))],
        },
    );
    success(child.execute(cells, true, put));
    let output = child
        .prepare_output(cells, &event, celld_logic::Channel::CellRpc)
        .unwrap();
    // A facet's output belongs to its root, which it trails as a reader.
    // The facet's own write is proved on the facet's stream.
    assert_eq!(output.scope, root.scope);
    assert_eq!(output.ticket.epoch, Some(root.epoch));
    assert_eq!(output.ticket.position, None);
    assert_eq!(
        output.facet,
        Some((
            crate::engine_api::facet_cell(&root.scope, &names),
            root.epoch
        ))
    );
    assert_ne!(output.scope, facet);

    // The facet commits on its own file: a root transaction neither holds
    // its output back nor takes its writes with it when it rolls back.
    let start = request(root, Operation::TransactionStart { origin: None });
    let Value::Transaction { transaction } = success(root.execute(cells, true, start)) else {
        panic!("root transaction did not start")
    };
    let event = child.begin_event(cells).unwrap();
    let put = request(
        &child,
        Operation::PutJson {
            entries: vec![("value".into(), serde_json::json!(2))],
        },
    );
    success(child.execute(cells, true, put));
    let output = child
        .prepare_output(cells, &event, celld_logic::Channel::CellRpc)
        .unwrap();
    assert!(output.facet.is_some());
    let rollback = request(
        root,
        Operation::TransactionFinish {
            transaction,
            action: TransactionAction::Rollback,
        },
    );
    success(root.execute(cells, true, rollback));
    child.revoke(cells).unwrap();
    let mut child = StorageSession::new(cells, facet.into(), root.epoch).unwrap();
    let get = request(
        &child,
        Operation::Get {
            keys: vec!["value".into()],
        },
    );
    assert_eq!(
        success(child.execute(cells, true, get)),
        Value::Entries {
            entries: vec![(
                "value".into(),
                wire::StoredValue::Json { value: "2".into() }
            )]
        }
    );
    child.revoke(cells).unwrap();
    let _installed = cells.install();
    storage::close(facet);
}

/// Facet event sessions live inside their root session: opened and routed
/// by it, gated by its residency, each facet a file and stream of its own,
/// and retired with it. The guest names no storage scope.
fn check_facet_sessions(cells: &Cells, directory: &Path) {
    let facet_request = |grant: &str, operation: Operation| Request {
        version: wire::VERSION,
        grant: grant.into(),
        operation,
    };
    let open = |root: &mut StorageSession, owner: &str, name: &str| {
        let operation = Operation::FacetOpen {
            owner: owner.into(),
            name: name.into(),
            class: "Facet".into(),
        };
        let request = facet_request(root.grant(), operation);
        let reply = root.execute(cells, true, request);
        match reply.outcome {
            Outcome::Ok {
                value:
                    Value::Facet {
                        scope,
                        grant,
                        depth,
                    },
            } => Ok((scope, grant, depth)),
            Outcome::Error { message } => Err(message),
            other => panic!("unexpected facet open reply {other:?}"),
        }
    };
    let run = |root: &mut StorageSession, grant: &str, operation: Operation| match root
        .execute(cells, true, facet_request(grant, operation))
        .outcome
    {
        Outcome::Ok { value } => Ok(value),
        Outcome::Error { message } => Err(message),
    };
    let put = |value: i64| Operation::PutJson {
        entries: vec![("value".into(), serde_json::json!(value))],
    };
    let get = || Operation::Get {
        keys: vec!["value".into()],
    };
    let json = |value: i64| Value::Entries {
        entries: vec![(
            "value".into(),
            wire::StoredValue::Json {
                value: value.to_string(),
            },
        )],
    };
    let turn = |root: &mut StorageSession, grant: &str| {
        let Value::Turn { turn } = run(root, grant, Operation::TurnStart {}).unwrap() else {
            panic!("no facet turn")
        };
        turn
    };
    let finish = |root: &mut StorageSession, grant: &str, turn: String| {
        run(root, grant, Operation::TurnFinish { turn })
    };
    let path = directory.join("facet-root.sqlite");
    let root_scope = scope("facet-root");
    {
        let _installed = cells.install();
        storage::open_at_epoch(&root_scope, path.to_str().unwrap(), 9, false, None, false).unwrap();
    }
    let mut root = StorageSession::new(cells, root_scope.clone(), 9).unwrap();
    let root_grant = root.grant().to_owned();

    // The host derives the scope from the live root; storage is routed by
    // the facet's own grant through the root session.
    let (facet, grant, depth) = open(&mut root, "1:1", "child").unwrap();
    assert!(facet.starts_with("Facet:") && depth == 1);
    assert_ne!(grant, root_grant);
    let t = turn(&mut root, &grant);
    run(&mut root, &grant, put(1)).unwrap();
    finish(&mut root, &grant, t).unwrap();
    assert_eq!(run(&mut root, &grant, get()).unwrap(), json(1));
    // An unknown or foreign grant is never routed, and cannot reach the root.
    let unrelated = StorageSession::new(cells, scope("one"), 7).unwrap();
    assert!(run(&mut root, unrelated.grant(), get())
        .unwrap_err()
        .contains("unknown storage handle"));
    assert!(run(&mut root, &"a".repeat(64), get())
        .unwrap_err()
        .contains("unknown storage handle"));
    // Only one event's session ends: a sibling event keeps the same cell.
    let (_, sibling, ..) = open(&mut root, "1:1", "child").unwrap();
    run(
        &mut root,
        &grant,
        Operation::FacetClose {
            facet: grant.clone(),
        },
    )
    .unwrap();
    assert!(run(&mut root, &grant, get())
        .unwrap_err()
        .contains("unknown storage handle"));
    assert_eq!(run(&mut root, &sibling, get()).unwrap(), json(1));
    // A facet cannot end another event's session.
    assert!(run(
        &mut root,
        &sibling,
        Operation::FacetClose {
            facet: root_grant.clone()
        }
    )
    .is_err());

    // A facet is a database of its own: a root transaction neither holds
    // its writes nor discards them when it rolls back.
    let Value::Transaction { transaction } = run(
        &mut root,
        &root_grant,
        Operation::TransactionStart { origin: None },
    )
    .unwrap() else {
        panic!("no root transaction")
    };
    let t = turn(&mut root, &sibling);
    run(&mut root, &sibling, put(2)).unwrap();
    finish(&mut root, &sibling, t).unwrap();
    run(
        &mut root,
        &root_grant,
        Operation::TransactionFinish {
            transaction,
            action: TransactionAction::Rollback,
        },
    )
    .unwrap();
    let (_, again, _) = open(&mut root, "1:1", "child").unwrap();
    assert_eq!(run(&mut root, &again, get()).unwrap(), json(2));
    let Value::Transaction { transaction } = run(
        &mut root,
        &root_grant,
        Operation::TransactionStart { origin: None },
    )
    .unwrap() else {
        panic!("no root transaction")
    };
    let t = turn(&mut root, &again);
    run(&mut root, &again, put(3)).unwrap();
    finish(&mut root, &again, t).unwrap();
    run(
        &mut root,
        &root_grant,
        Operation::TransactionFinish {
            transaction,
            action: TransactionAction::Commit,
        },
    )
    .unwrap();
    let event = root.begin_event(cells).unwrap();
    root.prepare_output(cells, &event, celld_logic::Channel::Response)
        .unwrap();

    // Depth: the root plus three facets is the most. `again` is level one.
    let mut parent = again.clone();
    for level in 2..=3u32 {
        let operation = Operation::FacetOpen {
            owner: format!("2:{level}"),
            name: format!("level-{level}"),
            class: "Facet".into(),
        };
        let Value::Facet { grant, depth, .. } = run(&mut root, &parent, operation).unwrap() else {
            panic!("no nested facet")
        };
        assert_eq!(depth, level);
        parent = grant;
    }
    assert!(run(
        &mut root,
        &parent,
        Operation::FacetOpen {
            owner: "2:4".into(),
            name: "level-4".into(),
            class: "Facet".into(),
        }
    )
    .unwrap_err()
    .contains("nesting depth limit"));
    // Nested sessions are reached through the root's routing too.
    assert!(run(&mut root, &parent, get()).is_ok());

    // A facet whose SQL failed fatally fails its turn and its output.
    let t = turn(&mut root, &again);
    run(&mut root, &again, put(4)).unwrap();
    {
        let _installed = cells.install();
        storage::poison_sql_for_test(&facet, "injected facet write failure");
    }
    assert!(finish(&mut root, &again, t)
        .unwrap_err()
        .contains("injected facet write failure"));

    // Revoking the root retires every event session and closes the cells.
    let facet_scope = facet.clone();
    {
        let _installed = cells.install();
        assert!(storage::activation_epoch(&facet_scope).is_some());
    }
    root.revoke(cells).unwrap();
    assert!(run(&mut root, &again, get()).is_err());
    assert!(run(&mut root, &parent, get()).is_err());
    {
        let _installed = cells.install();
        assert!(storage::activation_epoch(&facet_scope).is_none());
    }

    // A facet of a retired root cannot act on the root's successor, and its
    // late write lands in the retired epoch's file, never the successor's.
    {
        let _installed = cells.install();
        storage::close(&root_scope);
        storage::open_at_epoch(&root_scope, path.to_str().unwrap(), 10, false, None, false)
            .unwrap();
    }
    let mut successor = StorageSession::new(cells, root_scope.clone(), 10).unwrap();
    let (stale_scope, stale, _) = open(&mut successor, "3:1", "child").unwrap();
    {
        let _installed = cells.install();
        storage::close(&root_scope);
        storage::open_at_epoch(&root_scope, path.to_str().unwrap(), 11, false, None, false)
            .unwrap();
    }
    assert!(run(&mut successor, &stale, get()).is_err());
    {
        let _installed = cells.install();
        storage::close(&stale_scope);
    }

    // A reserved object class has no facets.
    let reserved = format!("{}:{}", celld_logic::kv::RESERVED_CLASS, "b".repeat(64));
    {
        let _installed = cells.install();
        storage::open_at_epoch(
            &reserved,
            directory.join("reserved.sqlite").to_str().unwrap(),
            9,
            false,
            None,
            false,
        )
        .unwrap();
    }
    let mut internal = StorageSession::new(cells, reserved.clone(), 9).unwrap();
    assert!(open(&mut internal, "4:1", "child")
        .unwrap_err()
        .contains("not available"));
    internal.revoke(cells).unwrap();
    let _installed = cells.install();
    storage::close(&reserved);
    storage::close(&root_scope);
}

/// A facet turn whose stream cannot be proved is answered with an error
/// reply: the guest never sees the write acknowledged, and its storage
/// stream and session stay up for the next turn.
async fn check_facet_prove_failure(cells: &Cells, directory: &Path) {
    let call = |root: &mut StorageSession, grant: &str, operation: Operation| {
        root.execute(
            cells,
            true,
            Request {
                version: wire::VERSION,
                grant: grant.into(),
                operation,
            },
        )
        .outcome
    };
    let put = |value: i64| Operation::PutJson {
        entries: vec![("value".into(), serde_json::json!(value))],
    };
    let get = || Operation::Get {
        keys: vec!["value".into()],
    };
    let root_scope = scope("facet-prove-root");
    {
        let _installed = cells.install();
        let path = directory.join("facet-prove-root.sqlite");
        storage::open_at_epoch(&root_scope, path.to_str().unwrap(), 9, false, None, false).unwrap();
    }
    let mut root = StorageSession::new(cells, root_scope.clone(), 9).unwrap();
    let root_grant = root.grant().to_owned();
    let Outcome::Ok {
        value: Value::Facet { grant, .. },
    } = call(
        &mut root,
        &root_grant,
        Operation::FacetOpen {
            owner: "1:1".into(),
            name: "child".into(),
            class: "Facet".into(),
        },
    )
    else {
        panic!("facet did not open")
    };
    // One facet turn: the write, then the turn's finish, whose reply waits
    // for the proof of the facet's stream.
    let finish = |root: &mut StorageSession, value: i64| {
        let Outcome::Ok {
            value: Value::Turn { turn },
        } = call(root, &grant, Operation::TurnStart {})
        else {
            panic!("no facet turn")
        };
        assert!(matches!(call(root, &grant, put(value)), Outcome::Ok { .. }));
        Request {
            version: wire::VERSION,
            grant: grant.clone(),
            operation: Operation::TurnFinish { turn },
        }
    };
    let settle = |root: &mut StorageSession, request: Request| {
        let reply = root.execute(cells, true, request.clone());
        assert!(matches!(reply.outcome, Outcome::Ok { .. }), "{reply:?}");
        let followup = StorageSession::facet_followup(&request);
        assert!(
            followup.is_some(),
            "a facet turn finish has a host-loop step"
        );
        (reply, followup)
    };
    assert!(root.facet_stream(&grant).is_some());

    crate::js::FACET_PROVE_FAILURE.with(|failure| {
        *failure.borrow_mut() = Some("follower unreachable".into());
    });
    let request = finish(&mut root, 1);
    let (native, followup) = settle(&mut root, request);
    let reply = root.finish_facet_request(followup.as_ref(), native).await;
    let Outcome::Error { message } = reply.outcome else {
        panic!("an unproved facet turn was acknowledged")
    };
    assert!(
        message.contains("facet output") && message.contains("follower unreachable"),
        "{message}"
    );
    // The failed proof is not a failed session: the next turn runs, and
    // once the stream proves, its reply is the native one.
    crate::js::FACET_PROVE_FAILURE.with(|failure| *failure.borrow_mut() = None);
    let request = finish(&mut root, 2);
    let (native, followup) = settle(&mut root, request);
    let reply = root
        .finish_facet_request(followup.as_ref(), native.clone())
        .await;
    assert_eq!(reply, native);
    assert!(matches!(
        call(&mut root, &grant, get()),
        Outcome::Ok {
            value: Value::Entries { .. }
        }
    ));
    // Only a turn finish waits for a proof; other replies pass untouched.
    let ping = Request {
        version: wire::VERSION,
        grant: grant.clone(),
        operation: get(),
    };
    assert!(StorageSession::facet_followup(&ping).is_none());
    let native = root.execute(cells, true, ping);
    let unchanged = root.finish_facet_request(None, native.clone()).await;
    assert_eq!(unchanged, native);
    root.revoke(cells).unwrap();
    let _installed = cells.install();
    storage::close(&root_scope);
}

fn check_same_epoch_replacement(cells: &Cells, scope: &str, path: &Path) {
    let mut old = StorageSession::new(cells, scope.into(), 7).unwrap();
    let old_cursors =
        Arc::new(CursorResidency::new(&old, Arc::new(AtomicBool::new(true))).unwrap());
    old.cursor_residency = Some(old_cursors.clone());
    let turn = request(&old, Operation::TurnStart {});
    success(old.execute(cells, true, turn));
    let read = request(
        &old,
        Operation::SqlStart {
            query: "SELECT 11 UNION ALL SELECT 22".into(),
            bindings: vec![],
        },
    );
    success(old.execute(cells, true, read));
    let event = old.begin_event(cells).unwrap();
    let output = old
        .prepare_output(cells, &event, celld_logic::Channel::Response)
        .unwrap();
    assert!(output.resident());
    let start = request(&old, Operation::TransactionStart { origin: None });
    success(old.execute(cells, true, start));
    let put = request(
        &old,
        Operation::PutJson {
            entries: vec![("discarded-old-transaction".into(), serde_json::json!(true))],
        },
    );
    success(old.execute(cells, true, put));
    let mut waiting =
        crate::js::cell_gate_wait(scope).expect("old transaction did not hold input gate");
    {
        let _installed = cells.install();
        storage::close(scope);
        storage::open_at_epoch(scope, path.to_str().unwrap(), 7, false, None, false).unwrap();
    }
    assert!(waiting
        .try_recv()
        .unwrap()
        .err()
        .unwrap()
        .contains("residency changed"));
    assert!(
        !output.resident(),
        "prepared output survived connection retirement"
    );
    assert!(old
        .prepare_output(cells, &event, celld_logic::Channel::Response)
        .is_err());
    let mut replacement = StorageSession::new(cells, scope.into(), 7).unwrap();
    let start = request(&replacement, Operation::TransactionStart { origin: None });
    let Value::Transaction { transaction } = success(replacement.execute(cells, true, start))
    else {
        panic!("replacement transaction did not start")
    };
    let put = request(
        &replacement,
        Operation::PutJson {
            entries: vec![("replacement-transaction".into(), serde_json::json!(true))],
        },
    );
    success(replacement.execute(cells, true, put));
    let read = request(
        &replacement,
        Operation::SqlStart {
            query: "SELECT 33 UNION ALL SELECT 44".into(),
            bindings: vec![],
        },
    );
    let Value::Cursor {
        cursor: Some(replacement_cursor),
        ..
    } = success(replacement.execute(cells, true, read))
    else {
        panic!("replacement cursor missing");
    };
    old_cursors.close(cells);
    let next = request(
        &replacement,
        Operation::SqlNext {
            cursor: replacement_cursor,
        },
    );
    assert!(
        matches!(success(replacement.execute(cells, true, next)), Value::Row { row: Some(row), .. } if row == vec![wire::SqlValue::Integer { value: 44 }])
    );
    let put = request(
        &old,
        Operation::PutJson {
            entries: vec![("retired-write".into(), serde_json::json!(true))],
        },
    );
    assert!(
        matches!(old.execute(cells, true, put).outcome, Outcome::Error { message } if message.contains("residency changed"))
    );
    // Both sessions used the same savepoint name at this epoch. Retiring
    // the old session must not roll back the replacement's transaction.
    old.revoke(cells).unwrap();
    assert!(
        crate::js::cell_gate_wait(scope).is_some(),
        "late old cleanup opened the replacement input gate"
    );
    assert!(replacement.sample_output(cells).unwrap().in_transaction);
    let commit = request(
        &replacement,
        Operation::TransactionFinish {
            transaction,
            action: TransactionAction::Commit,
        },
    );
    success(replacement.execute(cells, true, commit));
    {
        let _installed = cells.install();
        assert!(storage::get_stored(scope, "replacement-transaction")
            .unwrap()
            .is_some());
        assert!(storage::get_stored(scope, "discarded-old-transaction")
            .unwrap()
            .is_none());
        assert!(storage::get_stored(scope, "retired-write")
            .unwrap()
            .is_none());
    }
    replacement.revoke(cells).unwrap();
}

fn check_generation(address: &ResourceAddress, id: u64) -> crate::generation::Generation {
    use crate::generation::{DeploymentGraph, Generation, GenerationOptions};
    let script_name = address.storage_root();
    assert_eq!(
        address.namespace_key("Counter"),
        crate::js::namespace_key(&script_name, "Counter")
    );
    // This trusted host stub contains no application source. Untrusted
    // JavaScript executes exclusively in the selected guest process/VM.
    Generation::build(
            id,
            DeploymentGraph::single(crate::fleet::LoadedDeployment {
                options: crate::js::WorkerConfigOptions {
                    src: "export class Counter {} export class Alternate {} export default { fetch() { return new Response('bridge'); } }".into(),
                    script_name: script_name.clone(),
                    do_classes: vec!["Counter".into(), "Alternate".into(), crate::deploy::D1_CLASS.into(), crate::deploy::KV_CLASS.into()],
                    bindings: vec![], r2_bindings: vec![], d1_bindings: vec![],
                    kv_bindings: vec![], queue_bindings: vec![], queue_consumers: vec![],
                    workflow_bindings: vec![], vars: vec![], node: "host".into(),
                    modules: vec![], compat: Default::default(),
                },
                script_name,
                version: "check".into(), prefix: "check".into(),
                asset_binding: None, loader_bindings: vec![], assets: None,
                services: vec![], crons: vec![], containers: vec![], fence_image: None,
            }),
            GenerationOptions { node: "host".into(), region: "local".into() },
        ).unwrap()
}

/// Real original lifecycle/pool integration; its controlled activation has
/// no Actor lease or replicator and is not a production handler endpoint.
pub(crate) async fn check_runtime_storage_bridge() {
    use crate::runtime::{RuntimeManager, RuntimeOptions};
    let directory = tempfile::tempdir().unwrap();
    let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let output = observed.clone();
    let address = address("runtime");
    let generation = check_generation(&address, 1);
    let runtime = RuntimeManager::start(
        generation,
        RuntimeOptions {
            data_dir: directory.path().join("data"),
            replication: None,
            wake: None,
            alarm_observer: std::sync::Arc::new(move |scope, alarm| {
                output.lock().unwrap().push((scope, alarm.at_ms()))
            }),
            node: "host".into(),
            region: "local".into(),
            bucket: None,
        },
    )
    .unwrap();
    let one = address.cell_scope("Counter", "named-object").unwrap();
    let two = address.cell_scope("Counter", "foreign-object").unwrap();
    for (cell, epoch) in [(&one, 7), (&two, 8)] {
        runtime
            .restore_cell(
                cell,
                &celld_logic::RestoreSpec {
                    epoch,
                    fresh: true,
                    took_over: false,
                    resume_local: false,
                    prior: None,
                },
            )
            .await
            .unwrap();
        runtime.start_cell(cell.clone(), epoch, true).await.unwrap();
        assert!(runtime.storage_turn(cell, epoch, |_| ()).await.is_err());
        runtime.publish_cell(cell, epoch).unwrap();
    }
    assert!(runtime.storage_turn(&one, 8, |_| ()).await.is_err());
    let mut session = runtime
        .storage_turn(&one, 7, |cells| {
            storage::set_actor_name(&one, "named-object").unwrap();
            storage::put_many_serialized(&one, &[("js-clone".into(), vec![255, 15, 0, 254])])
                .unwrap();
            StorageSession::new(cells, one.clone(), 7).unwrap()
        })
        .await
        .unwrap();
    let event = runtime
        .storage_turn(&one, 7, |cells| session.begin_event(cells).unwrap())
        .await
        .unwrap();
    let mut foreign = runtime
        .storage_turn(&two, 8, |cells| {
            StorageSession::new(cells, two.clone(), 8).unwrap()
        })
        .await
        .unwrap();
    let foreign_request = request(
        &foreign,
        Operation::SqlStart {
            query: "SELECT 123".into(),
            bindings: vec![],
        },
    );
    let Value::Cursor {
        cursor: Some(foreign_cursor),
        ..
    } = runtime
        .storage_turn(&two, 8, |cells| {
            success(foreign.execute(cells, true, foreign_request))
        })
        .await
        .unwrap()
    else {
        panic!("foreign cursor missing")
    };
    runtime
        .storage_turn(&one, 7, |cells| {
            check_native_storage_operations(
                cells,
                &mut session,
                &event,
                foreign.grant(),
                &foreign_cursor,
            );
            session.revoke(cells).unwrap();
            assert!(!storage::sync_sample(&one).unwrap().unwrap().in_transaction);
            assert!(storage::get_stored(&one, "uncommitted-on-disconnect")
                .unwrap()
                .is_none());
            assert!(
                storage::get_stored(&one, "uncommitted-nested-on-disconnect")
                    .unwrap()
                    .is_none()
            );
        })
        .await
        .unwrap();
    for operation in [
        Operation::SetAlarm {
            at_ms: 9223372036854775000,
        },
        Operation::DeleteAlarm {},
    ] {
        runtime
            .storage_turn(&one, 7, |cells| {
                let mut alarm = StorageSession::new(cells, one.clone(), 7).unwrap();
                let request = request(&alarm, operation);
                success(alarm.execute(cells, true, request));
                alarm.revoke(cells).unwrap();
            })
            .await
            .unwrap();
    }
    assert_eq!(runtime.alarm(&one), None);
    assert!(observed
        .lock()
        .unwrap()
        .iter()
        .any(|(scope, alarm)| scope == &one && *alarm == Some(9223372036854775000)));
    runtime
        .storage_turn(&two, 8, |cells| {
            assert!(storage::get_stored(&two, "foreign-write")
                .unwrap()
                .is_none());
            foreign.revoke(cells).unwrap();
        })
        .await
        .unwrap();
    let mut stale = runtime
        .storage_turn(&one, 7, |cells| {
            StorageSession::new(cells, one.clone(), 7).unwrap()
        })
        .await
        .unwrap();
    let stale_admission = runtime.admit_storage_request(&one, 7).unwrap();
    runtime.swap_out_cell(&one, 7).await.unwrap();
    assert!(runtime.storage_turn(&one, 7, |_| ()).await.is_err());
    runtime.start_cell(one.clone(), 7, false).await.unwrap();
    runtime.publish_cell(&one, 7).unwrap();
    runtime.quarantine_storage_request(&stale_admission);
    assert!(runtime
        .admitted_storage_turn(&stale_admission, |_| {
            panic!("old guest admission entered a replacement at the same epoch")
        })
        .await
        .is_err());
    drop(stale_admission);
    runtime.storage_turn(&one, 7, |cells| {
            let get = request(&stale, Operation::GetAlarm {});
            assert!(matches!(stale.execute(cells, true, get).outcome, Outcome::Error { message } if message.contains("residency changed")));
            stale.revoke(cells).unwrap();
            let mut replacement = StorageSession::new(cells, one.clone(), 7).unwrap();
            let query = request(&replacement, Operation::SqlStart { query: "SELECT value FROM counter".into(), bindings: vec![] });
            assert!(matches!(success(replacement.execute(cells, true, query)), Value::Cursor { row: Some(row), .. } if row == vec![wire::SqlValue::Integer { value: 1 }]));
            replacement.revoke(cells).unwrap();
        }).await.unwrap();
    crate::runtime::check_queued_storage_retirement(&runtime, &one, 7).await;
    runtime.swap_out_cell(&two, 8).await.unwrap();
    eprintln!("application runtime storage bridge: native storage, alarms, rollback and same-epoch retirement through original pool checked");
}

/// Actual Actor routing, admission, output gate and eviction, using its
/// in-memory ownership backend. This proves no provider/replication claim.
/// The original Actor/runtime hosting one application resource. Abort the
/// returned Actor task when finished.
pub(crate) async fn actor_bridge(
    directory: &Path,
    address: &ResourceAddress,
) -> (
    crate::actor::AppHandle,
    crate::runtime::RuntimeManager,
    tokio::task::JoinHandle<()>,
) {
    use crate::actor::{
        Actor, ActorIdentity, ActorServices, AdmissionLimits, AppHandle, DrainPinRegistry,
    };
    use crate::runtime::{RuntimeManager, RuntimeOptions};
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc,
    };
    use tokio::sync::mpsc;
    let (tx, rx) = mpsc::unbounded_channel();
    let observe = tx.clone();
    let runtime = RuntimeManager::start(
        check_generation(address, 1),
        RuntimeOptions {
            data_dir: directory.join("data"),
            replication: None,
            wake: None,
            alarm_observer: Arc::new(move |cell, alarm| {
                let _ = observe.send(crate::actor::Message::AlarmObserved {
                    cell,
                    alarm,
                    covered: false,
                });
            }),
            node: "host".into(),
            region: "local".into(),
            bucket: None,
        },
    )
    .unwrap();
    let drain_pins = DrainPinRegistry::default();
    let peer_auth = Arc::new(crate::peer_auth::PeerAuth::new([0; 32], "host").unwrap());
    let peer_http = reqwest::Client::new();
    let (fence, _fenced) = mpsc::unbounded_channel();
    let actor = Actor::from_environment_with_services(
        AdmissionLimits {
            resident: 16,
            activations: 4,
            releases: 4,
            placement_weight: 1,
            #[cfg(all(test, celld_internal_tests))]
            evictions: 4,
        },
        false,
        fence,
        ActorServices {
            runtime: Some(runtime.clone()),
            drain_pins: drain_pins.clone(),
            ownership: None,
            peer_http: peer_http.clone(),
            peer_auth: peer_auth.clone(),
        },
        ActorIdentity {
            node: "host".into(),
            advertise: "http://127.0.0.1:1".into(),
            region: "local".into(),
        },
        None,
    )
    .await
    .unwrap();
    let running = tokio::spawn(actor.run(rx));
    let (reload, _reload_rx) = crate::generation::reload_channel();
    let (websockets, _websocket_rx) = mpsc::unbounded_channel();
    let app = AppHandle {
        tx,
        runtime: Some(runtime.clone()),
        reload,
        peer_http,
        peer_auth,
        #[cfg(all(test, celld_internal_tests))]
        scripted_activity_host: None,
        advertise: "http://127.0.0.1:1".into(),
        websockets,
        max_outbound_websockets: 16,
        draining: Arc::new(AtomicBool::new(false)),
        fleet_ready: Arc::new(AtomicBool::new(false)),
        public_in_flight: Arc::new(AtomicUsize::new(0)),
        drain_pins,
        follower: None,
        trust_forwarded_headers: false,
        max_request_body_bytes: 8 * 1024 * 1024,
        operation_deadline_ms: crate::actor::operation_deadline_ms().unwrap(),
    };
    (app, runtime, running)
}

pub(crate) async fn check_actor_handler_bridge() {
    let directory = tempfile::tempdir().unwrap();
    let address = address("actor");
    let one = address.cell_scope("Counter", "named-object").unwrap();
    let (app, runtime, running) = actor_bridge(directory.path(), &address).await;
    check_owned_handler_bridge(&app, &runtime, &address, &one).await;
    check_guest_input_gate_bridge(&app, &runtime, &one).await;
    #[cfg(target_os = "linux")]
    crate::application_vm::tests::check_supervised_handler_bridge(&app, &runtime, &address, &one).await;
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    check_guest_socket_lifecycle().await;
    eprintln!("application Actor bridge: native input/output gates, streamed replies, joined cancellation, rollback and eviction checked");
}

/// What settles a hibernatable socket's host registry entry, through the
/// real Actor and runtime: an event that ends or is cancelled releases what
/// no upgrade claimed; a claimed socket dropped before its upgrade is
/// abandoned; one an upgrade took survives its event and is closed by the
/// original drain handoff. The guest itself is not involved.
async fn check_guest_socket_lifecycle() {
    use crate::application_guest::{tests::invoke, GuestStorage};
    use crate::application_guest_transport::GuestSocket;
    use tokio::time::{sleep, timeout, Duration};
    async fn accept(access: &GuestStorage, claim: bool) -> Result<u64> {
        let reply = access
            .execute(Request {
                version: wire::VERSION,
                grant: access.grant().into(),
                operation: Operation::SocketAccept {
                    ws: 1,
                    accepted: false,
                    tags: vec!["lifecycle".into()],
                },
            })
            .await?;
        let Value::Socket { ws } = success(reply) else {
            anyhow::bail!("not a socket")
        };
        // The transport claims a socket when it takes the reply carrying it.
        if claim {
            crate::js::ws_claim_guest(ws)?;
        }
        Ok(ws)
    }
    async fn gone(id: u64) {
        timeout(Duration::from_secs(5), async {
            while crate::js::ws_scope_of(id).is_some() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the socket was never released");
    }
    let directory = tempfile::tempdir().unwrap();
    let address = address("sockets");
    let cell = address.cell_scope("Counter", "named-object").unwrap();
    let (app, _runtime, running) = actor_bridge(directory.path(), &address).await;
    let event = |claim: bool, hold: bool| {
        let (accepted, accepting) = tokio::sync::oneshot::channel();
        let work = invoke(
            app.clone(),
            cell.clone(),
            None,
            Duration::from_secs(10),
            move |access| async move {
                let id = accept(&access, claim).await?;
                let _ = accepted.send(id);
                if hold {
                    std::future::pending::<()>().await;
                }
                Ok(id)
            },
        );
        (Box::pin(work), accepting)
    };

    // An event that ends without an upgrade carrying its socket releases it.
    let (work, _) = event(false, false);
    let completed = work.await.unwrap();
    let id = *completed.result.as_ref().unwrap();
    drop(completed);
    assert_eq!(
        crate::js::ws_scope_of(id),
        None,
        "unclaimed socket outlived its event"
    );

    // A cancelled event releases it too, after its native settlement.
    let (mut work, accepting) = event(false, true);
    let id = crate::asyncrt::select! {
        id = accepting => id.unwrap(),
        _ = &mut work => panic!("a held event ended"),
    };
    assert_eq!(crate::js::ws_scope_of(id).as_deref(), Some(cell.as_str()));
    drop(work);
    gone(id).await;

    // A claimed socket survives its event; its guard, dropped before an
    // upgrade takes it, abandons it. The core learns of it first.
    let (work, _) = event(true, false);
    let completed = work.await.unwrap();
    let id = *completed.result.as_ref().unwrap();
    drop(completed);
    assert_eq!(crate::js::ws_scope_of(id).as_deref(), Some(cell.as_str()));
    let guard = GuestSocket::new(app.clone(), id, cell.clone());
    guard.register().await.unwrap();
    drop(guard);
    gone(id).await;

    // An upgrade that took the socket owns it; the original drain handoff
    // then closes it for its connection (1012) and forgets it.
    let (work, _) = event(true, false);
    let completed = work.await.unwrap();
    let id = *completed.result.as_ref().unwrap();
    drop(completed);
    let guard = GuestSocket::new(app.clone(), id, cell.clone());
    guard.register().await.unwrap();
    let target = guard.into_target();
    assert_eq!((target.id, target.scope.as_str()), (id, cell.as_str()));
    sleep(Duration::from_millis(100)).await;
    assert!(
        crate::js::ws_scope_of(id).is_some(),
        "a taken upgrade was abandoned"
    );
    let (frames, mut client) = tokio::sync::mpsc::unbounded_channel();
    crate::js::ws_register(id, frames);
    app.tx.send(crate::actor::Message::ReleaseAll).unwrap();
    let closed = timeout(Duration::from_secs(10), client.recv())
        .await
        .expect("drain never closed the socket")
        .expect("the socket's channel dropped without a close");
    assert!(
        matches!(closed, crate::js::WsOut::Close(1012, _)),
        "drain closed with another frame"
    );
    gone(id).await;

    // A peer owner's upgrade is parked here, not in this registry: taken, it
    // splices; dropped unclaimed, only the parked stream is released.
    static RELEASED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    crate::application_guest_transport::install_parked_release(|parked| {
        RELEASED.store(parked, std::sync::atomic::Ordering::SeqCst);
    });
    let parked = |tunnel| crate::js::WsTarget {
        id: 0,
        scope: cell.clone(),
        tunnel: Some(tunnel),
    };
    let remote = GuestSocket::remote(app.clone(), parked(41));
    let target = remote.into_target();
    assert_eq!(target.tunnel, Some(41));
    assert_eq!(
        RELEASED.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "a taken upgrade was released"
    );
    drop(GuestSocket::remote(app.clone(), parked(42)));
    assert_eq!(
        RELEASED.load(std::sync::atomic::Ordering::SeqCst),
        42,
        "an abandoned upgrade stayed parked"
    );
    let remote = GuestSocket::remote(app.clone(), parked(7));
    assert!(
        remote.key() > 1 << 52,
        "a remote id can collide with a registry id"
    );
    remote.into_target();
    crate::application_guest_transport::tests::check_socket_dial(&app).await;
    running.abort();
    let _ = running.await;
}

/// Real Actor/native concurrency, not a VM qualification. An already
/// delivered event's read and another event's initial delivery both wait
/// behind the transaction; cancellation rolls back before either resumes.
async fn check_guest_input_gate_bridge(
    app: &crate::actor::AppHandle,
    runtime: &crate::runtime::RuntimeManager,
    cell: &str,
) {
    use crate::application_guest::{tests::invoke, GuestStorage};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use tokio::sync::oneshot;
    use tokio::time::{timeout, Duration};
    async fn execute(access: &GuestStorage, operation: Operation) -> Result<Value> {
        Ok(success(
            access
                .execute(Request {
                    version: wire::VERSION,
                    grant: access.grant().into(),
                    operation,
                })
                .await?,
        ))
    }
    let (reader_entered, entered) = oneshot::channel();
    let (read_now, read) = oneshot::channel();
    let mut reader = Box::pin(invoke(
        app.clone(),
        cell.into(),
        None,
        Duration::from_secs(10),
        move |access| async move {
            let _ = reader_entered.send(());
            read.await?;
            let rows = execute(
                &access,
                Operation::Get {
                    keys: vec!["input-gate-outer".into(), "input-gate-inner".into()],
                },
            )
            .await?;
            assert!(matches!(rows, Value::Entries { entries } if entries.is_empty()));
            execute(
                &access,
                Operation::PutJson {
                    entries: vec![("input-gate-ack".into(), serde_json::json!(true))],
                },
            )
            .await?;
            Ok(())
        },
    ));
    crate::asyncrt::select! {
        result = timeout(Duration::from_secs(5), entered) => result.unwrap().unwrap(),
        _ = &mut reader => panic!("reader ended before its pause"),
    }
    let (writer_ready, ready) = oneshot::channel();
    let mut writer = Box::pin(invoke(
        app.clone(),
        cell.into(),
        None,
        Duration::from_secs(10),
        move |access| async move {
            execute(&access, Operation::TransactionStart { origin: None }).await?;
            execute(
                &access,
                Operation::PutJson {
                    entries: vec![("input-gate-outer".into(), serde_json::json!(true))],
                },
            )
            .await?;
            let nested = execute(&access, Operation::TransactionStart { origin: None }).await?;
            let Value::Transaction { transaction } = nested else {
                panic!("missing nested transaction");
            };
            execute(
                &access,
                Operation::PutJson {
                    entries: vec![("input-gate-inner".into(), serde_json::json!(true))],
                },
            )
            .await?;
            execute(
                &access,
                Operation::TransactionFinish {
                    transaction,
                    action: TransactionAction::Commit,
                },
            )
            .await?;
            let _ = writer_ready.send(());
            std::future::pending::<Result<()>>().await
        },
    ));
    crate::asyncrt::select! {
        result = timeout(Duration::from_secs(5), ready) => result.unwrap().unwrap(),
        _ = &mut writer => panic!("writer ended before its transaction hold"),
    }
    // The original JS delivery API sees exactly this guest's hold.
    assert!(crate::js::cell_gate_wait(cell).is_some());
    read_now.send(()).unwrap();
    let delivered = Arc::new(AtomicBool::new(false));
    let observed = delivered.clone();
    let mut queued = Box::pin(invoke(
        app.clone(),
        cell.into(),
        None,
        Duration::from_secs(10),
        move |_| async move {
            observed.store(true, Ordering::Release);
            Ok(())
        },
    ));
    crate::asyncrt::select! {
        result = timeout(Duration::from_secs(5), async {
            while crate::js::live_input_gate_waiters(cell) < 2 { tokio::task::yield_now().await; }
        }) => result.unwrap(),
        _ = &mut reader => panic!("guest read crossed an uncommitted transaction"),
        _ = &mut queued => panic!("guest initial delivery crossed a closed gate"),
    }
    assert!(!delivered.load(Ordering::Acquire));
    drop(queued);
    timeout(Duration::from_secs(5), async {
        while app.public_in_flight() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!delivered.load(Ordering::Acquire));
    drop(writer);
    let completed = timeout(Duration::from_secs(5), reader)
        .await
        .unwrap()
        .unwrap();
    assert!(completed.result.is_ok());
    drop(completed);
    timeout(Duration::from_secs(5), async {
        while app.public_in_flight() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(crate::js::cell_gate_wait(cell).is_none());
    let epoch = app.resident_epoch(cell.into()).await.unwrap();
    let admission = runtime.admit_storage_request(cell, epoch).unwrap();
    runtime.admitted_storage_turn(&admission, |_| {
            assert!(storage::get_stored(cell, "input-gate-outer").unwrap().is_none());
            assert!(storage::get_stored(cell, "input-gate-inner").unwrap().is_none());
            assert!(matches!(storage::get_stored(cell, "input-gate-ack").unwrap(), Some(storage::StoredValue::LegacyJson(value)) if value == "true"));
        }).await.unwrap();
    drop(admission);
    app.evict(cell.into()).await.unwrap();
    eprintln!("guest input gate: original Actor/native turns blocked an already-delivered read and initial delivery, retained the nested hold, cancelled a queued frontend and rolled back before an acknowledged successor write");
}

/// Real Unix control/storage transport and original native Actor/storage;
/// the guest adapter is controlled here, not a VM background task.
async fn check_split_reply_transport(
    app: &crate::actor::AppHandle,
    address: &ResourceAddress,
    cell: &str,
) {
    use crate::application_guest::{read_frame, write_frame};
    use celld_applications::invocation_wire::{
        GuestControl, HostControl, HttpResponse, HttpResult, StorageConnect,
    };
    use tokio::net::{UnixListener, UnixStream};
    use tokio::time::timeout;
    async fn operation(socket: &mut UnixStream, grant: &str, operation: Operation) -> Value {
        write_frame(
            socket,
            &Request {
                version: wire::VERSION,
                grant: grant.into(),
                operation,
            }
            .encode()
            .unwrap(),
        )
        .await
        .unwrap();
        success(
            Reply::decode(
                &timeout(Duration::from_secs(5), read_frame(socket))
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
            )
            .unwrap(),
        )
    }
    for joined_failure in [false, true] {
        let baseline = app.public_in_flight();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("storage.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let (host, mut guest) = UnixStream::pair().unwrap();
        write_frame(
            &mut guest,
            &GuestControl::Ready {
                version: wire::VERSION,
                adapter: celld_applications::Language::Javascript,
                execution_turns: true,
                quiescent_terminals: true,
                cleanup_turns: true,
                callback_turns: true,
                response_turns: true,
                reply_drain: true,
                resident_objects: false,
                object_retirement: false,
                response_streams: false,
                memory_diagnostics: false,
                idle_lifecycle: false,
            }
            .encode()
            .unwrap(),
        )
        .await
        .unwrap();
        let dispatcher = crate::application_guest_transport::Dispatcher::connect(
            address.application.clone(),
            celld_applications::Language::Javascript,
            host,
            listener,
            None,
        )
        .await
        .unwrap();
        let client = dispatcher.client();
        let mut work = Box::pin(client.invoke(
            app.clone(),
            address.clone(),
            "Counter".into(),
            "named-object".into(),
            Duration::from_millis(500),
            celld_applications::invocation_wire::HttpRequest {
                method: "GET".into(),
                url: "https://example.test/split".into(),
                headers: vec![],
                body: vec![],
            },
        ));
        let invoke = crate::asyncrt::select! {
            message = timeout(Duration::from_secs(5), read_frame(&mut guest)) => HostControl::decode(&message.unwrap().unwrap().unwrap()).unwrap(),
            _ = &mut work => panic!("split invocation ended before dispatch"),
        };
        let HostControl::InvokeHttp(invoke) = invoke else {
            panic!("missing dispatch")
        };
        let mut socket = UnixStream::connect(&path).await.unwrap();
        write_frame(
            &mut socket,
            &StorageConnect {
                version: wire::VERSION,
                invocation: invoke.invocation.clone(),
                grant: invoke.storage_grant.clone(),
                cleanup: false,
                facet: false,
            }
            .encode()
            .unwrap(),
        )
        .await
        .unwrap();
        let grant = &invoke.storage_grant;
        let Value::Turn { turn } = operation(&mut socket, grant, Operation::TurnStart {}).await
        else {
            panic!("missing turn")
        };
        let Value::Transaction { transaction } = operation(
            &mut socket,
            grant,
            Operation::TransactionStart { origin: None },
        )
        .await
        else {
            panic!("missing transaction")
        };
        operation(
            &mut socket,
            grant,
            Operation::PutJson {
                entries: vec![("wire-reply-before".into(), serde_json::json!(true))],
            },
        )
        .await;
        operation(&mut socket, grant, Operation::TurnFinish { turn }).await;
        write_frame(
            &mut guest,
            &GuestControl::HttpReply(HttpResult {
                version: wire::VERSION,
                invocation: invoke.invocation.clone(),
                response: HttpResponse {
                    status: 200,
                    headers: vec![],
                    body: vec![0, 255],
                },
                socket: None,
            })
            .encode()
            .unwrap(),
        )
        .await
        .unwrap();
        crate::asyncrt::select! {
            result = timeout(Duration::from_secs(5), async { while crate::js::live_input_gate_waiters(cell) == 0 { tokio::task::yield_now().await; } }) => result.unwrap(),
            _ = &mut work => panic!("reply crossed held native transaction"),
        }
        crate::asyncrt::select! {
            _ = tokio::time::sleep(Duration::from_millis(600)) => {},
            _ = &mut work => panic!("ready reply charged handler budget or bypassed proof"),
        }
        let Value::Turn { turn } = operation(&mut socket, grant, Operation::TurnStart {}).await
        else {
            panic!("missing resumed turn")
        };
        operation(
            &mut socket,
            grant,
            Operation::TransactionFinish {
                transaction,
                action: TransactionAction::Commit,
            },
        )
        .await;
        operation(&mut socket, grant, Operation::TurnFinish { turn }).await;
        let completion = timeout(Duration::from_secs(5), work)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completion.result.as_ref().unwrap().body, [0, 255]);
        drop(completion);
        assert_eq!(client.pending(), 1);
        assert_eq!(app.public_in_flight(), baseline + 1);
        assert!(matches!(
            app.evict(cell.into()).await,
            Err(crate::actor::EvictError::Refused(
                crate::actor::EvictRefusal::CellActive
            ))
        ));
        let Value::Turn { turn } = operation(&mut socket, grant, Operation::TurnStart {}).await
        else {
            panic!("missing background turn")
        };
        operation(
            &mut socket,
            grant,
            Operation::PutJson {
                entries: vec![("wire-reply-after".into(), serde_json::json!(true))],
            },
        )
        .await;
        operation(&mut socket, grant, Operation::TurnFinish { turn }).await;
        drop(socket);
        write_frame(
            &mut guest,
            &if joined_failure {
                GuestControl::Failed {
                    version: wire::VERSION,
                    invocation: invoke.invocation,
                    message: "controlled background failure".into(),
                }
            } else {
                GuestControl::Drained {
                    version: wire::VERSION,
                    invocation: invoke.invocation,
                }
            }
            .encode()
            .unwrap(),
        )
        .await
        .unwrap();
        timeout(Duration::from_secs(5), async {
            while client.pending() != 0 || app.public_in_flight() != baseline {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let runtime = app.runtime.as_ref().unwrap();
        let admission = runtime
            .admit_storage_request(cell, app.resident_epoch(cell.into()).await.unwrap())
            .unwrap();
        runtime.admitted_storage_turn(&admission, |_| {
            assert!(!storage::sync_sample(cell).unwrap().unwrap().in_transaction);
            for key in ["wire-reply-before", "wire-reply-after"] {
                assert!(matches!(storage::get_stored(cell, key).unwrap(), Some(storage::StoredValue::LegacyJson(value)) if value == "true"));
            }
        }).await.unwrap();
        drop(admission);
        use tokio::io::AsyncWriteExt;
        let (shutdown, ()) = tokio::join!(dispatcher.shutdown(), async {
            assert!(matches!(
                HostControl::decode(&read_frame(&mut guest).await.unwrap().unwrap()).unwrap(),
                HostControl::Shutdown { .. }
            ));
            guest.shutdown().await.unwrap();
        });
        shutdown.unwrap();
        eprintln!("split reply transport: controlled Unix adapter kept original storage pump progressing during native transaction-held proof beyond handler budget; logical reply retained effects/activity until distinct joined drain; both native writes persisted; joined_failure={joined_failure}");
    }
}

/// Controlled wire producer, real original Actor/storage/output gates.
/// Not a language adapter, Firecracker or independent-host proof fixture.
async fn check_stream_reply_transport(
    app: &crate::actor::AppHandle,
    address: &ResourceAddress,
    cell: &str,
) {
    use crate::application_guest::{read_frame, write_frame};
    use celld_applications::invocation_wire::{
        GuestControl, HostControl, HttpStreamHead, StorageConnect,
    };
    use futures_util::StreamExt;
    use tokio::{
        net::{UnixListener, UnixStream},
        time::timeout,
    };
    async fn operation(socket: &mut UnixStream, grant: &str, operation: Operation) -> Value {
        write_frame(
            socket,
            &Request {
                version: wire::VERSION,
                grant: grant.into(),
                operation,
            }
            .encode()
            .unwrap(),
        )
        .await
        .unwrap();
        success(
            Reply::decode(
                &timeout(Duration::from_secs(5), read_frame(socket))
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
            )
            .unwrap(),
        )
    }
    async fn native_operation(
        access: &crate::application_guest::GuestStorage,
        operation: Operation,
    ) -> Value {
        success(
            access
                .execute(Request {
                    version: wire::VERSION,
                    grant: access.grant().into(),
                    operation,
                })
                .await
                .unwrap(),
        )
    }
    for mode in ["normal", "error", "drop", "cancel"] {
        let baseline = app.public_in_flight();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("storage.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let (host, mut guest) = UnixStream::pair().unwrap();
        write_frame(
            &mut guest,
            &GuestControl::Ready {
                version: wire::VERSION,
                adapter: celld_applications::Language::Javascript,
                execution_turns: true,
                quiescent_terminals: true,
                cleanup_turns: true,
                callback_turns: true,
                response_turns: true,
                response_streams: true,
                reply_drain: true,
                resident_objects: false,
                object_retirement: false,
                memory_diagnostics: false,
                idle_lifecycle: false,
            }
            .encode()
            .unwrap(),
        )
        .await
        .unwrap();
        let dispatcher = crate::application_guest_transport::Dispatcher::connect(
            address.application.clone(),
            celld_applications::Language::Javascript,
            host,
            listener,
            None,
        )
        .await
        .unwrap();
        let client = dispatcher.client();
        let mut work = Box::pin(client.invoke_request(
            app.clone(),
            address.clone(),
            "Counter".into(),
            "named-object".into(),
            Some(7301),
            Duration::from_secs(5),
            celld_applications::invocation_wire::HttpRequest {
                method: "GET".into(),
                url: "https://example.test/stream".into(),
                headers: vec![],
                body: vec![],
            },
            None,
        ));
        let invoke = crate::asyncrt::select! {
            message = read_frame(&mut guest) => HostControl::decode(&message.unwrap().unwrap()).unwrap(),
            _ = &mut work => panic!("stream invocation ended before dispatch"),
        };
        let HostControl::InvokeHttp(invoke) = invoke else {
            panic!("missing dispatch")
        };
        let mut socket = UnixStream::connect(&path).await.unwrap();
        write_frame(
            &mut socket,
            &StorageConnect {
                version: wire::VERSION,
                invocation: invoke.invocation.clone(),
                grant: invoke.storage_grant.clone(),
                cleanup: false,
                facet: false,
            }
            .encode()
            .unwrap(),
        )
        .await
        .unwrap();
        // A hello write is not host acceptance. Original guest execution
        // obtains a native turn before emitting its response head.
        let Value::Turn { turn } =
            operation(&mut socket, &invoke.storage_grant, Operation::TurnStart {}).await
        else {
            panic!("initial turn missing")
        };
        operation(
            &mut socket,
            &invoke.storage_grant,
            Operation::TurnFinish { turn },
        )
        .await;
        write_frame(
            &mut guest,
            &GuestControl::HttpStreamHead(HttpStreamHead {
                version: wire::VERSION,
                invocation: invoke.invocation.clone(),
                status: 201,
                headers: vec![
                    ("set-cookie".into(), "one=1".into()),
                    ("set-cookie".into(), "two=2".into()),
                ],
            })
            .encode()
            .unwrap(),
        )
        .await
        .unwrap();
        let mut completion = timeout(Duration::from_secs(5), work)
            .await
            .unwrap()
            .unwrap();
        let response = completion.result.as_mut().unwrap();
        assert_eq!(response.status, 201);
        assert_eq!(response.headers.len(), 2);
        assert!(
            response.body.is_empty()
                && response.write_position.is_none()
                && response.observed_position.is_none()
        );
        let mut body = response.stream.take().unwrap();
        drop(completion);
        assert_eq!(app.public_in_flight(), baseline + 1);
        assert!(matches!(
            app.evict(cell.into()).await,
            Err(crate::actor::EvictError::Refused(
                crate::actor::EvictRefusal::CellActive
            ))
        ));
        let (mut reader, mut writer) = guest.into_split();
        // Keep the read alive across observation, even if a bad producer
        // starts a partial frame. No body poll means no host credit.
        let mut pull = Box::pin(read_frame(&mut reader));
        crate::asyncrt::select! {
            _ = &mut pull => panic!("unpolled body issued credit"),
            _ = tokio::time::sleep(Duration::from_millis(25)) => {},
        }
        let grant = &invoke.storage_grant;
        let Value::Turn { turn } = operation(&mut socket, grant, Operation::TurnStart {}).await
        else {
            panic!("turn missing")
        };
        let Value::Transaction { transaction } = operation(
            &mut socket,
            grant,
            Operation::TransactionStart { origin: None },
        )
        .await
        else {
            panic!("transaction missing")
        };
        operation(
            &mut socket,
            grant,
            Operation::PutJson {
                entries: vec![(
                    if mode == "cancel" {
                        "stream-host-rollback"
                    } else {
                        "stream-host-committed"
                    }
                    .into(),
                    serde_json::json!(true),
                )],
            },
        )
        .await;
        operation(&mut socket, grant, Operation::TurnFinish { turn }).await;
        let mut next = Box::pin(body.next());
        let message = crate::asyncrt::select! {
            message = &mut pull => HostControl::decode(&message.unwrap().unwrap()).unwrap(),
            _ = &mut next => panic!("body published without guest data"),
        };
        assert!(
            matches!(message, HostControl::ResponsePull { invocation, sequence: 0, .. } if invocation == invoke.invocation)
        );
        drop(pull);
        write_frame(
            &mut writer,
            &GuestControl::ResponseChunk {
                version: wire::VERSION,
                invocation: invoke.invocation.clone(),
                sequence: 0,
                data: vec![0, 255],
            }
            .encode()
            .unwrap(),
        )
        .await
        .unwrap();
        crate::asyncrt::select! {
            _ = &mut next => panic!("stream chunk crossed native transaction hold"),
            observed = timeout(Duration::from_secs(5), async {
                while crate::js::live_input_gate_waiters(cell) == 0 { tokio::task::yield_now().await; }
            }) => observed.unwrap(),
        }
        if mode == "cancel" {
            assert!(app
                .drain_pins
                .cancel_request(7301, "stream_proof_cancel")
                .is_some());
            assert_eq!(
                timeout(Duration::from_secs(5), &mut next)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_err(),
                "guest response transport retired"
            );
            drop(next);
            drop(body);
            let cancel = HostControl::decode(
                &timeout(Duration::from_secs(5), read_frame(&mut reader))
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            assert!(
                matches!(cancel, HostControl::Cancel { invocation, .. } if invocation == invoke.invocation)
            );
            assert_eq!(client.pending(), 1);
            assert_eq!(app.public_in_flight(), baseline + 1);
            write_frame(
                &mut writer,
                &GuestControl::Cancelled {
                    version: wire::VERSION,
                    invocation: invoke.invocation.clone(),
                }
                .encode()
                .unwrap(),
            )
            .await
            .unwrap();
        } else {
            let Value::Turn { turn } = operation(&mut socket, grant, Operation::TurnStart {}).await
            else {
                panic!("resumed turn missing")
            };
            operation(
                &mut socket,
                grant,
                Operation::TransactionFinish {
                    transaction,
                    action: TransactionAction::Commit,
                },
            )
            .await;
            operation(&mut socket, grant, Operation::TurnFinish { turn }).await;
            assert_eq!(
                timeout(Duration::from_secs(5), &mut next)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                [0, 255]
            );
            drop(next);
            // Dropped bodies retain the native transaction/admission until a
            // joined cancellation, then roll back its unaccepted suffix.
            if mode == "drop" {
                let Value::Turn { turn } =
                    operation(&mut socket, grant, Operation::TurnStart {}).await
                else {
                    panic!("drop turn missing")
                };
                let Value::Transaction { .. } = operation(
                    &mut socket,
                    grant,
                    Operation::TransactionStart { origin: None },
                )
                .await
                else {
                    panic!("drop transaction missing")
                };
                operation(
                    &mut socket,
                    grant,
                    Operation::PutJson {
                        entries: vec![("stream-host-rollback".into(), serde_json::json!(true))],
                    },
                )
                .await;
                operation(&mut socket, grant, Operation::TurnFinish { turn }).await;
                drop(body);
                let cancel = HostControl::decode(
                    &timeout(Duration::from_secs(5), read_frame(&mut reader))
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap(),
                )
                .unwrap();
                assert!(
                    matches!(cancel, HostControl::Cancel { invocation, .. } if invocation == invoke.invocation)
                );
                assert_eq!(client.pending(), 1);
                assert_eq!(app.public_in_flight(), baseline + 1);
                write_frame(
                    &mut writer,
                    &GuestControl::Cancelled {
                        version: wire::VERSION,
                        invocation: invoke.invocation.clone(),
                    }
                    .encode()
                    .unwrap(),
                )
                .await
                .unwrap();
            } else {
                let mut next = Box::pin(body.next());
                let message = crate::asyncrt::select! {
                    message = read_frame(&mut reader) => HostControl::decode(&message.unwrap().unwrap()).unwrap(),
                    _ = &mut next => panic!("body ended without guest terminal"),
                };
                assert!(matches!(
                    message,
                    HostControl::ResponsePull { sequence: 1, .. }
                ));
                // A different native event holds output while this guest
                // quiesces. Its storage pump may progress; the joined
                // guest's ordinary channel must no longer admit effects.
                let (entered, receive) = tokio::sync::oneshot::channel();
                let (release, blocked) = tokio::sync::oneshot::channel::<()>();
                let peer = tokio::spawn(crate::application_guest::tests::invoke(
                    app.clone(),
                    cell.into(),
                    Some(7302),
                    Duration::from_secs(15),
                    move |access| async move {
                        entered
                            .send(access)
                            .map_err(|_| anyhow::anyhow!("peer fixture retired"))?;
                        blocked.await?;
                        Ok(())
                    },
                ));
                let peer_access = receive.await.unwrap();
                let Value::Turn { turn } =
                    native_operation(&peer_access, Operation::TurnStart {}).await
                else {
                    panic!("terminal turn missing")
                };
                let Value::Transaction { transaction } =
                    native_operation(&peer_access, Operation::TransactionStart { origin: None })
                        .await
                else {
                    panic!("terminal transaction missing")
                };
                native_operation(
                    &peer_access,
                    Operation::PutJson {
                        entries: vec![("stream-host-terminal".into(), serde_json::json!(true))],
                    },
                )
                .await;
                native_operation(&peer_access, Operation::TurnFinish { turn }).await;
                write_frame(
                    &mut writer,
                    &GuestControl::ResponseEnd {
                        version: wire::VERSION,
                        invocation: invoke.invocation.clone(),
                        sequence: 1,
                        error: (mode == "error").then(|| "private producer diagnostic".into()),
                    }
                    .encode()
                    .unwrap(),
                )
                .await
                .unwrap();
                // Joined drain may arrive while the end frame's native proof
                // is still pending. It cannot bypass that publication future.
                write_frame(
                    &mut writer,
                    &GuestControl::Drained {
                        version: wire::VERSION,
                        invocation: invoke.invocation.clone(),
                    }
                    .encode()
                    .unwrap(),
                )
                .await
                .unwrap();
                crate::asyncrt::select! {
                    _ = &mut next => panic!("joined drain bypassed terminal native proof"),
                    observed = timeout(Duration::from_secs(5), async {
                        while crate::js::live_input_gate_waiters(cell) == 0 { tokio::task::yield_now().await; }
                    }) => observed.unwrap(),
                }
                assert_eq!(app.public_in_flight(), baseline + 2);
                timeout(Duration::from_secs(5), async {
                    while !client.joined(&invoke.invocation) {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                write_frame(
                    &mut socket,
                    &Request {
                        version: wire::VERSION,
                        grant: grant.into(),
                        operation: Operation::TurnStart {},
                    }
                    .encode()
                    .unwrap(),
                )
                .await
                .unwrap();
                let refused = Reply::decode(
                    &timeout(Duration::from_secs(5), read_frame(&mut socket))
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap(),
                )
                .unwrap();
                assert!(
                    matches!(refused.outcome, Outcome::Error { message } if message.contains("refused storage admission"))
                );
                let Value::Turn { turn } =
                    native_operation(&peer_access, Operation::TurnStart {}).await
                else {
                    panic!("terminal resumed turn missing")
                };
                native_operation(
                    &peer_access,
                    Operation::TransactionFinish {
                        transaction,
                        action: TransactionAction::Commit,
                    },
                )
                .await;
                native_operation(&peer_access, Operation::TurnFinish { turn }).await;
                release.send(()).unwrap();
                let peer_completion = timeout(Duration::from_secs(5), peer)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert!(peer_completion.result.is_ok());
                drop(peer_completion);
                let result = timeout(Duration::from_secs(5), &mut next).await.unwrap();
                if mode == "error" {
                    assert_eq!(
                        result.unwrap().unwrap_err(),
                        "guest response producer failed"
                    );
                } else {
                    assert!(result.is_none());
                }
                drop(next);
                // A terminal-error stream retains body lifetime until EOF or
                // drop, even if the joined native event already finished.
                if mode == "error" {
                    assert!(body.next().await.is_none());
                }
                drop(body);
            }
        }
        drop(socket);
        timeout(Duration::from_secs(5), async {
            while client.pending() != 0 || app.public_in_flight() != baseline {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let runtime = app.runtime.as_ref().unwrap();
        let admission = runtime
            .admit_storage_request(cell, app.resident_epoch(cell.into()).await.unwrap())
            .unwrap();
        runtime.admitted_storage_turn(&admission, |_| {
                assert!(!storage::sync_sample(cell).unwrap().unwrap().in_transaction);
                assert!(matches!(storage::get_stored(cell, "stream-host-committed").unwrap(), Some(storage::StoredValue::LegacyJson(value)) if value == "true"));
                assert!(storage::get_stored(cell, "stream-host-rollback").unwrap().is_none());
            }).await.unwrap();
        drop(admission);
        use tokio::io::AsyncWriteExt;
        let (shutdown, ()) = tokio::join!(dispatcher.shutdown(), async {
            assert!(matches!(
                HostControl::decode(&read_frame(&mut reader).await.unwrap().unwrap()).unwrap(),
                HostControl::Shutdown { .. }
            ));
            writer.shutdown().await.unwrap();
        });
        shutdown.unwrap();
        eprintln!("trusted host response stream ({mode}): no consumer poll/no credit; native transaction withheld chunk while storage pump committed; escaped body retained Actor/public admission; terminal proof waited on separate native event while joined guest effects refused; generic error or joined cancellation/rollback; controlled Unix producer, not VM/replication qualification");
    }
}

async fn check_live_reply_output(
    app: &crate::actor::AppHandle,
    address: &ResourceAddress,
    cell: &str,
) {
    async fn operation(
        access: &crate::application_guest::GuestStorage,
        operation: Operation,
    ) -> Value {
        success(
            tokio::time::timeout(
                Duration::from_secs(5),
                access.execute(Request {
                    version: wire::VERSION,
                    grant: access.grant().into(),
                    operation,
                }),
            )
            .await
            .expect("early-output wait retained the event mutex")
            .unwrap(),
        )
    }
    let public_before = app.public_in_flight();
    let (entered, receive) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let mut work = Box::pin(crate::application_guest::tests::invoke_scoped(
        app.clone(),
        address.clone(),
        "Counter".into(),
        "named-object".into(),
        Some(105),
        Duration::from_millis(500),
        move |access, response| async move {
            assert!(entered.send((access, response)).is_ok());
            released.await.context("live output test stopped")?;
            Ok(None)
        },
    ));
    let (access, response) = crate::asyncrt::select! {
        access = receive => access.unwrap(),
        _ = &mut work => panic!("event settled before early-output check"),
    };
    let cursors = CursorResidencies::default();
    access.retain_read_cursors(&cursors).await.unwrap();
    let Value::Turn { turn } = operation(&access, Operation::TurnStart {}).await else {
        panic!("turn missing")
    };
    let Value::Transaction { transaction: outer } =
        operation(&access, Operation::TransactionStart { origin: None }).await
    else {
        panic!("transaction missing")
    };
    let Value::Transaction { transaction: inner } =
        operation(&access, Operation::TransactionStart { origin: None }).await
    else {
        panic!("nested transaction missing")
    };
    operation(
        &access,
        Operation::PutJson {
            entries: vec![("early-output-before".into(), serde_json::json!(true))],
        },
    )
    .await;
    operation(&access, Operation::TurnFinish { turn }).await;
    assert!(response.send(Ok(())).is_ok());
    crate::asyncrt::select! {
        _ = &mut work => panic!("early reply sampled its own uncommitted transaction"),
        result = tokio::time::timeout(Duration::from_secs(5), async {
            while crate::js::live_input_gate_waiters(cell) == 0 { tokio::task::yield_now().await; }
        }) => result.expect("early output did not wait at original input gate"),
    }
    crate::asyncrt::select! {
        _ = &mut work => panic!("reply proof or background work was charged to handler budget"),
        _ = tokio::time::sleep(Duration::from_millis(600)) => {},
    }
    let Value::Turn { turn } = operation(&access, Operation::TurnStart {}).await else {
        panic!("resume turn missing")
    };
    operation(
        &access,
        Operation::TransactionFinish {
            transaction: inner,
            action: TransactionAction::Commit,
        },
    )
    .await;
    operation(&access, Operation::TurnFinish { turn }).await;
    crate::asyncrt::select! {
        _ = &mut work => panic!("nested commit opened the outer reply hold"),
        _ = tokio::time::sleep(Duration::from_millis(20)) => {},
    }
    let Value::Turn { turn } = operation(&access, Operation::TurnStart {}).await else {
        panic!("commit turn missing")
    };
    operation(
        &access,
        Operation::TransactionFinish {
            transaction: outer,
            action: TransactionAction::Commit,
        },
    )
    .await;
    operation(&access, Operation::TurnFinish { turn }).await;
    let completion = tokio::time::timeout(Duration::from_secs(5), work)
        .await
        .unwrap()
        .unwrap();
    assert!(completion.result.is_ok());
    drop(completion);
    assert_eq!(app.public_in_flight(), public_before + 1);
    assert_eq!(
        app.evict(cell.into()).await,
        Err(crate::actor::EvictError::Refused(
            crate::actor::EvictRefusal::CellActive
        ))
    );
    let Value::Turn { turn } = operation(&access, Operation::TurnStart {}).await else {
        panic!("background turn missing")
    };
    operation(
        &access,
        Operation::PutJson {
            entries: vec![("early-output-after".into(), serde_json::json!(true))],
        },
    )
    .await;
    operation(&access, Operation::TurnFinish { turn }).await;
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while app.public_in_flight() != public_before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background admission did not finish after release");
    assert!(access
        .prepare_output()
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("retired"));
    cursors.close().await;
    assert_eq!(app.public_in_flight(), public_before);
    eprintln!("host reply/drain split: host-controlled scoped event kept polling during transaction-held reply proof beyond handler budget; delivered proved reply before drain; frontend completion drop retained activity/effects until final native settlement");
    for (request_id, unfinished_cursor) in [(106, false), (107, true)] {
        let (entered, receive) = tokio::sync::oneshot::channel();
        let (mut blocked, waiting) = tokio::sync::oneshot::channel::<()>();
        let joined = tokio_util::sync::CancellationToken::new();
        let witness = joined.clone();
        let cursors = Arc::new(CursorResidencies::default());
        let owner = cursors.clone();
        let mut work = Box::pin(crate::application_guest::tests::invoke_scoped(
            app.clone(),
            address.clone(),
            "Counter".into(),
            "named-object".into(),
            Some(request_id),
            Duration::from_secs(5),
            move |access, response| async move {
                access.retain_read_cursors(&owner).await?;
                access.register_settlement(crate::application_guest::GuestSettlement {
                    event: witness,
                    process: tokio_util::sync::CancellationToken::new(),
                })?;
                if unfinished_cursor {
                    let Value::Turn { turn } = operation(&access, Operation::TurnStart {}).await
                    else {
                        panic!("write cursor turn missing")
                    };
                    operation(
                        &access,
                        Operation::SqlIngest {
                            input: "CREATE TABLE early_reply_cursor(value INTEGER);".into(),
                        },
                    )
                    .await;
                    operation(
                        &access,
                        Operation::SqlStart {
                            query: "INSERT INTO early_reply_cursor VALUES(7) RETURNING value"
                                .into(),
                            bindings: vec![],
                        },
                    )
                    .await;
                    operation(&access, Operation::TurnFinish { turn }).await;
                }
                assert!(response.send(Ok(7u8)).is_ok());
                assert!(entered.send(access).is_ok());
                waiting.await.context("reply drain test stopped")?;
                Ok(None)
            },
        ));
        let access = crate::asyncrt::select_biased! {
            "the delivered access wins a tie with settlement so the premature-settlement panic fires only when no access arrived";
            access = receive => access.unwrap(),
            _ = &mut work => panic!("event settled before drain witness"),
        };
        if unfinished_cursor {
            crate::asyncrt::select! {
                _ = &mut work => panic!("failed early proof delivered before joined settlement"),
                _ = blocked.closed() => {},
            }
        } else {
            let completion = tokio::time::timeout(Duration::from_secs(5), &mut work)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(completion.result.unwrap(), 7);
            drop(completion.activity);
            let Value::Turn { turn } = operation(&access, Operation::TurnStart {}).await else {
                panic!("post-reply turn missing")
            };
            operation(&access, Operation::TransactionStart { origin: None }).await;
            operation(
                &access,
                Operation::PutJson {
                    entries: vec![("reply-drain-uncommitted".into(), serde_json::json!(true))],
                },
            )
            .await;
            operation(&access, Operation::TurnFinish { turn }).await;
            assert_eq!(
                app.drain_pins.cancel_request(request_id, "test_lifecycle"),
                Some("guest_background")
            );
            tokio::time::timeout(Duration::from_secs(5), blocked.closed())
                .await
                .unwrap();
        }
        assert_eq!(app.public_in_flight(), public_before + 1);
        assert_eq!(
            app.evict(cell.into()).await,
            Err(crate::actor::EvictError::Refused(
                crate::actor::EvictRefusal::CellActive
            ))
        );
        let denied = access
            .execute(Request {
                version: wire::VERSION,
                grant: access.grant().into(),
                operation: Operation::GetAlarm {},
            })
            .await
            .unwrap();
        assert!(matches!(denied.outcome, Outcome::Error { .. }));
        joined.cancel();
        if unfinished_cursor {
            let completion = tokio::time::timeout(Duration::from_secs(5), work)
                .await
                .unwrap()
                .unwrap();
            assert!(completion
                .result
                .as_ref()
                .err()
                .unwrap()
                .to_string()
                .contains("unfinished SQL write cursor"));
            drop(completion);
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while app.public_in_flight() != public_before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled/proof-refused drain did not settle");
        assert!(access.prepare_output().await.is_err());
        cursors.close().await;
        let runtime = app.runtime.as_ref().unwrap();
        let epoch = app.resident_epoch(cell.into()).await.unwrap();
        let admission = runtime.admit_storage_request(cell, epoch).unwrap();
        runtime.admitted_storage_turn(&admission, |cells| {
                assert!(!storage::sync_sample(cell).unwrap().unwrap().in_transaction);
                if unfinished_cursor {
                    let mut session = StorageSession::new(cells, cell.into(), epoch).unwrap();
                    let reply = request(&session, Operation::SqlStart { query: "SELECT value FROM early_reply_cursor".into(), bindings: vec![] });
                    assert!(matches!(success(session.execute(cells, true, reply)), Value::Cursor { row: Some(row), .. } if row == vec![wire::SqlValue::Integer { value: 7 }]));
                    session.revoke(cells).unwrap();
                } else {
                    assert!(storage::get_stored(cell, "reply-drain-uncommitted").unwrap().is_none());
                }
            }).await.unwrap();
    }
    eprintln!("host reply/drain failure: lifecycle cancellation after reply and unfinished-cursor proof refusal revoked effects, retained public/activity pins until controlled joined witness, then settled through original native cleanup/proof");
}

async fn check_owned_handler_bridge(
    app: &crate::actor::AppHandle,
    runtime: &crate::runtime::RuntimeManager,
    address: &ResourceAddress,
    cell: &str,
) {
    check_live_reply_output(app, address, cell).await;
    check_split_reply_transport(app, address, cell).await;
    check_stream_reply_transport(app, address, cell).await;
    let (entered, receive) = tokio::sync::oneshot::channel();
    let mut work = Box::pin(crate::application_guest::tests::invoke(
        app.clone(),
        cell.into(),
        Some(102),
        Duration::from_secs(15),
        move |access| async move {
            let abort = Request {
                version: wire::VERSION,
                grant: access.grant().into(),
                operation: Operation::AbortObject {
                    reason: "unscoped abort".into(),
                },
            };
            assert!(access
                .execute(abort)
                .await
                .unwrap_err()
                .to_string()
                .contains("not application scoped"));
            for operation in [
                Operation::TransactionStart { origin: None },
                Operation::PutJson {
                    entries: vec![("uncommitted-handler".into(), serde_json::json!(true))],
                },
                Operation::TransactionStart { origin: None },
                Operation::PutJson {
                    entries: vec![("uncommitted-nested-handler".into(), serde_json::json!(true))],
                },
            ] {
                success(
                    access
                        .execute(Request {
                            version: wire::VERSION,
                            grant: access.grant().into(),
                            operation,
                        })
                        .await?,
                );
            }
            entered.send(()).unwrap();
            std::future::pending::<Result<()>>().await
        },
    ));
    crate::asyncrt::select! {
        _ = receive => {},
        _ = &mut work => panic!("guest completed before frontend drop"),
    }
    assert_eq!(
        app.evict(cell.into()).await,
        Err(crate::actor::EvictError::Refused(
            crate::actor::EvictRefusal::CellActive
        ))
    );
    drop(work);
    tokio::time::timeout(Duration::from_secs(5), async {
        while app.public_in_flight() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned guest cleanup did not finish after frontend drop");
    let epoch = app.resident_epoch(cell.into()).await.unwrap();
    let old = runtime.admit_storage_request(cell, epoch).unwrap();
    runtime
        .admitted_storage_turn(&old, |_| {
            assert!(!storage::sync_sample(cell).unwrap().unwrap().in_transaction);
            assert!(storage::get_stored(cell, "uncommitted-handler")
                .unwrap()
                .is_none());
            assert!(storage::get_stored(cell, "uncommitted-nested-handler")
                .unwrap()
                .is_none());
        })
        .await
        .unwrap();
    for mode in ["deadline", "panic", "error"] {
        let completion = crate::application_guest::tests::invoke(
            app.clone(),
            cell.into(),
            None,
            if mode == "deadline" {
                Duration::from_millis(100)
            } else {
                Duration::from_secs(2)
            },
            move |access| async move {
                for operation in [
                    Operation::TransactionStart { origin: None },
                    Operation::PutJson {
                        entries: vec![("abandoned-handler".into(), serde_json::json!(mode))],
                    },
                ] {
                    let request = Request {
                        version: wire::VERSION,
                        grant: access.grant().into(),
                        operation,
                    };
                    success(access.execute(request).await?);
                }
                match mode {
                    "deadline" => std::future::pending::<Result<()>>().await,
                    "panic" => panic!("guest transport fault injection"),
                    _ => anyhow::bail!("guest handler error"),
                }
            },
        )
        .await
        .unwrap();
        let failure = completion.result.err().unwrap().to_string();
        assert!(failure.contains(if mode == "deadline" {
            "deadline"
        } else if mode == "panic" {
            "panicked"
        } else {
            "handler error"
        }));
        runtime
            .admitted_storage_turn(&old, |_| {
                assert!(!storage::sync_sample(cell).unwrap().unwrap().in_transaction);
                assert!(storage::get_stored(cell, "abandoned-handler")
                    .unwrap()
                    .is_none());
            })
            .await
            .unwrap();
    }
    let native = runtime.clone();
    let scope = cell.to_owned();
    let completion = crate::application_guest::tests::invoke(
            app.clone(), cell.into(), None, Duration::from_secs(5),
            move |access| async move {
                let stopped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                let observe = stopped.clone();
                let request = Request { version: wire::VERSION, grant: access.grant().into(), operation: Operation::PutJson { entries: vec![("queued-after-transport-retirement".into(), serde_json::json!(true))] } };
                let mut queued = std::pin::pin!(access.execute_while(request, || !observe.load(std::sync::atomic::Ordering::Acquire)));
                crate::runtime::check_queued_guest_acceptance(&native, &scope, queued.as_mut(), || {
                    stopped.store(true, std::sync::atomic::Ordering::Release);
                }).await;
                assert!(matches!(queued.await?.outcome, Outcome::Error { message } if message.contains("refused storage admission")));
                Ok(())
            },
        ).await.unwrap();
    completion.result.unwrap();
    drop(completion.activity);
    runtime
        .admitted_storage_turn(&old, |_| {
            assert!(
                storage::get_stored(cell, "queued-after-transport-retirement")
                    .unwrap()
                    .is_none()
            );
        })
        .await
        .unwrap();
    let unfinished = crate::application_guest::tests::invoke(
        app.clone(),
        cell.into(),
        None,
        Duration::from_secs(2),
        move |access| async move {
            for query in [
                "CREATE TABLE unfinished_handler(value INTEGER)",
                "INSERT INTO unfinished_handler VALUES(1),(2) RETURNING value",
            ] {
                let request = Request {
                    version: wire::VERSION,
                    grant: access.grant().into(),
                    operation: Operation::SqlStart {
                        query: query.into(),
                        bindings: vec![],
                    },
                };
                success(access.execute(request).await?);
            }
            Ok(())
        },
    )
    .await
    .unwrap();
    assert!(unfinished
        .result
        .err()
        .unwrap()
        .to_string()
        .contains("unfinished SQL write cursor"));
    drop(unfinished.activity);
    // Host-only fault injection removes a native savepoint without changing
    // the guest capability ledger, exercising a real failed rollback.
    let native = runtime.clone();
    let scope = cell.to_owned();
    let failed = crate::application_guest::tests::invoke(
        app.clone(),
        cell.into(),
        Some(103),
        Duration::from_secs(5),
        move |access| async move {
            for operation in [
                Operation::TransactionStart { origin: None },
                Operation::PutJson {
                    entries: vec![("quarantine-uncommitted".into(), serde_json::json!(true))],
                },
                Operation::TransactionStart { origin: None },
            ] {
                let request = Request {
                    version: wire::VERSION,
                    grant: access.grant().into(),
                    operation,
                };
                success(access.execute(request).await?);
            }
            native
                .storage_turn(&scope, epoch, |_| {
                    storage::transaction_control(&scope, "rollback", true, "cells_tx_2").unwrap()
                })
                .await?;
            Ok(())
        },
    )
    .await;
    assert!(failed
        .err()
        .unwrap()
        .to_string()
        .contains("could not settle"));
    assert!(runtime.admit_storage_request(cell, epoch).is_err());
    assert!(runtime
        .admitted_storage_turn(&old, |_| panic!("quarantined work entered native storage"))
        .await
        .is_err());
    // Recovery uses the real original generation swap. Closing the old
    // connection rolls back its remaining outer transaction; no files or
    // proof receipts are manufactured by the guest driver.
    runtime.adopt(check_generation(address, 2));
    app.tx
        .send(crate::actor::Message::GenerationChanged {
            generation: 2,
            max_age_ms: 0,
            eager_classes: vec!["Counter".into()],
        })
        .unwrap();
    let routed = app.request(cell.into()).await.unwrap();
    let activity = app.activity(routed.request, cell.into());
    let next_epoch = app.resident_epoch(cell.into()).await.unwrap();
    let next = runtime.admit_storage_request(cell, next_epoch).unwrap();
    runtime.quarantine_storage_request(&old);
    runtime
        .admitted_storage_turn(&next, |cells| {
            let mut session = StorageSession::new(cells, cell.into(), next_epoch).unwrap();
            assert!(storage::get_stored(cell, "quarantine-uncommitted")
                .unwrap()
                .is_none());
            session.revoke(cells).unwrap();
        })
        .await
        .unwrap();
    drop(next);
    drop(old);
    drop(activity);
    app.evict(cell.into()).await.unwrap();
    eprintln!("owned guest driver: native response proof, frontend-drop cleanup and failed-rollback quarantine/replacement checked");
}

fn check_object_abort(cells: &Cells, scope: &str) {
    fn operation(session: &mut StorageSession, cells: &Cells, op: Operation) -> Value {
        let request = request(session, op);
        success(session.execute(cells, true, request))
    }
    let mut writer = StorageSession::new(cells, scope.into(), 7).unwrap();
    let abort = request(
        &writer,
        Operation::AbortObject {
            reason: "stop".into(),
        },
    );
    assert!(matches!(
        writer.execute(cells, true, abort).outcome,
        Outcome::Error { .. }
    ));
    assert!(writer.abort_reason.is_none());
    let owner = Arc::new(AtomicBool::new(true));
    let resident = Arc::new(CursorResidency::new(&writer, owner.clone()).unwrap());
    writer.cursor_residency = Some(resident.clone());
    let event = writer.begin_event(cells).unwrap();
    let abort = request(
        &writer,
        Operation::AbortObject {
            reason: "stop".into(),
        },
    );
    assert!(matches!(
        writer.execute(cells, true, abort).outcome,
        Outcome::Error { .. }
    ));
    assert!(!resident.retired.load(Ordering::Acquire));
    operation(&mut writer, cells, Operation::TurnStart {});
    let mut forged = request(
        &writer,
        Operation::AbortObject {
            reason: "forged".into(),
        },
    );
    forged.grant = "0".repeat(64);
    assert!(matches!(
        writer.execute(cells, true, forged).outcome,
        Outcome::Error { .. }
    ));
    assert!(writer.abort_reason.is_none());
    operation(
        &mut writer,
        cells,
        Operation::PutJson {
            entries: vec![("abort-preserved".into(), serde_json::json!(17))],
        },
    );
    let Value::Cursor {
        cursor: Some(cursor),
        ..
    } = operation(
        &mut writer,
        cells,
        Operation::SqlStart {
            query: "SELECT 1 UNION ALL SELECT 2".into(),
            bindings: vec![],
        },
    )
    else {
        panic!("saved read cursor missing")
    };
    operation(
        &mut writer,
        cells,
        Operation::TransactionStart { origin: None },
    );
    operation(
        &mut writer,
        cells,
        Operation::PutJson {
            entries: vec![("abort-uncommitted".into(), serde_json::json!(true))],
        },
    );
    let mut reader = StorageSession::new(cells, scope.into(), 7).unwrap();
    reader.cursor_residency = Some(resident.clone());
    let pending = request(&reader, Operation::TurnStart {});
    let StorageAttempt::Wait {
        mut gate,
        request: pending,
    } = reader.try_execute(cells, true, pending)
    else {
        panic!("object abort did not retain its input hold")
    };
    operation(
        &mut writer,
        cells,
        Operation::AbortObject {
            reason: "explicit stop".into(),
        },
    );
    assert!(resident.retired.load(Ordering::Acquire));
    for op in [
        Operation::PutJson {
            entries: vec![("abort-forbidden".into(), serde_json::json!(true))],
        },
        Operation::SqlNext { cursor },
    ] {
        let attempted = request(&writer, op);
        assert!(matches!(
            writer.execute(cells, true, attempted).outcome,
            Outcome::Error { .. }
        ));
    }
    assert_eq!(
        writer
            .prepare_output(cells, &event, celld_logic::Channel::Response)
            .err()
            .unwrap()
            .to_string(),
        "__CELLD_ACTOR_ABORT__:explicit stop"
    );
    assert!(matches!(
        gate.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    {
        let _installed = cells.install();
        assert!(storage::sync_sample(scope).unwrap().unwrap().in_transaction);
    }
    // VM retirement itself cannot release the original transaction/hold.
    owner.store(false, Ordering::Release);
    assert!(matches!(
        gate.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    let (_, refused) = writer.finish_event(cells, &event).unwrap();
    assert_eq!(
        refused.unwrap().to_string(),
        "__CELLD_ACTOR_ABORT__:explicit stop"
    );
    assert_eq!(
        gate.try_recv().unwrap(),
        Err("__CELLD_ACTOR_ABORT__:explicit stop".into())
    );
    assert!(matches!(
        reader.execute(cells, true, pending).outcome,
        Outcome::Error { .. }
    ));
    reader.revoke(cells).unwrap();
    {
        let _installed = cells.install();
        assert!(!storage::sync_sample(scope).unwrap().unwrap().in_transaction);
        assert_eq!(storage::sql_cursor_count(scope), 0);
        assert!(
            matches!(storage::get_stored(scope, "abort-preserved").unwrap().unwrap(),
                storage::StoredValue::LegacyJson(value) if value == "17")
        );
        for key in ["abort-uncommitted", "abort-forbidden"] {
            assert!(storage::get_stored(scope, key).unwrap().is_none());
        }
    }
    let mut successor = StorageSession::new(cells, scope.into(), 7).unwrap();
    let replacement =
        Arc::new(CursorResidency::new(&successor, Arc::new(AtomicBool::new(true))).unwrap());
    assert_ne!(resident.id, replacement.id);
    successor.cursor_residency = Some(replacement);
    operation(&mut successor, cells, Operation::TurnStart {});
    operation(
        &mut successor,
        cells,
        Operation::Get {
            keys: vec!["abort-preserved".into()],
        },
    );
    successor.revoke(cells).unwrap();
}

fn check_critical_sections(cells: &Cells, scope: &str) {
    fn operation(session: &mut StorageSession, cells: &Cells, op: Operation) -> Value {
        let request = request(session, op);
        success(session.execute(cells, true, request))
    }
    fn start(session: &mut StorageSession, cells: &Cells) -> String {
        let Value::CriticalSection { critical_section } = operation(
            session,
            cells,
            Operation::CriticalSectionStart { origin: None },
        ) else {
            panic!("critical section token missing")
        };
        critical_section
    }
    let mut writer = StorageSession::new(cells, scope.into(), 7).unwrap();
    let owner = Arc::new(AtomicBool::new(true));
    let resident = Arc::new(CursorResidency::new(&writer, owner.clone()).unwrap());
    writer.cursor_residency = Some(resident.clone());
    let event = writer.begin_event(cells).unwrap();
    let Value::Turn { turn } = operation(&mut writer, cells, Operation::TurnStart {}) else {
        panic!()
    };
    let outer = start(&mut writer, cells);
    let inner = start(&mut writer, cells);
    let forged = request(
        &writer,
        Operation::CriticalSectionFinish {
            critical_section: outer.clone(),
            failed: true,
        },
    );
    assert!(matches!(
        writer.execute(cells, true, forged).outcome,
        Outcome::Error { .. }
    ));
    assert!(!writer.critical_failed);
    operation(&mut writer, cells, Operation::TurnFinish { turn });
    let mut reader = StorageSession::new(cells, scope.into(), 7).unwrap();
    reader.cursor_residency = Some(resident.clone());
    let pending = request(&reader, Operation::TurnStart {});
    let StorageAttempt::Wait {
        mut gate,
        request: pending,
    } = reader.try_execute(cells, true, pending)
    else {
        panic!("critical hold did not survive turn finish")
    };
    let Value::Turn { turn } = operation(&mut writer, cells, Operation::TurnStart {}) else {
        panic!()
    };
    operation(
        &mut writer,
        cells,
        Operation::CriticalSectionFinish {
            critical_section: inner,
            failed: false,
        },
    );
    assert!(matches!(
        gate.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    operation(
        &mut writer,
        cells,
        Operation::CriticalSectionFinish {
            critical_section: outer,
            failed: false,
        },
    );
    assert!(matches!(
        gate.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    operation(&mut writer, cells, Operation::TurnFinish { turn });
    assert_eq!(gate.try_recv().unwrap(), Ok(()));
    assert!(writer.finish_event(cells, &event).unwrap().1.is_none());
    success(reader.execute(cells, true, pending));
    reader.revoke(cells).unwrap();

    let mut writer = StorageSession::new(cells, scope.into(), 7).unwrap();
    writer.cursor_residency = Some(resident.clone());
    let event = writer.begin_event(cells).unwrap();
    let Value::Turn { turn } = operation(&mut writer, cells, Operation::TurnStart {}) else {
        panic!()
    };
    operation(
        &mut writer,
        cells,
        Operation::SqlStart {
            query: "SELECT 71 UNION ALL SELECT 72 UNION ALL SELECT 73".into(),
            bindings: vec![],
        },
    );
    let outer = start(&mut writer, cells);
    let inner = start(&mut writer, cells);
    operation(
        &mut writer,
        cells,
        Operation::TransactionStart { origin: None },
    );
    operation(
        &mut writer,
        cells,
        Operation::PutJson {
            entries: vec![("critical-uncommitted".into(), serde_json::json!(true))],
        },
    );
    operation(&mut writer, cells, Operation::TurnFinish { turn });
    let mut reader = StorageSession::new(cells, scope.into(), 7).unwrap();
    reader.cursor_residency = Some(resident.clone());
    let pending = request(&reader, Operation::TurnStart {});
    let StorageAttempt::Wait {
        mut gate,
        request: pending,
    } = reader.try_execute(cells, true, pending)
    else {
        panic!()
    };
    let Value::Turn { turn } = operation(&mut writer, cells, Operation::TurnStart {}) else {
        panic!()
    };
    operation(
        &mut writer,
        cells,
        Operation::CriticalSectionFinish {
            critical_section: inner,
            failed: true,
        },
    );
    // Catching an inner failure and successfully leaving the outer section
    // cannot reopen the gate or restore effect/output authority.
    operation(
        &mut writer,
        cells,
        Operation::CriticalSectionFinish {
            critical_section: outer,
            failed: false,
        },
    );
    let write = request(
        &writer,
        Operation::PutJson {
            entries: vec![("critical-forbidden".into(), serde_json::json!(true))],
        },
    );
    assert!(
        matches!(writer.execute(cells, true, write).outcome, Outcome::Error { message } if message == CRITICAL_SECTION_FAILED)
    );
    operation(&mut writer, cells, Operation::TurnFinish { turn });
    assert!(writer
        .prepare_output(cells, &event, celld_logic::Channel::Response)
        .is_err());
    assert!(matches!(
        gate.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    assert!(writer.finish_event(cells, &event).unwrap().1.is_some());
    assert_eq!(
        gate.try_recv().unwrap(),
        Err(CRITICAL_SECTION_FAILED.into())
    );
    assert!(matches!(
        reader.execute(cells, true, pending).outcome,
        Outcome::Error { .. }
    ));
    // Queued user code is refused, but its joined cancellation/finalizers
    // must still acquire a native turn for the old connection. Retirement
    // never grants storage effects or a successor residency on that channel.
    let cleanup = request(&reader, Operation::TurnStart {});
    let StorageAttempt::Reply(reply) = reader.try_cleanup_turn(cells, true, cleanup) else {
        panic!()
    };
    let Value::Turn { turn } = success(reply) else {
        panic!()
    };
    let finish = request(&reader, Operation::TurnFinish { turn });
    let StorageAttempt::Reply(reply) = reader.try_cleanup_turn(cells, true, finish) else {
        panic!()
    };
    success(reply);
    reader.revoke(cells).unwrap();
    {
        let _installed = cells.install();
        assert!(!storage::sync_sample(scope).unwrap().unwrap().in_transaction);
        assert_eq!(storage::sql_cursor_count(scope), 0);
        for key in ["critical-uncommitted", "critical-forbidden"] {
            assert!(storage::get_stored(scope, key).unwrap().is_none());
        }
    }
    let mut next = StorageSession::new(cells, scope.into(), 7).unwrap();
    let replacement = Arc::new(CursorResidency::new(&next, owner).unwrap());
    assert_ne!(resident.id, replacement.id);
    assert!(!resident.matches(&next));
    next.cursor_residency = Some(replacement);
    let event = next.begin_event(cells).unwrap();
    operation(&mut next, cells, Operation::TurnStart {});
    for _ in 0..MAX_CRITICAL_SECTIONS {
        start(&mut next, cells);
    }
    let overflow = request(&next, Operation::CriticalSectionStart { origin: None });
    assert!(
        matches!(next.execute(cells, true, overflow).outcome, Outcome::Error { message } if message.contains("depth limit"))
    );
    let mut abandoned_waiter = crate::js::cell_gate_wait(scope).unwrap();
    assert!(next.finish_event(cells, &event).unwrap().1.is_some());
    assert_eq!(
        abandoned_waiter.try_recv().unwrap(),
        Err(CRITICAL_SECTION_FAILED.into())
    );
    eprintln!("native critical sections: nested holds survive turns, tokens finish LIFO, failure is sticky, queued events refuse only after rollback, saved cursors retire, replacement gets fresh identity, depth and abandonment bounded");
}

fn check_sync_list_cursor(cells: &Cells, scope: &str) {
    fn run(session: &mut StorageSession, cells: &Cells, operation: Operation) -> Outcome {
        let request = request(session, operation);
        session.execute(cells, true, request).outcome
    }
    fn stale(outcome: Outcome) -> bool {
        matches!(outcome, Outcome::Error { message } if message.contains("invalidated"))
    }
    let list = || Operation::SyncListStart {
        options: wire::ListOptions {
            prefix: Some("sync-list-".into()),
            ..Default::default()
        },
    };
    let next = |cursor: &str| Operation::SyncListNext {
        cursor: cursor.into(),
    };
    let mut session = StorageSession::new(cells, scope.into(), 7).unwrap();
    session.begin_event(cells).unwrap();
    let mut foreign = StorageSession::new(cells, scope.into(), 7).unwrap();
    let open = |session: &mut StorageSession| {
        let Outcome::Ok {
            value:
                Value::Cursor {
                    cursor: Some(cursor),
                    ..
                },
        } = run(session, cells, list())
        else {
            panic!("sync list cursor missing")
        };
        cursor
    };
    let first = open(&mut session);
    // Another session's token is not this cursor (before a transaction
    // closes its input gate).
    assert!(stale(run(&mut foreign, cells, next(&first))));
    run(
        &mut session,
        cells,
        Operation::TransactionStart { origin: None },
    );
    let entries = (0..2)
        .map(|n| (format!("sync-list-{n}"), serde_json::json!(n)))
        .collect();
    run(&mut session, cells, Operation::PutJson { entries });
    let second = open(&mut session);
    assert!(stale(run(&mut session, cells, next(&first))));
    // A numeric token and an SQL cursor are not this cursor either.
    assert!(matches!(
        run(&mut session, cells, next("1")),
        Outcome::Error { .. }
    ));
    let sql = Operation::SqlStart {
        query: "SELECT 1 UNION ALL SELECT 2".into(),
        bindings: vec![],
    };
    let Outcome::Ok {
        value: Value::Cursor {
            cursor: Some(sql), ..
        },
    } = run(&mut session, cells, sql)
    else {
        panic!("SQL cursor missing")
    };
    assert!(stale(run(&mut session, cells, next(&sql))));
    for key in ["sync-list-0", "sync-list-1"] {
        let Outcome::Ok {
            value: Value::Entries { entries },
        } = run(&mut session, cells, next(&second))
        else {
            panic!("sync list entry missing")
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, key);
    }
    let Outcome::Ok {
        value: Value::Entries { entries },
    } = run(&mut session, cells, next(&second))
    else {
        panic!("sync list EOF missing")
    };
    assert!(entries.is_empty());
    assert!(stale(run(&mut session, cells, next(&second))));
    session.revoke(cells).unwrap();
    foreign.revoke(cells).unwrap();
}

fn check_resident_read_cursors(cells: &Cells, scope: &str) {
    let mut first = StorageSession::new(cells, scope.into(), 7).unwrap();
    let owner = Arc::new(AtomicBool::new(true));
    let resident = Arc::new(CursorResidency::new(&first, owner.clone()).unwrap());
    first.cursor_residency = Some(resident.clone());
    let event = first.begin_event(cells).unwrap();
    let ungated = request(&first, Operation::GetAlarm {});
    assert!(
        matches!(first.execute(cells, true, ungated).outcome, Outcome::Error { message } if message.contains("active execution turn"))
    );
    let turn = request(&first, Operation::TurnStart {});
    let Value::Turn { turn: held } = success(first.execute(cells, true, turn)) else {
        panic!("execution turn missing")
    };
    let resume = request(&first, Operation::TurnStart {});
    assert!(
        matches!(first.try_cleanup_turn(cells, true, resume), StorageAttempt::Reply(Reply { outcome: Outcome::Ok { value: Value::Turn { turn } }, .. }) if turn == held)
    );
    let write = request(
        &first,
        Operation::PutJson {
            entries: vec![("cleanup-forged-write".into(), serde_json::json!(true))],
        },
    );
    assert!(
        matches!(first.try_cleanup_turn(cells, true, write), StorageAttempt::Reply(Reply { outcome: Outcome::Error { message }, .. }) if message.contains("only permits execution turns"))
    );
    let wrong_finish = request(
        &first,
        Operation::TurnFinish {
            turn: "0".repeat(64),
        },
    );
    assert!(matches!(
        first.try_cleanup_turn(cells, true, wrong_finish),
        StorageAttempt::Reply(Reply {
            outcome: Outcome::Error { .. },
            ..
        })
    ));
    assert_eq!(first.turn.as_deref(), Some(held.as_str()));
    {
        let _installed = cells.install();
        assert!(storage::get_stored(scope, "cleanup-forged-write")
            .unwrap()
            .is_none());
    }
    let select = request(
        &first,
        Operation::SqlStart {
            query: "SELECT 11 UNION ALL SELECT 22 UNION ALL SELECT 33".into(),
            bindings: vec![],
        },
    );
    let Value::Cursor {
        cursor: Some(cursor),
        ..
    } = success(first.execute(cells, true, select))
    else {
        panic!("resident read cursor missing");
    };
    let native = *resident.cursors.lock().unwrap().get(&cursor).unwrap();
    let (_, refused) = first.finish_event(cells, &event).unwrap();
    assert!(refused.is_none());
    let next = request(
        &first,
        Operation::SqlNext {
            cursor: cursor.clone(),
        },
    );
    assert!(
        matches!(first.execute(cells, true, next).outcome, Outcome::Error { message } if message.contains("revoked"))
    );
    let mut second = StorageSession::new(cells, scope.into(), 7).unwrap();
    second.cursor_residency = Some(resident.clone());
    assert_ne!(first.grant(), second.grant());
    let event = second.begin_event(cells).unwrap();
    // A fresh grant from another VM cannot use this object's saved handle.
    let mut other_vm = StorageSession::new(cells, scope.into(), 7).unwrap();
    let foreign = request(
        &other_vm,
        Operation::SqlNext {
            cursor: cursor.clone(),
        },
    );
    assert!(
        matches!(other_vm.execute(cells, true, foreign).outcome, Outcome::Error { message } if message.contains("no longer active"))
    );
    other_vm.revoke(cells).unwrap();
    let turn = request(&second, Operation::TurnStart {});
    success(second.execute(cells, true, turn));
    let next = request(
        &second,
        Operation::SqlNext {
            cursor: cursor.clone(),
        },
    );
    assert!(
        matches!(success(second.execute(cells, true, next)), Value::Row { row: Some(row), .. } if row == vec![wire::SqlValue::Integer { value: 22 }])
    );
    // A write RETURNING statement must stay event-owned and refuse an
    // unfinished response even while read cursors remain resident.
    let write = request(&second, Operation::SqlStart {
            query: "CREATE TABLE cursor_lifetime(value INTEGER); INSERT INTO cursor_lifetime VALUES(1),(2) RETURNING value".into(), bindings: vec![],
        });
    success(second.execute(cells, true, write));
    assert_eq!(second.cursors.len(), 1);
    let (_, refused) = second.finish_event(cells, &event).unwrap();
    assert!(refused
        .unwrap()
        .to_string()
        .contains("unfinished SQL write cursor"));
    {
        let _installed = cells.install();
        assert_eq!(storage::sql_cursor_readonly(native), Some(true));
        assert_eq!(storage::sql_cursor_count(scope), 1);
        assert!(!storage::sync_sample(scope).unwrap().unwrap().in_transaction);
    }
    let mut third = StorageSession::new(cells, scope.into(), 7).unwrap();
    third.cursor_residency = Some(resident.clone());
    let event = third.begin_event(cells).unwrap();
    let turn = request(&third, Operation::TurnStart {});
    success(third.execute(cells, true, turn));
    let transaction = request(&third, Operation::TransactionStart { origin: None });
    success(third.execute(cells, true, transaction));
    let put = request(
        &third,
        Operation::PutJson {
            entries: vec![(
                "cursor-retirement-uncommitted".into(),
                serde_json::json!(true),
            )],
        },
    );
    success(third.execute(cells, true, put));
    owner.store(false, Ordering::Release);
    let next = request(&third, Operation::SqlNext { cursor });
    assert!(
        matches!(third.execute(cells, true, next).outcome, Outcome::Error { message } if message.contains("owner retired"))
    );
    resident.close(cells);
    assert!(
        third.wait_input(cells).unwrap().is_none(),
        "VM retirement prevented native rollback admission"
    );
    let (_, refused) = third.finish_event(cells, &event).unwrap();
    assert!(refused.unwrap().to_string().contains("owner retired"));
    let _installed = cells.install();
    assert_eq!(storage::sql_cursor_count(scope), 0);
    assert!(storage::get_stored(scope, "cursor-retirement-uncommitted")
        .unwrap()
        .is_none());
    eprintln!("resident SQL cursors: read survived two event retirements with fresh grants; foreign VM and old grants refused; unfinished write output refused; native read finalized on owner retirement");
}

fn check_execution_turn_delegation(cells: &Cells, scope: &str) {
    fn call(session: &mut StorageSession, cells: &Cells, op: Operation) -> Value {
        let request = request(session, op);
        success(session.execute(cells, true, request))
    }
    fn refuse(session: &mut StorageSession, cells: &Cells, op: Operation, message: &str) {
        let request = request(session, op);
        let outcome = session.execute(cells, true, request).outcome;
        assert!(
            matches!(&outcome, Outcome::Error { message: actual } if actual.contains(message)),
            "{outcome:?}"
        );
    }
    fn borrowed(turn: &str, op: Operation) -> Operation {
        Operation::InExecutionTurn {
            turn: turn.into(),
            operation: Box::new(op),
        }
    }
    fn put() -> Operation {
        Operation::PutJson {
            entries: vec![("foreign-checkpoint".into(), serde_json::json!(19))],
        }
    }
    let mut origin = StorageSession::new(cells, scope.into(), 7).unwrap();
    let owner = Arc::new(AtomicBool::new(true));
    let resident = Arc::new(CursorResidency::new(&origin, owner.clone()).unwrap());
    origin.cursor_residency = Some(resident.clone());
    let origin_event = origin.begin_event(cells).unwrap();
    let mut driver = StorageSession::new(cells, scope.into(), 7).unwrap();
    driver.cursor_residency = Some(resident.clone());
    let Value::Turn { turn } = call(&mut driver, cells, Operation::TurnStart {}) else {
        panic!("turn missing")
    };
    call(&mut origin, cells, borrowed(&turn, put()));
    assert!(origin.turn.is_none() && origin.turn_lease.is_none());
    assert_eq!(driver.turn.as_deref(), Some(turn.as_str()));
    assert!(crate::js::GuestInputGate::is_current_holder(
        scope,
        &driver.residency,
        driver.input_gate.event()
    ));
    let Value::Entries { entries } = call(
        &mut origin,
        cells,
        borrowed(
            &turn,
            Operation::Get {
                keys: vec!["foreign-checkpoint".into()],
            },
        ),
    ) else {
        panic!("get missing")
    };
    assert_eq!(
        entries,
        vec![(
            "foreign-checkpoint".into(),
            wire::StoredValue::Json { value: "19".into() }
        )]
    );
    let output = origin
        .prepare_output(cells, &origin_event, celld_logic::Channel::Response)
        .unwrap();
    assert!(output.ticket.position.is_some());
    refuse(
        &mut origin,
        cells,
        Operation::GetAlarm {},
        "active execution turn",
    );
    refuse(
        &mut origin,
        cells,
        borrowed(&"0".repeat(64), put()),
        "not active",
    );
    let mut wrong_vm = StorageSession::new(cells, scope.into(), 7).unwrap();
    wrong_vm.cursor_residency = Some(Arc::new(
        CursorResidency::new(&wrong_vm, Arc::new(AtomicBool::new(true))).unwrap(),
    ));
    refuse(&mut wrong_vm, cells, borrowed(&turn, put()), "not active");
    let mut wrong_object = StorageSession::new(cells, self::scope("two"), 8).unwrap();
    wrong_object.cursor_residency = Some(Arc::new(
        CursorResidency::new(&wrong_object, owner.clone()).unwrap(),
    ));
    refuse(
        &mut wrong_object,
        cells,
        borrowed(&turn, put()),
        "not active",
    );
    for op in [
        Operation::TransactionStart { origin: None },
        Operation::CriticalSectionStart { origin: None },
        Operation::TurnStart {},
        Operation::AbortObject {
            reason: "stop".into(),
        },
        borrowed(&turn, put()),
    ] {
        refuse(
            &mut origin,
            cells,
            borrowed(&turn, op),
            "only carries storage calls",
        );
    }
    let Value::Transaction { transaction } = call(
        &mut driver,
        cells,
        Operation::TransactionStart { origin: None },
    ) else {
        panic!("transaction missing")
    };
    refuse(
        &mut origin,
        cells,
        borrowed(&turn, put()),
        "transaction or critical",
    );
    call(
        &mut driver,
        cells,
        Operation::TransactionFinish {
            transaction,
            action: TransactionAction::Commit,
        },
    );
    call(&mut origin, cells, borrowed(&turn, Operation::GetAlarm {}));
    let Value::CriticalSection { critical_section } = call(
        &mut driver,
        cells,
        Operation::CriticalSectionStart { origin: None },
    ) else {
        panic!("critical missing")
    };
    refuse(
        &mut origin,
        cells,
        borrowed(&turn, put()),
        "transaction or critical",
    );
    call(
        &mut driver,
        cells,
        Operation::CriticalSectionFinish {
            critical_section,
            failed: false,
        },
    );
    call(&mut origin, cells, borrowed(&turn, Operation::GetAlarm {}));
    // A block that the driver holds for one foreign origin admits only
    // that origin's grant; another event and transactions stay refused.
    let mut bystander = StorageSession::new(cells, scope.into(), 7).unwrap();
    bystander.cursor_residency = Some(resident.clone());
    let own_grant = driver.grant.clone();
    refuse(
        &mut driver,
        cells,
        Operation::CriticalSectionStart {
            origin: Some(own_grant),
        },
        "names its own driver",
    );
    let Value::CriticalSection { critical_section } = call(
        &mut driver,
        cells,
        Operation::CriticalSectionStart {
            origin: Some(origin.grant.clone()),
        },
    ) else {
        panic!("foreign critical missing")
    };
    call(&mut origin, cells, borrowed(&turn, put()));
    refuse(
        &mut bystander,
        cells,
        borrowed(&turn, put()),
        "transaction or critical",
    );
    let Value::Transaction { transaction } = call(
        &mut driver,
        cells,
        Operation::TransactionStart { origin: None },
    ) else {
        panic!("transaction missing")
    };
    refuse(
        &mut origin,
        cells,
        borrowed(&turn, put()),
        "transaction or critical",
    );
    call(
        &mut driver,
        cells,
        Operation::TransactionFinish {
            transaction,
            action: TransactionAction::Commit,
        },
    );
    let Value::CriticalSection {
        critical_section: own,
    } = call(
        &mut driver,
        cells,
        Operation::CriticalSectionStart { origin: None },
    )
    else {
        panic!("nested critical missing")
    };
    refuse(
        &mut origin,
        cells,
        borrowed(&turn, put()),
        "transaction or critical",
    );
    for held in [own, critical_section] {
        call(
            &mut driver,
            cells,
            Operation::CriticalSectionFinish {
                critical_section: held,
                failed: false,
            },
        );
    }
    call(
        &mut bystander,
        cells,
        borrowed(&turn, Operation::GetAlarm {}),
    );
    // A delegated savepoint keeps the driver's gate and rollback duty
    // while only its origin's grant writes inside it.
    let Value::Transaction { transaction } = call(
        &mut driver,
        cells,
        Operation::TransactionStart {
            origin: Some(origin.grant.clone()),
        },
    ) else {
        panic!("foreign transaction missing")
    };
    call(
        &mut origin,
        cells,
        borrowed(
            &turn,
            Operation::PutJson {
                entries: vec![("foreign-transaction".into(), serde_json::json!(20))],
            },
        ),
    );
    refuse(
        &mut bystander,
        cells,
        borrowed(&turn, put()),
        "transaction or critical",
    );
    refuse(
        &mut driver,
        cells,
        Operation::TransactionStart { origin: None },
        "mixes events",
    );
    call(
        &mut driver,
        cells,
        Operation::TransactionFinish {
            transaction,
            action: TransactionAction::Rollback,
        },
    );
    let Value::Entries { entries } = call(
        &mut origin,
        cells,
        borrowed(
            &turn,
            Operation::Get {
                keys: vec!["foreign-transaction".into()],
            },
        ),
    ) else {
        panic!("get missing")
    };
    assert!(entries.is_empty(), "rolled-back delegated write survived");
    bystander.revoke(cells).unwrap();
    let invalid_grant = Request {
        version: wire::VERSION,
        grant: "0".repeat(64),
        operation: borrowed(&turn, put()),
    };
    assert!(
        matches!(origin.execute(cells, true, invalid_grant).outcome, Outcome::Error { message } if message.contains("unknown storage handle"))
    );
    let denied = request(&origin, borrowed(&turn, put()));
    assert!(
        matches!(origin.execute(cells, false, denied).outcome, Outcome::Error { message } if message.contains("refused storage admission"))
    );
    call(
        &mut driver,
        cells,
        Operation::TurnFinish { turn: turn.clone() },
    );
    refuse(&mut origin, cells, borrowed(&turn, put()), "not active");
    let cleanup = request(&driver, Operation::TurnStart {});
    let StorageAttempt::Reply(cleanup) = driver.try_cleanup_turn(cells, true, cleanup) else {
        panic!("cleanup waited")
    };
    let Value::Turn { turn: cleanup } = success(cleanup) else {
        panic!("cleanup missing")
    };
    refuse(
        &mut origin,
        cells,
        borrowed(&cleanup, put()),
        "transaction or critical",
    );
    let denied = Request {
        version: wire::VERSION,
        grant: "0".repeat(64),
        operation: Operation::GetAlarm {},
    };
    assert!(matches!(
        driver.execute(cells, true, denied).outcome,
        Outcome::Error { .. }
    ));
    refuse(
        &mut origin,
        cells,
        borrowed(&cleanup, put()),
        "transaction or critical",
    );
    call(&mut driver, cells, Operation::TurnFinish { turn: cleanup });
    let Value::Turn { turn: dropped } = call(&mut driver, cells, Operation::TurnStart {}) else {
        panic!("turn missing")
    };
    drop(driver);
    refuse(&mut origin, cells, borrowed(&dropped, put()), "not active");
    let mut driver = StorageSession::new(cells, scope.into(), 7).unwrap();
    driver.cursor_residency = Some(resident.clone());
    let Value::Turn { turn: revoked } = call(&mut driver, cells, Operation::TurnStart {}) else {
        panic!("turn missing")
    };
    driver.revoke(cells).unwrap();
    refuse(&mut origin, cells, borrowed(&revoked, put()), "not active");
    assert_eq!(resident.turns.lock().unwrap().len(), 0);
    owner.store(false, Ordering::Release);
    refuse(
        &mut origin,
        cells,
        borrowed(&revoked, put()),
        "owner retired",
    );
    origin.revoke(cells).unwrap();
    wrong_vm.revoke(cells).unwrap();
    wrong_object.revoke(cells).unwrap();
    eprintln!("native foreign checkpoint turns: origin grant retained under driver's ordinary host hold; same VM/native residency required, transaction/own-critical/cleanup holds refuse, a driver-held foreign block or savepoint admits only its origin grant and rolls back with the driver, malformed/nested/lifecycle operations refuse, grant/admission/owner/revoke/drop/finished-turn checks remain authoritative; native output position sampled on host");
}

// Retain the native storage contracts independently of a language SDK.
fn check_native_storage_operations(
    cells: &Cells,
    session: &mut StorageSession,
    event: &StorageEvent,
    foreign_grant: &str,
    foreign_cursor: &str,
) {
    fn run(cells: &Cells, session: &mut StorageSession, operation: Operation) -> Value {
        let operation = request(session, operation);
        success(session.execute(cells, true, operation))
    }
    fn transaction(cells: &Cells, session: &mut StorageSession) -> String {
        let Value::Transaction { transaction } =
            run(cells, session, Operation::TransactionStart { origin: None })
        else {
            panic!("transaction missing")
        };
        transaction
    }
    assert!(
        matches!(run(cells, session, Operation::GetActorName {}), Value::ActorName { name: Some(name) } if name == "named-object")
    );
    assert!(
        matches!(run(cells, session, Operation::Get { keys: vec!["js-clone".into()] }), Value::Entries { entries } if entries == vec![("js-clone".into(), wire::StoredValue::V8 { value: vec![255,15,0,254] })])
    );
    run(
        cells,
        session,
        Operation::PutClone {
            entries: vec![wire::CloneEntry {
                key: "clone-copy".into(),
                value: vec![255, 15, 0, 254],
            }],
        },
    );
    assert!(
        matches!(run(cells, session, Operation::Get { keys: vec!["clone-copy".into()] }), Value::Entries { entries } if entries == vec![("clone-copy".into(), wire::StoredValue::V8 { value: vec![255,15,0,254] })])
    );
    let outer = transaction(cells, session);
    run(
        cells,
        session,
        Operation::PutJson {
            entries: vec![("outer".into(), serde_json::json!(1))],
        },
    );
    let inner = transaction(cells, session);
    run(
        cells,
        session,
        Operation::PutJson {
            entries: vec![("inner".into(), serde_json::json!(2))],
        },
    );
    run(cells, session, Operation::SetAlarm { at_ms: 123 });
    run(
        cells,
        session,
        Operation::TransactionFinish {
            transaction: inner,
            action: TransactionAction::Rollback,
        },
    );
    assert!(
        matches!(run(cells, session, Operation::Get { keys: vec!["outer".into(), "inner".into()] }), Value::Entries { entries } if entries == vec![("outer".into(), wire::StoredValue::Json { value: "1".into() })])
    );
    assert!(matches!(
        run(cells, session, Operation::GetAlarm {}),
        Value::Alarm { at_ms: None }
    ));
    run(
        cells,
        session,
        Operation::TransactionFinish {
            transaction: outer,
            action: TransactionAction::Commit,
        },
    );
    let rollback = transaction(cells, session);
    run(
        cells,
        session,
        Operation::PutJson {
            entries: vec![("rolled-back".into(), serde_json::json!(3))],
        },
    );
    run(
        cells,
        session,
        Operation::TransactionFinish {
            transaction: rollback,
            action: TransactionAction::Rollback,
        },
    );
    assert!(
        matches!(run(cells, session, Operation::Get { keys: vec!["rolled-back".into()] }), Value::Entries { entries } if entries.is_empty())
    );
    run(
        cells,
        session,
        Operation::SetAlarm {
            at_ms: 9223372036854775000,
        },
    );
    run(
        cells,
        session,
        Operation::DeleteAll {
            delete_alarm: false,
        },
    );
    assert!(matches!(
        run(cells, session, Operation::GetAlarm {}),
        Value::Alarm {
            at_ms: Some(9223372036854775000)
        }
    ));
    run(cells, session, Operation::DeleteAlarm {});
    let exact = vec![
        wire::SqlValue::Integer { value: i64::MAX },
        wire::SqlValue::Real {
            bits: f64::INFINITY.to_bits(),
        },
        wire::SqlValue::Real {
            bits: (-0.0f64).to_bits(),
        },
        wire::SqlValue::Blob {
            value: vec![0, 255],
        },
        wire::SqlValue::Blob { value: vec![] },
        wire::SqlValue::Null {},
    ];
    let Value::Cursor {
        cursor: Some(cursor),
        row: Some(row),
        ..
    } = run(
        cells,
        session,
        Operation::SqlStart {
            query: "SELECT ?, ?, ?, ?, ?, ?".into(),
            bindings: exact.clone(),
        },
    )
    else {
        panic!("typed SQL row missing")
    };
    assert_eq!(row, exact);
    assert!(matches!(
        run(cells, session, Operation::SqlNext { cursor }),
        Value::Row { row: None, .. }
    ));
    run(
        cells,
        session,
        Operation::SqlStart {
            query: "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(0)".into(),
            bindings: vec![],
        },
    );
    let Value::Cursor {
        cursor: Some(cursor),
        row: Some(row),
        ..
    } = run(
        cells,
        session,
        Operation::SqlStart {
            query: "UPDATE counter SET value = value + 1 RETURNING value".into(),
            bindings: vec![],
        },
    )
    else {
        panic!("write cursor missing")
    };
    assert_eq!(row, vec![wire::SqlValue::Integer { value: 1 }]);
    assert!(session
        .sample_output(cells)
        .err()
        .unwrap()
        .to_string()
        .contains("unfinished SQL write cursor"));
    assert!(session
        .prepare_output(cells, event, celld_logic::Channel::Response)
        .is_err());
    assert!(matches!(
        run(cells, session, Operation::SqlNext { cursor }),
        Value::Row { row: None, .. }
    ));
    let forbidden = request(
        session,
        Operation::SqlStart {
            query: "SELECT * FROM _cf_KV".into(),
            bindings: vec![],
        },
    );
    assert!(matches!(
        session.execute(cells, true, forbidden).outcome,
        Outcome::Error { .. }
    ));
    let foreign = request(
        session,
        Operation::SqlNext {
            cursor: foreign_cursor.into(),
        },
    );
    assert!(matches!(
        session.execute(cells, true, foreign).outcome,
        Outcome::Error { .. }
    ));
    let foreign = Request {
        version: wire::VERSION,
        grant: foreign_grant.into(),
        operation: Operation::PutJson {
            entries: vec![("foreign-write".into(), serde_json::json!(true))],
        },
    };
    assert!(matches!(
        session.execute(cells, true, foreign).outcome,
        Outcome::Error { .. }
    ));
    let writer = session
        .prepare_output(cells, event, celld_logic::Channel::Response)
        .unwrap();
    let reader_event = session.begin_event(cells).unwrap();
    let reader = session
        .prepare_output(cells, &reader_event, celld_logic::Channel::Response)
        .unwrap();
    check_output_core(&writer, &reader);
    transaction(cells, session);
    run(
        cells,
        session,
        Operation::PutJson {
            entries: vec![("uncommitted-on-disconnect".into(), serde_json::json!(true))],
        },
    );
    transaction(cells, session);
    run(
        cells,
        session,
        Operation::PutJson {
            entries: vec![(
                "uncommitted-nested-on-disconnect".into(),
                serde_json::json!(true),
            )],
        },
    );
}

fn check_d1_reply_continuation(cells: &Cells, source: &str, directory: &std::path::Path) {
    let database = source.replacen("Counter:", "__D1Database:", 1);
    {
        let _installed = cells.install();
        storage::open_at_epoch(
            &database,
            directory.join("d1-chunks.sqlite").to_str().unwrap(),
            9,
            false,
            None,
            false,
        )
        .unwrap();
    }
    let mut session = StorageSession::new(cells, database.clone(), 9).unwrap();
    let mut foreign = StorageSession::new(cells, database.clone(), 9).unwrap();
    let query = || {
        Operation::D1Run {
            request: serde_json::json!({"mode":"prepared","sql":"SELECT replace(hex(zeroblob(40000)), '00', 'é☃') AS v","params":[],"first":true}).to_string(),
        }
    };
    let initial = request(&session, query());
    let first = success(session.execute(cells, true, initial));
    let Value::D1 {
        reply: mut assembled,
        complete: false,
    } = first
    else {
        panic!("D1 did not chunk its reply");
    };
    let foreign_next = request(&foreign, Operation::D1Next {});
    assert!(matches!(
        foreign.execute(cells, true, foreign_next).outcome,
        Outcome::Error { .. }
    ));
    let mut forged = request(&session, Operation::D1Next {});
    forged.grant = foreign.grant().to_owned();
    assert!(matches!(
        session.execute(cells, true, forged).outcome,
        Outcome::Error { .. }
    ));
    loop {
        let next = request(&session, Operation::D1Next {});
        let reply = session.execute(cells, true, next);
        assert!(reply.encode().unwrap().len() < wire::MAX_MESSAGE_BYTES);
        let Value::D1 { reply, complete } = success(reply) else {
            panic!("D1 continuation was refused");
        };
        assert!(reply.len() <= celld_applications::invocation_wire::MAX_CHUNK_BYTES);
        assembled.push_str(&reply);
        if complete {
            break;
        }
    }
    let decoded: serde_json::Value = serde_json::from_str(&assembled).unwrap();
    assert_eq!(decoded["ok"]["rows"][0][0], "é☃".repeat(40000));
    assert!(session.d1_reply.is_none());
    let mut upload = StorageSession::new(cells, database.clone(), 9).unwrap();
    let body = serde_json::json!({"mode":"prepared","sql":"SELECT length(?) AS n","params":["é☃".repeat(14000)],"first":true}).to_string();
    let start = || Operation::D1RequestStart {
        bytes: body.len() as u64,
    };
    let first = request(&upload, start());
    success(upload.execute(cells, true, first));
    let duplicate = request(&upload, start());
    assert!(matches!(
        upload.execute(cells, true, duplicate).outcome,
        Outcome::Error { .. }
    ));
    let early_end = request(&upload, Operation::D1RequestEnd {});
    assert!(matches!(
        upload.execute(cells, true, early_end).outcome,
        Outcome::Error { .. }
    ));
    assert!(upload.d1_request.is_none());
    // An oversize chunk abandons the upload, so a fresh start succeeds.
    let small = request(&upload, Operation::D1RequestStart { bytes: 1 });
    success(upload.execute(cells, true, small));
    let oversize = request(&upload, Operation::D1RequestChunk { data: "xx".into() });
    assert!(matches!(
        upload.execute(cells, true, oversize).outcome,
        Outcome::Error { .. }
    ));
    let restarted = request(&upload, start());
    success(upload.execute(cells, true, restarted));
    let mut offset = 0;
    while offset < body.len() {
        let mut end =
            (offset + celld_applications::invocation_wire::MAX_CHUNK_BYTES).min(body.len());
        while !body.is_char_boundary(end) {
            end -= 1;
        }
        let operation = Operation::D1RequestChunk {
            data: body[offset..end].to_owned(),
        };
        let mut forged = request(&upload, operation.clone());
        forged.grant = foreign.grant().to_owned();
        assert!(matches!(
            upload.execute(cells, true, forged).outcome,
            Outcome::Error { .. }
        ));
        let chunk = request(&upload, operation);
        success(upload.execute(cells, true, chunk));
        offset = end;
    }
    let end = request(&upload, Operation::D1RequestEnd {});
    let Value::D1 {
        reply,
        complete: true,
    } = success(upload.execute(cells, true, end))
    else {
        panic!("D1 request did not execute");
    };
    let result: serde_json::Value = serde_json::from_str(&reply).unwrap();
    assert_eq!(result["ok"]["rows"][0][0], 28000);
    let replay = request(&upload, Operation::D1RequestEnd {});
    assert!(matches!(
        upload.execute(cells, true, replay).outcome,
        Outcome::Error { .. }
    ));
    let abandoned = request(&upload, start());
    success(upload.execute(cells, true, abandoned));
    let after_revoke = request(&upload, Operation::D1RequestEnd {});
    upload.revoke(cells).unwrap();
    assert!(upload.d1_request.is_none());
    assert!(matches!(
        upload.execute(cells, true, after_revoke).outcome,
        Outcome::Error { .. }
    ));
    let pending = request(&session, query());
    success(session.execute(cells, true, pending));
    assert!(session.d1_reply.is_some());
    let revoked = request(&session, Operation::D1Next {});
    session.revoke(cells).unwrap();
    assert!(session.d1_reply.is_none());
    assert!(matches!(
        session.execute(cells, true, revoked).outcome,
        Outcome::Error { .. }
    ));
    foreign.revoke(cells).unwrap();
    let _installed = cells.install();
    storage::close(&database);
}

pub(crate) async fn check_storage_bridge() {
    let directory = tempfile::tempdir().unwrap();
    let cells = Cells::default();
    let one = scope("one");
    let two = scope("two");
    assert_ne!(one, two);
    struct CloseCells<'a>(&'a Cells, Vec<String>);
    impl Drop for CloseCells<'_> {
        fn drop(&mut self) {
            let _installed = self.0.install();
            for scope in &self.1 {
                storage::close(scope);
            }
        }
    }
    // Native cursor finalization uses the installed storage context,
    // including when an assertion unwinds before explicit teardown.
    let facet = two.replacen("Counter:", "Facet:", 1);
    let _cleanup = CloseCells(
        &cells,
        vec![
            facet.clone(),
            format!("{facet}-replacement"),
            one.clone(),
            two.clone(),
        ],
    );
    {
        let _installed = cells.install();
        storage::open_at_epoch(
            &one,
            directory.path().join("one.sqlite").to_str().unwrap(),
            7,
            false,
            None,
            false,
        )
        .unwrap();
        storage::open_at_epoch(
            &two,
            directory.path().join("two.sqlite").to_str().unwrap(),
            8,
            false,
            None,
            false,
        )
        .unwrap();
        storage::set_actor_name(&one, "named-object").unwrap();
        storage::put_many_serialized(&one, &[("js-clone".into(), vec![255, 15, 0, 254])]).unwrap();
    }
    let mut session = StorageSession::new(&cells, one.clone(), 7).unwrap();
    let mut foreign = StorageSession::new(&cells, two.clone(), 8).unwrap();
    let event = session.begin_event(&cells).unwrap();
    let first = session
        .prepare_output(&cells, &event, celld_logic::Channel::Response)
        .unwrap();
    assert_eq!(first.scope, one);
    assert_eq!(first.ticket.epoch, Some(7));
    assert!(first.ticket.position.is_none() && first.ticket.observed.is_none());
    let foreign_event = foreign.begin_event(&cells).unwrap();
    assert!(session
        .prepare_output(&cells, &foreign_event, celld_logic::Channel::Response)
        .is_err());
    assert!(session
        .sample_output(&cells)
        .unwrap()
        .observed_position
        .is_none());
    let foreign_request = request(
        &foreign,
        Operation::SqlStart {
            query: "SELECT 123".into(),
            bindings: vec![],
        },
    );
    let Value::Cursor {
        cursor: Some(foreign_cursor),
        ..
    } = success(foreign.execute(&cells, true, foreign_request))
    else {
        panic!("foreign cursor was not opened");
    };
    check_native_storage_operations(
        &cells,
        &mut session,
        &event,
        foreign.grant(),
        &foreign_cursor,
    );
    session.revoke(&cells).unwrap();
    let declined = request(
        &foreign,
        Operation::PutJson {
            entries: vec![("declined".into(), serde_json::json!(true))],
        },
    );
    assert!(matches!(
        foreign.execute(&cells, false, declined).outcome,
        Outcome::Error { .. }
    ));
    let foreign_next = request(
        &foreign,
        Operation::SqlNext {
            cursor: foreign_cursor,
        },
    );
    assert!(matches!(
        success(foreign.execute(&cells, true, foreign_next)),
        Value::Row { row: None, .. }
    ));
    check_facet_output(&cells, &mut foreign, &facet);
    check_facet_sessions(&cells, directory.path());
    check_facet_prove_failure(&cells, directory.path()).await;
    {
        let _installed = cells.install();
        assert!(storage::get_stored(&one, "uncommitted-on-disconnect")
            .unwrap()
            .is_none());
        assert!(
            storage::get_stored(&one, "uncommitted-nested-on-disconnect")
                .unwrap()
                .is_none()
        );
        assert!(storage::get_stored(&two, "foreign-write")
            .unwrap()
            .is_none());
        assert!(storage::get_stored(&two, "declined").unwrap().is_none());
    }
    check_critical_sections(&cells, &one);
    check_object_abort(&cells, &one);
    check_resident_read_cursors(&cells, &one);
    check_sync_list_cursor(&cells, &one);
    check_execution_turn_delegation(&cells, &one);
    check_d1_reply_continuation(&cells, &one, directory.path());
    check_same_epoch_replacement(&cells, &one, &directory.path().join("one.sqlite"));
    let mut stale = StorageSession::new(&cells, one.clone(), 7).unwrap();
    let stale_request = request(
        &stale,
        Operation::Get {
            keys: vec!["retired".into()],
        },
    );
    {
        let _installed = cells.install();
        storage::close(&one);
        storage::open_at_epoch(
            &one,
            directory.path().join("one.sqlite").to_str().unwrap(),
            9,
            false,
            None,
            false,
        )
        .unwrap();
    }
    assert!(
        matches!(stale.execute(&cells, true, stale_request).outcome, Outcome::Error { message } if message.contains("residency changed"))
    );
    let mut successor = StorageSession::new(&cells, one.clone(), 9).unwrap();
    successor.begin_event(&cells).unwrap();
    let select = request(
        &successor,
        Operation::SqlStart {
            query: "SELECT value FROM counter".into(),
            bindings: vec![],
        },
    );
    let Value::Cursor {
        cursor: Some(cursor),
        row: Some(row),
        ..
    } = success(successor.execute(&cells, true, select))
    else {
        panic!("successor failed to read SQL state");
    };
    assert_eq!(row, vec![wire::SqlValue::Integer { value: 1 }]);
    stale.revoke(&cells).unwrap();
    let next = request(&successor, Operation::SqlNext { cursor });
    success(successor.execute(&cells, true, next));
    let sample = successor.sample_output(&cells).unwrap();
    assert_eq!(sample.epoch, 9);
    assert!(!sample.in_transaction && sample.root_output_gate().is_none());
    successor.revoke(&cells).unwrap();
    foreign.revoke(&cells).unwrap();
    let revoked = request(&successor, Operation::GetAlarm {});
    assert!(
        matches!(successor.execute(&cells, true, revoked).outcome, Outcome::Error { message } if message.contains("revoked"))
    );
    let _installed = cells.install();
    storage::close(&one);
    storage::close(&two);
}
