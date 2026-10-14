use super::*;
use crate::application_storage::StorageSession;
use celld_applications::{ApplicationId, ResourceAddress, ResourceKind, ResourceRef};
use futures_util::StreamExt as _;

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

fn cell(app: &str) -> String {
    address(app).cell_scope("Counter", "named-object").unwrap()
}

struct Host {
    channel: Arc<Channel>,
    thread: Option<std::thread::JoinHandle<Vec<(String, storage::StoredValue)>>>,
}

impl Host {
    fn new(cell: String) -> Self {
        Self::named(cell, None)
    }

    fn named(cell: String, name: Option<&str>) -> Self {
        let name = name.map(str::to_owned);
        let (guest, mut host) = UnixStream::pair().unwrap();
        host.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        host.set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let (ready, grant) = std::sync::mpsc::channel();
        let host_cell = cell.clone();
        let thread = std::thread::spawn(move || {
            // Only this host thread owns the original connection. Guest
            // callbacks below install a different Cells through Worker::lock.
            let directory = tempfile::tempdir().unwrap();
            let cells = storage::Cells::default();
            let _installed = cells.install();
            storage::open_at_epoch(
                &host_cell,
                directory.path().join("host.sqlite").to_str().unwrap(),
                7,
                false,
                None,
                false,
            )
            .unwrap();
            if let Some(name) = name {
                storage::set_actor_name(&host_cell, &name).unwrap();
            }
            storage::put_many_json(&host_cell, &[("legacy".into(), "{\"answer\":42}".into())])
                .unwrap();
            let mut session = StorageSession::new(&cells, host_cell.clone(), 7).unwrap();
            ready.send(session.grant().to_owned()).unwrap();
            while let Some(frame) = wire::read_frame(&mut host).unwrap() {
                let request = Request::decode(&frame).unwrap();
                let refused = matches!(&request.operation, Operation::PutClone { entries } if entries.iter().any(|entry| entry.key == "host-refused"));
                let lost = matches!(&request.operation, Operation::PutClone { entries } if entries.iter().any(|entry| entry.key == "lost-reply"));
                let invalid = matches!(&request.operation, Operation::Get { keys } if keys == &["invalid-reply"]);
                let reply = if refused {
                    Reply {
                        version: wire::VERSION,
                        outcome: Outcome::Error {
                            message: "host deliberately refused this write".into(),
                        },
                    }
                } else {
                    session.execute(&cells, true, request)
                };
                if lost {
                    assert!(matches!(
                        reply.outcome,
                        Outcome::Ok {
                            value: Value::Unit {}
                        }
                    ));
                    break; // Commit happened; its reply never reaches V8.
                }
                let reply = if invalid {
                    Reply {
                        version: wire::VERSION,
                        outcome: Outcome::Ok {
                            value: Value::Unit {},
                        },
                    }
                } else {
                    reply
                };
                wire::write_frame(&mut host, &reply.encode().unwrap()).unwrap();
            }
            session.revoke(&cells).unwrap();
            assert!(
                !storage::sync_sample(&host_cell)
                    .unwrap()
                    .unwrap()
                    .in_transaction
            );
            assert_eq!(storage::sql_cursor_count(&host_cell), 0);
            let values =
                storage::list_stored_with_options(&host_cell, None, None, None, None, None, false)
                    .unwrap();
            storage::close(&host_cell);
            values
        });
        let grant = grant.recv_timeout(Duration::from_secs(5)).unwrap();
        Self {
            channel: Channel::new(cell, grant, guest).unwrap(),
            thread: Some(thread),
        }
    }

    fn finish(mut self) -> Vec<(String, storage::StoredValue)> {
        self.channel.retire();
        self.thread.take().unwrap().join().unwrap()
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        self.channel.retire();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

type HostOutcome = (Vec<(String, storage::StoredValue)>, usize, usize);

struct SharedHost {
    channels: Vec<Arc<Channel>>,
    thread: Option<std::thread::JoinHandle<HostOutcome>>,
}

impl SharedHost {
    fn new(cell: &str) -> Self {
        Self::with_channels(cell, 2)
    }

    fn with_channels(cell: &str, count: usize) -> Self {
        let mut streams = Vec::new();
        let mut guests = Vec::new();
        for _ in 0..count {
            let (guest, host) = UnixStream::pair().unwrap();
            guests.push(guest);
            streams.push(host);
        }
        let (ready, receive) = std::sync::mpsc::channel();
        let host_cell = cell.to_owned();
        let thread = std::thread::spawn(move || {
            crate::application_storage::tests::shared_v8_host(host_cell, streams, ready)
        });
        let grants = receive.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_ne!(grants[0], grants[1]);
        Self {
            channels: guests
                .into_iter()
                .zip(grants)
                .map(|(stream, grant)| Channel::new(cell.to_owned(), grant, stream).unwrap())
                .collect(),
            thread: Some(thread),
        }
    }

    fn finish(mut self) -> (Vec<(String, storage::StoredValue)>, usize, usize) {
        for channel in &self.channels {
            channel.retire();
        }
        self.thread.take().unwrap().join().unwrap()
    }
}

impl Drop for SharedHost {
    fn drop(&mut self) {
        for channel in &self.channels {
            channel.retire();
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn evaluate(worker: &Worker, context: &Arc<IoContext>, source: &str) -> Result<String, String> {
    evaluate_in(worker, context, context, source)
}

/// Run with CPED naming `origin` while `driver` owns the checkpoint, as a
/// foreign continuation does.
fn evaluate_in(
    worker: &Worker,
    origin: &Arc<IoContext>,
    driver: &Arc<IoContext>,
    source: &str,
) -> Result<String, String> {
    let (mut locker, _cells) = worker.lock();
    v8::scope!(let hs, &mut *locker);
    let realm = worker.realm(hs);
    let cs = &mut v8::ContextScope::new(hs, realm.context);
    let tc = std::pin::pin!(v8::TryCatch::new(cs));
    let tc = &mut tc.init();
    let _current = CurrentGuard::enter(driver.clone());
    let previous = install_io_context(tc, origin);
    let value = run_internal_snippet(tc, source).map(|value| value.to_rust_string_lossy(tc));
    let result = if let Some(error) = take_execution_termination(tc) {
        Err(error.to_string())
    } else if let Some(value) = value {
        Ok(value)
    } else {
        Err(tc
            .exception()
            .map(|value| value.to_rust_string_lossy(tc))
            .unwrap_or_else(|| "snippet failed".into()))
    };
    restore_io_context(tc, previous);
    result
}

fn check(worker: &Worker, context: &Arc<IoContext>, source: &str) {
    assert_eq!(
        evaluate(worker, context, source).unwrap(),
        "true",
        "{source}"
    );
}

fn checkpoint(worker: &Worker, context: &Arc<IoContext>) {
    let (mut locker, _installed) = worker.lock();
    v8::scope!(let hs, &mut *locker);
    let realm = worker.realm(hs);
    let cs = &mut v8::ContextScope::new(hs, realm.context);
    let _current = CurrentGuard::enter(context.clone());
    let previous = install_io_context(cs, context);
    cs.perform_microtask_checkpoint();
    assert!(take_execution_termination(cs).is_none());
    restore_io_context(cs, previous);
}

fn fetch_job(
    cell: &str,
    name: Option<&str>,
    path: &str,
) -> (
    CellJob,
    tokio::sync::oneshot::Receiver<Result<HttpResponse>>,
) {
    let (reply, receive) = tokio::sync::oneshot::channel();
    (
        CellJob::Fetch {
            request_id: None,
            scope: cell.to_owned(),
            name: name.map(str::to_owned),
            url: format!("http://check{path}"),
            method: "GET".into(),
            body: RequestBody::Bytes(bytes::Bytes::new()),
            headers: vec![],
            reply,
            order: None,
        },
        receive,
    )
}

// Test-only operation pump. Real guest scheduling must also coordinate
// foreign continuations, native critical sections, and reply publication.
async fn drain_entry(worker: &mut Worker, entry: &mut InFlight, ops: Vec<Op>, channel: &Channel) {
    async fn schedule((id, op): Op) -> (u64, Result<asyncrt::OpOut, String>) {
        (id, op.await)
    }
    let mut ops: futures_util::stream::FuturesUnordered<_> =
        ops.into_iter().map(schedule).collect();
    for _ in 0..4096 {
        if entry.finished() {
            return;
        }
        let (id, result) = tokio::time::timeout(Duration::from_secs(5), ops.next())
            .await
            .unwrap()
            .expect("entry is unfinished without an operation");
        channel.begin_turn().unwrap();
        ops.extend(
            worker
                .turn_deliver_guest_cell(entry, id, result)
                .unwrap()
                .into_iter()
                .map(schedule),
        );
        channel.finish_turn().unwrap();
    }
    panic!("guest entry did not retire");
}

pub(crate) async fn check_original_guest_cancellation() {
    // An ordinary admission task can hold this mutex while awaiting a
    // native gate. Revocation and cancelled-effect refusal must not wait
    // for that task, otherwise the V8 scheduler cannot read cancellation.
    let (ordinary, _retained) = UnixStream::pair().unwrap();
    let blocked = Channel::new(cell("blocked-cancellation"), "c".repeat(64), ordinary).unwrap();
    let (locked, ready) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let held = blocked.clone();
    let holder = std::thread::spawn(move || {
        let _state = held.state.lock().unwrap();
        locked.send(()).unwrap();
        released.recv_timeout(Duration::from_secs(5)).unwrap();
    });
    ready.recv_timeout(Duration::from_secs(5)).unwrap();
    let start = std::time::Instant::now();
    blocked.request_cleanup();
    assert!(execution_turn(&blocked)
        .unwrap_err()
        .to_string()
        .contains("lifecycle channel refuses"));
    cancel(&blocked);
    assert!(start.elapsed() < Duration::from_secs(1));
    release.send(()).unwrap();
    holder.join().unwrap();
    blocked.retire();
    // A refused/corrupt cleanup response cannot leave the old execution
    // token usable for cancellation code, or close the native effect pump.
    for outcome in [
        Outcome::Error {
            message: "host refused cleanup".into(),
        },
        Outcome::Ok {
            value: Value::Unit {},
        },
        Outcome::Ok {
            value: Value::Turn {
                turn: "b".repeat(64),
            },
        },
    ] {
        let scope = cell("cleanup-reply");
        let (ordinary, mut retained) = UnixStream::pair().unwrap();
        let channel = Channel::new(scope.clone(), "c".repeat(64), ordinary).unwrap();
        channel.state.lock().unwrap().turn = Some("a".repeat(64));
        let (cleanup, mut host) = UnixStream::pair().unwrap();
        cleanup
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        cleanup
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let responder = std::thread::spawn(move || {
            let hello = celld_applications::invocation_wire::StorageConnect::decode(
                &wire::read_frame(&mut host).unwrap().unwrap(),
            )
            .unwrap();
            assert!(hello.cleanup && hello.grant == "c".repeat(64));
            let request = Request::decode(&wire::read_frame(&mut host).unwrap().unwrap()).unwrap();
            assert!(matches!(request.operation, Operation::TurnStart {}));
            wire::write_frame(
                &mut host,
                &Reply {
                    version: wire::VERSION,
                    outcome,
                }
                .encode()
                .unwrap(),
            )
            .unwrap();
        });
        assert!(channel.begin_cleanup("f".repeat(64), cleanup).is_err());
        assert!(channel.require_turn(&scope).is_err());
        assert!(!channel.state.lock().unwrap().cleanup_admitted);
        assert!(channel.state.lock().unwrap().turn.is_none());
        retained.set_nonblocking(true).unwrap();
        assert_eq!(
            std::io::Read::read(&mut retained, &mut [0])
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::WouldBlock
        );
        channel.retire();
        responder.join().unwrap();
    }
    for mode in ["ready", "async", "never"] {
        check_original_guest_cancellation_event(mode).await;
    }
}

async fn check_original_guest_cancellation_event(mode: &str) {
    Engine::init();
    let resource = address("guest-cancellation");
    let cell = resource.cell_scope("Counter", "named-object").unwrap();
    let (ordinary, host_ordinary) = UnixStream::pair().unwrap();
    let (cleanup, host_cleanup) = UnixStream::pair().unwrap();
    cleanup
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    cleanup
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let (ready, grant) = std::sync::mpsc::channel();
    let (snapshots, requested) = std::sync::mpsc::channel();
    let host_cell = cell.clone();
    let host = std::thread::spawn(move || {
        crate::application_storage::tests::cancelled_v8_host(
            host_cell,
            host_ordinary,
            host_cleanup,
            ready,
            requested,
            None,
        )
    });
    let channel = Channel::new(
        cell.clone(),
        grant.recv_timeout(Duration::from_secs(5)).unwrap(),
        ordinary,
    )
    .unwrap();
    let source = r#"
            globalThis.armed = false;
            globalThis.abortSeen = false;
            globalThis.cleanupRuns = 0;
            globalThis.cleanupDenied = false;
            globalThis.cleanupComplete = false;
            globalThis.cleanupLate = 0;
            export class Counter {
                constructor(ctx) { this.ctx = ctx; }
                async fetch(request) {
                    await this.ctx.storage.put('acknowledged', 9);
                    await this.ctx.storage.transaction(async tx => {
                        await tx.put('uncommitted', 99);
                        armed = true;
                        try {
                            await new Promise((resolve, reject) => {
                                request.signal.addEventListener('abort', () => {
                                    abortSeen = request.signal.aborted;
                                    reject(new Error('cancelled by signal'));
                                }, { once: true });
                            });
                        } finally {
                            cleanupRuns++;
                            if ('__MODE__' === 'never') await new Promise(() => {});
                            if ('__MODE__' === 'async') {
                                Promise.allSettled = () => Promise.resolve([]);
                                this.ctx.waitUntil((async () => {
                                    await new Promise(resolve => setTimeout(resolve, 15));
                                    cleanupLate++;
                                    this.ctx.waitUntil((async () => {
                                        await new Promise(resolve => setTimeout(resolve, 15));
                                        cleanupLate++;
                                    })());
                                })());
                                await new Promise(resolve => setTimeout(resolve, 15));
                            }
                            try { this.ctx.storage.sql.exec('CREATE TABLE forbidden(value)'); }
                            catch(e) { cleanupDenied = e.message.includes('lifecycle channel refuses') || e.message.includes("this event's storage is closed"); }
                            cleanupComplete = true;
                        }
                    });
                    return new Response('must not reply');
                }
            }
            export default { fetch() { return new Response('guest'); } };
        "#;
    let mut worker = Worker::load_guest_config(Arc::new(WorkerConfig::new(WorkerConfigOptions {
        src: source.replace("__MODE__", mode),
        script_name: resource.storage_root(),
        do_classes: vec!["Counter".into()],
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
    })))
    .unwrap();
    channel.begin_turn().unwrap();
    let (mut job, mut reply) = fetch_job(&cell, Some("named-object"), "/cancel");
    let CellJob::Fetch { request_id, .. } = &mut job else {
        unreachable!()
    };
    *request_id = Some(next_request_id());
    let (entry, mut ops) = worker
        .turn_begin_guest_cell(job, None, channel.clone())
        .unwrap();
    let mut entry = entry.unwrap();
    channel.finish_turn().unwrap();
    for _ in 0..32 {
        channel.begin_turn().unwrap();
        let armed = evaluate(&worker, &entry.context, "return armed;").unwrap() == "true";
        channel.finish_turn().unwrap();
        if armed {
            break;
        }
        let (id, operation) = ops.pop().expect("cancel fixture stopped before arming");
        let result = tokio::time::timeout(Duration::from_secs(5), operation)
            .await
            .unwrap();
        channel.begin_turn().unwrap();
        ops.extend(
            worker
                .turn_deliver_guest_cell(&mut entry, id, result)
                .unwrap(),
        );
        channel.finish_turn().unwrap();
    }
    assert!(entry.cancellable());
    assert!(matches!(
        reply.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    channel.begin_turn().unwrap();
    check(
        &worker,
        &entry.context,
        "return armed && !abortSeen && cleanupRuns === 0;",
    );
    assert!(worker
        .turn_cancel_guest_cell(&mut entry)
        .err()
        .unwrap()
        .to_string()
        .contains("cleanup-only"));
    let previous = channel.state.lock().unwrap().turn.clone();
    channel.begin_cleanup("f".repeat(64), cleanup).unwrap();
    assert_eq!(channel.state.lock().unwrap().turn, previous);
    assert!(channel
        .state
        .lock()
        .unwrap()
        .retained_effect_stream
        .is_some());
    let mut cleanup_ops = worker.turn_cancel_guest_cell(&mut entry).unwrap();
    drop(ops);
    if mode == "async" {
        assert!(!entry.finished() && !cleanup_ops.is_empty());
        channel.finish_turn().unwrap();
        for _ in 0..32 {
            if entry.finished() {
                break;
            }
            let (id, operation) = cleanup_ops
                .pop()
                .expect("asynchronous cleanup lost owned operations");
            let result = tokio::time::timeout(Duration::from_secs(5), operation)
                .await
                .unwrap();
            channel.begin_turn().unwrap();
            cleanup_ops.extend(
                worker
                    .turn_deliver_guest_cell(&mut entry, id, result)
                    .unwrap(),
            );
            channel.finish_turn().unwrap();
        }
        assert!(entry.finished());
        channel.begin_turn().unwrap();
        check(
            &worker,
            &entry.context,
            "return cleanupComplete && cleanupLate === 2;",
        );
    } else {
        assert!(cleanup_ops.is_empty());
    }
    if mode == "never" {
        check(
            &worker,
            &entry.context,
            "return abortSeen && cleanupRuns === 1 && !cleanupComplete;",
        );
        assert!(
            !entry.finished(),
            "pending cleanup without native I/O published a terminal"
        );
        assert!(entry
            .cleanup
            .as_ref()
            .is_some_and(|promises| !promises.is_empty()));
    } else {
        check(
            &worker,
            &entry.context,
            "return abortSeen && cleanupRuns === 1 && cleanupDenied;",
        );
        assert!(entry.finished());
    }
    assert!(worker.runtime_state.promises.lock().unwrap().is_empty());
    assert_eq!(
        channel.state.lock().unwrap().transactions.is_empty(),
        mode != "never"
    );
    assert!(channel.state.lock().unwrap().critical_sections.is_empty());
    assert!(reply
        .try_recv()
        .unwrap()
        .err()
        .unwrap()
        .to_string()
        .contains("disconnected"));
    channel.finish_turn().unwrap();
    let (snapshot, result) = std::sync::mpsc::channel();
    snapshots.send(snapshot).unwrap();
    assert_eq!(
        result.recv_timeout(Duration::from_secs(5)).unwrap(),
        (1, 1, true, true)
    );
    // Non-cooperative local cleanup disposes the Worker, not a successful
    // adapter terminal. The separate VM case must prove physical reaping.
    drop(entry);
    drop(worker);
    channel.retire();
    let values = host.join().unwrap();
    assert!(values.iter().any(|(key, _)| key == "acknowledged"));
    assert!(!values
        .iter()
        .any(|(key, _)| key == "uncommitted" || key == "forbidden"));
    eprintln!("original V8 cancellation bridge ({mode}): same-grant cleanup retained native holds until trusted settlement; raw handler/waitUntil promises joined with late registrations despite overridden allSettled, or never-settling cleanup refused retirement and disposed the Worker; prefix retained, uncommitted/forbidden absent; this check uses local sockets, separate VM evidence required");
}

pub(crate) async fn check_original_guest_retirement() {
    Engine::init();
    let resource = address("guest-retirement");
    let cell = resource.cell_scope("Counter", "named-object").unwrap();
    let source = r#"
            globalThis.constructed = 0;
            globalThis.cleanupDenied = false;
            globalThis.asyncRelease = false;
            globalThis.releaseDone = false;
            globalThis.lateDone = false;
            globalThis.releaseMode = '';
            export class Counter {
                constructor(ctx) {
                    this.ctx = ctx; this.count = 0; constructed++;
                    ctx.facets._release = async () => {
                        if (releaseMode === '/never') await new Promise(() => {});
                        if (releaseMode === '/throw') throw new Error('release failed');
                        if (asyncRelease) {
                            Promise.allSettled = () => Promise.resolve([]);
                            ctx.waitUntil((async () => {
                                await new Promise(resolve => setTimeout(resolve, 5));
                                ctx.waitUntil((async () => {
                                    await new Promise(resolve => setTimeout(resolve, 5));
                                    lateDone = true;
                                })());
                            })());
                            await new Promise(resolve => setTimeout(resolve, 5));
                            releaseDone = true;
                        }
                        try { ctx.storage.sql.exec('CREATE TABLE forbidden(value)'); }
                        catch(e) { cleanupDenied = e.message.includes('lifecycle channel refuses'); }
                    };
                }
                async fetch(request) {
                    asyncRelease = new URL(request.url).pathname === '/arm';
                    releaseMode = new URL(request.url).pathname;
                    let persisted = await this.ctx.storage.get('durable');
                    if (persisted === undefined) {
                        persisted = 7;
                        await this.ctx.storage.put('durable', persisted);
                    }
                    return new Response(JSON.stringify({ count: ++this.count,
                        constructed, persisted, name: this.ctx.id.name, cleanupDenied }));
                }
            }
            export default { fetch() { return new Response('guest'); } };
        "#;
    let mut worker = Worker::load_guest_config(Arc::new(WorkerConfig::new(WorkerConfigOptions {
        src: source.into(),
        script_name: resource.storage_root(),
        do_classes: vec!["Counter".into()],
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
    })))
    .unwrap();
    // A deliberately planted guest-local database proves lifecycle
    // release cannot fall through to the native SQLite adoption half.
    let trap = tempfile::tempdir().unwrap();
    {
        let (_locker, _installed) = worker.lock();
        storage::open(&cell, trap.path().join("trap.sqlite").to_str().unwrap()).unwrap();
        storage::put_many_json(&cell, &[("native-trap".into(), "22".into())]).unwrap();
    }
    let host = SharedHost::with_channels(&cell, 8);
    let expected = |response: HttpResponse, constructed: u64, denied: bool| {
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
            serde_json::json!({"count":1,"constructed":constructed,"persisted":7,
                    "name":"named-object","cleanupDenied":denied})
        );
    };
    host.channels[0].begin_turn().unwrap();
    let (job, receive) = fetch_job(&cell, Some("named-object"), "/fetch");
    let (entry, ops) = worker
        .turn_begin_guest_cell(job, None, host.channels[0].clone())
        .unwrap();
    host.channels[0].finish_turn().unwrap();
    if let Some(mut entry) = entry {
        drain_entry(&mut worker, &mut entry, ops, &host.channels[0]).await;
    }
    expected(receive.await.unwrap().unwrap(), 1, false);
    host.channels[0].retire();

    let cleanup = host.channels[1].clone();
    cleanup.begin_turn().unwrap();
    let (reply, _) = tokio::sync::oneshot::channel();
    assert!(worker
        .turn_release_guest_cell(&cell, cleanup.clone(), reply)
        .err()
        .unwrap()
        .to_string()
        .contains("cleanup-only"));
    cleanup.restrict_to_cleanup();
    let (reply, _) = tokio::sync::oneshot::channel();
    assert!(worker
        .turn_release_guest_cell(
            &resource.cell_scope("Counter", "other-object").unwrap(),
            cleanup.clone(),
            reply
        )
        .err()
        .unwrap()
        .to_string()
        .contains("another object"));
    assert!(queue(&cleanup, vec![("forbidden".into(), vec![1])])
        .unwrap_err()
        .contains("lifecycle channel refuses"));
    let (reply, receive) = tokio::sync::oneshot::channel();
    let (entry, ops) = worker
        .turn_release_guest_cell(&cell, cleanup.clone(), reply)
        .unwrap();
    cleanup.finish_turn().unwrap();
    if let Some(mut entry) = entry {
        drain_entry(&mut worker, &mut entry, ops, &cleanup).await;
    }
    assert_eq!(receive.await.unwrap().unwrap(), None);
    cleanup.retire();
    {
        let (_locker, _installed) = worker.lock();
        assert!(matches!(storage::get_stored(&cell, "native-trap").unwrap(),
                Some(storage::StoredValue::LegacyJson(value)) if value == "22"));
    }
    host.channels[2].begin_turn().unwrap();
    let (job, receive) = fetch_job(&cell, Some("named-object"), "/arm");
    let (entry, ops) = worker
        .turn_begin_guest_cell(job, None, host.channels[2].clone())
        .unwrap();
    host.channels[2].finish_turn().unwrap();
    if let Some(mut entry) = entry {
        drain_entry(&mut worker, &mut entry, ops, &host.channels[2]).await;
    }
    expected(receive.await.unwrap().unwrap(), 2, true);
    host.channels[2].retire();
    let cleanup = host.channels[3].clone();
    cleanup.restrict_to_cleanup();
    cleanup.begin_turn().unwrap();
    let (reply, receive) = tokio::sync::oneshot::channel();
    let (entry, ops) = worker
        .turn_release_guest_cell(&cell, cleanup.clone(), reply)
        .unwrap();
    let mut entry = entry.expect("asynchronous release must retain its event");
    assert!(!entry.finished());
    cleanup.finish_turn().unwrap();
    drain_entry(&mut worker, &mut entry, ops, &cleanup).await;
    assert_eq!(receive.await.unwrap().unwrap(), None);
    check(
        &worker,
        &entry.context,
        "return releaseDone && lateDone && cleanupDenied;",
    );
    assert!(asyncrt::drain_spawns().is_empty());
    assert!(worker.runtime_state.promises.lock().unwrap().is_empty());
    cleanup.retire();
    for (index, path) in [(4, "/never"), (6, "/throw")] {
        let channel = host.channels[index].clone();
        channel.begin_turn().unwrap();
        let (job, receive) = fetch_job(&cell, Some("named-object"), path);
        let (entry, ops) = worker
            .turn_begin_guest_cell(job, None, channel.clone())
            .unwrap();
        channel.finish_turn().unwrap();
        if let Some(mut entry) = entry {
            drain_entry(&mut worker, &mut entry, ops, &channel).await;
        }
        receive.await.unwrap().unwrap();
        channel.retire();
        let cleanup = host.channels[index + 1].clone();
        cleanup.restrict_to_cleanup();
        cleanup.begin_turn().unwrap();
        let (reply, mut receive) = tokio::sync::oneshot::channel();
        let (entry, ops) = worker
            .turn_release_guest_cell(&cell, cleanup.clone(), reply)
            .unwrap();
        assert!(ops.is_empty());
        if path == "/never" {
            let mut entry = entry.unwrap();
            assert!(!entry.finished());
            assert!(matches!(
                receive.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
            assert!(worker.turn_poll_guest_cell(&mut entry).unwrap().is_empty());
            assert!(!entry.finished(), "pending raw promise without I/O retired");
            entry.abandon();
        } else {
            assert!(receive
                .await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("release failed"));
        }
        // Local fixture teardown only. The actual scheduler must refuse
        // ObjectRetired and the host must reap an unsuccessful guest.
        cleanup.finish_turn().unwrap();
        cleanup.retire();
    }
    let (values, _, _) = host.finish();
    assert!(values.iter().any(|(key, _)| key == "durable"));
    assert!(!values.iter().any(|(key, _)| key == "forbidden"));
    eprintln!("original V8 idle retirement: original cell release reset instance/name registries, constructor ran again with host-persisted data/name; ready and asynchronous cleanup joined under lifecycle turns, including nested late waitUntil despite overridden allSettled; cleanup-only storage/queued effects and wrong-cell/unrestricted release refused; no operation/resolver leak; planted guest-local SQLite remained untouched; local sockets, real-VM path checked separately");
}

/// A stateless guest fetch reads a credited request-body stream through
/// the original Worker request path.
pub(crate) async fn check_guest_request_stream() {
    Engine::init();
    let mut worker =
            Worker::load_guest_config(Arc::new(WorkerConfig::new(WorkerConfigOptions {
                src: "export default { async fetch(request) { let total = 0; for await (const chunk of request.body) total += chunk.byteLength; return new Response(String(total)); } };".into(),
                script_name: "guest-request-stream".into(),
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
            })))
            .unwrap();
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let stream = crate::js::register_body_stream(Box::pin(
        tokio_stream::wrappers::ReceiverStream::new(receiver),
    ))
    .unwrap();
    let feed = tokio::spawn(async move {
        for _ in 0..3 {
            sender.send(Ok(vec![5u8; 65536])).await.unwrap();
        }
    });
    let (reply, mut response) = tokio::sync::oneshot::channel();
    let (entry, ops) = worker.turn_begin(
        crate::WorkerJob::Fetch {
            queued_at: std::time::Instant::now(),
            entrypoint: None,
            invocation_limits: None,
            url: "http://check/upload".into(),
            method: "POST".into(),
            body: RequestBody::Stream(stream),
            headers: vec![],
            request_id: Some(crate::js::next_request_id()),
            tail_report: None,
            reply,
        },
        None,
    );
    let mut entry = entry.expect("stream fetch finished before reading");
    async fn run((id, op): Op) -> (u64, Result<asyncrt::OpOut, String>) {
        (id, op.await)
    }
    let mut pending: futures_util::stream::FuturesUnordered<_> = ops.into_iter().map(run).collect();
    while !entry.finished() {
        let (id, result) = tokio::time::timeout(Duration::from_secs(5), pending.next())
            .await
            .unwrap()
            .expect("stream fetch is waiting on nothing");
        pending.extend(
            worker
                .turn_deliver(&mut entry, id, result)
                .into_iter()
                .map(run),
        );
    }
    feed.await.unwrap();
    let response = response.try_recv().unwrap().unwrap();
    assert_eq!(response.body, (3 * 65536).to_string().into_bytes());
    eprintln!("guest request stream: a stateless guest fetch read a credited host body stream through the original Worker request path; local check");
}

pub(crate) async fn check_original_guest_entry() {
    Engine::init();
    let address = address("guest-entry");
    let one = address.cell_scope("Counter", "named-object").unwrap();
    let two = address.cell_scope("Counter", "other-object").unwrap();
    let source = format!(
        r#"
            globalThis.moduleTarget = {};
            globalThis.moduleEgressDenied = false;
            fetch('http://127.0.0.1:1/').catch(e => {{
                moduleEgressDenied = e.message.includes('not permitted to access the internet');
            }});
            globalThis.constructed = [];
            globalThis.alarmEvents = [];
            globalThis.alarmMode = 'ok';
            export class Counter {{
                constructor(ctx) {{
                    if (alarmMode === 'constructor') throw new Error('alarm constructor rejected');
                    this.ctx = ctx;
                    this.name = ctx.id.name;
                    constructed.push(this.name);
                    ctx.storage.sql.exec('CREATE TABLE fixture(value TEXT)');
                    ctx.storage.sql.exec('INSERT INTO fixture VALUES(?)', this.name);
                }}
                async fetch(request) {{
                    const mode = new URL(request.url).pathname;
                    if (mode === '/throw') throw new Error('entry rejected');
                    if (mode === '/lost') {{
                        globalThis.entryLostHooks = [];
                        try {{ await this.ctx.storage.put('lost-reply', 55); }}
                        catch(e) {{ entryLostHooks.push('catch'); }}
                        finally {{ entryLostHooks.push('finally'); }}
                        entryLostHooks.push('after');
                    }}
                    if (mode === '/slow') await new Promise(resolve => setTimeout(resolve, 30));
                    await this.ctx.storage.put('handler', this.name);
                    this.ctx.waitUntil((async () => {{
                        await new Promise(resolve => setTimeout(resolve, 2));
                        await this.ctx.storage.put('background', this.name);
                    }})());
                    return new Response(this.name + ':' + this.ctx.storage.sql.exec('SELECT value FROM fixture').one().value);
                }}
                async alarm(event) {{
                    alarmEvents.push([event.scheduledTime, event.retryCount, event.isRetry]);
                    await this.ctx.storage.put('alarm-handler', this.name);
                    this.ctx.waitUntil((async () => {{
                        await new Promise(resolve => setTimeout(resolve, 2));
                        await this.ctx.storage.put('alarm-background', this.name);
                    }})());
                    if (alarmMode === 'handler') throw new Error('alarm handler rejected');
                }}
            }}
            export default {{ fetch() {{ return new Response('guest'); }} }};
        "#,
        serde_json::to_string(&one).unwrap()
    );
    let config = Arc::new(WorkerConfig::new(WorkerConfigOptions {
        src: source,
        script_name: address.storage_root(),
        do_classes: vec!["Counter".into()],
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
    }));
    let mut worker = Worker::load_guest_config(config.clone()).unwrap();
    assert!(worker.runtime_state.egress == EgressPolicy::Deny);
    let inspection = IoContext::tracked(&worker.runtime_state);
    checkpoint(&worker, &inspection);
    check(
        &worker,
        &inspection,
        "return moduleEgressDenied && constructed.length === 0;",
    );
    check(&worker, &inspection, "let denied = false; try { __celld.__storage_get(moduleTarget, 'module-write'); } catch(e) { denied = e.message.includes('owning event'); } return denied;");
    let first = Host::named(one.clone(), Some("named-object"));
    let second = Host::named(two.clone(), Some("other-object"));

    // Missing admission, wrong target, native entry, and an adapter
    // shape absent from the wire all refuse before constructing anything.
    let (job, mut denied) = fetch_job(&one, Some("named-object"), "/slow");
    assert!(worker
        .turn_begin_guest_cell(job, None, first.channel.clone())
        .err()
        .unwrap()
        .to_string()
        .contains("active execution turn"));
    assert!(denied.try_recv().unwrap().is_err());
    first.channel.begin_turn().unwrap();
    let (job, mut denied) = fetch_job(&two, Some("other-object"), "/fast");
    assert!(worker
        .turn_begin_guest_cell(job, None, first.channel.clone())
        .err()
        .unwrap()
        .to_string()
        .contains("another object"));
    assert!(denied.try_recv().unwrap().is_err());
    let (job, mut denied) = fetch_job(&one, Some("named-object"), "/slow");
    assert!(worker.turn_begin_cell(job, None).0.is_none());
    assert!(denied.try_recv().unwrap().is_err());
    let (reply, mut denied) = tokio::sync::oneshot::channel();
    let job = CellJob::WsOpen {
        scope: one.clone(),
        ws_id: 1,
        facet_opening: None,
        protocol: String::new(),
        reply,
    };
    assert!(worker
        .turn_begin_guest_cell(job, None, first.channel.clone())
        .err()
        .unwrap()
        .to_string()
        .contains("buffered HTTP, RPC"));
    assert!(denied.try_recv().unwrap().is_err());
    let (reply, mut denied) = tokio::sync::oneshot::channel();
    let native_alarm = CellJob::Alarm {
        scope: one.clone(),
        request_id: Some(next_request_id()),
        scheduled_ms: 1,
        claim: AlarmDispatch::Due,
        reply,
    };
    assert!(worker
        .turn_begin_guest_cell(native_alarm, None, first.channel.clone())
        .is_err());
    assert!(denied.try_recv().unwrap().is_err());
    check(&worker, &inspection, "return constructed.length === 0;");

    // A suspends; B completes before A's timer is driven. Admission is
    // released while awaiting native ops, rather than held by a handler.
    let (job, mut a_reply) = fetch_job(&one, Some("named-object"), "/slow");
    let (a, a_ops) = worker
        .turn_begin_guest_cell(job, None, first.channel.clone())
        .unwrap();
    let mut a = a.unwrap();
    first.channel.finish_turn().unwrap();
    assert!(!a.finished());
    assert!(matches!(
        a_reply.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    assert!(worker
        .turn_poll_guest_cell(&mut a)
        .err()
        .unwrap()
        .to_string()
        .contains("active execution turn"));

    second.channel.begin_turn().unwrap();
    let (job, mut b_reply) = fetch_job(&two, Some("other-object"), "/fast");
    let (b, b_ops) = worker
        .turn_begin_guest_cell(job, None, second.channel.clone())
        .unwrap();
    let mut b = b.unwrap();
    second.channel.finish_turn().unwrap();
    let response = b_reply.try_recv().unwrap().unwrap();
    assert_eq!(response.body, b"other-object:other-object");
    assert_eq!(
        (response.write_position, response.observed_position),
        (None, None)
    );
    assert!(b.background.is_some());
    assert!(!b.finished());
    drain_entry(&mut worker, &mut b, b_ops, &second.channel).await;
    assert!(matches!(
        a_reply.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));

    // A's operations and continuations retain their own original context
    // through B's earlier settlement and background drain.
    let mut stranger = Worker::load_guest_config(config).unwrap();
    first.channel.begin_turn().unwrap();
    assert!(stranger
        .turn_poll_guest_cell(&mut a)
        .err()
        .unwrap()
        .to_string()
        .contains("another worker"));
    assert!(worker.turn_poll_guest_cell(&mut a).unwrap().is_empty());
    first.channel.finish_turn().unwrap();
    drain_entry(&mut worker, &mut a, a_ops, &first.channel).await;
    let response = a_reply.try_recv().unwrap().unwrap();
    assert_eq!(response.body, b"named-object:named-object");
    assert_eq!(
        (response.write_position, response.observed_position),
        (None, None)
    );
    check(
        &worker,
        &inspection,
        "return constructed.join(',') === 'named-object,other-object';",
    );
    // Use the real host-backed event channel and the original alarm entry.
    // Only the host's claim metadata can dispatch; no guest clock or native
    // alarm token grants authority. Background storage must join as well.
    let scheduled = crate::ownership_store::now_ms() as i64 + 60_000;
    let (reply, mut alarm_reply) = tokio::sync::oneshot::channel();
    let alarm_host = Host::named(one.clone(), Some("named-object"));
    alarm_host.channel.begin_turn().unwrap();
    let (entry, ops) = worker
        .turn_begin_guest_cell(
            CellJob::Alarm {
                scope: one.clone(),
                request_id: Some(next_request_id()),
                scheduled_ms: scheduled,
                claim: AlarmDispatch::Guest { retry: 3 },
                reply,
            },
            None,
            alarm_host.channel.clone(),
        )
        .unwrap();
    let mut entry = entry.expect("guest alarm entry was refused");
    alarm_host.channel.finish_turn().unwrap();
    drain_entry(&mut worker, &mut entry, ops, &alarm_host.channel).await;
    let (alarm, position) = alarm_reply.try_recv().unwrap().unwrap();
    assert!(
        alarm.at_ms().is_none() && position.is_none(),
        "guest alarm supplied native proof"
    );
    check(
        &worker,
        &inspection,
        &format!("return JSON.stringify(alarmEvents) === JSON.stringify([[{scheduled},3,true]]);"),
    );
    let values = alarm_host.finish();
    for key in ["alarm-handler", "alarm-background"] {
        assert!(
            values.iter().any(|(stored, _)| stored == key),
            "guest alarm did not join {key}"
        );
    }
    // Constructor and handler failures reject the original async alarm
    // promise and count. A failure before that promise exists does not.
    for (mode, counted) in [
        ("handler", true),
        ("constructor", true),
        ("dispatch", false),
    ] {
        evaluate(
            &worker,
            &inspection,
            &format!("globalThis.alarmMode = '{mode}';"),
        )
        .unwrap();
        if mode == "dispatch" {
            evaluate(&worker, &inspection, "globalThis.originalFireAlarm = __celld.__fireAlarm; __celld.__fireAlarm = () => { throw new Error('alarm dispatcher failed before promise'); };").unwrap();
        }
        let (scope, name) = if mode == "constructor" {
            (
                address.cell_scope("Counter", "constructor-object").unwrap(),
                "constructor-object",
            )
        } else {
            (one.clone(), "named-object")
        };
        let host = Host::named(scope.clone(), Some(name));
        let (reply, mut result) = tokio::sync::oneshot::channel();
        host.channel.begin_turn().unwrap();
        let (mut entry, ops) = worker
            .turn_begin_guest_cell(
                CellJob::Alarm {
                    scope: scope.clone(),
                    request_id: Some(next_request_id()),
                    scheduled_ms: scheduled,
                    claim: AlarmDispatch::Guest { retry: 0 },
                    reply,
                },
                None,
                host.channel.clone(),
            )
            .unwrap();
        host.channel.finish_turn().unwrap();
        if let Some(entry) = &mut entry {
            drain_entry(&mut worker, entry, ops, &host.channel).await;
        } else {
            assert!(ops.is_empty());
        }
        let error = result.try_recv().unwrap().unwrap_err();
        assert_eq!(
            error.is::<GuestAlarmRejection>(),
            counted,
            "{mode}: original retry classification changed"
        );
        crate::application_javascript_guest::tests::check_alarm_failure_publication(
            scope,
            entry,
            host.channel.clone(),
            error,
            counted,
        )
        .await;
        let values = host.finish();
        if mode == "handler" {
            assert!(
                values.iter().any(|(key, _)| key == "alarm-background"),
                "rejected alarm background did not join"
            );
        }
        if mode == "dispatch" {
            evaluate(
                &worker,
                &inspection,
                "__celld.__fireAlarm = originalFireAlarm;",
            )
            .unwrap();
        }
    }
    evaluate(&worker, &inspection, "globalThis.alarmMode = 'ok';").unwrap();
    eprintln!("original guest alarm retry classification: handler and constructor promise rejections counted after joined background; pre-promise dispatcher failure uncounted; original host-backed event grants and guest terminal codec checked");
    {
        let (_locker, _cells) = worker.lock();
        for cell in [&one, &two] {
            assert!(storage::activation_epoch(cell).is_none());
            assert!(storage::get_actor_name(cell).is_err());
        }
    }

    // Cached instances get fresh event grants and must recheck the host's
    // name. A previously registered JS display name grants no authority.
    let wrong_name = Host::named(one.clone(), Some("wrong-host-name"));
    wrong_name.channel.begin_turn().unwrap();
    let (job, mut refused) = fetch_job(&one, Some("named-object"), "/fast");
    assert!(worker
        .turn_begin_guest_cell(job, None, wrong_name.channel.clone())
        .unwrap()
        .0
        .is_none());
    assert!(refused
        .try_recv()
        .unwrap()
        .err()
        .unwrap()
        .to_string()
        .contains("host identity"));
    wrong_name.channel.finish_turn().unwrap();
    assert!(!wrong_name.finish().iter().any(|(key, _)| key == "handler"));

    let rejected = Host::named(one.clone(), Some("named-object"));
    rejected.channel.begin_turn().unwrap();
    let (job, mut reply) = fetch_job(&one, Some("named-object"), "/throw");
    let (entry, ops) = worker
        .turn_begin_guest_cell(job, None, rejected.channel.clone())
        .unwrap();
    let entry = entry.unwrap();
    assert!(entry.finished() && ops.is_empty());
    let error = reply.try_recv().unwrap().err().unwrap();
    assert!(error.to_string().contains("entry rejected"));
    assert_eq!(failed_write_position(&error), None);
    rejected.channel.finish_turn().unwrap();
    rejected.finish();

    let lost = Host::named(one.clone(), Some("named-object"));
    lost.channel.begin_turn().unwrap();
    let (job, mut reply) = fetch_job(&one, Some("named-object"), "/lost");
    let (entry, ops) = worker
        .turn_begin_guest_cell(job, None, lost.channel.clone())
        .unwrap();
    let entry = entry.unwrap();
    assert!(entry.finished() && ops.is_empty());
    assert!(reply
        .try_recv()
        .unwrap()
        .err()
        .unwrap()
        .to_string()
        .contains("guest storage retired"));
    assert!(lost.channel.finish_turn().is_err());
    check(
        &worker,
        &inspection,
        "return entryLostHooks.length === 0 && constructed.length === 2;",
    );
    assert!(lost.finish().iter().any(|(key, _)| key == "lost-reply"));

    for (host, name) in [(first, "named-object"), (second, "other-object")] {
        let values = host.finish();
        for key in ["handler", "background"] {
            let (_, storage::StoredValue::V8(bytes)) =
                values.iter().find(|(key_, _)| key_ == key).unwrap()
            else {
                panic!("missing host clone")
            };
            assert!(bytes
                .windows(name.len())
                .any(|part| part == name.as_bytes()));
        }
    }
    eprintln!("original V8 guest entry hooks: guest mode before module evaluation; host names and storage attached before constructor; original InFlight begin/deliver/poll and waitUntil; B completes while A suspends, host admission released between turns; no local SQLite/proof positions; unknown-write termination; local Unix fixture, no JS VM/full scheduler qualification");
    check_foreign_execution_turn().await;
    check_guest_request_stream().await;
    check_owned_critical_sections().await;
    check_original_guest_quiescence().await;
}

async fn check_original_guest_quiescence() {
    let address = address("quiescence");
    let cell = address
        .cell_scope("QuiescenceCounter", "named-object")
        .unwrap();
    for mode in ["/quiescence-wait", "/quiescence-fail"] {
        let host = SharedHost::new(&cell);
        let mut worker =
            Worker::load_guest_config(Arc::new(WorkerConfig::new(WorkerConfigOptions {
                src: include_str!("../../../../applications/guest/javascript/check.js").into(),
                script_name: address.storage_root(),
                do_classes: vec!["QuiescenceCounter".into()],
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
            })))
            .unwrap();
        let a_channel = &host.channels[0];
        let b_channel = &host.channels[1];
        a_channel.begin_turn().unwrap();
        let (job, mut response) = fetch_job(&cell, Some("named-object"), mode);
        let (entry, ops) = worker
            .turn_begin_guest_cell(job, None, a_channel.clone())
            .unwrap();
        let mut a = entry.unwrap();
        a_channel.finish_turn().unwrap();
        assert!(
            ops.is_empty() && !a.has_refed_ops(),
            "unresolved background unexpectedly had native I/O"
        );
        let observed = response.try_recv().unwrap();
        if mode == "/quiescence-wait" {
            assert_eq!(observed.unwrap().body, b"early");
        } else {
            assert!(observed
                .err()
                .unwrap()
                .to_string()
                .contains("quiescence handler failure"));
        }
        assert!(
            !a.finished(),
            "raw waitUntil was retired by overridden allSettled"
        );
        b_channel.begin_turn().unwrap();
        let (job, mut peer) = fetch_job(&cell, Some("named-object"), "/quiescence-resolve");
        let (entry, ops) = worker
            .turn_begin_guest_cell(job, None, b_channel.clone())
            .unwrap();
        let mut b = entry.unwrap();
        assert!(b.finished() && ops.is_empty());
        b_channel.finish_turn().unwrap();
        assert_eq!(peer.try_recv().unwrap().unwrap().body, b"peer");
        assert!(
            !a.finished(),
            "foreign completion discarded late registration"
        );
        let handed = a.take_handed_ops();
        drain_entry(&mut worker, &mut a, handed, a_channel).await;
        assert!(a.finished());
        a.abandon();
        b.abandon();
        assert!(
            worker.runtime_state.promises.lock().unwrap().is_empty(),
            "quiescence left a native resolver"
        );
        assert_eq!(
            asyncrt::spawn_count(),
            0,
            "quiescence left an unadopted operation"
        );
        let (values, borrowed, _) = host.finish();
        assert!(borrowed > 0);
        for key in [
            "quiescence-prefix",
            "quiescence-background",
            "quiescence-nested",
        ] {
            assert!(values.iter().any(|(stored, _)| stored == key));
        }
    }
    eprintln!("original V8 guest quiescence: raw background survives early reply/failure and overridden allSettled without native I/O; foreign peer completion retains nested waitUntil; no resolver/unadopted operation/native claim leak; local native fixture with separate real-VM coverage");
}

/// A host for one object with facets: the root stream plus every facet
/// event stream the guest opens, all served by the root's one session, as
/// the host transport serves them. Teardown revokes the root and requires
/// the first facet's grant and native cell to be gone.
struct FacetHost {
    channel: Arc<Channel>,
    stop: std::sync::mpsc::Sender<()>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl FacetHost {
    fn new(cell: &str) -> Self {
        let (guest, host) = UnixStream::pair().unwrap();
        let (streams, incoming) = std::sync::mpsc::channel::<UnixStream>();
        let (stop, stopped) = std::sync::mpsc::channel::<()>();
        let (ready, grant) = std::sync::mpsc::channel();
        let host_cell = cell.to_owned();
        let thread = std::thread::spawn(move || {
            let directory = tempfile::tempdir().unwrap();
            let cells = storage::Cells::default();
            let _installed = cells.install();
            storage::open_at_epoch(
                &host_cell,
                directory.path().join("host.sqlite").to_str().unwrap(),
                7,
                false,
                None,
                false,
            )
            .unwrap();
            storage::set_actor_name(&host_cell, "named-object").unwrap();
            let mut session = StorageSession::new(&cells, host_cell.clone(), 7).unwrap();
            ready.send(session.grant().to_owned()).unwrap();
            let (frames, received) = std::sync::mpsc::channel::<(usize, Option<Vec<u8>>)>();
            let mut writers: Vec<UnixStream> = Vec::new();
            let add = |stream: UnixStream, writers: &mut Vec<UnixStream>| {
                let index = writers.len();
                let mut reader = stream.try_clone().unwrap();
                writers.push(stream);
                let frames = frames.clone();
                std::thread::spawn(move || loop {
                    match wire::read_frame(&mut reader) {
                        Ok(Some(bytes)) => frames.send((index, Some(bytes))).unwrap(),
                        _ => {
                            let _ = frames.send((index, None));
                            break;
                        }
                    }
                });
            };
            add(host, &mut writers);
            let mut first = None;
            loop {
                while let Ok(stream) = incoming.try_recv() {
                    add(stream, &mut writers);
                }
                match received.recv_timeout(Duration::from_millis(5)) {
                    Ok((index, Some(bytes))) => {
                        let reply = session.execute(&cells, true, Request::decode(&bytes).unwrap());
                        if let Outcome::Ok {
                            value: Value::Facet { scope, grant, .. },
                        } = &reply.outcome
                        {
                            first.get_or_insert((scope.clone(), grant.clone()));
                        }
                        let _ = wire::write_frame(&mut writers[index], &reply.encode().unwrap());
                    }
                    Ok((_, None)) => {}
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        if stopped.try_recv().is_ok() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            session.revoke(&cells).unwrap();
            let (scope, grant) = first.expect("no facet was opened");
            let stale = session.execute(
                &cells,
                true,
                Request {
                    version: wire::VERSION,
                    grant,
                    operation: Operation::Get {
                        keys: vec!["v".into()],
                    },
                },
            );
            assert!(matches!(stale.outcome, Outcome::Error { .. }));
            assert!(storage::activation_epoch(&scope).is_none());
            storage::close(&host_cell);
        });
        let grant = grant.recv_timeout(Duration::from_secs(5)).unwrap();
        let channel = Channel::new(cell.to_owned(), grant, guest).unwrap();
        channel.set_invocation("a".repeat(64));
        let streams = Mutex::new(streams);
        let facet_cell = cell.to_owned();
        install_facet_connector(move |scope, invocation, grant| {
            assert_eq!(invocation, "a".repeat(64));
            assert_ne!(scope, facet_cell);
            let (guest, host) = UnixStream::pair()?;
            streams.lock().unwrap().send(host)?;
            let channel = Channel::new(scope.into(), grant.into(), guest)?;
            channel.set_invocation(invocation.into());
            Ok(channel)
        });
        Self {
            channel,
            stop,
            thread: Some(thread),
        }
    }

    fn finish(mut self) {
        self.channel.retire();
        let _ = self.stop.send(());
        self.thread.take().unwrap().join().unwrap();
    }
}

/// Original facets in a guest-mode Worker: the facet runs in a loaded
/// isolate of the guest, and its database is a file of its own, brokered
/// through the host under the root's session.
pub(crate) async fn check_original_guest_facets() {
    Engine::init();
    let address = address("guest-facets");
    let cell = address.cell_scope("Counter", "named-object").unwrap();
    let source = r##"
const facetCode = `
  import { DurableObject } from 'cloudflare:workers';
  export class F extends DurableObject {
    constructor(ctx, env) { super(ctx, env); this.n = 0; }
    add(k) {
      const sql = this.ctx.storage.sql;
      sql.exec('CREATE TABLE IF NOT EXISTS t(v INTEGER)');
      const cur = sql.exec('SELECT COALESCE(SUM(v), 0) AS s FROM t').one().s;
      sql.exec('INSERT INTO t VALUES(?)', k);
      return { n: ++this.n, sum: cur + k, props: this.ctx.props, depth: this.ctx.id !== undefined };
    }
    async put(v) { await this.ctx.storage.put('v', v); return await this.ctx.storage.get('v'); }
    async gated() {
      await this.ctx.blockConcurrencyWhile(async () => { await new Promise((resolve) => setTimeout(resolve, 5)); });
      return 'ok';
    }
    async egress() {
      try { await fetch('http://example.invalid/'); return 'reached'; } catch (e) { return String(e.message); }
    }
    async alarm() { try { await this.ctx.storage.setAlarm(Date.now() + 1000); return 'set'; } catch (e) { return e.message; } }
    async fetch(request) { return new Response('facet ' + new URL(request.url).pathname + ' ' + (await this.ctx.storage.get('v'))); }
  }
  export default { fetch() { return new Response('f'); } };
`;
export class Counter {
  constructor(ctx, env) { this.ctx = ctx; this.env = env; }
  async fetch(request) {
    const out = {};
    const cls = this.env.LOADER
      .get('code', () => ({ compatibilityDate: '2026-03-01', mainModule: 'main.js', modules: { 'main.js': facetCode } }))
      .getDurableObjectClass('F', { props: { tag: 'p' } });
    const f = () => this.ctx.facets.get('child', () => ({ class: cls }));
    out.first = await f().add(5);
    out.second = await f().add(7);
    out.put = await f().put(41);
    out.fetch = await (await f().fetch('http://facet/path')).text();
    out.gated = await f().gated();
    out.egress = await f().egress();
    out.alarm = await f().alarm();
    this.ctx.facets.abort('child', new Error('stop'));
    out.aborted = await f().add(1);
    this.ctx.facets.delete('child');
    out.deleted = await f().add(2);
    await this.ctx.storage.transaction(async (txn) => { await f().add(100); txn.rollback(); });
    out.rolledBack = await f().add(0);
    await this.ctx.storage.transaction(async () => { await f().add(1000); });
    out.committed = await f().add(0);
    await this.ctx.storage.put('root', 1);
    return new Response(JSON.stringify(out));
  }
}
export default { fetch() { return new Response('guest'); } };
"##;
    let mut worker = Worker::load_guest_config(Arc::new(
        WorkerConfig::new(WorkerConfigOptions {
            src: source.into(),
            script_name: address.storage_root(),
            do_classes: vec!["Counter".into()],
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
            compat: crate::worker_compat(&serde_json::json!({
                "compatibility_date": "2026-03-01",
                "compatibility_flags": ["js_rpc"],
            })),
        })
        .with_loaders(vec!["LOADER".into()]),
    ))
    .unwrap();
    let host = FacetHost::new(&cell);
    host.channel.begin_turn().unwrap();
    let (job, response) = fetch_job(&cell, Some("named-object"), "/run");
    let (entry, ops) = worker
        .turn_begin_guest_cell(job, None, host.channel.clone())
        .unwrap();
    host.channel.finish_turn().unwrap();
    if let Some(mut entry) = entry {
        drain_entry(&mut worker, &mut entry, ops, &host.channel).await;
    }
    let response = response.await.unwrap().unwrap();
    let out: serde_json::Value = serde_json::from_slice(&response.body)
        .unwrap_or_else(|_| panic!("{}", String::from_utf8_lossy(&response.body)));
    let step = |key: &str| (out[key]["n"].clone(), out[key]["sum"].clone());
    // One instance serves its calls; its database is the facet's own.
    assert_eq!(step("first"), (1.into(), 5.into()), "{out}");
    assert_eq!(out["first"]["props"]["tag"], "p", "{out}");
    assert_eq!(step("second"), (2.into(), 12.into()), "{out}");
    assert_eq!(out["put"], 41, "{out}");
    assert_eq!(out["fetch"], "facet /path 41", "{out}");
    assert_eq!(out["gated"], "ok", "{out}");
    assert!(
        out["egress"]
            .as_str()
            .unwrap()
            .contains("not permitted to access the internet"),
        "{out}"
    );
    assert_eq!(out["alarm"], "Facets currently cannot set alarms.", "{out}");
    // Abort drops the instance, not the stored image; delete drops both.
    assert_eq!(step("aborted"), (1.into(), 13.into()), "{out}");
    assert_eq!(step("deleted"), (1.into(), 2.into()), "{out}");
    // The facet is a database of its own: the root's transaction takes
    // neither its writes nor its instance with it, rolled back or not.
    assert_eq!(step("rolledBack"), (3.into(), 102.into()), "{out}");
    assert_eq!(step("committed"), (5.into(), 1102.into()), "{out}");
    // Resetting the idle object releases its facets without an event.
    worker.reset_guest_cell(&cell).unwrap();
    host.finish();
}

/// Original Worker Loader inside a guest-mode Worker: `Slot::standalone` and
/// `runtime::drive` run the stateless child under this current-thread
/// runtime, as under `celld-guest`. Egress-bearing options and facets refuse.
pub(crate) async fn check_original_guest_loader() {
    Engine::init();
    let address = address("guest-loader");
    let cell = address.cell_scope("Counter", "named-object").unwrap();
    let source = r##"
export class Counter {
  constructor(ctx, env) { this.ctx = ctx; this.env = env; }
  async fetch(request) {
    const L = this.env.LOADER, S = this.env.SVC, out = {};
    const main = (body, extra = {}) => ({ compatibilityDate: '2026-03-01', mainModule: 'main.js', modules: { 'main.js': body, ...extra } });
    const fail = async (f) => { try { await f(); return 'resolved'; } catch (e) { return String((e && e.message) || e); } };
    const text = async (stub, url = 'http://child/path') => (await stub.getEntrypoint().fetch(url)).text();
    const plain = 'export default { fetch() { return new Response("plain"); } }';
    let calls = 0;
    const named = () => L.get('named', () => { calls++; return { ...main(`
      import { WorkerEntrypoint } from 'cloudflare:workers';
      import { x } from './dep.js';
      import wasm from './empty.wasm';
      export class Api extends WorkerEntrypoint {
        add(a, b) { return a + b + x + (this.ctx.props?.bonus ?? 0); }
        spin() { for (;;) {} }
      }
      export default { fetch(req, env) { return new Response(new URL(req.url).pathname + ':' + env.plain + ':' + (wasm instanceof WebAssembly.Module)); } };
    `, { 'dep.js': 'export const x = 7;', 'empty.wasm': { wasm: new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]) } }), env: { plain: 'yes' } }; });
    out.fetch = await text(named());
    out.rpc = await named().getEntrypoint('Api', { props: { bonus: 10 } }).add(1, 2);
    out.limit = await fail(() => named().getEntrypoint('Api', { limits: { cpuMs: 20 } }).spin());
    out.after = await named().getEntrypoint('Api').add(1, 1);
    out.calls = calls;
    const anonymous = L.load(main('export default { fetch() { return new Response("anon"); } }'));
    out.load = await text(anonymous);
    anonymous.dispose();
    out.disposed = await fail(() => text(anonymous));
    out.egress = await text(L.load(main('export default { async fetch() { try { await fetch("http://example.invalid/"); return new Response("reached"); } catch (e) { return new Response(String(e.message)); } } }')));
    out.nullOutbound = await text(L.load({ ...main(plain), globalOutbound: null }));
    out.outbound = await fail(() => text(L.load({ ...main(plain), globalOutbound: S })));
    out.envService = await fail(() => text(L.load({ ...main(plain), env: { service: S } })));
    out.tails = await fail(() => text(L.load({ ...main(plain), tails: [S] })));
    out.textModule = await fail(() => text(L.load(main(plain, { 't.txt': { text: 'x' } }))));
    out.py = await fail(() => text(L.load(main(plain, { 'p.py': { py: 'x = 1' } }))));
    return new Response(JSON.stringify(out));
  }
}
export default { fetch() { return new Response('guest'); } };
"##;
    let mut worker = Worker::load_guest_config(Arc::new(
        WorkerConfig::new(WorkerConfigOptions {
            src: source.into(),
            script_name: address.storage_root(),
            do_classes: vec!["Counter".into()],
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
            compat: crate::worker_compat(&serde_json::json!({
                "compatibility_date": "2026-03-01",
                "compatibility_flags": ["js_rpc"],
            })),
        })
        .with_loaders(vec!["LOADER".into()])
        .with_services(vec![("SVC".into(), "SVC".into(), None)]),
    ))
    .unwrap();
    let host = Host::named(cell.clone(), Some("named-object"));
    host.channel.begin_turn().unwrap();
    let (job, response) = fetch_job(&cell, Some("named-object"), "/run");
    let (entry, ops) = worker
        .turn_begin_guest_cell(job, None, host.channel.clone())
        .unwrap();
    host.channel.finish_turn().unwrap();
    if let Some(mut entry) = entry {
        drain_entry(&mut worker, &mut entry, ops, &host.channel).await;
    }
    let response = response.await.unwrap().unwrap();
    let out: serde_json::Value = serde_json::from_slice(&response.body)
        .unwrap_or_else(|_| panic!("{}", String::from_utf8_lossy(&response.body)));
    let said = |key: &str| out[key].as_str().unwrap_or_default().to_owned();
    assert_eq!(said("fetch"), "/path:yes:true", "{out}");
    assert_eq!(out["rpc"], 20, "{out}");
    assert_ne!(said("limit"), "resolved", "{out}");
    assert_eq!(out["after"], 9, "{out}");
    assert_eq!(out["calls"], 1, "{out}");
    assert_eq!(said("load"), "anon", "{out}");
    assert!(said("disposed").contains("unknown worker"), "{out}");
    assert!(
        said("egress").contains("not permitted to access the internet"),
        "{out}"
    );
    assert_eq!(said("nullOutbound"), "plain", "{out}");
    for key in ["outbound", "envService", "tails"] {
        assert!(
            said(key).contains("not available in application guests"),
            "{key}: {out}"
        );
    }
    for key in ["textModule", "py"] {
        assert!(
            said(key).contains("must be a string or wasm bytes"),
            "{key}: {out}"
        );
    }
    eprintln!("original V8 guest Worker Loader: get/load, entrypoint fetch/RPC, props, sibling/Wasm modules, limits, dispose and memoization ran a child under the current-thread runtime; Fetcher/env service/tails/text/py refused; local check, separate VM evidence required");
    let _ = host.finish();
}

pub(crate) async fn check_original_guest_response() {
    Engine::init();
    let address = address("response-turn");
    let cell = address.cell_scope("Counter", "named-object").unwrap();
    let source = r#"
globalThis.responseReads = 0;
globalThis.pendingResponse = null;
export class Counter {
  constructor(ctx) { this.ctx = ctx; }
  async fetch(request) {
    const mode = new URL(request.url).pathname;
    const response = new Response('owned response', {status: 201});
    const headers = response.headers;
    let decoded = false;
    Object.defineProperty(response, 'headers', { get: () => {
      responseReads++;
      if (!decoded) {
        decoded = true;
        this.ctx.storage.put('response-prefix', 41);
        this.ctx.storage.sql.exec('CREATE TABLE response_turn(value INTEGER)');
        this.ctx.storage.sql.exec('INSERT INTO response_turn VALUES(42)');
        if (mode === '/throw') throw new Error('response getter failure');
        this.ctx.waitUntil((async () => {
          await new Promise(resolve => setTimeout(resolve, 5));
          this.ctx.storage.put('response-background', 43);
        })());
      }
      return headers;
    }});
    pendingResponse = response;
    if (mode === '/pending') await new Promise(resolve => setTimeout(resolve, 100));
    return response;
  }
}
export default {fetch() {return new Response('guest');}};
"#;
    let config = || {
        Arc::new(WorkerConfig::new(WorkerConfigOptions {
            src: source.into(),
            script_name: address.storage_root(),
            do_classes: vec!["Counter".into()],
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
        }))
    };
    for mode in ["/owned", "/throw"] {
        let mut worker = Worker::load_guest_config(config()).unwrap();
        let host = Host::named(cell.clone(), Some("named-object"));
        host.channel.begin_turn().unwrap();
        let (job, mut response) = fetch_job(&cell, Some("named-object"), mode);
        let (entry, ops) = worker
            .turn_begin_guest_cell(job, None, host.channel.clone())
            .unwrap();
        let mut entry = entry.unwrap();
        host.channel.finish_turn().unwrap();
        let observed = response.try_recv().unwrap();
        if mode == "/owned" {
            let response = observed.unwrap();
            assert_eq!(response.status, 201);
            assert_eq!(response.body, b"owned response");
            assert_eq!(
                (response.write_position, response.observed_position),
                (None, None)
            );
            assert!(
                !entry.finished(),
                "response getter background was dropped with the reply"
            );
        } else {
            assert!(observed.is_err());
        }
        drain_entry(&mut worker, &mut entry, ops, &host.channel).await;
        assert!(entry.finished());
        crate::application_javascript_guest::tests::check_pending_admission_publication(
            entry,
            host.channel.clone(),
        )
        .await;
        let values = host.finish();
        assert!(values.iter().any(|(key, _)| key == "response-prefix"));
        assert_eq!(
            values.iter().any(|(key, _)| key == "response-background"),
            mode == "/owned"
        );
    }
    for refusal in ["missing-turn", "foreign-driver", "cleanup"] {
        let mut worker = Worker::load_guest_config(config()).unwrap();
        let host = Host::named(cell.clone(), Some("named-object"));
        host.channel.begin_turn().unwrap();
        let (job, mut response) = fetch_job(&cell, Some("named-object"), "/pending");
        let (entry, ops) = worker
            .turn_begin_guest_cell(job, None, host.channel.clone())
            .unwrap();
        let mut entry = entry.unwrap();
        let owner = entry.context.continuation_id().unwrap();
        assert!(host
            .channel
            .require_response_turn(&cell, owner + 1)
            .is_err());
        assert!(host
            .channel
            .require_response_turn("Other:wrong", owner)
            .is_err());
        if refusal == "missing-turn" {
            host.channel.finish_turn().unwrap();
        }
        if refusal == "cleanup" {
            host.channel.restrict_to_cleanup();
        }
        {
            let (mut locker, _cells) = worker.lock();
            v8::scope!(let hs, &mut *locker);
            let realm = worker.realm(hs);
            let cs = &mut v8::ContextScope::new(hs, realm.context);
            let inspection = IoContext::tracked(&worker.runtime_state);
            let previous = install_io_context(
                cs,
                if refusal == "foreign-driver" {
                    &inspection
                } else {
                    &entry.context
                },
            );
            let _current = CurrentGuard::enter(entry.context.clone());
            let value = run_internal_snippet(cs, "return pendingResponse;").unwrap();
            entry.answer_settled(cs, value);
            let calls = run_internal_snippet(cs, "return responseReads;")
                .unwrap()
                .integer_value(cs)
                .unwrap();
            assert_eq!(calls, 0, "{refusal} executed response getters");
            restore_io_context(cs, previous);
        }
        assert!(response.try_recv().unwrap().is_err());
        entry.abandon();
        drop(ops);
        if refusal != "missing-turn" {
            host.channel.finish_turn().unwrap();
        }
        let values = host.finish();
        assert!(!values.iter().any(|(key, _)| key.starts_with("response-")));
    }
    eprintln!("original V8 guest response ownership: getter SQL/prefix write ran in owning ordinary turn; getter background stayed live after reply and drained; throwing getter retained accepted prefix; missing turn/foreign CPED driver/cleanup/wrong owner or cell refused before getter execution; guest replies supplied no native proof; local native Session fixture, not full streams/Actor/output-proof/independent-host qualification");
}

/// The exact upgrade returns an application guest publishes: an object's
/// `acceptWebSocket` + 101, and a handler returning the bodiless 101 the host
/// issued for a callee's socket. Both must be the one response kind
/// `publish` carries, never a Worker pair, tunnel or stream.
pub(crate) async fn check_guest_socket_upgrade() {
    Engine::init();
    let address = address("socket-upgrade");
    let cell = address.cell_scope("Counter", "named-object").unwrap();
    let source = r#"
export class Counter {
  constructor(ctx) { this.ctx = ctx; }
  async fetch(request) {
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server, ['t']);
    server.serializeAttachment({ n: 0 });
    globalThis.acceptedId = server._id;
    if (new URL(request.url).pathname === '/abandon') return new Response('not an upgrade');
    return new Response(null, { status: 101, webSocket: client });
  }
}
export default {
  fetch() {
    const target = { id: 7, scope: '' };
    return new Response(null, { status: 101, __wsTarget: target, webSocket: new WebSocket(undefined, [], target) });
  }
};
"#;
    let load = || {
        Worker::load_guest_config(Arc::new(WorkerConfig::new(WorkerConfigOptions {
            src: source.into(),
            script_name: address.storage_root(),
            do_classes: vec!["Counter".into()],
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
        })))
        .unwrap()
    };
    for path in ["/upgrade", "/abandon"] {
        let mut worker = load();
        let host = Host::named(cell.clone(), Some("named-object"));
        host.channel.begin_turn().unwrap();
        let (job, mut response) = fetch_job(&cell, Some("named-object"), path);
        let (entry, ops) = worker
            .turn_begin_guest_cell(job, None, host.channel.clone())
            .unwrap();
        let mut entry = entry.unwrap();
        host.channel.finish_turn().unwrap();
        let response = response.try_recv().unwrap().unwrap();
        if path == "/upgrade" {
            assert_eq!(response.status, 101);
            let Some(crate::js::HttpResponseWebSocket::Cell(target)) = &response.websocket else {
                panic!("the object's 101 is not a registry socket target");
            };
            assert!(target.tunnel.is_none());
            assert_eq!(
                crate::js::ws_scope_of(target.id).as_deref(),
                Some(cell.as_str()),
                "the target is not the host's id for this object's socket"
            );
            assert!(crate::application_javascript_guest::socket_upgrade_supported(&response));
            // The transport claims the socket when it takes the reply.
            assert_eq!(crate::js::ws_claim_guest(target.id).unwrap(), cell);
        } else {
            assert!(response.websocket.is_none());
        }
        drain_entry(&mut worker, &mut entry, ops, &host.channel).await;
        let accepted: u64 = {
            host.channel.begin_turn().unwrap();
            let id = evaluate(&worker, &entry.context, "return acceptedId;").unwrap();
            host.channel.finish_turn().unwrap();
            id.parse().unwrap()
        };
        if path == "/upgrade" {
            // A frame sent in the handler's first synchronous segment is still
            // queued when the event finishes: it must go out before `Drained`.
            crate::application_javascript_guest::tests::check_socket_frame_before_drain(
                entry,
                host.channel.clone(),
            )
            .await;
        }
        // Settlement releases what no upgrade claimed; a claimed 101 keeps its socket.
        host.finish();
        assert_eq!(
            crate::js::ws_scope_of(accepted).is_some(),
            path == "/upgrade",
            "{path}: wrong registry state after the event settled"
        );
        crate::js::ws_unregister(accepted);
    }
    // The handler's own return of an issued id, through the stateless entry.
    let mut worker = load();
    let (reply, mut response) = tokio::sync::oneshot::channel();
    let (entry, _ops) = worker.turn_begin(
        crate::WorkerJob::Fetch {
            queued_at: std::time::Instant::now(),
            entrypoint: None,
            invocation_limits: None,
            url: "http://check/".into(),
            method: "GET".into(),
            body: RequestBody::Bytes(bytes::Bytes::new()),
            headers: vec![],
            request_id: Some(next_request_id()),
            tail_report: None,
            reply,
        },
        None,
    );
    drop(entry);
    let response = response.try_recv().unwrap().unwrap();
    assert_eq!(response.status, 101);
    assert!(matches!(
        &response.websocket,
        Some(crate::js::HttpResponseWebSocket::Cell(target)) if target.id == 7 && target.tunnel.is_none()
    ));
    assert!(crate::application_javascript_guest::socket_upgrade_supported(&response));
    assert!(response.stream.is_none() && response.body.is_empty());
    eprintln!("original V8 guest socket upgrade: an object's acceptWebSocket + 101 and a handler's bound 101 both published as the registry-socket response kind; an accepted socket no response claimed was released at settlement");
}

async fn check_foreign_execution_turn() {
    let address = address("foreign-checkpoint");
    let cell = address.cell_scope("Counter", "named-object").unwrap();
    // A waits, B drives A's foreign block, C queues behind it, and D's
    // cleanup-only origin proves closed authority cannot start a block.
    let host = SharedHost::with_channels(&cell, 4);
    let source = r#"
            globalThis.foreignCompleted = false;
            globalThis.responseDecoded = false;
            globalThis.constructors = 0;
            export class Counter {
                constructor(ctx) { this.ctx = ctx; constructors++; }
                async fetch(request) {
                    if (new URL(request.url).pathname === '/wait') {
                        await new Promise(resolve => { this.resume = resolve; });
                        await this.ctx.storage.put('foreign-checkpoint', 71);
                        const value = await this.ctx.storage.get('foreign-checkpoint');
                        this.ctx.storage.sql.exec('CREATE TABLE foreign_reaction(value INTEGER)');
                        this.ctx.storage.sql.exec('INSERT INTO foreign_reaction VALUES(?)', value);
                        // B's checkpoint holds A's savepoints and blocks, as the
                        // original [origin, driver] block does; A's grant still
                        // performs their storage effects.
                        this.ctx.storage.transactionSync(() => {
                            this.ctx.storage.sql.exec('INSERT INTO foreign_reaction VALUES(?)', 72);
                        });
                        let foreignCritical = false, foreignTransaction = false;
                        await this.ctx.blockConcurrencyWhile(async () => {
                            this.ctx.storage.put('foreign-critical', 74);
                            await new Promise(resolve => setTimeout(resolve, 2));
                            foreignCritical = await this.ctx.storage.get('foreign-critical') === 74;
                            await this.ctx.storage.transaction(async tx => {
                                await tx.put('foreign-transaction', 75);
                                await new Promise(resolve => setTimeout(resolve, 2));
                                foreignTransaction = await tx.get('foreign-transaction') === 75;
                            });
                        });
                        foreignCompleted = value === 71 && this.ctx.storage.sql.exec(
                            'SELECT sum(value) AS value FROM foreign_reaction').one().value === 143
                            && foreignCritical && foreignTransaction;
                        const response = new Response('A:' + value, {status: 201});
                        const headers = response.headers;
                        Object.defineProperty(response, 'headers', {get: () => {
                            responseDecoded = true;
                            this.ctx.storage.put('response-owner', 73);
                            return headers;
                        }});
                        return response;
                    }
                    this.resume();
                    await this.ctx.storage.put('driver', 72);
                    return new Response('B');
                }
            }
            export default { fetch() { return new Response('guest'); } };
        "#;
    let mut worker = Worker::load_guest_config(Arc::new(WorkerConfig::new(WorkerConfigOptions {
        src: source.into(),
        script_name: address.storage_root(),
        do_classes: vec!["Counter".into()],
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
    })))
    .unwrap();
    let a_channel = host.channels[0].clone();
    let b_channel = host.channels[1].clone();
    let queued = host.channels[2].clone();
    let closed_channel = host.channels[3].clone();
    a_channel.begin_turn().unwrap();
    let (job, mut a_reply) = fetch_job(&cell, Some("named-object"), "/wait");
    let (entry, ops) = worker
        .turn_begin_guest_cell(job, None, a_channel.clone())
        .unwrap();
    let mut a = entry.unwrap();
    assert!(ops.is_empty() && !a.finished());
    a_channel.finish_turn().unwrap();
    assert!(a_channel.state.lock().unwrap().turn.is_none());
    b_channel.begin_turn().unwrap();
    let (job, mut b_reply) = fetch_job(&cell, Some("named-object"), "/resolve");
    let (entry, ops) = worker
        .turn_begin_guest_cell(job, None, b_channel.clone())
        .unwrap();
    let mut b = entry.unwrap();
    assert!(!ops.is_empty() && !b.finished(), "B retired A's held block");
    assert_eq!(b_reply.try_recv().unwrap().unwrap().body, b"B");
    {
        let state = b_channel.state.lock().unwrap();
        assert_eq!(state.critical_sections.len(), 1);
        assert!(
            state.critical_sections[0].2.is_some(),
            "driver's foreign block did not retain its origin"
        );
    }
    assert!(a_channel.state.lock().unwrap().critical_sections.is_empty());
    assert!(a_channel.state.lock().unwrap().turn.is_none());
    assert!(a.context.has_cross_entry_gate_claim() && !a.finished());
    let refusal = |cell: &str| {
        format!(
                "globalThis.gateRefusal = ''; const [, owner, acquired] = __celld.__with_input_gate_context(() => __celld.__gate_acquire({})); acquired.catch(error => {{ gateRefusal = error.message; }}); return owner === '';",
                serde_json::to_string(cell).unwrap()
            )
    };
    let inspection = IoContext::tracked(&worker.runtime_state);
    let refused = |origin: &Arc<IoContext>, cell: &str, message: &str| {
        assert_eq!(
            evaluate_in(&worker, origin, &b.context, &refusal(cell)).unwrap(),
            "true"
        );
        checkpoint(&worker, &b.context);
        check(
            &worker,
            &inspection,
            &format!(
                "return gateRefusal.includes({});",
                serde_json::to_string(message).unwrap()
            ),
        );
    };
    let closed = IoContext::tracked(&worker.runtime_state);
    closed.attach_guest_storage(closed_channel.clone()).unwrap();
    closed_channel.request_cleanup();
    refused(
        &closed,
        &cell,
        "lifecycle channel refuses critical sections",
    );
    refused(&a.context, "Other:cell", "belongs to another object");
    b_channel.finish_turn().unwrap();
    refused(&a.context, &cell, "requires its active execution turn");
    assert_eq!(b_channel.state.lock().unwrap().critical_sections.len(), 1);
    // The driver's block keeps the native gate across its turns.
    let admitted = Arc::new(AtomicBool::new(false));
    let start_queued = {
        let (queued, admitted) = (queued.clone(), admitted.clone());
        std::thread::spawn(move || {
            queued.begin_turn().unwrap();
            admitted.store(true, Ordering::Release);
        })
    };
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(
        !admitted.load(Ordering::Acquire),
        "another event entered a foreign block's native hold"
    );
    drain_entry(&mut worker, &mut b, ops, &b_channel).await;
    assert!(b_channel.state.lock().unwrap().critical_sections.is_empty());
    assert!(!a.context.has_cross_entry_gate_claim());
    start_queued.join().unwrap();
    assert!(admitted.load(Ordering::Acquire));
    queued.finish_turn().unwrap();
    check(
        &worker,
        &inspection,
        "return foreignCompleted && constructors === 1 && !responseDecoded;",
    );
    assert!(matches!(
        a_reply.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    a_channel.begin_turn().unwrap();
    assert!(worker.turn_poll_guest_cell(&mut a).unwrap().is_empty());
    assert!(a.finished());
    a_channel.finish_turn().unwrap();
    let response = a_reply.try_recv().unwrap().unwrap();
    assert_eq!(response.status, 201);
    assert_eq!(response.body, b"A:71");
    assert_eq!(
        (response.write_position, response.observed_position),
        (None, None)
    );
    {
        let (_locker, _cells) = worker.lock();
        assert!(storage::activation_epoch(&cell).is_none());
    }
    let (values, borrowed_calls, queued_turns) = host.finish();
    assert!(queued_turns > 0, "C was not queued behind B's held block");
    assert!(
        borrowed_calls >= 7,
        "foreign put/get/SQL and block effects did not borrow admission"
    );
    for key in [
        "foreign-checkpoint",
        "foreign-critical",
        "foreign-transaction",
        "driver",
        "response-owner",
    ] {
        assert!(matches!(
            values.iter().find(|(stored, _)| stored == key),
            Some((_, storage::StoredValue::V8(_)))
        ));
    }
    eprintln!("original V8 foreign checkpoint: A's queued put/get/SQL ran under B's ordinary turn after A released its hold; A's transactionSync and its block, with a nested async transaction, started in B's checkpoint were held by B's native turn with A's cross-entry claim, their effects used A's grant via B's delegated turn, C queued until it finished and A then retired normally; cleanup-only origin, wrong object and turnless driver refused before a native hold; A's response getter stayed deferred during B and wrote only in A's admitted poll; cached constructor once, host cleanup had no transaction/cursor/turn leak; local sockets only, not JS VM qualification");
}

async fn check_owned_critical_sections() {
    let address = address("own-critical");
    let cell = address.cell_scope("Counter", "named-object").unwrap();
    let host = SharedHost::new(&cell);
    let source = r#"
            globalThis.visited = [];
            export class Counter {
                constructor(ctx) {
                    this.ctx = ctx;
                    ctx.blockConcurrencyWhile(async () => {
                        await new Promise(resolve => setTimeout(resolve, 5));
                        await ctx.storage.put('constructor', 1);
                        visited.push('constructor');
                    });
                }
                async fetch(request) {
                    if (new URL(request.url).pathname === '/background') {
                        this.ctx.blockConcurrencyWhile(async () => {
                            await new Promise(resolve => setTimeout(resolve, 5));
                            await this.ctx.storage.put('late-critical', 5);
                            visited.push('late-critical');
                        });
                        return new Response('early');
                    }
                    if (new URL(request.url).pathname === '/ping') {
                        visited.push('B'); return new Response('B');
                    }
                    await this.ctx.blockConcurrencyWhile(async () => {
                        await this.ctx.storage.transaction(async tx => {
                            await tx.put('transaction', 3);
                            await new Promise(resolve => setTimeout(resolve, 5));
                            if (await tx.get('transaction') !== 3) throw new Error('transaction mismatch');
                            visited.push('transaction');
                        });
                        // Original outer block must drain an unawaited nested
                        // block before opening the host gate to B.
                        this.ctx.blockConcurrencyWhile(async () => {
                            await new Promise(resolve => setTimeout(resolve, 5));
                            await this.ctx.storage.put('nested', 2);
                            visited.push('nested');
                        });
                    });
                    visited.push('A'); return new Response('A');
                }
            }
            export default { fetch() { return new Response('guest'); } };
        "#;
    let mut worker = Worker::load_guest_config(Arc::new(WorkerConfig::new(WorkerConfigOptions {
        src: source.into(),
        script_name: address.storage_root(),
        do_classes: vec!["Counter".into()],
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
    })))
    .unwrap();
    let a_channel = host.channels[0].clone();
    let b_channel = host.channels[1].clone();
    a_channel.begin_turn().unwrap();
    let (job, mut a_reply) = fetch_job(&cell, Some("named-object"), "/block");
    let (entry, ops) = worker
        .turn_begin_guest_cell(job, None, a_channel.clone())
        .unwrap();
    let mut a = entry.unwrap();
    assert_eq!(a_channel.state.lock().unwrap().critical_sections.len(), 1);
    let event = a_channel.state.lock().unwrap().critical_sections[0].0;
    let owner = a.context.continuation_id().unwrap().to_string();
    check(
        &worker,
        &a.context,
        &format!(
            "__celld.__gate_release({}, {}, {}, null); return true;",
            serde_json::to_string(&cell).unwrap(),
            event + 1,
            serde_json::to_string(&owner).unwrap(),
        ),
    );
    assert_eq!(
        a_channel.state.lock().unwrap().critical_sections.len(),
        1,
        "stale local release consumed the live host critical capability"
    );
    let before = asyncrt::spawn_count();
    check(&worker, &a.context, &format!(
            "globalThis.stubGateRefused = false; __celld.__gate_wait({}).catch(e => {{ stubGateRefused = e.message.includes('guest stub input-gate wait'); }}); return true;",
            serde_json::to_string(&cell).unwrap(),
        ));
    checkpoint(&worker, &a.context);
    check(&worker, &a.context, "return stubGateRefused;");
    assert_eq!(
        asyncrt::spawn_count(),
        before,
        "guest stub wait queued a local native gate"
    );
    a_channel.finish_turn().unwrap();
    let b_admitted = Arc::new(AtomicBool::new(false));
    let admitted = b_admitted.clone();
    let queued = b_channel.clone();
    let start_b = std::thread::spawn(move || {
        queued.begin_turn().unwrap();
        admitted.store(true, Ordering::Release);
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(
        !b_admitted.load(Ordering::Acquire),
        "B entered the constructor's native hold"
    );
    drain_entry(&mut worker, &mut a, ops, &a_channel).await;
    assert_eq!(a_reply.try_recv().unwrap().unwrap().body, b"A");
    assert!(a_channel.state.lock().unwrap().critical_sections.is_empty());
    start_b.join().unwrap();
    assert!(b_admitted.load(Ordering::Acquire));
    let (job, mut b_reply) = fetch_job(&cell, Some("named-object"), "/ping");
    let (entry, ops) = worker
        .turn_begin_guest_cell(job, None, b_channel.clone())
        .unwrap();
    let b = entry.unwrap();
    assert!(b.finished() && ops.is_empty());
    b_channel.finish_turn().unwrap();
    assert_eq!(b_reply.try_recv().unwrap().unwrap().body, b"B");
    let inspection = IoContext::tracked(&worker.runtime_state);
    check(
        &worker,
        &inspection,
        "return visited.join(',') === 'constructor,transaction,nested,A,B';",
    );
    {
        let (_locker, _cells) = worker.lock();
        assert!(storage::activation_epoch(&cell).is_none());
    }
    let (values, borrowed, queued) = host.finish();
    assert_eq!(borrowed, 0);
    assert!(
        queued > 0,
        "host did not queue B behind the original native input gate"
    );
    for key in ["constructor", "transaction", "nested"] {
        assert!(matches!(
            values.iter().find(|(stored, _)| stored == key),
            Some((_, storage::StoredValue::V8(_)))
        ));
    }

    let background_host = Host::named(cell.clone(), Some("named-object"));
    background_host.channel.begin_turn().unwrap();
    let (job, mut reply) = fetch_job(&cell, Some("named-object"), "/background");
    let (entry, ops) = worker
        .turn_begin_guest_cell(job, None, background_host.channel.clone())
        .unwrap();
    let mut entry = entry.unwrap();
    let response = reply.try_recv().unwrap().unwrap();
    assert_eq!(response.body, b"early");
    assert_eq!(
        (response.write_position, response.observed_position),
        (None, None)
    );
    assert!(
        !entry.finished(),
        "guest-local reply retired its unawaited critical block"
    );
    assert!(background_host.channel.holds_critical_section());
    background_host.channel.finish_turn().unwrap();
    drain_entry(&mut worker, &mut entry, ops, &background_host.channel).await;
    assert!(!background_host.channel.holds_critical_section());
    check(
        &worker,
        &inspection,
        "return visited.join(',') === 'constructor,transaction,nested,A,B,late-critical';",
    );
    assert!(background_host
        .finish()
        .iter()
        .any(|(key, _)| key == "late-critical"));

    // Failure uses the original harness's reset/nested transaction unwind.
    // Host failure keeps native admission shut until trusted revocation.
    let failed_host = Host::named(cell.clone(), Some("named-object"));
    let failed_source = r#"
            export class Counter {
                constructor(ctx) { this.ctx = ctx; }
                async fetch() {
                    return this.ctx.blockConcurrencyWhile(async () => {
                        await this.ctx.storage.transaction(async tx => {
                            await tx.put('failed-transaction', 4);
                            await new Promise(resolve => setTimeout(resolve, 1));
                            throw new Error('critical-body-failure');
                        });
                    });
                }
            }
            export default { fetch() { return new Response('guest'); } };
        "#;
    let mut failed_worker =
        Worker::load_guest_config(Arc::new(WorkerConfig::new(WorkerConfigOptions {
            src: failed_source.into(),
            script_name: address.storage_root(),
            do_classes: vec!["Counter".into()],
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
        })))
        .unwrap();
    failed_host.channel.begin_turn().unwrap();
    let (job, mut reply) = fetch_job(&cell, Some("named-object"), "/fail");
    let (entry, ops) = failed_worker
        .turn_begin_guest_cell(job, None, failed_host.channel.clone())
        .unwrap();
    let mut entry = entry.unwrap();
    failed_host.channel.finish_turn().unwrap();
    drain_entry(&mut failed_worker, &mut entry, ops, &failed_host.channel).await;
    assert!(reply
        .try_recv()
        .unwrap()
        .err()
        .unwrap()
        .to_string()
        .contains("critical-body-failure"));
    failed_host.channel.begin_turn().unwrap();
    {
        let mut state = failed_host.channel.state.lock().unwrap();
        assert!(state.critical_sections.is_empty());
        assert!(failed_host
            .channel
            .exchange(&mut state, Operation::GetAlarm {})
            .unwrap_err()
            .to_string()
            .contains("critical section failed"));
    }
    failed_host.channel.finish_turn().unwrap();
    assert!(!failed_host
        .finish()
        .iter()
        .any(|(key, _)| key == "failed-transaction"));
    eprintln!("original V8 own-event critical sections: constructor block, awaited original async transaction and unawaited nested block held the original host gate across guest turns; B's native admission queued until all completed; host minted opaque capabilities, stale local release harmless, no guest SQLite; unawaited top-level block retained original InFlight/native work after guest-local reply and persisted its late write; original rejection rolled back and latched host refusal until trusted revoke; local sockets only, stub wait/production scheduler/JS VM unqualified here (foreign drivers: check_foreign_execution_turn)");
}

pub(crate) fn check_original_v8_storage() {
    Engine::init();
    let worker = Worker::load_config(Arc::new(WorkerConfig::new(WorkerConfigOptions {
            src: "export class Counter { constructor(ctx) { globalThis.storageFixture = ctx.storage; } fetch() { return new Response('bridge'); } } export default { fetch() { return new Response('bridge'); } }".into(),
            script_name: "js-guest-bridge-check".into(), do_classes: vec!["Counter".into()],
            bindings: vec![], r2_bindings: vec![], d1_bindings: vec![], kv_bindings: vec![],
            queue_bindings: vec![], queue_consumers: vec![], workflow_bindings: vec![],
            vars: vec![], node: "check".into(), modules: vec![], compat: Default::default(),
        }))).unwrap();
    let native = IoContext::tracked(&worker.runtime_state);
    let trap = cell("native-trap");
    let directory = tempfile::tempdir().unwrap();
    {
        let (_locker, _installed) = worker.lock();
        storage::open_at_epoch(
            &trap,
            directory.path().join("native.sqlite").to_str().unwrap(),
            7,
            false,
            None,
            false,
        )
        .unwrap();
    }
    let trap_js = serde_json::to_string(&trap).unwrap();
    check(&worker, &native, &format!("__celld.__storage_put({trap_js}, 'native', 3); return __celld.__storage_get({trap_js}, 'native') === 3;"));
    let native_bytes = {
        let (_locker, _installed) = worker.lock();
        let Some(storage::StoredValue::V8(bytes)) = storage::get_stored(&trap, "native").unwrap()
        else {
            panic!("native storage did not persist clone")
        };
        bytes
    };

    let first = Host::new(cell("one"));
    let second = Host::new(cell("two"));
    let a = IoContext::tracked(&worker.runtime_state);
    let b = IoContext::tracked(&worker.runtime_state);
    assert!(IoContext::new()
        .attach_guest_storage(first.channel.clone())
        .is_err());
    a.attach_guest_storage(first.channel.clone()).unwrap();
    b.attach_guest_storage(second.channel.clone()).unwrap();
    assert!(native.attach_guest_storage(first.channel.clone()).is_err());
    assert!(a.attach_guest_storage(second.channel.clone()).is_err());
    let one = serde_json::to_string(&first.channel.cell).unwrap();
    let two = serde_json::to_string(&second.channel.cell).unwrap();
    check(&worker, &a, &format!("let denied = false; try {{ __celld.__storage_put({one}, 'inactive', 1); }} catch(e) {{ denied = e.message.includes('active execution turn'); }} return denied;"));
    first.channel.begin_turn().unwrap();
    second.channel.begin_turn().unwrap();
    // T19: the original socket ops answer from the host's registry, scoped
    // to the issuing object, and frames leave only through the broker.
    let (socket_frames, mut socket_sent) = tokio::sync::mpsc::unbounded_channel();
    let frames_installed = install_sockets(socket_frames).is_ok();
    check(&worker, &a, &format!("const id = __celld.__ws_accept(4000000000, {one}, JSON.stringify(['t']), false); globalThis.hostSocket = id; const rows = JSON.parse(__celld.__ws_list({one}, 't')); return typeof id === 'number' && id !== 4000000000 && rows.length === 1 && rows[0].id === id && rows[0].tags[0] === 't' && rows[0].attachment === null;"));
    check(&worker, &a, &format!("__celld.__ws_attachment_set(hostSocket, new Uint8Array([1, 2, 3])); const rows = JSON.parse(__celld.__ws_list({one})); return rows.length === 1 && rows[0].attachment.join() === '1,2,3' && JSON.parse(__celld.__ws_list({one}, 'other')).length === 0;"));
    check(&worker, &a, &format!("__celld.__ws_auto_response_set({one}, 'ping', 'pong'); const pair = JSON.parse(__celld.__ws_auto_response_get({one})); const stamp = __celld.__ws_auto_response_ts(hostSocket); return pair[0] === 'ping' && pair[1] === 'pong' && stamp === null;"));
    check(&worker, &a, &format!("__celld.__ws_auto_response_set({one}, null, null); return __celld.__ws_auto_response_get({one}) === 'null';"));
    check(&worker, &a, &format!("let denied = false; try {{ __celld.__ws_list({two}, null); }} catch (e) {{ denied = e.message.includes('another object'); }} return denied;"));
    check(&worker, &a, "let denied = false; try { __celld.__ws_attachment_set(hostSocket + 1000, new Uint8Array([9])); } catch (e) { denied = e.message.includes('not a hibernatable socket of this object'); } return denied;");
    check(&worker, &a, "let denied = false; try { __celld.__ws_accept_regular(1, 'x'); } catch (e) { denied = e.message.includes('not connected in application guests'); } return denied;");
    check(&worker, &a, "__celld.__ws_send(hostSocket, 'hello'); __celld.__ws_send_binary(hostSocket, new Uint8Array([7, 8])); __celld.__ws_close(hostSocket, 1000, 'bye'); return true;");
    if frames_installed {
        use celld_applications::invocation_wire::SocketFrame;
        let host_socket: u64 = evaluate(&worker, &a, "return hostSocket;")
            .unwrap()
            .parse()
            .unwrap();
        let mut got = Vec::new();
        while let Ok(frame) = socket_sent.try_recv() {
            assert_eq!(frame.ws, host_socket);
            assert!(
                std::ptr::eq(frame.context.as_ptr(), Arc::as_ptr(&a)),
                "frame lost its event"
            );
            got.push(frame.frame);
        }
        assert_eq!(
            got,
            vec![
                SocketFrame::Text {
                    data: "hello".into()
                },
                SocketFrame::Binary { data: vec![7, 8] },
                SocketFrame::Close {
                    code: 1000,
                    reason: "bye".into()
                },
            ]
        );
    }
    check(&worker, &a, &format!("globalThis.fixtureReady = 'pending'; __celld.__dispatchTo({one}, 'http://check/', 'GET', '', '[]').then(() => globalThis.fixtureReady = 'ok', e => globalThis.fixtureReady = e.message); return true;"));
    checkpoint(&worker, &a);
    assert_eq!(
        evaluate(&worker, &a, "return globalThis.fixtureReady;").unwrap(),
        "ok"
    );
    check(
        &worker,
        &a,
        &format!(
            r#"
            const storage = globalThis.storageFixture;
            storage.sql.exec('CREATE TABLE js_sql (value INTEGER NOT NULL)');
            storage.sql.exec('INSERT INTO js_sql VALUES (?)', 23);
            const cursor = storage.sql.exec("SELECT value, x'00ff' AS bytes, NULL AS nil, 9e999 AS inf, 9223372036854775807 AS big FROM js_sql UNION ALL SELECT 24, x'', NULL, 9e999, 9223372036854775807");
            if (typeof cursor._cursorId !== 'string' || cursor._cursorId.length !== 64) return false;
            const rows = [...cursor.raw()];
            if (!(rows.length === 2 && rows[0][0] === 23 && rows[1][0] === 24
                && rows[0][1] instanceof ArrayBuffer && new Uint8Array(rows[0][1])[1] === 255
                && rows[1][1] instanceof ArrayBuffer && rows[1][1].byteLength === 0
                && rows[0][2] === null && rows[0][3] === Infinity && rows[1][3] === Infinity
                && rows[0][4] === Number('9223372036854775807') && cursor.rowsRead === 2)) return false;
            const blob = storage.sql.exec('SELECT ? AS bytes', new Uint8Array([1, 0, 255])).one().bytes;
            if (!(blob instanceof ArrayBuffer && new Uint8Array(blob)[2] === 255 && storage.sql.databaseSize > 0)) return false;
            const ingested = storage.sql.ingest('INSERT INTO js_sql VALUES (25);');
            if (!(ingested.rowsWritten === 1 && ingested.statementCount === 1 && ingested.remainder === '')) return false;
            const count = storage.sql.exec('SELECT count(*) AS n FROM js_sql').one().n;
            const cached = storage.sql.exec('SELECT count(*) AS n FROM js_sql');
            if (!(count === 2 && cached.one().n === 2 && cached.reusedCachedQueryForTest)) return false;
            let invalid = 0;
            try {{ __celld.__sql_cursor_start({one}, 'CREATE TABLE forbidden_json (v); SELECT ?', 'not-json'); }} catch(e) {{ invalid++; }}
            try {{ __celld.__sql_cursor_start({one}, 'CREATE TABLE forbidden_blob (v); SELECT ?, ?', '[null,null]', [[0, new Uint8Array([1])], [0, new Uint8Array([2])]]); }} catch(e) {{ invalid++; }}
            if (!(invalid === 2 && storage.sql.exec("SELECT count(*) AS n FROM sqlite_master WHERE name LIKE 'forbidden_%'").one().n === 0)) return false;
            globalThis.savedCursor = __celld.__sql_cursor_start({one}, 'SELECT 41 UNION ALL SELECT 42', '[]').cursorId;
            return true;
        "#
        ),
    );
    check(&worker, &b, "let denied = false; try { __celld.__sql_cursor_next(globalThis.savedCursor); } catch(e) { denied = e.message.includes('no longer active'); } return denied;");
    check(
        &worker,
        &b,
        "__celld.__sql_cursor_close(globalThis.savedCursor); return true;",
    );
    check(&worker, &a, "const row = __celld.__sql_cursor_next(globalThis.savedCursor); __celld.__sql_cursor_close(globalThis.savedCursor); let closed = false; try { __celld.__sql_cursor_next(globalThis.savedCursor); } catch(e) { closed = e.message.includes('no longer active'); } return row[0] === 42 && closed;");
    check(
        &worker,
        &a,
        &format!(
            r#"
            const storage = globalThis.storageFixture;
            __celld.__storage_queue_put({one}, 'parent-before-rollback', 31);
            storage.transactionSync(() => {{
                storage.kv.put('tx-commit', 32);
                storage.transactionSync(() => storage.kv.put('tx-nested-commit', 33));
                try {{ storage.transactionSync(() => {{ storage.kv.put('tx-inner-discarded', 34); throw new Error('inner fixture'); }}); }} catch(e) {{}}
            }});
            let rolled = false;
            try {{ storage.transactionSync(() => {{ storage.kv.put('tx-exception-rollback', 35); throw new Error('rollback fixture'); }}); }} catch(e) {{ rolled = e.message === 'rollback fixture'; }}
            __celld.__storage_transaction_control({one}, 'start', false, 'outer-check');
            __celld.__storage_transaction_control({one}, 'start', true, 'inner-check');
            let ordered = false; try {{ __celld.__storage_transaction_control({one}, 'commit', false, 'outer-check'); }} catch(e) {{ ordered = e.message.includes('nesting order'); }}
            __celld.__storage_transaction_control({one}, 'rollback', true, 'inner-check');
            __celld.__storage_transaction_control({one}, 'rollback', false, 'outer-check');
            return rolled && ordered && storage.kv.get('parent-before-rollback') === 31
                && storage.kv.get('tx-commit') === 32 && storage.kv.get('tx-nested-commit') === 33
                && storage.kv.get('tx-inner-discarded') === undefined
                && storage.kv.get('tx-exception-rollback') === undefined;
        "#
        ),
    );
    check(
        &worker,
        &a,
        &format!(
            r#"
            __celld.__alarm_set({one}, 1700000000123);
            __celld.__storage_transaction_control({one}, 'start', false, 'alarm-check');
            __celld.__alarm_set({one}, 1700000000456);
            __celld.__storage_transaction_control({one}, 'rollback', false, 'alarm-check');
            if (__celld.__alarm_get({one}) !== 1700000000123) return false;
            __celld.__alarm_delete({one});
            __celld.__storage_put_many({one}, [['list-a', 1], ['list-b', 2], ['list-c', 3]]);
            const listed = __celld.__storage_list({one}, JSON.stringify({{ prefix: 'list-', startAfter: 'list-a', end: 'list-d', limit: 2, reverse: true }}));
            if (!(listed instanceof Map && [...listed.keys()].join(',') === 'list-c,list-b')) return false;
            const deleted = __celld.__storage_delete_many({one}, ['list-a', 'list-a', 'missing']);
            return deleted === 1 && __celld.__storage_delete({one}, 'list-b') === true
                && __celld.__storage_delete({one}, 'list-b') === false && __celld.__alarm_get({one}) === null;
        "#
        ),
    );
    check(&worker, &b, &format!("__celld.__storage_put({two}, 'delete-all-test', 1); __celld.__alarm_set({two}, 1700000000000); __celld.__storage_delete_all({two}, false); if (__celld.__storage_get({two}, 'delete-all-test') !== undefined || __celld.__alarm_get({two}) !== 1700000000000) return false; __celld.__storage_delete_all({two}, true); return __celld.__alarm_get({two}) === null;"));
    check(
        &worker,
        &a,
        &format!(
            r#"
            let denied = 0;
            for (const call of [
                () => __celld.__storage_sync({one}),
                () => __celld.__d1_run({one}, '{{}}'),
                () => __celld.__queue_alarm_set_wait({one}, 1700000000000),
                () => __celld.__sql_cursor_start({trap_js}, 'DROP TABLE _cf_KV', '[]'),
                () => __celld.__storage_delete_all({trap_js}, true),
                () => __celld.__alarm_set({trap_js}, 1700000000000),
            ]) {{ try {{ call(); }} catch(e) {{ denied++; }} }}
            return denied === 6;
        "#
        ),
    );
    check(
        &worker,
        &a,
        &format!(
            r#"
            __celld.__storage_transaction_control({one}, 'start', false, 'sync-list');
            __celld.__storage_put_many({one}, [['sl-a', 1], ['sl-b', 2], ['sl-c', 3], ['sl-d', 4], ['zz', 5]]);
            __celld.__storage_put({one}, 'sl-bytes', new Uint8Array([0, 255]));
            __celld.__storage_put({one}, 'sl-big', 9007199254740993n);
            const sentinel = {{}};
            const drain = (handle) => {{
                const rows = [];
                for (let row; (row = __celld.__storage_sync_list_next(handle, sentinel)) !== null;) rows.push(row);
                return rows;
            }};
            const start = (options) => __celld.__storage_sync_list_start({one}, JSON.stringify(options));
            const rows = drain(start({{ prefix: 'sl-', startAfter: 'sl-a', limit: 3 }}));
            const reversed = drain(start({{ prefix: 'sl-', reverse: true, limit: 2 }}));
            const first = start({{ prefix: 'sl-' }});
            const second = start({{ prefix: 'sl-' }});
            const rejects = (call) => {{ try {{ call(); return false; }} catch(e) {{ return true; }} }};
            const ok = rows.map(row => row[0]).join() === 'sl-b,sl-big,sl-bytes'
                && rows[1][1] === 9007199254740993n && rows[2][1] instanceof Uint8Array && rows[2][1][1] === 255
                && reversed.map(row => row[0]).join() === 'sl-d,sl-c'
                && typeof first === 'string' && rejects(() => __celld.__storage_sync_list_next(first, sentinel))
                && drain(second).length === 6
                && rejects(() => __celld.__storage_sync_list_next(second, sentinel))
                && rejects(() => __celld.__storage_sync_list_next(1, sentinel))
                && rejects(() => __celld.__storage_sync_list_next('not a cursor', sentinel));
            __celld.__storage_transaction_control({one}, 'rollback', false, 'sync-list');
            return ok && __celld.__storage_get({one}, 'sl-a') === undefined;
        "#
        ),
    );
    check(
        &worker,
        &a,
        &format!(
            r#"
            const value = {{ big: 9007199254740993n, nan: NaN, minusZero: -0,
                bytes: new Uint8Array([0, 255, 42]), date: new Date(1700000000000),
                map: new Map([['answer', 42]]), set: new Set(['a', 'b']) }};
            value.self = value;
            __celld.__storage_put({one}, 'clone', value);
            const read = __celld.__storage_get({one}, 'clone');
            const many = __celld.__storage_get_many({one}, ['legacy', 'clone', 'missing', 'clone']);
            if (!(read.self === read && read.big === value.big && Number.isNaN(read.nan)
                && Object.is(read.minusZero, -0) && read.bytes instanceof Uint8Array
                && read.bytes[1] === 255 && read.date.getTime() === value.date.getTime()
                && read.map.get('answer') === 42 && read.set.has('b')
                && many instanceof Map && many.size === 2 && many.get('legacy').answer === 42
                && __celld.__storage_get({one}, 'missing') === undefined)) return false;
            __celld.__storage_put_many({one}, [['batch-a', 11], ['batch-b', 12]]);
            const bytes = __celld.__sc_encode({{ stored: 'tree' }});
            const tagged = new Uint8Array(bytes.length + 1); tagged[0] = 1; tagged.set(bytes, 1);
            const sentinel = {{}};
            __celld.__storage_put_serialized({one}, 'tagged', tagged);
            const tree = __celld.__storage_get({one}, 'tagged', sentinel);
            return tree[0] === sentinel && tree[1].stored === 'tree';
        "#
        ),
    );
    check(&worker, &a, &format!("let refused = false; try {{ __celld.__storage_put({one}, 'host-refused', 1); }} catch(e) {{ refused = e.message.includes('deliberately refused'); }} __celld.__storage_put({one}, 'after-refusal', 9); return refused && __celld.__storage_get({one}, 'after-refusal') === 9;"));
    check(&worker, &a, &format!("let denied = false; try {{ __celld.__storage_put({two}, 'foreign', 1); }} catch(e) {{ denied = e.message.includes('another object'); }} return denied;"));
    check(&worker, &native, &format!("let denied = false; try {{ __celld.__storage_put({trap_js}, 'native', 99); }} catch(e) {{ denied = e.message.includes('owning event'); }} return denied;"));
    {
        let (_locker, _installed) = worker.lock();
        assert!(storage::activation_epoch(&first.channel.cell).is_none());
        assert!(storage::activation_epoch(&second.channel.cell).is_none());
        let Some(storage::StoredValue::V8(bytes)) = storage::get_stored(&trap, "native").unwrap()
        else {
            panic!("native fallback trap changed")
        };
        assert!(!bytes.is_empty());
    }

    check(
        &worker,
        &a,
        &format!("__celld.__storage_queue_put({one}, 'queued-a', 17); return true;"),
    );
    assert!(first
        .channel
        .finish_turn()
        .unwrap_err()
        .to_string()
        .contains("not flushed"));
    let pending_bytes = first.channel.state.lock().unwrap().pending_bytes;
    assert!(queue(
        &first.channel,
        vec![("x".repeat(wire::MAX_MESSAGE_BYTES), vec![])]
    )
    .unwrap_err()
    .contains("message limit"));
    assert_eq!(
        first.channel.state.lock().unwrap().pending_bytes,
        pending_bytes
    );
    let (unused_guest, _unused_host) = UnixStream::pair().unwrap();
    let same_object =
        Channel::new(first.channel.cell.clone(), "a".repeat(64), unused_guest).unwrap();
    let c = IoContext::tracked(&worker.runtime_state);
    c.attach_guest_storage(same_object.clone()).unwrap();
    check(
        &worker,
        &c,
        &format!("__celld.__storage_cancel_pending_puts({one}); return true;"),
    );
    assert_eq!(first.channel.state.lock().unwrap().pending.len(), 1);
    check(&worker, &b, &format!("__celld.__storage_queue_put_many({two}, [['queued-b', 18], ['queued-c', 19]]); __celld.__storage_flush_pending_puts({two}); return __celld.__storage_get({two}, 'queued-b') === 18;"));
    assert_eq!(first.channel.state.lock().unwrap().pending.len(), 1);
    check(&worker, &a, &format!("__celld.__storage_flush_pending_puts({one}); __celld.__storage_queue_put_serialized({one}, 'cancelled', __celld.__sc_encode(88)); __celld.__storage_cancel_pending_puts({one}); __celld.__storage_flush_pending_puts({one}); return __celld.__storage_get({one}, 'queued-a') === 17 && __celld.__storage_get({one}, 'cancelled') === undefined;"));

    // Create the reaction under A, resolve and checkpoint under B. CPED
    // must supply A's channel even though the thread's CurrentGuard is B.
    check(&worker, &a, &format!("globalThis.reactionResult = 'pending'; new Promise(resolve => globalThis.releaseReaction = resolve).then(() => {{ __celld.__storage_put({one}, 'reaction-a', 21); globalThis.reactionResult = 'ok'; }}).catch(e => globalThis.reactionResult = e.message); return true;"));
    {
        let (mut locker, _installed) = worker.lock();
        v8::scope!(let hs, &mut *locker);
        let realm = worker.realm(hs);
        let cs = &mut v8::ContextScope::new(hs, realm.context);
        let _current = CurrentGuard::enter(b.clone());
        let previous = install_io_context(cs, &b);
        run_internal_snippet(cs, "globalThis.releaseReaction(); return true;").unwrap();
        cs.perform_microtask_checkpoint();
        assert_eq!(
            run_internal_snippet(cs, "return globalThis.reactionResult;")
                .unwrap()
                .to_rust_string_lossy(cs),
            "ok"
        );
        restore_io_context(cs, previous);
    }
    check(
        &worker,
        &a,
        &format!("return __celld.__storage_get({one}, 'reaction-a') === 21;"),
    );
    check(
        &worker,
        &b,
        &format!("return __celld.__storage_get({two}, 'reaction-a') === undefined;"),
    );
    first.channel.finish_turn().unwrap();
    second.channel.finish_turn().unwrap();

    // Host refusal is catchable above. An uncertain committed write or a
    // semantically corrupt reply terminates execution, including finally.
    first.channel.begin_turn().unwrap();
    let lost = evaluate(&worker, &a, &format!("globalThis.lostHooks = []; try {{ __celld.__storage_put({one}, 'lost-reply', 55); }} catch(e) {{ lostHooks.push('catch'); }} finally {{ lostHooks.push('finally'); }} lostHooks.push('after'); return true;"));
    assert!(lost.unwrap_err().contains("guest storage retired"));
    check(
        &worker,
        &native,
        "return globalThis.lostHooks.length === 0;",
    );
    assert!(first.channel.begin_turn().is_err());
    second.channel.begin_turn().unwrap();
    let invalid = evaluate(&worker, &b, &format!("globalThis.invalidHooks = []; try {{ __celld.__storage_get({two}, 'invalid-reply'); }} catch(e) {{ invalidHooks.push('catch'); }} finally {{ invalidHooks.push('finally'); }} invalidHooks.push('after'); return true;"));
    assert!(invalid.unwrap_err().contains("invalid get reply"));
    check(
        &worker,
        &native,
        "return globalThis.invalidHooks.length === 0;",
    );
    same_object.retire();
    let first_values = first.finish();
    let second_values = second.finish();
    for forbidden in ["inactive", "foreign", "host-refused", "cancelled"] {
        assert!(!first_values
            .iter()
            .chain(&second_values)
            .any(|(key, _)| key == forbidden));
    }
    for key in [
        "clone",
        "batch-a",
        "batch-b",
        "tagged",
        "after-refusal",
        "queued-a",
        "reaction-a",
        "lost-reply",
    ] {
        let value = first_values.iter().find(|(found, _)| found == key).unwrap();
        let storage::StoredValue::V8(bytes) = &value.1 else {
            panic!("clone format changed")
        };
        assert!(bytes.starts_with(if key == "tagged" { &[1] } else { &[255, 15] }));
    }
    assert!(!second_values.iter().any(|(key, _)| key == "reaction-a"));
    {
        let (_locker, _installed) = worker.lock();
        let Some(storage::StoredValue::V8(bytes)) = storage::get_stored(&trap, "native").unwrap()
        else {
            panic!("native fallback trap changed")
        };
        assert_eq!(bytes, native_bytes);
        storage::close(&trap);
    }
    eprintln!("original V8 guest storage foundation: host-owned get/put and queued clones, legacy JSON/stored-tree envelopes, event grants and cross-event reaction attribution; unknown write/corrupt reply skips catch/finally; local Unix sockets, no JS VM or full driver qualification");
    eprintln!("original V8 SQL/transaction callbacks: original SqlStorage/SqlCursor and transactionSync wrappers use host opaque handles; typed rows, BLOB binds, cached-query metadata, ingest/size, nested commit/rollback, list/delete/alarm state, validation-before-prefix effects and fail-closed unbridged operations; local fixture, no JS VM/full lifecycle/proof qualification");
}
