use super::*;

#[cfg(test)]
pub(crate) async fn check_socket_frame_before_drain(entry: js::InFlight, channel: Arc<Channel>) {
    use std::os::unix::net::UnixStream;
    assert!(entry.finished());
    let (socket, mut peer) = UnixStream::pair().unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let control = Control::new(File::from(OwnedFd::from(socket))).unwrap();
    let (_reply, response) = oneshot::channel();
    let (sender, mut sockets) = mpsc::unbounded_channel();
    sender
        .send(entry.frame_for_test(
            7,
            celld_applications::invocation_wire::SocketFrame::Text {
                data: "late".into(),
            },
        ))
        .unwrap();
    let mut events = vec![Event {
        invocation: "a".repeat(64),
        cell: "Counter:socket-frame".into(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(5),
        cancelled: false,
        phase: Phase::Running(Running {
            channel,
            entry: Some(entry),
            response: Response::Http(response),
            ops: FuturesUnordered::new(),
            admission: None,
            delivery: None,
            poll_requested: false,
            reply_sent: true,
            failure: None,
            turns: 1,
            cancel_started: false,
            body: None,
            sockets: SocketOut::default(),
        }),
        rpc_upload: None,
    }];
    let dials = HashMap::new();
    publish(
        &control,
        &mut events,
        None,
        &mut HashMap::new(),
        &mut HashSet::new(),
        Some((&mut sockets, &dials)),
    )
    .await
    .unwrap();
    assert!(
        events.is_empty(),
        "the event with its frame sent did not drain"
    );
    let read = tokio::task::spawn_blocking(move || {
        let mut next =
            || GuestControl::decode(&wire::read_frame(&mut peer).unwrap().unwrap()).unwrap();
        (next(), next())
    })
    .await
    .unwrap();
    assert!(
        matches!(
            &read.0,
            GuestControl::SocketFrame {
                ws: 7,
                fetch: None,
                ..
            }
        ),
        "the frame was lost or overtaken: {read:?}"
    );
    assert!(matches!(read.1, GuestControl::Drained { .. }));
}

#[cfg(test)]
#[tokio::test]
async fn upgrade_fetch_head_is_a_returnable_bodiless_response() {
    let (outbound, mut sent) = mpsc::unbounded_channel();
    let (items, receiver) = mpsc::channel(2);
    let (reply, response) = oneshot::channel();
    let pump = tokio::spawn(fetch_pump(
        "a".repeat(64),
        "b".repeat(64),
        reply,
        receiver,
        outbound,
    ));
    items
        .send(BodyItem::Head(101, vec![("x".into(), "y".into())], Some(9)))
        .await
        .unwrap();
    let response = response.await.unwrap().unwrap();
    assert_eq!(response.status, 101);
    assert!(response.stream.is_none() && response.body.is_empty());
    assert!(matches!(
        &response.websocket,
        Some(js::HttpResponseWebSocket::Cell(target)) if target.id == 9 && target.tunnel.is_none()
    ));
    assert!(socket_upgrade_supported(&response));
    pump.await.unwrap();
    assert!(matches!(
        sent.recv().await,
        Some(GuestControl::FetchCancel { .. })
    ));
    // A non-upgrade head still streams its body under credit.
    let (items, receiver) = mpsc::channel(2);
    let (reply, response) = oneshot::channel();
    let (outbound, _sent) = mpsc::unbounded_channel();
    let pump = tokio::spawn(fetch_pump(
        "a".repeat(64),
        "b".repeat(64),
        reply,
        receiver,
        outbound,
    ));
    items.send(BodyItem::Head(200, vec![], None)).await.unwrap();
    let response = response.await.unwrap().unwrap();
    assert!(response.stream.is_some() && response.websocket.is_none());
    drop(response);
    drop(items);
    pump.await.unwrap();
}

#[cfg(test)]
#[tokio::test]
async fn service_upload_credit_and_cancellation() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct DropCount(Arc<AtomicUsize>);
    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let guard = DropCount(drops.clone());
    let observed = polls.clone();
    let source = futures_util::stream::poll_fn(move |_| {
        let _ = &guard;
        if observed.fetch_add(1, Ordering::SeqCst) == 0 {
            std::task::Poll::Ready(Some(Ok(vec![9; MAX_CHUNK_BYTES * 2 + 1])))
        } else {
            std::task::Poll::Pending
        }
    });
    let (credit, credits) = mpsc::channel(1);
    let (outbound, mut messages) = mpsc::unbounded_channel();
    let task = tokio::spawn(service_upload(
        "a".repeat(64),
        "b".repeat(64),
        Box::pin(source),
        credits,
        outbound,
    ));
    tokio::task::yield_now().await;
    assert_eq!(
        polls.load(Ordering::SeqCst),
        0,
        "source polled without credit"
    );
    for sequence in 0..3 {
        credit.send(sequence).await.unwrap();
        let message = tokio::time::timeout(Duration::from_secs(2), messages.recv())
            .await
            .unwrap()
            .unwrap();
        let GuestControl::FetchRequestChunk {
            sequence: actual,
            data,
            ..
        } = message
        else {
            panic!("expected chunk");
        };
        assert_eq!(actual, sequence);
        assert_eq!(
            data,
            vec![9; if sequence < 2 { MAX_CHUNK_BYTES } else { 1 }]
        );
        assert_eq!(
            polls.load(Ordering::SeqCst),
            1,
            "read ahead despite retained chunk bytes"
        );
        tokio::task::yield_now().await;
        assert!(
            messages.try_recv().is_err(),
            "sent bytes without another credit"
        );
    }
    credit.send(3).await.unwrap();
    while polls.load(Ordering::SeqCst) < 2 {
        tokio::task::yield_now().await;
    }
    drop(credit);
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1, "cancelled source leaked");
    assert!(messages.recv().await.is_none());

    let (credit, credits) = mpsc::channel(1);
    let (outbound, mut messages) = mpsc::unbounded_channel();
    let source = futures_util::stream::once(async { Err("producer failed".into()) });
    let task = tokio::spawn(service_upload(
        "a".repeat(64),
        "b".repeat(64),
        Box::pin(source),
        credits,
        outbound,
    ));
    credit.send(0).await.unwrap();
    assert!(
        matches!(messages.recv().await.unwrap(), GuestControl::FetchRequestEnd { sequence: 0, error: Some(error), .. } if error == "producer failed")
    );
    task.await.unwrap();
}

#[cfg(test)]
pub(crate) async fn check_pending_admission_publication(
    entry: js::InFlight,
    channel: Arc<Channel>,
) {
    use std::os::unix::net::UnixStream;
    assert!(entry.finished());
    let (socket, _peer) = UnixStream::pair().unwrap();
    let control = Control::new(File::from(OwnedFd::from(socket))).unwrap();
    let (_reply, response) = oneshot::channel();
    let mut events = vec![Event {
        invocation: "a".repeat(64),
        cell: "Counter:pending-admission".into(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(5),
        cancelled: false,
        phase: Phase::Running(Running {
            channel,
            entry: Some(entry),
            response: Response::Http(response),
            ops: FuturesUnordered::new(),
            admission: Some(tokio::spawn(std::future::pending())),
            delivery: None,
            poll_requested: false,
            reply_sent: true,
            failure: None,
            turns: 1,
            cancel_started: false,
            body: None,
            sockets: SocketOut::default(),
        }),
        rpc_upload: None,
    }];
    publish(
        &control,
        &mut events,
        Some(1),
        &mut HashMap::new(),
        &mut HashSet::new(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        events.len(),
        1,
        "publication inspected/retired an event before its admission joined"
    );
    let Phase::Running(event) = &mut events[0].phase else {
        unreachable!()
    };
    assert!(event.entry.is_some() && event.admission.is_some());
    let admission = event.admission.take().unwrap();
    admission.abort();
    assert!(admission.await.unwrap_err().is_cancelled());
    eprintln!("original V8 pending admission publication: completed entry retained untouched until admission joins; scheduler does not inspect pending channel critical claims");
}

#[cfg(test)]
pub(crate) async fn check_alarm_failure_publication(
    cell: String,
    entry: Option<js::InFlight>,
    channel: Arc<Channel>,
    error: anyhow::Error,
    counted: bool,
) {
    use std::os::unix::net::UnixStream;
    assert!(entry.as_ref().is_none_or(|entry| entry.finished()));
    let (socket, mut peer) = UnixStream::pair().unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let control = Control::new(File::from(OwnedFd::from(socket))).unwrap();
    let (reply, result) = oneshot::channel();
    reply.send(Err(error)).unwrap();
    let mut events = vec![Event {
        invocation: "a".repeat(64),
        cell,
        deadline: tokio::time::Instant::now() + Duration::from_secs(5),
        cancelled: false,
        phase: Phase::Running(Running {
            channel,
            entry,
            response: Response::Alarm {
                result,
                counts_against_limit: false,
            },
            ops: FuturesUnordered::new(),
            admission: None,
            delivery: None,
            poll_requested: false,
            reply_sent: false,
            failure: None,
            turns: 1,
            cancel_started: false,
            body: None,
            sockets: SocketOut::default(),
        }),
        rpc_upload: None,
    }];
    publish(
        &control,
        &mut events,
        None,
        &mut HashMap::new(),
        &mut HashSet::new(),
        None,
    )
    .await
    .unwrap();
    assert!(events.is_empty(), "joined alarm outcome did not retire");
    let terminal = GuestControl::decode(&wire::read_frame(&mut peer).unwrap().unwrap()).unwrap();
    assert_eq!(matches!(terminal, GuestControl::Failed { .. }), counted);
    assert_eq!(matches!(terminal, GuestControl::Cancelled { .. }), !counted);
}

#[cfg(test)]
pub(crate) async fn check_queue_event() {
    use crate::application_guest::{decode_queue_batch, decode_queue_result, encode_queue_batch};
    use js::{QueueBatch, QueueContentType, QueueMessage, QueueMetrics, QueueOutcome};
    use std::os::unix::net::UnixStream;
    js::Engine::init();
    let worker = |source: &str| {
        let config = WorkerConfig::new(WorkerConfigOptions {
            src: source.into(),
            script_name: "queue-check".into(),
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
        Worker::load_guest_config(Arc::new(config)).unwrap()
    };
    // The queue name selects what the handler does with its two messages.
    let mut consumer = worker(
        r#"export default { fetch() { return new Response('x'); }, async queue(batch) {
            const [first, second] = batch.messages;
            if (first.body !== 'one' || second.body.n !== 2 || second.attempts !== 3)
                throw new Error('batch mismatch');
            switch (batch.queue) {
                case 'explicit': first.ack(); second.retry({delaySeconds: 7}); break;
                case 'ack-all': batch.ackAll(); break;
                case 'retry-all': batch.retryAll({delaySeconds: 3}); break;
                case 'throw': first.ack(); throw new Error('boom');
                case 'implicit': await Promise.resolve(); break;
            }
        } };"#,
    );
    let mut fetch_only = worker("export default { fetch() { return new Response('x'); } };");
    let (socket, mut peer) = UnixStream::pair().unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let control = Control::new(File::from(OwnedFd::from(socket))).unwrap();
    let settle = |worker: &mut Worker, queue: &str| {
        let batch = QueueBatch {
            queue: queue.into(),
            messages: vec![
                QueueMessage {
                    id: "m1".into(),
                    timestamp_ms: 1,
                    body: b"one".to_vec(),
                    content_type: QueueContentType::Text,
                    attempts: 1,
                },
                QueueMessage {
                    id: "m2".into(),
                    timestamp_ms: 2,
                    body: br#"{"n":2}"#.to_vec(),
                    content_type: QueueContentType::Json,
                    attempts: 3,
                },
            ],
            metrics: QueueMetrics {
                backlog_count: 5.0,
                backlog_bytes: 9.0,
                oldest_message_timestamp_ms: None,
            },
        };
        let batch = decode_queue_batch(&encode_queue_batch(&batch).unwrap()).unwrap();
        assert_eq!(batch.messages[1].body, br#"{"n":2}"#);
        let (reply, result) = oneshot::channel();
        let (entry, ops) = worker.turn_begin(
            crate::WorkerJob::Queue {
                queued_at: std::time::Instant::now(),
                batch,
                reply,
            },
            None,
        );
        assert!(ops.is_empty() && entry.as_ref().is_some_and(|entry| entry.finished()));
        (entry, result)
    };
    for (queue, ack_all, retry_batch, acks, retries, outcome) in [
        (
            "explicit",
            false,
            None,
            vec!["m1"],
            vec![("m2", Some(7))],
            QueueOutcome::Ok,
        ),
        ("ack-all", true, None, vec![], vec![], QueueOutcome::Ok),
        (
            "retry-all",
            false,
            Some(3),
            vec![],
            vec![],
            QueueOutcome::Ok,
        ),
        // A throw keeps what the handler already acknowledged.
        (
            "throw",
            false,
            None,
            vec!["m1"],
            vec![],
            QueueOutcome::Exception,
        ),
        ("implicit", false, None, vec![], vec![], QueueOutcome::Ok),
    ] {
        let (entry, result) = settle(&mut consumer, queue);
        let mut events = vec![Event {
            invocation: "a".repeat(64),
            cell: String::new(),
            deadline: tokio::time::Instant::now() + Duration::from_secs(5),
            cancelled: false,
            phase: Phase::Running(Running {
                channel: Channel::detached(),
                entry,
                response: Response::Queue(result),
                ops: FuturesUnordered::new(),
                admission: None,
                delivery: None,
                poll_requested: false,
                reply_sent: false,
                failure: None,
                turns: 1,
                cancel_started: false,
                body: None,
                sockets: SocketOut::default(),
            }),
            rpc_upload: None,
        }];
        publish(
            &control,
            &mut events,
            Some(0),
            &mut HashMap::new(),
            &mut HashSet::new(),
            None,
        )
        .await
        .unwrap();
        assert!(events.is_empty(), "{queue}: a settled queue event drains");
        let frame = wire::read_frame(&mut peer).unwrap().unwrap();
        let GuestControl::HttpReply(reply) = GuestControl::decode(&frame).unwrap() else {
            panic!("{queue}: queue handler did not reply");
        };
        assert_eq!(reply.response.status, 200);
        let result = decode_queue_result(&reply.response.body).unwrap();
        assert_eq!(result.outcome, outcome, "{queue}: {:?}", result.error);
        assert_eq!(result.error.is_some(), outcome == QueueOutcome::Exception);
        assert_eq!(result.ack_all, ack_all, "{queue}");
        assert_eq!(result.retry_batch.retry, retry_batch.is_some(), "{queue}");
        assert_eq!(result.retry_batch.delay_seconds, retry_batch, "{queue}");
        assert_eq!(result.explicit_acks, acks, "{queue}");
        assert_eq!(
            result
                .retry_messages
                .iter()
                .map(|retry| (retry.msg_id.as_str(), retry.delay_seconds))
                .collect::<Vec<_>>(),
            retries,
            "{queue}"
        );
        // The drain follows the reply and never carries native proof.
        let frame = wire::read_frame(&mut peer).unwrap().unwrap();
        assert!(matches!(
            GuestControl::decode(&frame).unwrap(),
            GuestControl::Drained { .. }
        ));
    }
    // No queue handler is the original exception outcome, not a transport failure.
    let (_entry, mut result) = settle(&mut fetch_only, "explicit");
    let result = result.try_recv().unwrap().unwrap();
    assert_eq!(result.outcome, QueueOutcome::Exception);
    assert!(result.error.is_some() && result.explicit_acks.is_empty());
    assert!(decode_queue_batch(b"{}").is_err());
    eprintln!("guest queue event: explicit ack/retry delay, ackAll, retryAll, throw keeping acks, implicit ack and no-handler exception settle as HttpReply JSON");
}

#[cfg(test)]
pub(crate) fn load_application(bootstrap: &Path, artifact: &Path) -> Result<PreparedApplication> {
    load_application_with(bootstrap, artifact, None)
}

#[cfg(test)]
fn check_asset_binding() {
    let reference = serde_json::json!({
        "index": "assets.json", "sha256": "c".repeat(64), "file_count": 1, "total_bytes": 3,
    });
    let site = serde_json::json!({"kind": "resource", "resource": {"kind": "static_assets", "id": "site"}});
    let handler = |bindings: serde_json::Value, assets: bool| -> celld_applications::Handler {
        serde_json::from_value(serde_json::json!({
            "id": "main", "language": "javascript", "entrypoint": "main.mjs",
            "artifact_sha256": "a".repeat(64), "runtime_image_sha256": "b".repeat(64),
            "bindings": bindings,
            "worker_config": if assets { serde_json::json!({"assets": reference}) } else { serde_json::json!({}) },
        }))
        .unwrap()
    };
    let (alias, found) = asset_binding(&handler(serde_json::json!({"ASSETS": site}), true))
        .unwrap()
        .unwrap();
    assert_eq!(
        (alias.as_str(), found.sha256.as_str()),
        ("ASSETS", "c".repeat(64).as_str())
    );
    assert!(asset_binding(&handler(serde_json::json!({}), false))
        .unwrap()
        .is_none());
    assert!(asset_binding(&handler(serde_json::json!({}), true)).is_err());
    assert!(asset_binding(&handler(serde_json::json!({"ASSETS": site}), false)).is_err());
    assert!(asset_binding(&handler(serde_json::json!({"A": site, "B": site}), true)).is_err());
}

#[cfg(test)]
fn check_queue_consumers() {
    use celld_applications::{Handler, Name, ResourceRef};
    let resource = |kind, id: &str| ResourceRef {
        kind,
        id: Name::try_from(id.to_owned()).unwrap(),
    };
    let handler = |worker_config: serde_json::Value| -> Handler {
        serde_json::from_value(serde_json::json!({
            "id": "main", "language": "javascript", "entrypoint": "main.mjs",
            "artifact_sha256": "a".repeat(64), "runtime_image_sha256": "b".repeat(64),
            "worker_config": worker_config,
        }))
        .unwrap()
    };
    let declare = |queue: &str, dead_letter: Option<&str>, concurrency: Option<u16>| {
        let mut value = serde_json::json!({
            "queue": queue, "max_batch_size": 10, "max_batch_timeout": 5, "max_retries": 3,
        });
        if let Some(dead_letter) = dead_letter {
            value["dead_letter_queue"] = dead_letter.into();
        }
        if let Some(concurrency) = concurrency {
            value["max_concurrency"] = concurrency.into();
        }
        value
    };
    let consumers =
        |declared: serde_json::Value| handler(serde_json::json!({"queue_consumers": declared}));
    let one = HashSet::from([
        resource(ResourceKind::Queue, "jobs"),
        resource(ResourceKind::Queue, "dead"),
    ]);
    // `dead` is a queue of another application, and only a KV namespace here.
    let two = HashSet::from([
        resource(ResourceKind::Queue, "jobs"),
        resource(ResourceKind::Kv, "dead"),
    ]);
    let cap = crate::application_guest_transport::QUEUE_MAX_CONCURRENCY;
    let concurrency = |declared, registered| {
        queue_consumers(&consumers(declared), registered).unwrap()[0].max_concurrency
    };
    // Effective concurrency is an upper bound by VM event capacity.
    assert_eq!(
        concurrency(serde_json::json!([declare("jobs", None, Some(250))]), &one),
        Some(cap)
    );
    assert_eq!(
        concurrency(serde_json::json!([declare("jobs", None, None)]), &one),
        Some(cap)
    );
    assert_eq!(
        concurrency(serde_json::json!([declare("jobs", None, Some(4))]), &one),
        Some(4)
    );
    let accepted = queue_consumers(
        &consumers(serde_json::json!([declare("jobs", Some("dead"), None)])),
        &one,
    )
    .unwrap();
    assert_eq!(accepted[0].dead_letter_queue.as_deref(), Some("dead"));
    assert!(queue_consumers(&handler(serde_json::json!({})), &one)
        .unwrap()
        .is_empty());
    let mut unmodeled = declare("jobs", None, None);
    unmodeled["unmodeled"] = 1.into();
    for (what, declared, registered) in [
        (
            "queue that is not a resource",
            serde_json::json!([declare("other", None, None)]),
            &one,
        ),
        (
            "dead letter queue of another application",
            serde_json::json!([declare("jobs", Some("dead"), None)]),
            &two,
        ),
        (
            "dead letter queue of itself",
            serde_json::json!([declare("jobs", Some("jobs"), None)]),
            &one,
        ),
        (
            "duplicate consumer",
            serde_json::json!([declare("jobs", None, None), declare("jobs", None, None)]),
            &one,
        ),
        ("unmodeled field", serde_json::json!([unmodeled]), &one),
        ("not a list", serde_json::json!({}), &one),
    ] {
        assert!(
            queue_consumers(&consumers(declared), registered).is_err(),
            "{what} was accepted"
        );
    }
}

#[cfg(test)]
pub(crate) fn check_application_bootstrap() {
    use celld_applications::bootstrap::Bootstrap;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("application");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("main.mjs"), "import {VERSION} from './helper.mjs'; import label from './message.txt'; import wasm from './empty.wasm'; import {env} from 'cloudflare:workers'; if (VERSION !== 'artifact' || label !== 'text artifact' || !(wasm instanceof WebAssembly.Module)) throw new Error('artifact module mismatch'); export class Counter { fetch() { return new Response('artifact'); } } export class Alternate extends Counter {} export default {get fetch() { if (env.PRIVATE_TOKEN !== 'resolved-private-value' || env.PUBLIC !== 'config') throw new Error('private bootstrap env mismatch'); return () => new Response('artifact'); }};").unwrap();
    std::fs::write(
        root.join("helper.mjs"),
        "export const VERSION = 'artifact';",
    )
    .unwrap();
    std::fs::write(root.join("message.txt"), "text artifact").unwrap();
    std::fs::write(root.join("empty.wasm"), [0, 97, 115, 109, 1, 0, 0, 0]).unwrap();
    let value = serde_json::json!({"version":1,"application":{"app":"one"},"deployment":"v1","manifest_sha256":"a".repeat(64),
        "handler":{"id":"main","language":"javascript","entrypoint":"main.mjs","artifact_sha256":"b".repeat(64),"runtime_image_sha256":"c".repeat(64),
            "bindings":{"COUNTER":{"kind":"resource","resource":{"kind":"durable_object","id":"counter"}},"SECOND":{"kind":"resource","resource":{"kind":"durable_object","id":"second"}},"DB":{"kind":"resource","resource":{"kind":"d1","id":"db"}}},
            "environment":{"PUBLIC":"config","PRIVATE_TOKEN":"resolved-private-value"},
            "worker_config":{"do_classes":["Counter"],"compatibility_date":"2026-03-01","compatibility_flags":["js_rpc"],"modules":[
                {"name":"helper.mjs","path":"helper.mjs","kind":"es_module"},
                {"name":"./message.txt","path":"message.txt","kind":"text"},
                {"name":"empty.wasm","path":"empty.wasm","kind":"wasm"}]}}});
    let disk = directory.path().join("bootstrap.bin");
    let write = |value: serde_json::Value| {
        let bootstrap: Bootstrap = serde_json::from_value(value).unwrap();
        std::fs::write(&disk, bootstrap.encode_disk().unwrap()).unwrap();
    };
    write(value.clone());
    let prepared = load_application(&disk, &root).unwrap();
    assert_eq!(prepared.resources.len(), 3);
    // Each binding is an env namespace keyed by its own resource, not by script.
    assert_eq!(prepared.namespaces.len(), 3);
    for resource in prepared
        .resources
        .iter()
        .filter(|resource| resource.resource.kind == ResourceKind::DurableObject)
    {
        assert!(
            prepared
                .namespaces
                .iter()
                .any(|(_, class, key)| class == "Counter"
                    && *key == resource.namespace_key("Counter"))
        );
    }
    assert!(prepared
        .namespaces
        .iter()
        .any(|(alias, ..)| alias == "SECOND"));
    // A D1 resource is the original internal database class under its own key.
    let database = prepared
        .resources
        .iter()
        .find(|resource| resource.resource.kind == ResourceKind::D1)
        .unwrap();
    assert!(prepared
        .namespaces
        .iter()
        .any(|(alias, class, key)| alias == "DB"
            && class == crate::deploy::D1_CLASS
            && *key == database.namespace_key(crate::deploy::D1_CLASS)));
    assert!(prepared
        .resources
        .iter()
        .all(|resource| resource.application.app.as_str() == "one"));
    assert!(prepared.options.compat.js_rpc && prepared.options.compat.delete_all_deletes_alarm);
    assert_eq!(
        prepared
            .options
            .vars
            .iter()
            .find(|(name, _)| name == "PRIVATE_TOKEN")
            .unwrap()
            .1,
        "resolved-private-value"
    );
    js::Engine::init();
    let config = WorkerConfig::new(prepared.options).with_keyed_namespaces(prepared.namespaces);
    let worker = Worker::load_guest_config(Arc::new(config)).unwrap();
    drop(worker);
    // Original configuration and normalized upload metadata resolve aliases
    // to declared classes without changing the resource's stable namespace.
    let mut multi = value.clone();
    multi["handler"]["worker_config"]["do_classes"] = serde_json::json!(["Counter", "Alternate"]);
    multi["handler"]["worker_config"]["durable_objects"] = serde_json::json!({"bindings":[
        {"name":"COUNTER","class_name":"Counter"},
        {"name":"SECOND","class_name":"Alternate"}
    ]});
    write(multi.clone());
    let prepared = load_application(&disk, &root).unwrap();
    let expected = prepared.namespaces.clone();
    let bootstrap: Bootstrap = serde_json::from_value(multi.clone()).unwrap();
    let host_namespaces = object_namespaces(&bootstrap.application, &bootstrap.handler).unwrap();
    for (alias, class, key) in &expected {
        let resource = &multi["handler"]["bindings"][alias]["resource"];
        let address = ResourceAddress {
            application: serde_json::from_value(multi["application"].clone()).unwrap(),
            resource: serde_json::from_value(resource.clone()).unwrap(),
        };
        assert_eq!(key, &address.namespace_key(class));
        assert!(host_namespaces.iter().any(
            |(bound_alias, bound_resource, bound_class)| bound_alias == alias
                && bound_resource == &address
                && bound_class == class
        ));
        assert_eq!(
            class,
            match alias.as_str() {
                "COUNTER" => "Counter",
                "SECOND" => "Alternate",
                "DB" => crate::deploy::D1_CLASS,
                _ => unreachable!(),
            }
        );
    }
    let config = WorkerConfig::new(prepared.options).with_keyed_namespaces(prepared.namespaces);
    drop(Worker::load_guest_config(Arc::new(config)).unwrap());
    let mut normalized = multi.clone();
    normalized["handler"]["worker_config"]
        .as_object_mut()
        .unwrap()
        .remove("durable_objects");
    normalized["handler"]["worker_config"]["bindings"] = serde_json::json!([
        {"type":"durable_object_namespace","name":"COUNTER","class_name":"Counter"},
        {"type":"durable_object_namespace","name":"SECOND","class_name":"Alternate"}
    ]);
    write(normalized.clone());
    assert_eq!(load_application(&disk, &root).unwrap().namespaces, expected);
    let mut renamed = multi.clone();
    renamed["deployment"] = serde_json::json!("v2");
    let second = renamed["handler"]["bindings"]
        .as_object_mut()
        .unwrap()
        .remove("SECOND")
        .unwrap();
    renamed["handler"]["bindings"]["RENAMED"] = second;
    renamed["handler"]["worker_config"]["durable_objects"]["bindings"][1]["name"] =
        serde_json::json!("RENAMED");
    write(renamed);
    let renamed = load_application(&disk, &root).unwrap();
    assert_eq!(
        renamed
            .namespaces
            .iter()
            .find(|(alias, ..)| alias == "RENAMED")
            .unwrap()
            .2,
        expected
            .iter()
            .find(|(alias, ..)| alias == "SECOND")
            .unwrap()
            .2
    );
    for declarations in [
        serde_json::json!([]),
        serde_json::json!([{"name":"COUNTER","class_name":"Counter"}]),
        serde_json::json!([{"name":"COUNTER","class_name":"Counter"},{"name":"SECOND","class_name":"Missing"}]),
        serde_json::json!([{"name":"COUNTER","class_name":"Counter"},{"name":"SECOND","class_name":"Counter"}]),
        serde_json::json!([{"name":"COUNTER","class_name":"Counter"},{"name":"ORPHAN","class_name":"Alternate"}]),
        serde_json::json!([{"name":"COUNTER","class_name":"Counter"},{"name":"COUNTER","class_name":"Alternate"}]),
        serde_json::json!([{"name":"COUNTER","class_name":"Counter"},{"name":"DB","class_name":"Alternate"}]),
    ] {
        let mut invalid = multi.clone();
        invalid["handler"]["worker_config"]["durable_objects"]["bindings"] = declarations;
        write(invalid);
        assert!(load_application(&disk, &root).is_err());
    }
    let mut ambiguous = value.clone();
    ambiguous["handler"]["worker_config"]["do_classes"] =
        serde_json::json!(["Counter", "Alternate"]);
    write(ambiguous);
    assert!(load_application(&disk, &root).is_err());
    for binding in [
        serde_json::json!({"type":"durable_object_namespace","name":"COUNTER","class_name":"Counter"}),
        serde_json::json!({"type":"durable_object_namespace","name":"COUNTER","class_name":"Alternate"}),
        serde_json::json!({"type":"service","name":"COUNTER","class_name":"Counter"}),
        serde_json::json!({"name":"COUNTER","class_name":"Counter"}),
    ] {
        let mut invalid = multi.clone();
        invalid["handler"]["worker_config"]["bindings"] = serde_json::json!([binding]);
        write(invalid);
        assert!(load_application(&disk, &root).is_err());
    }
    for (field, replacement) in [
        (
            "modules",
            serde_json::json!([{"name":"helper.mjs","path":"../outside","kind":"es_module"}]),
        ),
        (
            "modules",
            serde_json::json!([{"name":"node:fs","path":"helper.mjs","kind":"es_module"}]),
        ),
        (
            "modules",
            serde_json::json!([{"name":"worker.js","path":"helper.mjs","kind":"es_module"}]),
        ),
        (
            "modules",
            serde_json::json!([{"name":"same.mjs","path":"helper.mjs","kind":"es_module"},{"name":"same.mjs","path":"helper.mjs","kind":"text"}]),
        ),
        ("do_classes", serde_json::json!(["Counter", "Counter"])),
        ("do_classes", serde_json::json!(["__D1Database"])),
        ("do_classes", serde_json::json!(["__Workflow.application"])),
        ("do_classes", serde_json::json!([".cron"])),
    ] {
        let mut invalid = value.clone();
        invalid["handler"]["worker_config"][field] = replacement;
        write(invalid);
        assert!(load_application(&disk, &root).is_err());
    }
    // A stateless handler declares neither classes nor namespaces; either one
    // alone cannot host objects and is refused.
    let mut stateless = value.clone();
    stateless["handler"]["bindings"] = serde_json::json!({});
    stateless["handler"]["worker_config"]
        .as_object_mut()
        .unwrap()
        .remove("do_classes");
    write(stateless.clone());
    let prepared = load_application(&disk, &root).unwrap();
    assert!(prepared.resources.is_empty() && prepared.options.do_classes.is_empty());
    assert!(prepared.namespaces.is_empty());
    let mut service_only = stateless.clone();
    service_only["handler"]["bindings"] =
        serde_json::json!({"SVC":{"kind":"handler","handler":"backend"}});
    write(service_only.clone());
    let prepared = load_application(&disk, &root).unwrap();
    assert!(prepared.namespaces.is_empty() && prepared.resources.is_empty());
    assert_eq!(prepared.services, vec![("SVC".into(), "SVC".into(), None)]);
    service_only["handler"]["worker_config"]["services"] =
        serde_json::json!([{"binding":"SVC","service":"backend","entrypoint":"Named"}]);
    write(service_only.clone());
    assert_eq!(
        load_application(&disk, &root).unwrap().services,
        vec![("SVC".into(), "SVC".into(), Some("Named".into()))]
    );
    service_only["handler"]["worker_config"]["services"][0]["service"] = "foreign".into();
    write(service_only);
    assert!(load_application(&disk, &root).is_err());
    // A database needs no class declaration.
    let mut database_only = stateless.clone();
    database_only["handler"]["bindings"] = value["handler"]["bindings"]
        .as_object()
        .map(|bindings| serde_json::json!({"DB": bindings["DB"]}))
        .unwrap();
    write(database_only);
    let prepared = load_application(&disk, &root).unwrap();
    assert_eq!(prepared.namespaces.len(), 1);
    assert!(prepared.options.do_classes.is_empty());
    // A KV namespace is the original internal class under its own resource key,
    // never another application's, and no Durable Object binding can declare it.
    let kv_binding = |app: &str| {
        let mut kv = stateless.clone();
        kv["application"]["app"] = serde_json::json!(app);
        kv["handler"]["bindings"] =
            serde_json::json!({"CACHE":{"kind":"resource","resource":{"kind":"kv","id":"cache"}}});
        kv
    };
    write(kv_binding("one"));
    let prepared = load_application(&disk, &root).unwrap();
    let kv = celld_logic::kv::RESERVED_CLASS;
    assert!(
        matches!(prepared.namespaces.as_slice(), [(alias, class, key)]
            if alias == "CACHE" && class == kv && *key == prepared.resources[0].namespace_key(kv))
    );
    assert!(prepared.options.do_classes.is_empty());
    let one = prepared.namespaces[0].2.clone();
    drop(
        Worker::load_guest_config(Arc::new(
            WorkerConfig::new(prepared.options).with_keyed_namespaces(prepared.namespaces),
        ))
        .unwrap(),
    );
    write(kv_binding("two"));
    assert_ne!(load_application(&disk, &root).unwrap().namespaces[0].2, one);
    // Workflow replay is the original reserved guest cell, keyed to exactly
    // the bound resource. Its user class is never a trusted-host class.
    let workflow_binding = |app: &str| {
        let mut workflow = stateless.clone();
        workflow["application"]["app"] = serde_json::json!(app);
        workflow["handler"]["bindings"] = serde_json::json!({"FLOW":{"kind":"resource","resource":{"kind":"workflow","id":"flow"}}});
        workflow["handler"]["worker_config"]["workflows"] =
            serde_json::json!([{"binding":"FLOW","name":"flow","class_name":"Flow"}]);
        workflow
    };
    let original_source = std::fs::read_to_string(root.join("main.mjs")).unwrap();
    std::fs::write(root.join("main.mjs"), format!("{original_source}\nimport {{WorkflowEntrypoint}} from 'cloudflare:workers'; export class Flow extends WorkflowEntrypoint {{ async run(event, step) {{ return step.do('value', () => event.payload); }} }}")).unwrap();
    write(workflow_binding("one"));
    let prepared = load_application(&disk, &root).unwrap();
    let class = crate::deploy::workflow_class("flow");
    assert_eq!(prepared.options.do_classes, vec![class.clone()]);
    assert!(
        matches!(prepared.options.workflow_bindings.as_slice(), [binding] if binding.environment == "FLOW" && binding.workflow == "flow" && binding.class == "Flow")
    );
    let address = prepared.resources[0].clone();
    assert_eq!(
        prepared.namespaces[0],
        ("FLOW".into(), class.clone(), address.namespace_key(&class))
    );
    let key = prepared.namespaces[0].2.clone();
    drop(
        Worker::load_guest_config(Arc::new(
            WorkerConfig::new(prepared.options).with_keyed_namespaces(prepared.namespaces),
        ))
        .unwrap(),
    );
    let object = |name: &str| {
        let scope = address.cell_scope(&class, name).unwrap();
        celld_applications::invocation_wire::ObjectTarget {
            class: class.clone(),
            id: scope.split_once(':').unwrap().1.into(),
            name: Some(name.into()),
        }
    };
    assert!(namespace_owns_object(
        &address,
        &class,
        &object("flow/instance")
    ));
    assert!(!namespace_owns_object(
        &address,
        &class,
        &object("other/instance")
    ));
    let mut unnamed = object("flow/instance");
    unnamed.name = None;
    assert!(!namespace_owns_object(&address, &class, &unnamed));
    write(workflow_binding("two"));
    assert_ne!(load_application(&disk, &root).unwrap().namespaces[0].2, key);
    let mut normalized = workflow_binding("one");
    normalized["handler"]["worker_config"]
        .as_object_mut()
        .unwrap()
        .remove("workflows");
    normalized["handler"]["worker_config"]["bindings"] = serde_json::json!([{"type":"workflow","name":"FLOW","workflow_name":"flow","class_name":"Flow"}]);
    write(normalized);
    assert_eq!(load_application(&disk, &root).unwrap().namespaces[0].2, key);
    for (field, value) in [
        ("name", "other"),
        ("class_name", "__Workflow.flow"),
        ("script_name", "other"),
    ] {
        let mut invalid = workflow_binding("one");
        invalid["handler"]["worker_config"]["workflows"][0][field] = value.into();
        write(invalid);
        assert!(load_application(&disk, &root).is_err());
    }
    std::fs::write(root.join("main.mjs"), original_source).unwrap();
    let mut declared = kv_binding("one");
    declared["handler"]["worker_config"]["do_classes"] = serde_json::json!(["Counter"]);
    declared["handler"]["worker_config"]["durable_objects"] =
        serde_json::json!({"bindings":[{"name":"CACHE","class_name":"Counter"}]});
    write(declared);
    assert!(load_application(&disk, &root).is_err());
    // A queue is the original producer binding named by its resource, under
    // that resource's key, and no Durable Object binding can declare it.
    let queue_binding = |app: &str| {
        let mut queue = stateless.clone();
        queue["application"]["app"] = serde_json::json!(app);
        queue["handler"]["bindings"] =
            serde_json::json!({"JOBS":{"kind":"resource","resource":{"kind":"queue","id":"jobs"}}});
        queue
    };
    write(queue_binding("one"));
    let prepared = load_application(&disk, &root).unwrap();
    let queue = celld_logic::queue::RESERVED_CLASS;
    assert!(
        matches!(prepared.namespaces.as_slice(), [(alias, class, key)]
            if alias == "JOBS" && class == queue && *key == prepared.resources[0].namespace_key(queue))
    );
    assert!(
        matches!(prepared.options.queue_bindings.as_slice(), [binding]
            if binding.environment == "JOBS" && binding.queue == "jobs" && binding.delivery_delay == 0)
    );
    assert!(prepared.options.do_classes.is_empty());
    let one = prepared.namespaces[0].2.clone();
    drop(
        Worker::load_guest_config(Arc::new(
            WorkerConfig::new(prepared.options).with_keyed_namespaces(prepared.namespaces),
        ))
        .unwrap(),
    );
    write(queue_binding("two"));
    assert_ne!(load_application(&disk, &root).unwrap().namespaces[0].2, one);
    // The guest keeps the original consumer declaration.
    let mut consumer = queue_binding("one");
    consumer["handler"]["worker_config"]["queue_consumers"] = serde_json::json!([{
        "queue": "jobs", "max_batch_size": 10, "max_batch_timeout": 5, "max_retries": 3
    }]);
    write(consumer);
    assert_eq!(
        load_application(&disk, &root)
            .unwrap()
            .options
            .queue_consumers[0]
            .queue,
        "jobs"
    );
    check_queue_consumers();
    check_asset_binding();
    crate::application_vm::tests::check_queue_catalog();
    crate::application_vm::tests::check_cron_catalog();
    crate::js::check_host_verified_actor_names();
    let mut declared = queue_binding("one");
    declared["handler"]["worker_config"]["do_classes"] = serde_json::json!(["Counter"]);
    declared["handler"]["worker_config"]["durable_objects"] =
        serde_json::json!({"bindings":[{"name":"JOBS","class_name":"Counter"}]});
    write(declared);
    assert!(load_application(&disk, &root).is_err());
    let mut classes_only = stateless;
    classes_only["handler"]["worker_config"]["do_classes"] = serde_json::json!(["Counter"]);
    write(classes_only);
    assert!(load_application(&disk, &root).is_err());
    let mut namespace_only = value.clone();
    namespace_only["handler"]["worker_config"]["do_classes"] = serde_json::json!([]);
    write(namespace_only);
    assert!(load_application(&disk, &root).is_err());
    // A Worker Loader is a declared binding name bound to a worker_loader
    // resource alias. Anything looser is refused.
    let mut loader = value.clone();
    loader["handler"]["bindings"]["LOADER"] =
        serde_json::json!({"kind":"resource","resource":{"kind":"worker_loader","id":"loader"}});
    loader["handler"]["worker_config"]["worker_loaders"] =
        serde_json::json!([{"binding":"LOADER"}]);
    write(loader.clone());
    let prepared = load_application(&disk, &root).unwrap();
    assert_eq!(prepared.loader_bindings, vec!["LOADER".to_owned()]);
    assert_eq!(prepared.resources.len(), 3);
    assert_eq!(prepared.namespaces.len(), 3);
    let refused = |edit: &dyn Fn(&mut serde_json::Value)| {
        let mut value = loader.clone();
        edit(&mut value);
        write(value);
        assert!(load_application(&disk, &root).is_err());
    };
    refused(&|v| v["handler"]["worker_config"]["worker_loaders"] = serde_json::json!(["LOADER"]));
    refused(&|v| {
        v["handler"]["worker_config"]["worker_loaders"][0]["limits"] = serde_json::json!({})
    });
    refused(&|v| v["handler"]["worker_config"]["worker_loaders"][0]["binding"] = "OTHER".into());
    refused(&|v| v["handler"]["worker_config"]["worker_loaders"] = serde_json::json!([]));
    refused(&|v| {
        v["handler"]["worker_config"]["worker_loaders"] =
            serde_json::json!([{"binding":"LOADER"},{"binding":"LOADER"}])
    });
    let mut unbound = value.clone();
    unbound["handler"]["worker_config"]["worker_loaders"] =
        serde_json::json!([{"binding":"LOADER"}]);
    write(unbound);
    assert!(load_application(&disk, &root).is_err());
    let mut r2 = value.clone();
    r2["handler"]["bindings"]["COUNTER"]["resource"]["kind"] = serde_json::json!("r2");
    write(r2.clone());
    assert_eq!(
        load_application(&disk, &root).unwrap().options.r2_bindings,
        vec![("COUNTER".into(), "COUNTER".into())]
    );
    let bucket_name = r2["handler"]["bindings"]["COUNTER"]["resource"]["id"].clone();
    r2["handler"]["worker_config"]["r2_buckets"] =
        serde_json::json!([{"binding":"COUNTER","bucket_name":bucket_name}]);
    write(r2.clone());
    assert!(load_application(&disk, &root).is_ok());
    r2["handler"]["worker_config"]["r2_buckets"][0]["bucket_name"] = serde_json::json!("foreign");
    write(r2);
    assert!(load_application(&disk, &root).is_err());
    // Modules marked `shared` load from the runtime drive, binary ones as data;
    // without that drive, or outside it, they are refused.
    let drive = directory.path().join("drive");
    std::fs::create_dir(&drive).unwrap();
    std::fs::write(drive.join("glue.mjs"), "export const GLUE = 1;").unwrap();
    std::fs::write(drive.join("blob.bin"), [0, 255, 7]).unwrap();
    let mut python = value.clone();
    python["handler"]["worker_config"]["modules"]
        .as_array_mut()
        .unwrap()
        .extend([
            serde_json::json!({"name":"glue.mjs","path":"glue.mjs","kind":"es_module","shared":true}),
            serde_json::json!({"name":"blob.bin","path":"blob.bin","kind":"data","shared":true}),
        ]);
    write(python.clone());
    assert!(load_application(&disk, &root).is_err());
    let prepared = load_application_with(&disk, &root, Some(&drive)).unwrap();
    assert!(prepared
        .options
        .modules
        .iter()
        .any(|(name, source)| name == "blob.bin"
            && matches!(source, ModuleSource::Data(bytes) if bytes[..] == [0, 255, 7])));
    python["handler"]["worker_config"]["modules"][3]["path"] = "../application/main.mjs".into();
    write(python);
    assert!(load_application_with(&disk, &root, Some(&drive)).is_err());
    write(value);
    std::fs::write(root.join("helper.mjs"), [255, 254]).unwrap();
    assert!(load_application(&disk, &root).is_err());
    std::fs::remove_file(root.join("helper.mjs")).unwrap();
    std::os::unix::fs::symlink(
        directory.path().join("bootstrap.bin"),
        root.join("helper.mjs"),
    )
    .unwrap();
    assert!(load_application(&disk, &root).is_err());
    let mut remaining = 1;
    assert!(read_artifact(&root.canonicalize().unwrap(), "main.mjs", &mut remaining).is_err());
    eprintln!("JavaScript application bootstrap: shared private disk, two same-application namespace identities, a resource-keyed D1 database, resolved vars, original compatibility and ES/text/Wasm artifact modules; traversal/symlink/builtin shadow/duplicates/reserved classes/unsupported metadata or resources/non-UTF8/budget refused; local loader check, no production readiness or host durability qualification");
}
