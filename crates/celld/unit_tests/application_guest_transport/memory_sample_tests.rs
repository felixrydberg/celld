use super::*;
use celld_applications::invocation_wire::HeapMemory;

fn stats() -> GuestMemorySample {
    GuestMemorySample {
        rss_bytes: 42,
        available_bytes: None,
        root: HeapMemory::default(),
        total: HeapMemory::default(),
        isolates: 1,
        busy_isolates: 0,
        loading_isolates: 0,
        complete: true,
        jemalloc: None,
        libc_malloc: None,
    }
}

async fn fixture(capability: bool) -> (tempfile::TempDir, Dispatcher, UnixStream) {
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
            response_streams: false,
            memory_diagnostics: capability,
            idle_lifecycle: false,
        }
        .encode()
        .unwrap(),
    )
    .await
    .unwrap();
    let dispatcher = Dispatcher::connect(
        ApplicationId {
            app: "memory-sample".to_owned().try_into().unwrap(),
        },
        Language::Javascript,
        host,
        listener,
        None,
    )
    .await
    .unwrap();
    (directory, dispatcher, guest)
}

async fn next_sample(guest: &mut UnixStream) -> String {
    match HostControl::decode(
        &tokio::time::timeout(Duration::from_secs(1), read_frame(guest))
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
    )
    .unwrap()
    {
        HostControl::MemorySample { sample, .. } => sample,
        other => panic!("expected diagnostic, got {other:?}"),
    }
}

async fn reply(guest: &mut UnixStream, sample: String) {
    write_frame(
        guest,
        &GuestControl::MemorySample {
            version: VERSION,
            sample,
            stats: stats(),
        }
        .encode()
        .unwrap(),
    )
    .await
    .unwrap();
}

async fn cleared(client: &Client) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if client.0.registry.lock().unwrap().memory_sample.is_none() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn idle_reply_correlation_and_late_receivers() {
    let mut registry = Registry {
        running: true,
        memory_sample: None,
        idle_probe: None,
        pending: HashMap::new(),
        retired: HashSet::new(),
        retiring_objects: HashSet::new(),
        native_events: Vec::new(),
        retired_objects: HashSet::new(),
    };
    assert!(registry.idle_reply(&"a".repeat(64), true).is_err());
    let (send, receive) = oneshot::channel();
    registry.idle_probe = Some(("a".repeat(64), send));
    assert!(registry.idle_reply(&"b".repeat(64), true).is_err());
    assert!(registry.idle_probe.is_some());
    drop(receive);
    registry.idle_reply(&"a".repeat(64), false).unwrap();
    assert!(registry.idle_probe.is_none());
    assert!(registry.idle_reply(&"a".repeat(64), true).is_err());
    let (send, mut receive) = oneshot::channel();
    registry.idle_probe = Some(("c".repeat(64), send));
    registry.idle_reply(&"c".repeat(64), true).unwrap();
    assert!(receive.try_recv().unwrap());
    assert!(registry.pending.is_empty() && registry.native_events.is_empty());
}

#[test]
fn memory_reply_correlation_and_late_receivers() {
    let mut registry = Registry {
        running: true,
        memory_sample: None,
        idle_probe: None,
        pending: HashMap::new(),
        retired: HashSet::new(),
        retiring_objects: HashSet::new(),
        native_events: Vec::new(),
        retired_objects: HashSet::new(),
    };
    assert!(registry.memory_reply(&"a".repeat(64), stats()).is_err());
    let (send, receive) = oneshot::channel();
    registry.memory_sample = Some(("a".repeat(64), send));
    assert!(registry.memory_reply(&"b".repeat(64), stats()).is_err());
    assert!(registry.memory_sample.is_some());
    drop(receive);
    registry.memory_reply(&"a".repeat(64), stats()).unwrap();
    assert!(registry.memory_sample.is_none());
    assert!(registry.memory_reply(&"a".repeat(64), stats()).is_err());
}

async fn timed_out_sample_keeps_nonce_until_late_reply() {
    let (_directory, dispatcher, mut guest) = fixture(true).await;
    let client = dispatcher.client();
    let observer = client.clone();
    let sample_task = tokio::spawn(async move { observer.memory_sample().await });
    let sample = next_sample(&mut guest).await;
    assert!(sample_task.await.unwrap().unwrap().is_none());
    assert_eq!(client.pending(), 0);
    assert!(client.memory_sample().await.unwrap().is_none());
    assert!(client.0.registry.lock().unwrap().memory_sample.is_some());
    reply(&mut guest, sample).await;
    cleared(&client).await;
    assert!(!client.0.stop.is_cancelled());
    let observer = client.clone();
    let sample_task = tokio::spawn(async move { observer.memory_sample().await });
    let sample = next_sample(&mut guest).await;
    reply(&mut guest, sample).await;
    assert_eq!(sample_task.await.unwrap().unwrap(), Some(stats()));
    assert_eq!(client.pending(), 0);
    client.retire();
    assert!(dispatcher.join().await.is_err());
}

async fn cancelled_sample_skips_until_reply_and_retirement_clears_it() {
    let (_directory, dispatcher, mut guest) = fixture(true).await;
    let client = dispatcher.client();
    let observer = client.clone();
    let sample_task = tokio::spawn(async move { observer.memory_sample().await });
    let sample = next_sample(&mut guest).await;
    sample_task.abort();
    assert!(sample_task.await.is_err());
    assert!(client.memory_sample().await.unwrap().is_none());
    reply(&mut guest, sample).await;
    cleared(&client).await;
    assert!(!client.0.stop.is_cancelled());
    let observer = client.clone();
    let sample_task = tokio::spawn(async move { observer.memory_sample().await });
    let _sample = next_sample(&mut guest).await;
    client.retire();
    assert!(client.0.registry.lock().unwrap().memory_sample.is_none());
    assert!(sample_task.await.unwrap().is_err());
    assert!(dispatcher.join().await.is_err());
}

async fn diagnostics_leave_busy_event_ownership_unchanged() {
    let (_directory, dispatcher, mut guest) = fixture(true).await;
    let client = dispatcher.client();
    let id = "b".repeat(64);
    let settled = CancellationToken::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    client.0.registry.lock().unwrap().pending.insert(
        id.clone(),
        Pending {
            execution: None,
            retirement: false,
            grant: "c".repeat(64),
            storage: None,
            response: None,
            response_seen: false,
            terminal: None,
            terminal_seen: false,
            connected: true,
            cleanup_connected: false,
            access: None,
            cancelled: false,
            deadline: Some(deadline),
            settled: settled.clone(),
            stream: None,
            request: None,
            fetches: HashMap::new(),
            app: None,
            sockets: None,
            dials: HashMap::new(),
            issued_sockets: HashMap::new(),
        },
    );
    let observer = client.clone();
    let sample_task = tokio::spawn(async move { observer.memory_sample().await });
    let sample = next_sample(&mut guest).await;
    assert_eq!(client.pending(), 1);
    reply(&mut guest, sample).await;
    assert_eq!(sample_task.await.unwrap().unwrap(), Some(stats()));
    {
        let registry = client.0.registry.lock().unwrap();
        let event = registry.pending.get(&id).unwrap();
        assert_eq!(event.deadline, Some(deadline));
        assert!(!event.cancelled && !event.terminal_seen && !settled.is_cancelled());
        assert!(registry.retired.is_empty() && registry.native_events.is_empty());
    }
    assert_eq!(client.pending(), 1);
    client.retire();
    assert!(dispatcher.join().await.is_err());
}

async fn unsupported_samples_skip_and_unsolicited_replies_retire() {
    for capability in [false, true] {
        let (_directory, dispatcher, mut guest) = fixture(capability).await;
        if !capability {
            assert!(dispatcher.client().memory_sample().await.unwrap().is_none());
        }
        reply(&mut guest, "a".repeat(64)).await;
        assert!(
            tokio::time::timeout(Duration::from_secs(1), dispatcher.join())
                .await
                .unwrap()
                .is_err()
        );
    }
}
// The production facade owns one process runtime; run all async cases in it.
#[tokio::test]
#[ignore = "run alone with an exact selector: binds the process-wide production async runtime"]
async fn memory_sample_host_boundary() {
    crate::asyncrt::bind_test_runtime();
    timed_out_sample_keeps_nonce_until_late_reply().await;
    cancelled_sample_skips_until_reply_and_retirement_clears_it().await;
    diagnostics_leave_busy_event_ownership_unchanged().await;
    unsupported_samples_skip_and_unsolicited_replies_retire().await;
}
