use super::*;
use crate::actor::Message;
use crate::peer_auth::{PROTOCOL_VERSION_TEXT, RESPONSE_VERSION_HEADER};
use celld_logic::wake::{entry_key, PublicationId};
use celld_logic::WakeHintScope;
use std::sync::atomic::Ordering::SeqCst;
use std::time::Instant;
use tokio::sync::mpsc::unbounded_channel;

/// An in-memory store that counts the LISTs it serves and the reads of the
/// waker lease, so a test can assert what a tick costs the bucket.
mod counting_store {
    use futures_util::stream::BoxStream;
    use futures_util::TryStreamExt as _;
    use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    use std::sync::Arc;

    #[derive(Debug, Default)]
    pub(super) struct Store {
        inner: InMemory,
        pub(super) lists: AtomicUsize,
        pub(super) waker_reads: AtomicUsize,
    }

    impl std::fmt::Display for Store {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("counting store")
        }
    }

    impl Store {
        pub(super) fn bucket(self: &Arc<Self>) -> crate::bucket::Bucket {
            crate::bucket::Bucket::with_paginated_store_for_test(
                self.clone(),
                self.clone(),
                String::new(),
            )
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for Store {
        async fn put_opts(
            &self,
            key: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(key, payload, options).await
        }
        async fn put_multipart_opts(
            &self,
            key: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(key, options).await
        }
        async fn get_opts(
            &self,
            key: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if key.as_ref() == "wake/waker.json" {
                self.waker_reads.fetch_add(1, SeqCst);
            }
            self.inner.get_opts(key, options).await
        }
        async fn delete(&self, key: &Path) -> object_store::Result<()> {
            self.inner.delete(key).await
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.lists.fetch_add(1, SeqCst);
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.lists.fetch_add(1, SeqCst);
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    #[async_trait::async_trait]
    impl PaginatedListStore for Store {
        async fn list_paginated(
            &self,
            prefix: Option<&str>,
            options: PaginatedListOptions,
        ) -> object_store::Result<PaginatedListResult> {
            self.lists.fetch_add(1, SeqCst);
            let mut objects: Vec<ObjectMeta> = self
                .inner
                .list(Some(&Path::from(prefix.unwrap_or_default())))
                .try_collect()
                .await?;
            objects.retain(|object| {
                options
                    .page_token
                    .as_ref()
                    .is_none_or(|token| object.location.as_ref() > token.as_str())
            });
            objects.sort_by(|a, b| a.location.cmp(&b.location));
            let limit = options.max_keys.unwrap_or(usize::MAX);
            let more = objects.len() > limit;
            objects.truncate(limit);
            let page_token = more.then(|| objects.last().unwrap().location.to_string());
            Ok(PaginatedListResult {
                result: ListResult {
                    common_prefixes: Vec::new(),
                    objects,
                },
                page_token,
            })
        }
    }
}

fn ownership(bucket: &Bucket, node: &str) -> BucketOwnership {
    BucketOwnership::new(
        bucket.clone(),
        bucket.clone(),
        node.to_owned(),
        String::new(),
    )
}

fn entry(cell: &str, entry_ms: i64, maintain: bool) -> PushedEntry {
    PushedEntry {
        cell: cell.to_owned(),
        entry_ms,
        maintain,
    }
}

#[tokio::test]
async fn probe_pushes_live_foreign_owners_and_keeps_every_other_entry() {
    let store = Arc::new(counting_store::Store::default());
    let bucket = store.bucket();
    for (cell, owner) in [
        ("live-a1", r#"{"node":"peer-a","epoch":1}"#),
        ("live-a2", r#"{"node":"peer-a","epoch":4}"#),
        ("live-b", r#"{"node":"peer-b","epoch":2}"#),
        ("dead", r#"{"node":"gone","epoch":1}"#),
        ("mine", r#"{"node":"me","epoch":3}"#),
        ("released", r#"{"node":"","epoch":2}"#),
        // A failed read: the record does not decode.
        ("garbled", "not json"),
        // And "unknown" has no owner record at all.
    ] {
        bucket
            .put(&format!("cells/{cell}/own.json"), owner)
            .await
            .unwrap();
    }
    let live = LiveNodes::from([
        ("me".to_owned(), "10.0.0.9:1".to_owned()),
        ("peer-a".to_owned(), "10.0.0.1:1".to_owned()),
        ("peer-b".to_owned(), "10.0.0.2:1".to_owned()),
    ]);
    let entries = [
        "live-a2", "live-a1", "live-b", "dead", "mine", "released", "garbled", "unknown",
    ]
    .map(|cell| (cell.to_owned(), 1000))
    .to_vec();

    let probed = probe_entries(&ownership(&bucket, "me"), "me", &live, entries).await;

    // Dead, unowned, released, this node's own and unreadable all stay here.
    let local = ["dead", "garbled", "mine", "released", "unknown"];
    assert_eq!(
        probed.local,
        local.map(|cell| (cell.to_owned(), 1000)).to_vec()
    );
    // A live foreign owner gets its entries, with the address its lease gave.
    let group = |addr: &str, cells: &[&str]| OwnerGroup {
        addr: addr.to_owned(),
        entries: cells
            .iter()
            .map(|cell| ((*cell).to_owned(), 1000))
            .collect(),
    };
    assert_eq!(
        probed.pushed,
        BTreeMap::from([
            (
                "peer-a".to_owned(),
                group("10.0.0.1:1", &["live-a1", "live-a2"])
            ),
            ("peer-b".to_owned(), group("10.0.0.2:1", &["live-b"])),
        ])
    );
}

#[test]
fn push_bodies_split_an_owner_past_the_cap_and_fit_the_receiver() {
    let entries: Vec<(String, i64)> = (0..2 * PUSH_ENTRIES_MAX + 5)
        .map(|n| (format!("cell-{n:05}"), n as i64))
        .collect();
    let bodies: Vec<WakePush> = push_bodies(&entries, true).collect();
    assert_eq!(
        bodies
            .iter()
            .map(|body| body.entries.len())
            .collect::<Vec<_>>(),
        [PUSH_ENTRIES_MAX, PUSH_ENTRIES_MAX, 5]
    );
    // Nothing lost or reordered, and every entry carries the pass's flag.
    let sent: Vec<&PushedEntry> = bodies.iter().flat_map(|body| &body.entries).collect();
    assert!(sent.iter().all(|entry| entry.maintain));
    assert_eq!(
        sent.iter()
            .map(|entry| (entry.cell.clone(), entry.entry_ms))
            .collect::<Vec<_>>(),
        entries
    );
    // The worst full request is under the receiver's bound
    // (`MAX_PEER_WAKE_BODY_BYTES` in main.rs): 255-byte names, widest timestamp.
    let worst = WakePush {
        entries: vec![entry(&"c".repeat(255), i64::MIN, false); PUSH_ENTRIES_MAX],
    };
    assert!(serde_json::to_vec(&worst).unwrap().len() <= PUSH_ENTRIES_MAX * 512);
}

#[test]
fn push_body_round_trips() {
    let push = WakePush {
        entries: vec![
            entry("Counter:a", 1_700_000_000_000, true),
            entry("Counter:b", 1_700_000_060_000, false),
        ],
    };
    let bytes = serde_json::to_vec(&push).unwrap();
    assert_eq!(
        String::from_utf8(bytes.clone()).unwrap(),
        r#"{"entries":[{"cell":"Counter:a","entry_ms":1700000000000,"maintain":true},{"cell":"Counter:b","entry_ms":1700000060000,"maintain":false}]}"#
    );
    assert_eq!(serde_json::from_slice::<WakePush>(&bytes).unwrap(), push);
}

#[test]
fn a_push_reaches_the_actor_as_the_owners_own_scan_did() {
    let (actor, mut messages) = unbounded_channel();
    let push = WakePush {
        entries: vec![
            entry("refreshed", 5, true),
            // Not a cell name the wake index could have listed.
            entry("../escape", 6, false),
            entry("due", 7, false),
        ],
    };
    assert!(deliver_push(&actor, push));
    let mut seen = Vec::new();
    while let Ok(message) = messages.try_recv() {
        seen.push(match message {
            Message::MaintainWake { cell } => format!("maintain {cell}"),
            Message::WakeHint {
                cell,
                entry_ms,
                scope,
            } => {
                // Fleet admits a persisted local owner absent from RAM; Owned would not.
                assert_eq!(scope, WakeHintScope::Fleet);
                format!("hint {cell} {entry_ms}")
            }
            _ => panic!("a push sends only wake messages"),
        });
    }
    assert_eq!(
        seen,
        ["maintain refreshed", "hint refreshed 5", "hint due 7"]
    );

    drop(messages);
    assert!(!deliver_push(
        &actor,
        WakePush {
            entries: vec![entry("due", 7, false)]
        }
    ));
}

#[tokio::test]
async fn a_node_without_the_waker_role_reads_the_lease_and_lists_nothing() {
    crate::asyncrt::bind_test_runtime();
    const TICK_MS: u128 = 20;
    let store = Arc::new(counting_store::Store::default());
    let bucket = store.bucket();
    let now = crate::asyncrt::wall_ms();
    bucket
        .put(
            "wake/waker.json",
            format!(r#"{{"node":"other","expires_ms":{}}}"#, now + 3_600_000),
        )
        .await
        .unwrap();
    let key = entry_key(
        now - 120_000,
        "due",
        PublicationId {
            epoch: 1,
            sequence: 1,
        },
    );
    bucket.put(&key, "{}").await.unwrap();
    // The counter works: a due scan is LISTs. Then count from zero.
    assert_eq!(due_scan(&bucket, now).await.len(), 1);
    assert!(store.lists.load(SeqCst) > 0);
    store.lists.store(0, SeqCst);
    store.waker_reads.store(0, SeqCst);

    let (actor, mut hints) = unbounded_channel();
    let started = Instant::now();
    let task = tokio::spawn(run_periodic(
        bucket.clone(),
        Arc::new(ownership(&bucket, "me")),
        "me".to_owned(),
        actor,
        TICK_MS as u64,
        reqwest::Client::new(),
        Arc::new(PeerAuth::new([3; 32], "me").unwrap()),
    ));
    while store.waker_reads.load(SeqCst) < 8 {
        assert!(started.elapsed() < Duration::from_secs(10), "no ticks");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    task.abort();
    let _ = task.await;
    let elapsed_ms = started.elapsed().as_millis();

    assert_eq!(store.lists.load(SeqCst), 0, "a non-elected tick listed");
    // The due and cleanup loops share one election: a tick is one lease read,
    // so the reads cannot outnumber the ticks the elapsed time allows. Two
    // reads a tick would reach 8 by the 4th tick, over this bound of about 5.
    // The spare tick is for tick spacing that rounds down to whole milliseconds.
    let reads = store.waker_reads.load(SeqCst) as u128;
    assert!(
        reads <= elapsed_ms / TICK_MS + 1,
        "{reads} lease reads in {elapsed_ms} ms of {TICK_MS} ms ticks"
    );
    assert!(hints.try_recv().is_err(), "a non-elected node hinted");
}

#[tokio::test]
async fn the_elected_waker_hints_its_own_entries_and_pushes_the_rest() {
    crate::asyncrt::bind_test_runtime();
    use axum::http::HeaderMap;
    let received = Arc::new(Mutex::new(Vec::<(HeaderMap, bytes::Bytes)>::new()));
    let recorded = received.clone();
    let router = axum::Router::new().route(
        "/peer/wake",
        axum::routing::post(move |headers: HeaderMap, body: bytes::Bytes| {
            let recorded = recorded.clone();
            async move {
                recorded.lock().unwrap().push((headers, body));
                (
                    [(RESPONSE_VERSION_HEADER, PROTOCOL_VERSION_TEXT)],
                    "{\"ok\":true}",
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let store = Arc::new(counting_store::Store::default());
    let bucket = store.bucket();
    let now = crate::asyncrt::wall_ms();
    for (node, addr) in [("me", "127.0.0.1:1"), ("peer-a", peer_addr.as_str())] {
        let lease = format!(
            r#"{{"node":"{node}","expires_ms":{},"addr":"{addr}"}}"#,
            now + 3_600_000
        );
        bucket
            .put(&format!("nodes/{node}.json"), lease)
            .await
            .unwrap();
    }
    for (cell, owner) in [("pushed", "peer-a"), ("mine", "me")] {
        let record = format!(r#"{{"node":"{owner}","epoch":1}}"#);
        bucket
            .put(&format!("cells/{cell}/own.json"), record)
            .await
            .unwrap();
    }
    // "orphan" has no owner record. All three are due and unretired, so the
    // due scan and the cleanup refresh both find them.
    let mut minute_ms = 0;
    for cell in ["pushed", "mine", "orphan"] {
        let key = entry_key(
            now - 120_000,
            cell,
            PublicationId {
                epoch: 1,
                sequence: 1,
            },
        );
        minute_ms = parse_entry_key(&key).unwrap().minute_ms;
        bucket.put(&key, "{}").await.unwrap();
    }

    let (actor, mut hints) = unbounded_channel();
    let task = tokio::spawn(run_periodic(
        bucket.clone(),
        Arc::new(ownership(&bucket, "me")),
        "me".to_owned(),
        actor,
        50,
        reqwest::Client::new(),
        Arc::new(PeerAuth::new([3; 32], "me").unwrap()),
    ));
    // One request from the due scan (no maintain) and one from cleanup (maintain).
    let flags = || -> BTreeSet<bool> {
        received
            .lock()
            .unwrap()
            .iter()
            .map(|(_, body)| serde_json::from_slice::<WakePush>(body).unwrap().entries[0].maintain)
            .collect()
    };
    let started = Instant::now();
    while flags().len() < 2 {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "no pushes: {:?}",
            flags()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    task.abort();
    let _ = task.await;

    // Each request is signed for the owner, carries only its entry, and one
    // request does not mix the two passes' flags.
    let peer = PeerAuth::new([3; 32], "peer-a").unwrap();
    for (headers, body) in received.lock().unwrap().iter() {
        peer.verify(
            &axum::http::Method::POST,
            "/peer/wake",
            headers,
            body,
            "peer-a",
        )
        .expect("signed for the owner");
        let push: WakePush = serde_json::from_slice(body).unwrap();
        assert_eq!(push.entries.len(), 1);
        assert_eq!(push.entries[0].cell, "pushed");
        assert_eq!(push.entries[0].entry_ms, minute_ms);
    }
    // The waker hints what is not a live foreign owner's, and never the pushed cell.
    let mut hinted = BTreeSet::new();
    while let Ok(message) = hints.try_recv() {
        match message {
            Message::WakeHint { cell, scope, .. } => {
                assert_eq!(scope, WakeHintScope::Fleet);
                assert_ne!(cell, "pushed");
                hinted.insert(cell);
            }
            Message::MaintainWake { cell } => assert_ne!(cell, "pushed"),
            _ => panic!("the waker sends only wake messages"),
        }
    }
    assert_eq!(
        hinted,
        BTreeSet::from(["mine".to_owned(), "orphan".to_owned()])
    );
}
