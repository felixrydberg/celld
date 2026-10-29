use super::*;
use crate::applications::tests::metadata_fault_store::Store;
use futures_util::future::join_all;
use std::sync::Arc;

const BYTES: u64 = 1 << 20;

/// A counting in-memory bucket, and a directory to cache it in.
struct Fixture {
    directory: tempfile::TempDir,
    backend: Arc<Store>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            directory: tempfile::tempdir().unwrap(),
            backend: Arc::default(),
        }
    }

    fn root(&self) -> PathBuf {
        self.directory.path().join("cache")
    }

    /// A node's cache over the same directory: calling it twice is a restart.
    fn cache(&self, max_bytes: u64) -> ObjectCache {
        ObjectCache::at(self.backend.bucket(), self.root(), max_bytes).unwrap()
    }

    async fn put(&self, key: &str, body: Vec<u8>) {
        self.backend.bucket().put(key, body).await.unwrap();
    }

    fn gets(&self, key: &str) -> usize {
        let calls = self.backend.calls.lock().unwrap();
        calls
            .iter()
            .filter(|(operation, called)| operation == "get" && called == key)
            .count()
    }
}

fn app(name: &str) -> ApplicationId {
    ApplicationId {
        app: name.to_owned().try_into().unwrap(),
    }
}

async fn read(cache: &ObjectCache, key: &str) -> Option<Bytes> {
    cache.immutable(key, 4096, "too large").await.unwrap()
}

/// Bytes and files under `<root>/objects`.
fn tier(root: &Path) -> (u64, usize) {
    let mut totals = (0, 0);
    for shard in std::fs::read_dir(root.join("objects")).unwrap() {
        for file in std::fs::read_dir(shard.unwrap().path()).unwrap() {
            totals.0 += file.unwrap().metadata().unwrap().len();
            totals.1 += 1;
        }
    }
    totals
}

#[tokio::test]
async fn a_cached_object_skips_the_bucket_and_survives_a_restart() {
    let fixture = Fixture::new();
    fixture.put("a/index.json", b"one".to_vec()).await;
    let cache = fixture.cache(BYTES);
    assert_eq!(
        read(&cache, "a/index.json").await.unwrap(),
        "one".as_bytes()
    );
    assert_eq!(
        read(&cache, "a/index.json").await.unwrap(),
        "one".as_bytes()
    );
    assert_eq!(fixture.gets("a/index.json"), 1);

    let restarted = fixture.cache(BYTES);
    assert_eq!(
        read(&restarted, "a/index.json").await.unwrap(),
        "one".as_bytes()
    );
    assert_eq!(fixture.gets("a/index.json"), 1, "a restart starts warm");

    // A bound the object exceeds is the caller's error, not a hit.
    let error = restarted.immutable("a/index.json", 2, "too large").await;
    assert!(error.is_err());
}

#[tokio::test]
async fn an_absent_object_is_not_cached() {
    let fixture = Fixture::new();
    let cache = fixture.cache(BYTES);
    assert!(read(&cache, "late").await.is_none());
    assert!(read(&cache, "late").await.is_none());
    assert_eq!(fixture.gets("late"), 2);
    // Published since: the next read finds it.
    fixture.put("late", b"here".to_vec()).await;
    assert_eq!(read(&cache, "late").await.unwrap(), "here".as_bytes());
}

#[tokio::test]
async fn concurrent_misses_of_one_key_fetch_once() {
    let fixture = Fixture::new();
    fixture.put("busy", b"body".to_vec()).await;
    let cache = fixture.cache(BYTES);
    let bodies = join_all((0..8).map(|_| read(&cache, "busy"))).await;
    assert!(bodies
        .iter()
        .all(|body| body.as_deref() == Some("body".as_bytes())));
    assert_eq!(fixture.gets("busy"), 1);
    // And with the disk off, they share the bytes.
    let memory = fixture.cache(0);
    let bodies = join_all((0..8).map(|_| read(&memory, "busy"))).await;
    assert!(bodies.iter().all(Option::is_some));
    assert_eq!(fixture.gets("busy"), 2);
}

#[tokio::test]
async fn eviction_keeps_the_tier_within_its_bound() {
    let fixture = Fixture::new();
    for index in 0..10 {
        fixture
            .put(&format!("blob/{index}"), vec![b'x'; 1000])
            .await;
    }
    let cache = fixture.cache(3500);
    for index in 0..10 {
        assert!(read(&cache, &format!("blob/{index}")).await.is_some());
    }
    let (bytes, files) = tier(&fixture.root());
    assert!(bytes <= 3500, "{bytes} bytes in {files} files");
    assert!(files > 0, "the bound evicts the oldest, not everything");
}

#[tokio::test]
async fn zero_bytes_leaves_the_disk_alone() {
    let fixture = Fixture::new();
    fixture.put("direct", b"body".to_vec()).await;
    let cache = fixture.cache(0);
    assert!(read(&cache, "direct").await.is_some());
    assert!(read(&cache, "direct").await.is_some());
    assert_eq!(fixture.gets("direct"), 2);
    let id = app("one");
    cache.distrust().await;
    cache
        .store_metadata(&id, b"body", "tok", cache.generation())
        .await;
    cache.trust();
    assert!(cache.metadata(&id).await.is_none());
    assert!(!fixture.root().exists());
}

#[tokio::test]
async fn metadata_is_served_only_while_trusted_and_not_forgotten() {
    let fixture = Fixture::new();
    let (one, two) = (app("one"), app("two"));
    let cache = fixture.cache(BYTES);
    assert!(cache.metadata(&one).await.is_none());

    // Nothing has emptied the tier yet, so it cannot be trusted.
    cache
        .store_metadata(&one, b"x", "tok", cache.generation())
        .await;
    cache.trust();
    assert!(cache.metadata(&one).await.is_none());

    cache.distrust().await;
    let body = b"{\"a\":\n1}";
    cache
        .store_metadata(&one, body, "\"etag-1\"", cache.generation())
        .await;
    cache
        .store_metadata(&two, b"2", "etag-2", cache.generation())
        .await;
    assert!(cache.metadata(&one).await.is_none(), "distrusted");
    cache.trust();
    let (found, token) = cache.metadata(&one).await.unwrap();
    assert_eq!((found.as_ref(), token.as_str()), (&body[..], "\"etag-1\""));

    cache.forget(&one).await;
    assert!(cache.metadata(&one).await.is_none());
    assert!(
        cache.metadata(&two).await.is_some(),
        "only that application"
    );

    // A read that began before a forget must not write its outdated answer.
    let began = cache.generation();
    cache.forget(&one).await;
    cache.store_metadata(&one, b"old", "old", began).await;
    assert!(cache.metadata(&one).await.is_none());

    cache.distrust().await;
    cache.trust();
    assert!(cache.metadata(&two).await.is_none(), "distrust deletes all");
}

#[tokio::test]
async fn a_disk_backed_object_is_opened_not_read_into_memory() {
    let fixture = Fixture::new();
    fixture.put("blob", vec![b'x'; 3000]).await;
    let cache = fixture.cache(BYTES);
    // The first read fills the tier; the second finds it there.
    for _ in 0..2 {
        let Some(Object::File(mut file, length)) = cache
            .immutable_open("blob", 4096, "too large")
            .await
            .unwrap()
        else {
            panic!("the tier holds it as a file");
        };
        let mut body = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut file, &mut body)
            .await
            .unwrap();
        assert_eq!((length, body), (3000, vec![b'x'; 3000]));
    }
    assert_eq!(fixture.gets("blob"), 1);
    assert!(cache
        .immutable_open("absent", 4096, "x")
        .await
        .unwrap()
        .is_none());
    assert!(cache.immutable_open("blob", 10, "too large").await.is_err());
    // With the tier off there is no file to open: the bytes are handed over.
    let memory = fixture.cache(0);
    assert!(matches!(
        memory.immutable_open("blob", 4096, "too large").await.unwrap(),
        Some(Object::Bytes(body)) if body.len() == 3000
    ));
}

/// A child node reads through its relay: Linux only, like the sync it rides on.
#[cfg(target_os = "linux")]
mod relayed {
    use super::*;
    use crate::applications::ApplicationStore;
    use crate::catalog_sync::{CatalogSync, Upstream, View};
    use crate::peer_auth::{PeerAuth, PROTOCOL_VERSION_TEXT, RESPONSE_VERSION_HEADER};
    use axum::http::StatusCode;
    use std::sync::Mutex;

    const ROUTE: &str = "applications/v2/routes/0123456789abcdef0123456789abcdef.json";
    const ROUTE_QUERY: &str =
        "key=applications%2Fv2%2Froutes%2F0123456789abcdef0123456789abcdef.json";

    async fn serve(router: axum::Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        address
    }

    /// A canned relay that answers every `/peer/object` with `status` and
    /// `body`, and records the queries it was sent.
    async fn relay(
        status: StatusCode,
        body: impl Into<Bytes>,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        use axum::extract::RawQuery;
        let body: Bytes = body.into();
        let queries = Arc::new(Mutex::new(Vec::new()));
        let recorded = queries.clone();
        let router = axum::Router::new().route(
            "/peer/object",
            axum::routing::get(move |RawQuery(query): RawQuery| {
                let (recorded, body) = (recorded.clone(), body.clone());
                async move {
                    recorded.lock().unwrap().push(query.unwrap_or_default());
                    (
                        status,
                        [
                            (RESPONSE_VERSION_HEADER, PROTOCOL_VERSION_TEXT),
                            (TOKEN_HEADER, "relay-token"),
                        ],
                        body,
                    )
                }
            }),
        );
        (serve(router).await, queries)
    }

    /// A node over `fixture`'s bucket that follows the relay at `address`.
    fn follower(fixture: &Fixture, address: &str) -> (ObjectCache, Arc<CatalogSync>) {
        let upstream = Upstream {
            node: "relay".to_owned(),
            addr: address.to_owned(),
        };
        let sync = Arc::new(CatalogSync::new(vec![upstream]));
        sync.set_current(Some(0));
        let cache = fixture.cache(BYTES);
        let auth = Arc::new(PeerAuth::new([0; 32], "child").unwrap());
        cache.set_relay(sync.clone(), reqwest::Client::new(), auth);
        (cache, sync)
    }

    /// A follower whose bucket holds `ROUTE` as "from-bucket", and whose relay
    /// answers every ask with `status` and `body`.
    async fn node(
        status: StatusCode,
        body: impl Into<Bytes>,
    ) -> (
        Fixture,
        ObjectCache,
        Arc<CatalogSync>,
        Arc<Mutex<Vec<String>>>,
    ) {
        let fixture = Fixture::new();
        fixture.put(ROUTE, b"from-bucket".to_vec()).await;
        let (address, queries) = relay(status, body).await;
        let (cache, sync) = follower(&fixture, &address);
        (fixture, cache, sync, queries)
    }

    fn view_at(seq: u64) -> View {
        let epoch = serde_json::json!({"version": 1, "log": "aa", "seq": seq, "changes": []});
        View {
            epoch: Some(Arc::new(serde_json::from_value(epoch).unwrap())),
            age: Duration::ZERO,
            received: Instant::now(),
            pointer: None,
        }
    }

    #[tokio::test]
    async fn a_relay_answers_before_the_bucket_and_its_404_is_final() {
        let (fixture, cache, _, queries) = node(StatusCode::OK, "from-relay").await;
        for _ in 0..2 {
            assert_eq!(read(&cache, ROUTE).await.unwrap(), "from-relay".as_bytes());
        }
        assert_eq!(
            *queries.lock().unwrap(),
            [ROUTE_QUERY],
            "the second read came from the tier"
        );
        assert_eq!(fixture.gets(ROUTE), 0);

        let (fixture, cache, _, _) = node(StatusCode::NOT_FOUND, "").await;
        assert!(read(&cache, ROUTE).await.is_none(), "the relay's word");
        assert_eq!(fixture.gets(ROUTE), 0);
    }

    #[tokio::test]
    async fn a_relayed_artifact_must_match_its_digest() {
        let key = format!(
            "applications/v2/apps/one/artifacts/{}",
            content_digest(b"good")
        );
        for (relayed, bucket_reads) in [("evil", 1), ("good", 0)] {
            let fixture = Fixture::new();
            fixture.put(&key, b"good".to_vec()).await;
            let (address, _) = relay(StatusCode::OK, relayed).await;
            let (cache, _) = follower(&fixture, &address);
            assert_eq!(read(&cache, &key).await.unwrap(), "good".as_bytes());
            assert_eq!(fixture.gets(&key), bucket_reads, "relay answered {relayed:?}");
        }
    }

    #[tokio::test]
    async fn a_relay_that_fails_or_is_not_followed_leaves_the_bucket_to_answer() {
        let from_bucket = |body: Option<Bytes>| assert_eq!(body.unwrap(), "from-bucket".as_bytes());
        // An error status, and an object over the bound.
        for (status, body) in [
            (StatusCode::INTERNAL_SERVER_ERROR, vec![b'x'; 10]),
            (StatusCode::OK, vec![b'x'; 5000]),
        ] {
            let (fixture, cache, _, queries) = node(status, body).await;
            from_bucket(read(&cache, ROUTE).await);
            assert_eq!(queries.lock().unwrap().len(), 1);
            assert_eq!(fixture.gets(ROUTE), 1);
            let link = cache.relay.get().unwrap();
            assert!(
                link.failing.load(Ordering::Relaxed),
                "logged once, not per object"
            );
        }

        // The follower has no upstream up: the relay is not asked.
        let (fixture, cache, sync, queries) = node(StatusCode::OK, "from-relay").await;
        sync.set_current(None);
        from_bucket(read(&cache, ROUTE).await);
        assert!(queries.lock().unwrap().is_empty());
        // A key the relay does not serve is not asked for either.
        sync.set_current(Some(0));
        fixture.put("a/index.json", b"from-bucket".to_vec()).await;
        from_bucket(read(&cache, "a/index.json").await);
        assert!(queries.lock().unwrap().is_empty());

        // A node that does not speak the protocol says 404 to any path: not an answer.
        let fixture = Fixture::new();
        fixture.put(ROUTE, b"from-bucket".to_vec()).await;
        let (cache, _) = follower(&fixture, &serve(axum::Router::new()).await);
        from_bucket(read(&cache, ROUTE).await);
    }

    #[tokio::test]
    async fn metadata_comes_from_the_relay_with_its_token_and_the_view_position() {
        // A committed record, as the relay's own bucket holds it.
        let root = Fixture::new();
        let store = ApplicationStore::new(root.backend.bucket());
        let id = app("one");
        let mut snapshot = store.read(&id).await.unwrap();
        snapshot.create(id.clone()).unwrap();
        store.commit(snapshot).await.unwrap();
        let key = "applications/v2/metadata/one.json";
        let (record, _) = root.backend.bucket().get(key).await.unwrap().unwrap();

        let (address, queries) = relay(StatusCode::OK, record).await;
        let node = Fixture::new();
        let (cache, sync) = follower(&node, &address);
        let store = ApplicationStore::new(node.backend.bucket());
        store.enable_object_cache(cache);
        let snapshot = store.read(&id).await.unwrap();
        assert_eq!(snapshot.revision(), Some("relay-token"));
        assert!(snapshot.application(&id).is_some());
        assert_eq!(node.gets(key), 0, "the bucket was not read");
        // With a view, its position goes along; a read that skips the disk
        // sends none, so that the relay reads through too.
        sync.publish(view_at(7));
        store.read(&id).await.unwrap();
        store.read_fresh(&id).await.unwrap();
        let asked = "key=applications%2Fv2%2Fmetadata%2Fone.json";
        assert_eq!(
            *queries.lock().unwrap(),
            [
                asked.to_owned(),
                format!("{asked}&log=aa&seq=7"),
                asked.to_owned()
            ]
        );
    }
}
