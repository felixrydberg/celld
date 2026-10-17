use super::*;
use crate::peer_auth::{PROTOCOL_VERSION_TEXT, RESPONSE_VERSION_HEADER};
use celld_applications::ApplicationId;
use serde_json::json;
use tokio_util::task::AbortOnDropHandle;

fn epoch(seq: u64) -> EpochLog {
    serde_json::from_value(json!({
        "version": 1, "log": "aa", "seq": seq,
        "changes": [{"seq": seq, "application": {"app": "one"}, "revision": "r"}],
    }))
    .unwrap()
}

fn view_at(seq: u64) -> View {
    View {
        epoch: Some(Arc::new(epoch(seq))),
        age: Duration::ZERO,
        received: Instant::now(),
    }
}

fn body(reply: &(StatusCode, Vec<u8>)) -> serde_json::Value {
    serde_json::from_slice(&reply.1).unwrap()
}

fn store() -> (tempfile::TempDir, Arc<ApplicationStore>) {
    let directory = tempfile::tempdir().unwrap();
    let bucket = crate::dev::open_local_bucket(&directory.path().join("bucket.sqlite")).unwrap();
    (directory, Arc::new(ApplicationStore::new(bucket)))
}

async fn commit(store: &ApplicationStore, app: &str) {
    let id = ApplicationId {
        app: app.to_owned().try_into().unwrap(),
    };
    let mut snapshot = store.read(&id).await.unwrap();
    snapshot.create(id.clone()).unwrap();
    store.commit(snapshot).await.unwrap();
}

#[tokio::test]
async fn answer_serves_a_behind_asker_at_once_and_holds_a_current_one() {
    let sync = Arc::new(CatalogSync::new(Vec::new()));
    assert!(!sync.listening());
    // Nothing known: held, then refused.
    let reply = sync.answer(Some("aa"), Some(1), false).await;
    assert_eq!(reply.0, StatusCode::SERVICE_UNAVAILABLE);
    assert!(sync.listening(), "a child that asked is listening");

    // Behind, on another log, or without a view: answered at once.
    sync.publish(view_at(2));
    let began = Instant::now();
    for (log, seq) in [(Some("aa"), Some(1)), (Some("bb"), Some(2)), (None, None)] {
        let reply = sync.answer(log, seq, false).await;
        assert_eq!(reply.0, StatusCode::OK);
        assert_eq!(body(&reply)["epoch"]["seq"], 2);
    }
    assert!(began.elapsed() < HOLD);

    // Current: held until something is published.
    let later = sync.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        later.publish(view_at(3));
    });
    let began = Instant::now();
    let reply = sync.answer(Some("aa"), Some(2), false).await;
    assert_eq!(body(&reply)["epoch"]["seq"], 3);
    assert!(began.elapsed() >= Duration::from_millis(100) && began.elapsed() < HOLD);

    // Current and nothing comes: the same view after `HOLD`, and older.
    let young = body(&sync.answer(None, None, false).await)["age_ms"]
        .as_u64()
        .unwrap();
    let reply = sync.answer(Some("aa"), Some(3), false).await;
    assert_eq!(reply.0, StatusCode::OK);
    assert_eq!(body(&reply)["epoch"]["seq"], 3);
    let old = body(&reply)["age_ms"].as_u64().unwrap();
    assert!(
        old > young && old >= HOLD.as_millis() as u64,
        "{young} -> {old}"
    );

    // A view with no log is a position too.
    sync.publish(View {
        epoch: None,
        ..view_at(0)
    });
    assert_eq!(
        body(&sync.answer(None, None, false).await)["epoch"],
        json!(null)
    );
    let began = Instant::now();
    let reply = sync.answer(Some(""), None, false).await;
    assert_eq!(reply.0, StatusCode::OK);
    assert!(
        began.elapsed() >= HOLD,
        "an asker with the same empty view is held"
    );

    let began = Instant::now();
    let reply = sync.answer(Some("aa"), Some(3), true).await;
    assert_eq!(reply.0, StatusCode::SERVICE_UNAVAILABLE);
    assert!(began.elapsed() < HOLD, "a draining node does not hold");
}

#[tokio::test]
async fn bucket_reads_wake_children_on_a_change_only() {
    let (_directory, store) = store();
    let sync = CatalogSync::new(Vec::new());
    let mut views = sync.subscribe();
    assert_eq!(sync.read_bucket(&store).await.unwrap(), None);
    assert!(views.has_changed().unwrap(), "the first view wakes");
    let _ = views.borrow_and_update();
    sync.read_bucket(&store).await.unwrap();
    assert!(!views.has_changed().unwrap(), "an unchanged epoch does not");
    commit(&store, "one").await;
    let read = sync.read_bucket(&store).await.unwrap().unwrap();
    assert_eq!(read.seq, 1);
    assert!(views.has_changed().unwrap(), "a commit does");
    assert_eq!(sync.epoch().unwrap().unwrap(), read);
}

/// A canned `/peer/sync` upstream that answers `reply` after a short hold, and
/// records each query it was sent.
async fn upstream(reply: serde_json::Value) -> (String, Arc<Mutex<Vec<String>>>) {
    use axum::extract::RawQuery;
    let queries = Arc::new(Mutex::new(Vec::new()));
    let recorded = queries.clone();
    let router = axum::Router::new().route(
        "/peer/sync",
        axum::routing::get(move |RawQuery(query): RawQuery| {
            let (recorded, reply) = (recorded.clone(), reply.clone());
            async move {
                recorded.lock().unwrap().push(query.unwrap_or_default());
                sleep(Duration::from_millis(50)).await;
                (
                    [(RESPONSE_VERSION_HEADER, PROTOCOL_VERSION_TEXT)],
                    axum::Json(reply),
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (address, queries)
}

/// An address nothing listens on.
async fn dead() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().to_string()
}

fn follower(
    upstreams: &[(&str, &str)],
    store: Arc<ApplicationStore>,
) -> (Arc<CatalogSync>, AbortOnDropHandle<()>) {
    let sync = Arc::new(CatalogSync::new(
        upstreams
            .iter()
            .map(|(node, addr)| Upstream {
                node: (*node).to_owned(),
                addr: (*addr).to_owned(),
            })
            .collect(),
    ));
    let auth = Arc::new(PeerAuth::new([0; 32], "child").unwrap());
    let task = AbortOnDropHandle::new(tokio::spawn(follow(
        sync.clone(),
        store,
        reqwest::Client::new(),
        auth,
    )));
    (sync, task)
}

async fn first_view(sync: &CatalogSync) -> View {
    let mut views = sync.subscribe();
    let seen = timeout(Duration::from_secs(5), views.wait_for(Option::is_some))
        .await
        .expect("the follower publishes a view")
        .unwrap()
        .clone();
    seen.unwrap()
}

#[tokio::test]
async fn follower_leaves_a_stale_upstream_for_a_fresh_one_and_stays() {
    let (stale, stale_queries) = upstream(json!({"epoch": null, "age_ms": 120_000})).await;
    let (fresh, fresh_queries) = upstream(json!({"epoch": epoch(7), "age_ms": 1_500})).await;
    let (_directory, store) = store();
    let (sync, _task) = follower(
        &[("node-a", stale.as_str()), ("node-b", fresh.as_str())],
        store,
    );
    let view = first_view(&sync).await;
    assert_eq!(view.epoch.as_ref().unwrap().seq, 7);
    assert!(view.age >= Duration::from_millis(1_500) && view.age < STALE);
    timeout(Duration::from_secs(5), async {
        while fresh_queries.lock().unwrap().len() < 2 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the follower asks the fresh upstream again");
    let queries = fresh_queries.lock().unwrap().clone();
    assert_eq!(queries[..2], ["", "log=aa&seq=7"]);
    assert_eq!(
        stale_queries.lock().unwrap().len(),
        1,
        "and does not go back"
    );
}

#[tokio::test]
async fn follower_reads_the_bucket_when_every_upstream_fails() {
    let (down, also_down) = (dead().await, dead().await);
    let (_directory, store) = store();
    commit(&store, "one").await;
    let (sync, _task) = follower(
        &[("node-a", down.as_str()), ("node-b", also_down.as_str())],
        store.clone(),
    );
    let view = first_view(&sync).await;
    assert_eq!(view.epoch, store.epoch().await.unwrap().map(Arc::new));
    assert_eq!(view.epoch.as_ref().unwrap().seq, 1);
    assert!(
        view.age_now() < Duration::from_secs(1),
        "a bucket read is fresh"
    );
}

#[tokio::test]
async fn follower_reads_the_bucket_once_and_returns_to_a_recovered_upstream() {
    use axum::response::IntoResponse;
    use std::sync::atomic::{AtomicBool, Ordering};
    let up = Arc::new(AtomicBool::new(false));
    let serving = up.clone();
    let router = axum::Router::new().route(
        "/peer/sync",
        axum::routing::get(move || {
            let serving = serving.clone();
            async move {
                if !serving.load(Ordering::SeqCst) {
                    return StatusCode::SERVICE_UNAVAILABLE.into_response();
                }
                (
                    [(RESPONSE_VERSION_HEADER, PROTOCOL_VERSION_TEXT)],
                    axum::Json(json!({"epoch": epoch(9), "age_ms": 0})),
                )
                    .into_response()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let (_directory, store) = store();
    let (sync, _task) = follower(&[("node-a", address.as_str())], store.clone());
    assert!(first_view(&sync).await.epoch.is_none(), "the bucket has no log yet");
    // Many failed rounds, one bucket read: the fallback period bounds them.
    sleep(ROUND_RETRY * 10).await;
    assert_eq!(store.catalog_ops.load(Ordering::SeqCst), 1);
    // A restarted upstream is followed again within a round.
    up.store(true, Ordering::SeqCst);
    timeout(ROUND_RETRY * 10, async {
        while sync.epoch().flatten().is_none_or(|epoch| epoch.seq != 9) {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the recovered upstream is followed");
}

#[test]
fn upstream_list_is_validated() {
    use crate::env_vars::parse_sync_upstreams as parse;
    assert!(parse("").unwrap().is_empty());
    assert_eq!(
        parse("node-a@10.0.1.5:9001,node-b@host:9001").unwrap(),
        [
            ("node-a".to_owned(), "10.0.1.5:9001".to_owned()),
            ("node-b".to_owned(), "host:9001".to_owned()),
        ]
    );
    for bad in [
        "node-a",
        "node-a@host",
        "node-a@:9001",
        "node-a@host:port",
        "node-a@host:9001,",
        "bad node@host:9001",
        "node-a@one:1,node-a@two:2",
    ] {
        assert!(parse(bad).is_err(), "{bad:?} must be refused");
    }
}
