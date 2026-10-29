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
        pointer: None,
    }
}

fn pointer(version: &str) -> DeployPointer {
    serde_json::from_value(json!({
        "version": version, "prefix": "deploy/p", "rollout": {"percent": 100},
    }))
    .unwrap()
}

fn pointer_of(sync: &CatalogSync) -> Option<DeployPointer> {
    let views = sync.subscribe();
    let view = views.borrow();
    view.as_ref().and_then(|view| view.pointer.clone())
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

#[tokio::test]
async fn publish_pointer_wakes_children_on_a_change_only() {
    let sync = CatalogSync::new(Vec::new());
    let mut views = sync.subscribe();
    sync.publish_pointer(pointer("one"));
    assert!(views.has_changed().unwrap(), "the first view wakes");
    let _ = views.borrow_and_update();
    assert!(
        sync.epoch().unwrap().is_none(),
        "a node with no view starts one without an epoch"
    );
    assert_eq!(pointer_of(&sync), Some(pointer("one")));
    sync.publish_pointer(pointer("one"));
    assert!(!views.has_changed().unwrap(), "the same pointer does not");
    let mut rollout = pointer("one");
    rollout.rollout.percent = 50;
    sync.publish_pointer(rollout);
    assert!(views.has_changed().unwrap(), "a rollout change does");
    let _ = views.borrow_and_update();
    sync.publish_pointer(pointer("two"));
    assert!(views.has_changed().unwrap(), "a new version does");
}

#[tokio::test]
async fn bucket_reads_and_upstream_views_keep_the_pointer() {
    let (_directory, store) = store();
    let sync = CatalogSync::new(Vec::new());
    sync.publish_pointer(pointer("one"));
    sync.read_bucket(&store).await.unwrap();
    assert_eq!(
        pointer_of(&sync),
        Some(pointer("one")),
        "an epoch read keeps it"
    );
    commit(&store, "one").await;
    sync.read_bucket(&store).await.unwrap();
    assert_eq!(pointer_of(&sync), Some(pointer("one")));
    assert_eq!(
        sync.epoch().unwrap().unwrap().seq,
        1,
        "and the epoch is there too"
    );

    // An upstream that predates the pointer sends none: the held one stays.
    sync.publish(view_at(2));
    assert_eq!(pointer_of(&sync), Some(pointer("one")));
    assert_eq!(sync.epoch().unwrap().unwrap().seq, 2);
    sync.publish(View {
        pointer: Some(pointer("two")),
        ..view_at(3)
    });
    assert_eq!(
        pointer_of(&sync),
        Some(pointer("two")),
        "one that carries it replaces it"
    );
}

#[tokio::test]
async fn the_peer_reply_carries_the_pointer_and_a_change_of_it_wakes_a_held_child() {
    let sync = Arc::new(CatalogSync::new(Vec::new()));
    sync.publish(view_at(2));
    assert_eq!(
        body(&sync.answer(None, None, false).await)["pointer"],
        json!(null)
    );
    sync.publish_pointer(pointer("one"));
    let reply = sync.answer_peer(Some("log=aa&seq=1"), false).await;
    assert_eq!(reply.0, StatusCode::OK);
    assert_eq!(body(&reply)["epoch"]["seq"], 2);
    assert_eq!(
        body(&reply)["pointer"],
        json!({"version": "one", "prefix": "deploy/p", "rollout": {"percent": 100}})
    );

    // A child current on the epoch is woken by a new pointer alone.
    let later = sync.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        later.publish_pointer(pointer("two"));
    });
    let began = Instant::now();
    let reply = sync.answer_peer(Some("log=aa&seq=2"), false).await;
    assert_eq!(body(&reply)["pointer"]["version"], "two");
    assert!(began.elapsed() >= Duration::from_millis(100) && began.elapsed() < HOLD);
}

#[test]
fn a_relayed_watcher_takes_each_pointer_of_its_view_once() {
    let sync = CatalogSync::new(Vec::new());
    let (mut views, mut handled) = (sync.subscribe(), None);
    let mut next = || unhandled_pointer(&mut views, &mut handled);
    assert_eq!(next(), None, "no view yet");
    sync.publish(view_at(1));
    assert_eq!(next(), None, "a view without one");
    sync.publish(View {
        pointer: Some(pointer("one")),
        ..view_at(2)
    });
    assert_eq!(next(), Some(pointer("one")));
    sync.publish(view_at(3));
    assert_eq!(next(), None, "already handled");
    sync.publish(View {
        pointer: Some(pointer("two")),
        ..view_at(4)
    });
    assert_eq!(next(), Some(pointer("two")));
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
    follower_of(upstreams, Some(store), None)
}

fn follower_of(
    upstreams: &[(&str, &str)],
    store: Option<Arc<ApplicationStore>>,
    deploy: Option<Bucket>,
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
        deploy,
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

/// A deploy bucket, with `deploy/current.json` naming `pointer` if given.
async fn deploy_bucket(pointer: Option<&DeployPointer>) -> (tempfile::TempDir, Bucket) {
    let directory = tempfile::tempdir().unwrap();
    let bucket = crate::dev::open_local_bucket(&directory.path().join("deploy.sqlite")).unwrap();
    if let Some(pointer) = pointer {
        let body = serde_json::to_vec(pointer).unwrap();
        bucket.put("deploy/current.json", body).await.unwrap();
    }
    (directory, bucket)
}

#[tokio::test]
async fn follower_takes_the_pointer_from_its_upstream_and_parses_an_older_reply() {
    let reply = json!({"epoch": epoch(7), "age_ms": 0, "pointer": pointer("one")});
    let (new, _) = upstream(reply).await;
    let (old, _) = upstream(json!({"epoch": epoch(7), "age_ms": 0})).await;
    let (_directory, store) = store();
    let (sync, _task) = follower(&[("node-a", new.as_str())], store.clone());
    let view = first_view(&sync).await;
    assert_eq!(view.epoch.as_ref().unwrap().seq, 7);
    assert_eq!(view.pointer, Some(pointer("one")));

    // A reply without `pointer` is an older binary's: still a view, with none.
    let (sync, _task) = follower(&[("node-a", old.as_str())], store);
    let view = first_view(&sync).await;
    assert_eq!(view.epoch.as_ref().unwrap().seq, 7);
    assert_eq!(view.pointer, None);
}

#[tokio::test]
async fn follower_reads_the_pointer_from_the_deploy_bucket_when_every_upstream_fails() {
    let (down, also_down) = (dead().await, dead().await);
    let upstreams = [("node-a", down.as_str()), ("node-b", also_down.as_str())];
    let (_deploy_directory, deploy) = deploy_bucket(Some(&pointer("one"))).await;

    // Worker-only: no applications, so there is no epoch to read.
    let (sync, _task) = follower_of(&upstreams, None, Some(deploy.clone()));
    let view = first_view(&sync).await;
    assert!(view.epoch.is_none());
    assert_eq!(view.pointer, Some(pointer("one")));
    assert!(
        view.age_now() < Duration::from_secs(1),
        "a bucket read is fresh"
    );

    // Both sources: the one view carries both.
    let (_directory, catalog) = store();
    commit(&catalog, "one").await;
    let (sync, _task) = follower_of(&upstreams, Some(catalog), Some(deploy));
    let view = first_view(&sync).await;
    assert_eq!(view.epoch.as_ref().unwrap().seq, 1);
    assert_eq!(view.pointer, Some(pointer("one")));

    // A pointer that cannot be read leaves the rest of the read standing.
    let (_other_directory, catalog) = store();
    commit(&catalog, "one").await;
    let (_empty_directory, empty) = deploy_bucket(None).await;
    let (sync, _task) = follower_of(&upstreams, Some(catalog), Some(empty));
    let view = first_view(&sync).await;
    assert_eq!(view.epoch.as_ref().unwrap().seq, 1);
    assert_eq!(view.pointer, None);
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
fn a_read_is_redone_only_when_the_log_named_a_commit_it_may_predate() {
    let one = ApplicationId {
        app: "one".to_owned().try_into().unwrap(),
    };
    let other = ApplicationId {
        app: "two".to_owned().try_into().unwrap(),
    };
    let sync = CatalogSync::new(Vec::new());
    let at = |seq: u64| Some(Some(("aa".to_owned(), seq)));
    // No view yet: nothing was followed, so nothing was skipped.
    assert!(!sync.changed_since(&None, &one, Some("r")));
    // `epoch(n)` names `one` at revision "r" as change n.
    sync.publish(view_at(3));
    assert!(sync.changed_since(&None, &one, Some("r")), "began with no view");
    assert!(!sync.changed_since(&at(3), &one, Some("old")), "nothing past the start");
    assert!(!sync.changed_since(&at(2), &one, Some("r")), "it read that revision");
    assert!(sync.changed_since(&at(2), &one, Some("old")), "a commit it may predate");
    assert!(!sync.changed_since(&at(2), &other, Some("old")), "another application's");
    assert!(sync.changed_since(&at(1), &one, Some("r")), "trimmed past the start");
    assert!(sync.changed_since(&Some(Some(("bb".into(), 2))), &one, Some("r")), "another log");
    assert!(!sync.changed_since(&Some(None), &other, None));
    assert!(sync.changed_since(&Some(None), &one, None), "a log that appeared names it");
    sync.publish(View {
        epoch: None,
        ..view_at(0)
    });
    assert!(sync.changed_since(&at(3), &one, Some("r")), "the log became unreadable");
    assert!(!sync.changed_since(&Some(None), &one, Some("r")));
}

#[test]
fn a_disk_copy_is_checked_against_the_newest_named_revision() {
    let one = ApplicationId {
        app: "one".to_owned().try_into().unwrap(),
    };
    let other = ApplicationId {
        app: "two".to_owned().try_into().unwrap(),
    };
    let sync = CatalogSync::new(Vec::new());
    assert!(!sync.names_other(&one, Some("old")), "no view says nothing");
    // `epoch(3)` names `one` at revision "r".
    sync.publish(view_at(3));
    assert!(!sync.names_other(&one, Some("r")));
    assert!(sync.names_other(&one, Some("old")), "a copy older than the log");
    assert!(sync.names_other(&one, None), "a copy of an application it says exists");
    assert!(!sync.names_other(&other, Some("old")), "the log does not name it");
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
