// Catalog sync holds and retries peer requests on real timers and reads the
// wall clock for jitter: host control-plane work outside the Actor execution
// domain.
#![allow(clippy::disallowed_methods)]
// Only application hosting (Linux) asks most of what a node's sync answers.
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

//! A relay tree for the catalog epoch (`applications/v2/epoch`) and the Worker
//! deployment pointer (`deploy/current.json`). By default a node reads both
//! from the bucket every poll period, so bucket cost grows with node count.
//! With `CELLD_SYNC_UPSTREAM` it long-polls its upstream nodes instead, and
//! serves what it has to its own children on `/peer/sync`. The protocol in and
//! out is the same, so relays feed relays.
//!
//! Ages are relative (how old the root's bucket read was when a node got it),
//! so no clock is shared between nodes. A view older than `STALE` is refused,
//! and that one rule is what survives a dead root, a partition and two nodes
//! feeding each other: the view just ages until the children fail over.
//!
//! Catalog objects travel the same tree: a relayed node reads them from the
//! upstream it currently follows (`current_upstream`, see `object_cache`),
//! and the bucket only if that fails.

use crate::applications::{ApplicationStore, EpochLog};
use crate::bucket::Bucket;
use crate::peer_auth::PeerAuth;
use crate::protocol::DeployPointer;
use anyhow::{ensure, Context, Result};
use axum::http::StatusCode;
use celld_applications::ApplicationId;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::time::{sleep, timeout};

/// How long a child that is up to date is held before it gets the view anyway.
const HOLD: Duration = if cfg!(test) {
    Duration::from_millis(300)
} else {
    Duration::from_secs(20)
};

/// What a child allows its request beyond `HOLD`.
const REQUEST_SLACK: Duration = Duration::from_secs(10);

/// A view this old is not followed, and a child silent this long is not
/// listening.
const STALE: Duration = Duration::from_secs(60);

/// The longest a bucket-mode node goes without waking its children, so an
/// unchanged epoch still delivers a fresh age.
const HEARTBEAT: Duration = Duration::from_secs(20);

/// Between upstream attempts, so a dead list does not spin.
const RETRY: Duration = if cfg!(test) {
    Duration::from_millis(20)
} else {
    Duration::from_secs(1)
};

/// Between rounds while every upstream is failing, so a restarted one is
/// followed again within seconds.
const ROUND_RETRY: Duration = if cfg!(test) {
    Duration::from_millis(20)
} else {
    Duration::from_secs(5)
};

/// Seconds between reads of the bucket while every upstream is failing
/// (`CELLD_SYNC_FALLBACK_S`).
const FALLBACK: u64 = 300;

const MAX_REPLY_BYTES: usize = 256 * 1024;

#[derive(Clone)]
pub(crate) struct Upstream {
    pub(crate) node: String,
    /// `host:port` of the node's internal listener.
    pub(crate) addr: String,
}

/// The epoch and deploy pointer as this node last learned them.
#[derive(Clone)]
pub struct View {
    /// `None`: the bucket has no readable log, or this node hosts no
    /// applications.
    pub(crate) epoch: Option<Arc<EpochLog>>,
    /// How old the root's bucket read was when this node got it.
    pub(crate) age: Duration,
    pub(crate) received: Instant,
    /// `deploy/current.json` as the root last read it. `None` until some node
    /// up the tree has read one, and from upstreams that predate it.
    pub pointer: Option<DeployPointer>,
}

impl View {
    pub(crate) fn age_now(&self) -> Duration {
        self.age + self.received.elapsed()
    }

    fn position(&self) -> Option<(&str, u64)> {
        self.epoch
            .as_deref()
            .map(|epoch| (epoch.log.as_str(), epoch.seq))
    }
}

/// One node's view of the epoch and the deploy pointer, and the children
/// waiting on it. A relay that serves application nodes must host applications
/// itself (or read the epoch through `read_bucket`): a view that only ever got
/// a pointer has no epoch, and its children would see none.
pub struct CatalogSync {
    upstreams: Vec<Upstream>,
    /// Which upstream `follow` is on while it answers: where objects are read
    /// too. `None` while every one fails, and in bucket mode.
    current: Mutex<Option<usize>>,
    /// The log position `poll_epoch` has acted on: this node's disk copies are
    /// current as of it, which is what a child's object read is checked against.
    processed: Mutex<Option<(String, u64)>>,
    view: watch::Sender<Option<View>>,
    /// When a child last asked.
    listened: Mutex<Option<Instant>>,
    /// When `read_bucket` last woke the children.
    notified: Mutex<Instant>,
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().expect("catalog sync state poisoned")
}

impl CatalogSync {
    pub(crate) fn new(upstreams: Vec<Upstream>) -> Self {
        Self {
            upstreams,
            current: Mutex::new(None),
            processed: Mutex::new(None),
            view: watch::channel(None).0,
            listened: Mutex::new(None),
            notified: Mutex::new(Instant::now()),
        }
    }

    /// From `CELLD_SYNC_UPSTREAM`, which `env_vars::validate` has checked.
    pub fn from_env() -> Self {
        let upstreams = crate::env_vars::sync_upstreams().expect("validated CELLD_SYNC_UPSTREAM");
        Self::new(
            upstreams
                .into_iter()
                .map(|(node, addr)| Upstream { node, addr })
                .collect(),
        )
    }

    /// Reads upstreams, not the bucket.
    pub fn relayed(&self) -> bool {
        !self.upstreams.is_empty()
    }

    /// A child asked recently, so this node keeps its view current even if it
    /// serves nothing itself.
    pub(crate) fn listening(&self) -> bool {
        lock(&self.listened).is_some_and(|at| at.elapsed() < STALE)
    }

    /// The upstream the follower last heard a fresh view from, if it is not
    /// failing now. Children of a dead list would otherwise wait out a timeout
    /// per object, so only this one is asked.
    pub(crate) fn current_upstream(&self) -> Option<Upstream> {
        let current = (*lock(&self.current))?;
        self.upstreams.get(current).cloned()
    }

    pub(crate) fn set_current(&self, current: Option<usize>) {
        *lock(&self.current) = current;
    }

    /// `poll_epoch` has acted on the log up to `position` (`None`: no readable
    /// log, or it was reset).
    pub(crate) fn mark_processed(&self, position: Option<(String, u64)>) {
        *lock(&self.processed) = position;
    }

    /// Whether the log was acted on at least up to `seq` of `log`.
    pub(crate) fn has_processed(&self, log: &str, seq: u64) -> bool {
        lock(&self.processed)
            .as_ref()
            .is_some_and(|(processed, at)| processed == log && *at >= seq)
    }

    pub fn subscribe(&self) -> watch::Receiver<Option<View>> {
        self.view.subscribe()
    }

    /// The epoch of the current view; `None` before there is one.
    pub(crate) fn epoch(&self) -> Option<Option<Arc<EpochLog>>> {
        self.view.borrow().as_ref().map(|view| view.epoch.clone())
    }

    /// Where the current view's log stands; `None` before there is a view.
    pub(crate) fn position(&self) -> Option<Option<(String, u64)>> {
        self.view.borrow().as_ref().map(|view| {
            view.position()
                .map(|(log, seq)| (log.to_owned(), seq))
        })
    }

    /// Whether the newest change the view's log names for `application` is at
    /// a revision other than `revision`. A disk copy is checked against it: the
    /// poll drops the copies the log names, but a read can take one first.
    pub(crate) fn names_other(&self, application: &ApplicationId, revision: Option<&str>) -> bool {
        let view = self.view.borrow();
        let Some(epoch) = view.as_ref().and_then(|view| view.epoch.as_deref()) else {
            return false;
        };
        epoch
            .changes
            .iter()
            .rev()
            .find(|change| change.application == *application)
            .is_some_and(|change| {
                Some(change.revision.as_str()).filter(|named| !named.is_empty()) != revision
            })
    }

    /// Whether the view, past `before` (a `position`), names a commit to
    /// `application` other than `revision`, or cannot say: `before` had no
    /// view, or the log was replaced or trimmed past it. A read that began at
    /// `before` may then predate that commit.
    pub(crate) fn changed_since(
        &self,
        before: &Option<Option<(String, u64)>>,
        application: &ApplicationId,
        revision: Option<&str>,
    ) -> bool {
        let view = self.view.borrow();
        // Nothing processed yet: the first look checks every cached application.
        let Some(view) = view.as_ref() else {
            return false;
        };
        let changes = match (before, view.epoch.as_deref()) {
            (None, _) => return true,
            (Some(None), None) => return false,
            (Some(None), Some(epoch)) => &epoch.changes[..],
            (Some(Some(_)), None) => return true,
            (Some(Some((log, seq))), Some(epoch)) => match epoch.since(log, *seq) {
                Some(changes) => changes,
                None => return true,
            },
        };
        changes.iter().any(|change| {
            change.application == *application
                && Some(change.revision.as_str()).filter(|named| !named.is_empty()) != revision
        })
    }

    /// Bucket mode: one GET of the epoch. Every read refreshes the view's age,
    /// but held children are woken only by a change or, so that they get a
    /// fresh age, once per `HEARTBEAT`.
    pub(crate) async fn read_bucket(
        &self,
        store: &ApplicationStore,
    ) -> Result<Option<Arc<EpochLog>>> {
        let epoch = store.epoch().await?.map(Arc::new);
        self.refresh(Some(epoch.clone()), None);
        Ok(epoch)
    }

    /// Bucket mode: the node's own read of `deploy/current.json`.
    // ponytail: one age covers both, so this read also freshens an epoch that
    // `read_bucket` has not re-read since: an idle root's, until the first
    // child's ask makes it poll again (up to `CELLD_APPLICATION_POLL_S`). Keep
    // an age per part if that staleness ever matters.
    pub fn publish_pointer(&self, pointer: DeployPointer) {
        self.refresh(None, Some(pointer));
    }

    /// This node's own bucket read: the epoch and/or pointer it got replace
    /// those in the view (one it did not read stays), and the view is as fresh
    /// as the read. Held children are woken by a change or once per `HEARTBEAT`.
    /// A node with no view yet creates one.
    fn refresh(&self, epoch: Option<Option<Arc<EpochLog>>>, pointer: Option<DeployPointer>) {
        let now = Instant::now();
        let mut notified = lock(&self.notified);
        self.view.send_if_modified(|current| {
            let created = current.is_none();
            let view = current.get_or_insert_with(|| View {
                epoch: None,
                age: Duration::ZERO,
                received: now,
                pointer: None,
            });
            let mut changed = created;
            if let Some(epoch) = epoch {
                changed |= view.epoch != epoch;
                view.epoch = epoch;
            }
            if let Some(pointer) = pointer {
                changed |= view.pointer.as_ref() != Some(&pointer);
                view.pointer = Some(pointer);
            }
            view.age = Duration::ZERO;
            view.received = now;
            let wake = changed || now.duration_since(*notified) >= HEARTBEAT;
            if wake {
                *notified = now;
            }
            wake
        });
    }

    /// An upstream's answer. Always wakes the children, heartbeats included, so
    /// a fresh age flows down the tree. The pointer already held stays unless
    /// the answer carries one: an older upstream sends none.
    pub(crate) fn publish(&self, mut view: View) {
        self.view.send_modify(|current| {
            if view.pointer.is_none() {
                view.pointer = current.take().and_then(|current| current.pointer);
            }
            *current = Some(view);
        });
    }

    /// What a child that has `log` at `seq` is told. A child that is behind, or
    /// has no view (`log` absent; empty: a view with no log), gets the view at
    /// once. One that is current is held for the next wake, up to `HOLD`, and
    /// then gets the view as it stands.
    // ponytail: the position names the epoch only, so a pointer that changes in
    // the gap between a child's answer and its next ask reaches it at the next
    // wake (`HOLD` at most). Ask with the pointer's version too if that matters.
    pub(crate) async fn answer(
        &self,
        log: Option<&str>,
        seq: Option<u64>,
        draining: bool,
    ) -> (StatusCode, Vec<u8>) {
        *lock(&self.listened) = Some(Instant::now());
        if draining {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                br#"{"ok":false,"draining":true}"#.to_vec(),
            );
        }
        let asked = log.map(|log| (!log.is_empty()).then_some((log, seq.unwrap_or(0))));
        let mut views = self.view.subscribe();
        let behind = views
            .borrow()
            .as_ref()
            .is_some_and(|view| asked != Some(view.position()));
        if !behind {
            let _ = timeout(HOLD, views.changed()).await;
        }
        let current = views.borrow();
        match current.as_ref() {
            Some(view) => (
                StatusCode::OK,
                serde_json::json!({
                    "epoch": view.epoch.as_deref(),
                    "age_ms": view.age_now().as_millis() as u64,
                    "pointer": view.pointer.as_ref(),
                })
                .to_string()
                .into_bytes(),
            ),
            None => (
                StatusCode::SERVICE_UNAVAILABLE,
                br#"{"error":"no catalog view yet"}"#.to_vec(),
            ),
        }
    }

    /// `answer` for a `/peer/sync` request: `log` and `seq` in `query` say
    /// where the child's view of the epoch stands.
    pub async fn answer_peer(&self, query: Option<&str>, draining: bool) -> (StatusCode, Vec<u8>) {
        let (mut log, mut seq) = (None, None);
        for (key, value) in url::form_urlencoded::parse(query.unwrap_or_default().as_bytes()) {
            match &*key {
                "log" => log = Some(value.into_owned()),
                "seq" => seq = value.parse::<u64>().ok(),
                _ => {}
            }
        }
        self.answer(log.as_deref(), seq, draining).await
    }

    /// The query that tells an upstream where this node's view stands; empty
    /// when it has none.
    fn asking(&self) -> String {
        let view = self.view.borrow();
        let Some(view) = view.as_ref() else {
            return String::new();
        };
        let (log, seq) = view.position().unwrap_or_default();
        url::form_urlencoded::Serializer::new(String::new())
            .append_pair("log", log)
            .append_pair("seq", &seq.to_string())
            .finish()
    }

    /// One long-poll of `upstream`. Refuses anything but a fresh, well-formed
    /// view.
    async fn fetch(
        &self,
        http: &reqwest::Client,
        auth: &PeerAuth,
        upstream: &Upstream,
    ) -> Result<View> {
        #[derive(serde::Deserialize)]
        struct Reply {
            epoch: Option<EpochLog>,
            age_ms: u64,
            /// Absent from an upstream that predates it.
            #[serde(default)]
            pointer: Option<DeployPointer>,
        }
        let query = self.asking();
        let path = if query.is_empty() {
            "/peer/sync".to_owned()
        } else {
            format!("/peer/sync?{query}")
        };
        let mut response = auth
            .sign(
                http.get(format!("http://{}{path}", upstream.addr)),
                "GET",
                &path,
                &[],
                &upstream.node,
            )?
            .timeout(HOLD + REQUEST_SLACK)
            .send()
            .await
            .context("request the catalog view")?;
        let status = response.status();
        ensure!(
            status == reqwest::StatusCode::OK,
            "catalog view refused with HTTP {status}"
        );
        crate::peer_auth::validate_response(response.headers())?;
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.context("read the catalog view")? {
            ensure!(
                body.len() + chunk.len() <= MAX_REPLY_BYTES,
                "catalog view is oversized"
            );
            body.extend_from_slice(&chunk);
        }
        let reply: Reply = serde_json::from_slice(&body).context("decode the catalog view")?;
        let age = Duration::from_millis(reply.age_ms);
        ensure!(age <= STALE, "catalog view is stale ({age:?} old)");
        Ok(View {
            epoch: reply.epoch.map(Arc::new),
            age,
            received: Instant::now(),
            pointer: reply.pointer,
        })
    }
}

/// This node's sync, which `/peer/sync` answers from whatever the node hosts.
// ponytail: a process-wide `OnceLock`, like `application_vm::HOSTED`, rather
// than a field on `AppHandle`, which every node-level constructor would have to
// carry. Set once at startup.
static NODE: OnceLock<Arc<CatalogSync>> = OnceLock::new();

pub fn install(sync: Arc<CatalogSync>) -> Result<()> {
    ensure!(
        NODE.set(sync).is_ok(),
        "the catalog sync is already installed"
    );
    Ok(())
}

/// `None` until `install`.
pub fn node() -> Option<&'static Arc<CatalogSync>> {
    NODE.get()
}

/// A relayed node's deploy watcher, on a wake: the view's pointer if it is not
/// the one `handled` already (which it becomes).
pub fn unhandled_pointer(
    views: &mut watch::Receiver<Option<View>>,
    handled: &mut Option<DeployPointer>,
) -> Option<DeployPointer> {
    let pointer = views.borrow_and_update().as_ref()?.pointer.clone()?;
    if handled.as_ref() == Some(&pointer) {
        return None;
    }
    *handled = Some(pointer.clone());
    Some(pointer)
}

/// The relayed node's driver: long-polls one upstream and stays on it while it
/// answers fresh views. On an error, a refusal or a stale view it moves to the
/// next. When a whole round has failed it reads the bucket itself, at most once
/// per `CELLD_SYNC_FALLBACK_S` (±20 %), and tries the upstreams again from the
/// first every `ROUND_RETRY`. That read covers what the node has a source for:
/// the epoch if it hosts applications (`store`), the pointer if it has a deploy
/// bucket.
// ponytail: stays on whichever upstream answers; returning to the preferred
// first one once it recovers is left until an operator needs that ordering.
pub async fn follow(
    sync: Arc<CatalogSync>,
    store: Option<Arc<ApplicationStore>>,
    deploy: Option<Bucket>,
    http: reqwest::Client,
    auth: Arc<PeerAuth>,
) {
    let fallback = Duration::from_secs(
        crate::env_vars::positive_or("CELLD_SYNC_FALLBACK_S", FALLBACK)
            .expect("validated CELLD_SYNC_FALLBACK_S"),
    );
    let count = sync.upstreams.len();
    let (mut current, mut failures, mut healthy) = (0, 0, true);
    // When the bucket may be read again while every upstream fails.
    let mut bucket_due: Option<Instant> = None;
    loop {
        let upstream = &sync.upstreams[current];
        match sync.fetch(&http, &auth, upstream).await {
            Ok(view) => {
                if !healthy {
                    tracing::info!(upstream = %upstream.node, "catalog sync upstream answers again");
                    healthy = true;
                }
                failures = 0;
                sync.set_current(Some(current));
                sync.publish(view);
            }
            Err(error) => {
                // Objects are read from the bucket until an upstream answers again.
                sync.set_current(None);
                if healthy {
                    tracing::warn!(
                        upstream = %upstream.node,
                        error = %format!("{error:#}"),
                        "catalog sync upstream failed; trying the next"
                    );
                    healthy = false;
                }
                failures += 1;
                if failures < count {
                    current = (current + 1) % count;
                    sleep(RETRY).await;
                    continue;
                }
                (current, failures) = (0, 0);
                if bucket_due.is_none_or(|due| Instant::now() >= due) {
                    tracing::warn!(
                        fallback_s = fallback.as_secs(),
                        "every catalog sync upstream failed; reading the bucket"
                    );
                    bucket_due =
                        Some(Instant::now() + fallback.mul_f64(0.8 + 0.4 * rand::random::<f64>()));
                    let (mut epoch, mut pointer) = (None, None);
                    if let Some(store) = &store {
                        match store.epoch().await {
                            Ok(read) => epoch = Some(read.map(Arc::new)),
                            Err(error) => {
                                tracing::warn!(error = %format!("{error:#}"), "catalog epoch not read")
                            }
                        }
                    }
                    if let Some(bucket) = &deploy {
                        match crate::fleet::read_current_pointer(bucket).await {
                            Ok(read) => pointer = Some(read),
                            Err(error) => {
                                tracing::warn!(error = %format!("{error:#}"), "deployment pointer not read")
                            }
                        }
                    }
                    if epoch.is_some() || pointer.is_some() {
                        sync.refresh(epoch, pointer);
                    }
                }
                sleep(ROUND_RETRY).await;
            }
        }
    }
}

#[cfg(test)]
#[path = "unit_tests/catalog_sync/tests.rs"]
mod tests;
