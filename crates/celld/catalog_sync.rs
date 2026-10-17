// Catalog sync holds and retries peer requests on real timers and reads the
// wall clock for jitter: host control-plane work outside the Actor execution
// domain.
#![allow(clippy::disallowed_methods)]

//! A relay tree for the catalog epoch (`applications/v2/epoch`). By default a
//! node reads the epoch from the bucket every poll period, so bucket cost grows
//! with node count. With `CELLD_SYNC_UPSTREAM` it long-polls its upstream nodes
//! instead, and serves what it has to its own children on `/peer/sync`. The
//! protocol in and out is the same, so relays feed relays.
//!
//! Ages are relative (how old the root's bucket read was when a node got it),
//! so no clock is shared between nodes. A view older than `STALE` is refused,
//! and that one rule is what survives a dead root, a partition and two nodes
//! feeding each other: the view just ages until the children fail over.

use crate::applications::{ApplicationStore, EpochLog};
use crate::peer_auth::PeerAuth;
use anyhow::{ensure, Context, Result};
use axum::http::StatusCode;
use std::sync::{Arc, Mutex};
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

pub(crate) struct Upstream {
    pub(crate) node: String,
    /// `host:port` of the node's internal listener.
    pub(crate) addr: String,
}

/// The epoch as this node last learned it.
#[derive(Clone)]
pub(crate) struct View {
    /// `None`: the bucket has no readable log.
    pub(crate) epoch: Option<Arc<EpochLog>>,
    /// How old the root's bucket read was when this node got it.
    pub(crate) age: Duration,
    pub(crate) received: Instant,
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

/// One node's view of the epoch, and the children waiting on it.
pub(crate) struct CatalogSync {
    upstreams: Vec<Upstream>,
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
            view: watch::channel(None).0,
            listened: Mutex::new(None),
            notified: Mutex::new(Instant::now()),
        }
    }

    /// From `CELLD_SYNC_UPSTREAM`, which `env_vars::validate` has checked.
    pub(crate) fn from_env() -> Self {
        let upstreams = crate::env_vars::sync_upstreams().expect("validated CELLD_SYNC_UPSTREAM");
        Self::new(
            upstreams
                .into_iter()
                .map(|(node, addr)| Upstream { node, addr })
                .collect(),
        )
    }

    /// Reads upstreams, not the bucket.
    pub(crate) fn relayed(&self) -> bool {
        !self.upstreams.is_empty()
    }

    /// A child asked recently, so this node keeps its view current even if it
    /// serves nothing itself.
    pub(crate) fn listening(&self) -> bool {
        lock(&self.listened).is_some_and(|at| at.elapsed() < STALE)
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<Option<View>> {
        self.view.subscribe()
    }

    /// The epoch of the current view; `None` before there is one.
    pub(crate) fn epoch(&self) -> Option<Option<Arc<EpochLog>>> {
        self.view.borrow().as_ref().map(|view| view.epoch.clone())
    }

    /// Bucket mode: one GET of the epoch. Every read refreshes the view's age,
    /// but held children are woken only by a change or, so that they get a
    /// fresh age, once per `HEARTBEAT`.
    pub(crate) async fn read_bucket(
        &self,
        store: &ApplicationStore,
    ) -> Result<Option<Arc<EpochLog>>> {
        let epoch = store.epoch().await?.map(Arc::new);
        let now = Instant::now();
        let mut notified = lock(&self.notified);
        self.view.send_if_modified(|view| {
            let same = view.as_ref().is_some_and(|view| view.epoch == epoch);
            if same {
                if let Some(view) = view {
                    view.age = Duration::ZERO;
                    view.received = now;
                }
            } else {
                *view = Some(View {
                    epoch: epoch.clone(),
                    age: Duration::ZERO,
                    received: now,
                });
            }
            let wake = !same || now.duration_since(*notified) >= HEARTBEAT;
            if wake {
                *notified = now;
            }
            wake
        });
        Ok(epoch)
    }

    /// An upstream's answer. Always wakes the children, heartbeats included, so
    /// a fresh age flows down the tree.
    pub(crate) fn publish(&self, view: View) {
        self.view.send_replace(Some(view));
    }

    /// What a child that has `log` at `seq` is told. A child that is behind, or
    /// has no view (`log` absent; empty: a view with no log), gets the view at
    /// once. One that is current is held for the next wake, up to `HOLD`, and
    /// then gets the view as it stands.
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
        })
    }
}

/// The relayed node's driver: long-polls one upstream and stays on it while it
/// answers fresh views. On an error, a refusal or a stale view it moves to the
/// next. When a whole round has failed it reads the bucket itself, at most once
/// per `CELLD_SYNC_FALLBACK_S` (±20 %), and tries the upstreams again from the
/// first every `ROUND_RETRY`.
// ponytail: stays on whichever upstream answers; returning to the preferred
// first one once it recovers is left until an operator needs that ordering.
pub(crate) async fn follow(
    sync: Arc<CatalogSync>,
    store: Arc<ApplicationStore>,
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
                sync.publish(view);
            }
            Err(error) => {
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
                    match store.epoch().await {
                        Ok(epoch) => sync.publish(View {
                            epoch: epoch.map(Arc::new),
                            age: Duration::ZERO,
                            received: Instant::now(),
                        }),
                        Err(error) => {
                            tracing::warn!(error = %format!("{error:#}"), "catalog epoch not read")
                        }
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
