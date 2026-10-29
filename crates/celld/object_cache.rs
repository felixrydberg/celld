// The object cache is node-local disk state on the host's real filesystem,
// outside the Actor execution domain.
#![allow(clippy::disallowed_methods)]

//! A node-local disk tier for catalog objects. A cold request reads several
//! bucket objects in sequence (route and cell indexes, the application's
//! metadata, the deployment manifest, asset indexes and bodies); keeping them
//! on disk lets a node hold far more than its memory caches and start warm.
//! A read goes disk, then `fetch`, and what `fetch` returns is written back.
//! `fetch` asks the relay the node follows (`CELLD_SYNC_UPSTREAM`) before the
//! bucket, so in a relay tree each object costs the bucket one read per root and
//! a relay's single flight and disk serve everything below it.
//!
//! Immutable objects (everything but application metadata) are valid forever
//! and survive restarts. Metadata is mutable, so its disk copy is served only
//! while the node follows the epoch log (`trust`): the poll loop `forget`s each
//! application the log names, and `distrust`s everything whenever the log
//! cannot be followed.
//!
//! One node per cache directory, and a directory per fleet bucket: entries are
//! keyed by the bucket's scheme, name and prefix too, so a directory shared by
//! two fleets does not mix them, but two processes of one fleet would wipe and
//! evict each other's files.

use crate::applications::{content_digest, write_cache_file};
use crate::bucket::Bucket;
#[cfg(target_os = "linux")]
use crate::catalog_sync::{CatalogSync, Upstream};
#[cfg(target_os = "linux")]
use crate::peer_auth::PeerAuth;
use crate::single_flight::SingleFlight;
use anyhow::{Context, Result};
use bytes::Bytes;
use celld_applications::ApplicationId;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(target_os = "linux")]
use std::sync::Arc;
use std::time::{Duration, Instant};

const DEFAULT_BYTES: u64 = 2 << 30;

/// The header that carries the CAS token of application metadata on a
/// `/peer/object` reply.
pub(crate) const TOKEN_HEADER: &str = "x-celld-object-token";

/// What one object may take from a relay, body included.
// ponytail: flat, so a 64 MiB artifact gets down to about 1 MB/s; scale it by
// size, or time out on silence, if slower links matter.
#[cfg(target_os = "linux")]
const RELAY_TIMEOUT: Duration = Duration::from_secs(60);

/// What asking the relay came to: `None` to read the bucket instead, `Some(None)`
/// for no such object, else the body and, for metadata, its token.
type Relayed = Option<Option<(Bytes, Option<String>)>>;

/// The least gap between two scans for eviction, which read every file's size.
const SWEEP_EVERY: Duration = if cfg!(test) {
    Duration::ZERO
} else {
    Duration::from_secs(5)
};

/// What one fetch shared with the callers that waited on it.
#[derive(Clone)]
enum Fetched {
    Absent,
    /// Written to its path in the tier; each caller reads it from there.
    Disk,
    /// The tier is off, or could not take the write.
    Memory(Bytes),
}

/// An immutable object as `immutable_open` hands it over.
pub(crate) enum Object {
    /// Open in the tier, with its length: meant to be streamed, not read whole.
    File(tokio::fs::File, u64),
    /// The tier is off, or could not take the write.
    Bytes(Bytes),
}

/// The relay a node follows, which misses ask before the bucket.
#[cfg(target_os = "linux")]
struct Relay {
    sync: Arc<CatalogSync>,
    http: reqwest::Client,
    auth: Arc<PeerAuth>,
    /// Whether the last ask failed, so an outage is logged once, not per object.
    failing: AtomicBool,
}

pub(crate) struct ObjectCache {
    bucket: Bucket,
    /// `<root>/objects/<shard>/<digest>` holds immutable objects, bounded by
    /// `max_bytes`; `<root>/metadata/<shard>/<digest>` holds a CAS token, a
    /// newline and the body of one application's metadata, unbounded (one small
    /// file per application, wiped whenever the node cannot follow the log).
    root: PathBuf,
    /// `0` turns the tier off: reads go straight to the source.
    max_bytes: u64,
    /// Qualifies every digest, so fleets sharing a directory do not mix.
    scope: String,
    /// A burst of misses of one key fetches once and shares the result.
    flights: SingleFlight<String, Fetched>,
    swept: std::sync::Mutex<Option<Instant>>,
    /// Whether the disk copies of metadata may be served.
    trusted: AtomicBool,
    /// Whether the last `distrust` deleted every metadata file, which `trust`
    /// needs: a failed delete may leave outdated copies behind.
    emptied: AtomicBool,
    /// Bumped by `forget` and `distrust`: a metadata read that began before
    /// one must not write its (possibly outdated) answer after it.
    generation: AtomicU64,
    /// Orders metadata writes against the deletes that follow a generation bump.
    metadata_lock: tokio::sync::Mutex<()>,
    #[cfg(target_os = "linux")]
    relay: std::sync::OnceLock<Relay>,
}

impl ObjectCache {
    /// From `CELLD_OBJECT_CACHE_DIR` and `CELLD_OBJECT_CACHE_BYTES`
    /// (`env_vars::validate` has checked the latter).
    pub(crate) fn new(bucket: Bucket) -> Result<Self> {
        let root = std::env::var_os("CELLD_OBJECT_CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("celld").join("object-cache"));
        let max_bytes = crate::env_vars::with_default("CELLD_OBJECT_CACHE_BYTES", DEFAULT_BYTES)?;
        Self::at(bucket, root, max_bytes)
    }

    pub(crate) fn at(bucket: Bucket, root: PathBuf, max_bytes: u64) -> Result<Self> {
        if max_bytes > 0 {
            // Deployment config lives here: only the node's user may read it.
            let mut directory = std::fs::DirBuilder::new();
            directory.recursive(true);
            #[cfg(unix)]
            std::os::unix::fs::DirBuilderExt::mode(&mut directory, 0o700);
            directory
                .create(&root)
                .with_context(|| format!("create object cache directory {}", root.display()))?;
        }
        Ok(Self {
            scope: format!("{}://{}/{}\n", bucket.scheme(), bucket.name, bucket.prefix),
            bucket,
            root,
            max_bytes,
            flights: Default::default(),
            swept: Default::default(),
            trusted: AtomicBool::new(false),
            emptied: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            metadata_lock: Default::default(),
            #[cfg(target_os = "linux")]
            relay: Default::default(),
        })
    }

    fn enabled(&self) -> bool {
        self.max_bytes > 0
    }

    /// This node follows `sync`'s upstream: misses ask it before the bucket
    /// (first call wins).
    #[cfg(target_os = "linux")]
    pub(crate) fn set_relay(
        &self,
        sync: Arc<CatalogSync>,
        http: reqwest::Client,
        auth: Arc<PeerAuth>,
    ) {
        let _ = self.relay.set(Relay {
            sync,
            http,
            auth,
            failing: AtomicBool::new(false),
        });
    }

    fn object_path(&self, key: &str) -> Option<PathBuf> {
        self.enabled().then(|| {
            let digest = content_digest(format!("{}{key}", self.scope).as_bytes());
            self.root.join("objects").join(&digest[..2]).join(&digest)
        })
    }

    /// The immutable object at `key`: from disk, else fetched and written
    /// back. `None` (absent) is never cached: a read can race the first
    /// publication. Concurrent misses of one key fetch once; with the tier on,
    /// what they share is the file, so the bytes are not held once per waiter.
    pub(crate) async fn immutable(
        &self,
        key: &str,
        max: u64,
        too_large: &'static str,
    ) -> Result<Option<Bytes>> {
        let path = self.object_path(key);
        if let Some(body) = hit(path.as_deref(), max).await {
            return Ok(Some(body));
        }
        match self.fill(key, max, too_large, path.as_deref()).await? {
            Fetched::Absent => Ok(None),
            Fetched::Memory(body) => Ok(Some(body)),
            // Evicted since it was written: rare, and the source still has it.
            Fetched::Disk => match hit(path.as_deref(), max).await {
                Some(body) => Ok(Some(body)),
                None => self.fetch(key, max, too_large).await,
            },
        }
    }

    /// `immutable` for a reader that streams: with the tier on, the object is
    /// handed over as the open file, so hundreds of children asking for one
    /// 64 MiB artifact do not each hold it in memory.
    pub(crate) async fn immutable_open(
        &self,
        key: &str,
        max: u64,
        too_large: &'static str,
    ) -> Result<Option<Object>> {
        let path = self.object_path(key);
        if let Some((file, length)) = open(path.as_deref(), max).await {
            return Ok(Some(Object::File(file, length)));
        }
        match self.fill(key, max, too_large, path.as_deref()).await? {
            Fetched::Absent => Ok(None),
            Fetched::Memory(body) => Ok(Some(Object::Bytes(body))),
            Fetched::Disk => match open(path.as_deref(), max).await {
                Some((file, length)) => Ok(Some(Object::File(file, length))),
                None => Ok(self.fetch(key, max, too_large).await?.map(Object::Bytes)),
            },
        }
    }

    /// One fetch of `key`, shared by the callers that arrive meanwhile, and
    /// written to `path` when the tier is on.
    async fn fill(
        &self,
        key: &str,
        max: u64,
        too_large: &'static str,
        path: Option<&Path>,
    ) -> Result<Fetched> {
        self.flights
            .run(key.to_owned(), || async {
                let Some(body) = self.fetch(key, max, too_large).await? else {
                    return Ok(Fetched::Absent);
                };
                let Some(path) = path else {
                    return Ok(Fetched::Memory(body));
                };
                // The disk only saves reads of the source: a full or failing one must not fail the request.
                match write_cache_file(path, &body).await {
                    Ok(()) => {
                        self.sweep().await;
                        Ok(Fetched::Disk)
                    }
                    Err(error) => {
                        tracing::warn!(event = "object_cache_write_failed", %error);
                        Ok(Fetched::Memory(body))
                    }
                }
            })
            .await
    }

    /// Where a miss is filled from: the relay this node follows, else the bucket.
    async fn fetch(&self, key: &str, max: u64, too_large: &'static str) -> Result<Option<Bytes>> {
        if let Some(found) = self.relayed(key, max, false).await {
            return Ok(found.map(|(body, _)| body));
        }
        Ok(self
            .bucket
            .get_bounded(key, max, too_large)
            .await?
            .map(|(body, _)| body))
    }

    /// `key` from the relay this node follows. `None`: read the bucket, because
    /// there is no relay to ask (none is up, or it does not serve `key`) or it
    /// failed, which is logged once per outage. `Some(None)`: the relay says
    /// there is no such object. With `position`, the relay is told where this
    /// node's view of the epoch stands, so it may answer metadata from its disk.
    #[cfg(target_os = "linux")]
    pub(crate) async fn relayed(&self, key: &str, max: u64, position: bool) -> Relayed {
        crate::applications::object_kind(key)?;
        let relay = self.relay.get()?;
        let upstream = relay.sync.current_upstream()?;
        match relay.get(&upstream, key, max, position).await {
            Ok(found) => {
                if relay.failing.swap(false, Ordering::Relaxed) {
                    tracing::info!(upstream = %upstream.node, "object relay answers again");
                }
                Some(found)
            }
            Err(error) => {
                if !relay.failing.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        upstream = %upstream.node,
                        error = %format!("{error:#}"),
                        "object relay failed; reading the bucket"
                    );
                }
                None
            }
        }
    }

    /// Only Linux nodes run the application platform, so none has a relay.
    #[cfg(not(target_os = "linux"))]
    pub(crate) async fn relayed(&self, _key: &str, _max: u64, _position: bool) -> Relayed {
        None
    }

    /// Evicts the oldest files past `max_bytes`, at most once per `SWEEP_EVERY`.
    // ponytail: scans the whole tier and is awaited by the miss that triggers
    // it; spawn it, or keep an index of sizes, if tiers get big. Temp files a
    // crash stranded are never evicted; pass a max age if that adds up.
    async fn sweep(&self) {
        {
            let mut swept = self.swept.lock().expect("object cache poisoned");
            if swept.is_some_and(|at| at.elapsed() < SWEEP_EVERY) {
                return;
            }
            *swept = Some(Instant::now());
        }
        let (root, max_bytes) = (self.root.join("objects"), self.max_bytes);
        let swept = tokio::task::spawn_blocking(move || {
            crate::assets::cleanup_cache_files(&root, max_bytes, None)
        })
        .await;
        match swept {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(event = "object_cache_sweep_failed", %error),
            Err(error) => tracing::warn!(event = "object_cache_sweep_failed", %error),
        }
    }

    fn metadata_path(&self, id: &ApplicationId) -> PathBuf {
        let digest = content_digest(format!("{}{}", self.scope, id.app.as_str()).as_bytes());
        self.root.join("metadata").join(&digest[..2]).join(&digest)
    }

    /// The disk copy of `id`'s metadata and its CAS token, only while trusted.
    pub(crate) async fn metadata(&self, id: &ApplicationId) -> Option<(Bytes, String)> {
        if !self.enabled() || !self.trusted.load(Ordering::SeqCst) {
            return None;
        }
        let file = tokio::fs::read(self.metadata_path(id)).await.ok()?;
        let newline = file.iter().position(|byte| *byte == b'\n')?;
        let token = String::from_utf8(file[..newline].to_vec()).ok()?;
        Some((Bytes::from(file).slice(newline + 1..), token))
    }

    /// Taken before the bucket read that `store_metadata` will write.
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Writes what a bucket read of `id` returned, unless `forget` or
    /// `distrust` ran since `generation` was taken: the read may predate the
    /// change they answer.
    pub(crate) async fn store_metadata(
        &self,
        id: &ApplicationId,
        body: &[u8],
        token: &str,
        generation: u64,
    ) {
        if !self.enabled() {
            return;
        }
        let _write = self.metadata_lock.lock().await;
        if self.generation() != generation {
            return;
        }
        let file = [token.as_bytes(), b"\n", body].concat();
        if let Err(error) = write_cache_file(&self.metadata_path(id), &file).await {
            tracing::warn!(event = "object_cache_write_failed", %error);
        }
    }

    /// `id` changed in the bucket: its disk copy is outdated.
    pub(crate) async fn forget(&self, id: &ApplicationId) {
        if !self.enabled() {
            return;
        }
        self.generation.fetch_add(1, Ordering::SeqCst);
        let _write = self.metadata_lock.lock().await;
        match tokio::fs::remove_file(self.metadata_path(id)).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                // The outdated copy is still there: serve none until a wipe.
                tracing::warn!(event = "object_cache_forget_failed", %error);
                self.trusted.store(false, Ordering::SeqCst);
                self.emptied.store(false, Ordering::SeqCst);
            }
        }
    }

    /// The node cannot say what changed: serve no disk metadata and delete it
    /// all.
    pub(crate) async fn distrust(&self) {
        self.trusted.store(false, Ordering::SeqCst);
        if !self.enabled() {
            return;
        }
        self.generation.fetch_add(1, Ordering::SeqCst);
        let _write = self.metadata_lock.lock().await;
        let emptied = match tokio::fs::remove_dir_all(self.root.join("metadata")).await {
            Ok(()) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
            Err(error) => {
                tracing::warn!(event = "object_cache_wipe_failed", %error);
                false
            }
        };
        self.emptied.store(emptied, Ordering::SeqCst);
    }

    /// The node follows the epoch log again, from the state `distrust` left.
    /// Refused if that delete failed; a later `distrust` can succeed.
    pub(crate) fn trust(&self) {
        if self.emptied.load(Ordering::SeqCst) {
            self.trusted.store(true, Ordering::SeqCst);
        }
    }
}

#[cfg(target_os = "linux")]
impl Relay {
    /// One GET of `/peer/object`. Anything but an object or a 404 from a node
    /// speaking this protocol is an error, so that an older binary's 404 for an
    /// unknown path is never read as an absent object.
    async fn get(
        &self,
        upstream: &Upstream,
        key: &str,
        max: u64,
        position: bool,
    ) -> Result<Option<(Bytes, Option<String>)>> {
        // In a block: the serializer is not `Send`, so it must not live across
        // the awaits below.
        let path = {
            let mut query = url::form_urlencoded::Serializer::new(String::new());
            query.append_pair("key", key);
            let asked = if position {
                self.sync.position().flatten()
            } else {
                None
            };
            if let Some((log, seq)) = asked {
                query
                    .append_pair("log", &log)
                    .append_pair("seq", &seq.to_string());
            }
            format!("/peer/object?{}", query.finish())
        };
        let mut response = self
            .auth
            .sign(
                self.http.get(format!("http://{}{path}", upstream.addr)),
                "GET",
                &path,
                &[],
                &upstream.node,
            )?
            .timeout(RELAY_TIMEOUT)
            .send()
            .await
            .context("request the object")?;
        crate::peer_auth::validate_response(response.headers())?;
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        anyhow::ensure!(
            status == reqwest::StatusCode::OK,
            "object refused with HTTP {status}"
        );
        anyhow::ensure!(
            response.content_length().is_none_or(|length| length <= max),
            "object exceeds its limit"
        );
        let token = response
            .headers()
            .get(TOKEN_HEADER)
            .and_then(|token| token.to_str().ok())
            .map(str::to_owned);
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.context("read the object")? {
            anyhow::ensure!(
                (body.len() + chunk.len()) as u64 <= max,
                "object exceeds its limit"
            );
            body.extend_from_slice(&chunk);
        }
        // A wrong body would otherwise be written to the tier before a caller's
        // own digest check, and served from there until evicted. Fail it here,
        // so the read goes to the bucket instead.
        if let Some((_, digest)) = key.rsplit_once("/artifacts/") {
            anyhow::ensure!(
                content_digest(&body) == digest,
                "relayed object does not match its digest"
            );
        }
        Ok(Some((Bytes::from(body), token)))
    }
}

/// The cached object, if the tier is on and has it within `max`.
async fn hit(path: Option<&Path>, max: u64) -> Option<Bytes> {
    let path = path?;
    let body = tokio::fs::read(path).await.ok()?;
    if body.len() as u64 > max {
        return None;
    }
    crate::assets::touch_cache_file(path.to_owned());
    Some(Bytes::from(body))
}

/// `hit` for a reader that streams: the open file and its length.
async fn open(path: Option<&Path>, max: u64) -> Option<(tokio::fs::File, u64)> {
    let path = path?;
    let file = tokio::fs::File::open(path).await.ok()?;
    let length = file.metadata().await.ok()?.len();
    if length > max {
        return None;
    }
    crate::assets::touch_cache_file(path.to_owned());
    Some((file, length))
}

/// `key` through `cache` when the node has one, else straight from `bucket`.
pub(crate) async fn immutable_object(
    cache: Option<&ObjectCache>,
    bucket: &Bucket,
    key: &str,
    max: u64,
    too_large: &'static str,
) -> Result<Option<Bytes>> {
    match cache {
        Some(cache) => cache.immutable(key, max, too_large).await,
        None => Ok(bucket
            .get_bounded(key, max, too_large)
            .await?
            .map(|(body, _)| body)),
    }
}

#[cfg(test)]
#[path = "unit_tests/object_cache/tests.rs"]
mod tests;
