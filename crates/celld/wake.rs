// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Immutable alarm discovery publications and restart-safe reclamation.
//!
//! SQLite assigns each installation a writer epoch and sequence. A foreground
//! PUT proves discovery before acknowledgment. Background work proves SQLite
//! replication and ownership before it advances a monotone retirement record.
//! Paginated, repeated LIST passes reclaim older objects, including mutations
//! whose caller stopped waiting before their remote effect.
use crate::bucket::Bucket;
use crate::dead_node_gc::LiveNodes;
use crate::ownership_store::BucketOwnership;
use crate::peer_auth::PeerAuth;
use celld_logic::wake::elected_hint_needed;
use celld_logic::wake::parse_entry_key;
use celld_logic::wake::WakeCore;
use celld_logic::wake::{AlarmSnapshot, PublicationId, Retirement, ENTRY_PREFIX};
use futures_util::stream::{self, StreamExt as _};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::warn;

/// Stay-resident threshold: alarms due sooner than this keep their cell
/// resident when residency is cheaper than a wake cycle. Alarms further out
/// are evicted behind an entry.
pub fn resident_ms() -> i64 {
    static MS: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *MS.get_or_init(|| {
        crate::env_vars::with_default("CELLD_ALARM_RESIDENT_MS", 3_600_000)
            .expect("validated CELLD_ALARM_RESIDENT_MS")
    })
}

/// Scan the due wake buckets: used by the boot-time orphan scan and the
/// periodic waker tick.
///
/// A delimiter listing enumerates armed minutes, then opens only due minutes.
/// Cleanup separately pages through all publications, including future ones.
/// Parsing every prefix avoids relying on provider ordering for discovery.
///
/// The returned minute bounds an Actor wake hint; SQLite supplies the actual
/// deadline and delivery authority. Malformed entries cannot reach ownership
/// or cleanup, because deleting an unclassified name could destroy live work.
pub async fn due_scan(bucket: &Bucket, now_ms: i64) -> Vec<(String, i64)> {
    /// Bounded like every other fan-out against the store: a long outage can
    /// leave one due bucket per minute of backlog, and opening them all at
    /// once is the thundering herd the eviction bound exists to prevent.
    const BUCKET_READ_CONCURRENCY: usize = 16;

    let mut due = Vec::new();
    let minutes = match bucket.common_prefixes(ENTRY_PREFIX).await {
        Ok(minutes) => minutes,
        Err(e) => {
            warn!(error = %e, "wake due scan bucket listing failed");
            return due;
        }
    };
    let overdue: Vec<String> = minutes
        .into_iter()
        .filter(|minute| {
            celld_logic::wake::parse_minute_prefix(minute).is_some_and(|ms| ms <= now_ms)
        })
        .collect();

    let mut listings = futures_util::stream::iter(
        overdue
            .into_iter()
            .map(|minute| async move { (bucket.list(&minute).await, minute) }),
    )
    .buffer_unordered(BUCKET_READ_CONCURRENCY);
    while let Some((objects, minute)) = listings.next().await {
        let objects = match objects {
            Ok(objects) => objects,
            Err(e) => {
                warn!(error = %e, %minute, "wake due scan entry listing failed");
                continue;
            }
        };
        for object in objects {
            if let Some(entry) = parse_entry_key(object.location.as_ref()) {
                if entry.minute_ms <= now_ms {
                    due.push((entry.cell, entry.minute_ms));
                }
            }
        }
    }
    due.sort();
    due.dedup_by(|a, b| a.0 == b.0);
    due
}

/// Owner records the elected waker reads at once while it sorts the fleet's
/// due entries. These reads are deliberately outside the activation queue:
/// they hold no permit, so the node's own alarm wakes and cold requests never
/// queue behind the fleet's due set, and 32 in flight clears a thousand
/// entries in about a second of store latency.
const ELECTED_PROBE_CONCURRENCY: usize = 32;

/// Owners the waker pushes to at once. A push is one small request, so this
/// bounds sockets, not work.
const PUSH_CONCURRENCY: usize = 16;

/// Entries in one `POST /peer/wake`. An owner with more gets several requests
/// in a row; with cell names of at most 255 bytes, a body stays under the
/// receiver's limit however deep the backlog is.
pub const PUSH_ENTRIES_MAX: usize = 512;

/// One entry of a push: a cell whose wake entry is due and whose owner record
/// names the receiver.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct PushedEntry {
    pub cell: String,
    pub entry_ms: i64,
    /// A cleanup refresh also has the owner re-check its retirement
    /// (`MaintainWake`) before the hint; a due-scan entry does not.
    pub maintain: bool,
}

/// The body of `POST /peer/wake`.
#[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct WakePush {
    pub entries: Vec<PushedEntry>,
}

/// The due entries of one live owner, and where its lease says to reach it.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OwnerGroup {
    pub(crate) addr: String,
    pub(crate) entries: Vec<(String, i64)>,
}

/// The due entries the elected waker found an owner for, sorted by what to do
/// with them.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Probed {
    /// Hinted by this node: no owner, a dead owner, this node, or a failed read.
    pub(crate) local: Vec<(String, i64)>,
    /// Owned by another live node: pushed to it, by owner.
    pub(crate) pushed: BTreeMap<String, OwnerGroup>,
}

/// The entries the boot-time scan hints for itself. A live foreign owner's
/// entries are left to its own boot scan and to the elected waker, which
/// pushes them to that owner.
pub async fn elected_due(
    ownership: &BucketOwnership,
    node: &str,
    live: &LiveNodes,
    due: Vec<(String, i64)>,
) -> Vec<(String, i64)> {
    probe_entries(ownership, node, live, due).await.local
}

/// The elected waker's pass over a set of due entries.
///
/// One owner read per entry, without an activation permit, decides through
/// `elected_hint_needed` against the node leases the same tick's dead-node
/// scan read. Before this pass existed, every due entry in the fleet went
/// through the core's route on every node: an owner read under a permit,
/// then `Remote`, for cells whose live owners were about to fire the alarm
/// themselves. A live foreign owner is not dropped, though: no other node
/// scans, so a dormant cell of its own would never wake. Its entries come back
/// grouped for a push. A read that fails stays local, because the core's own
/// resolution is the authority and an entry dropped here on a transient
/// error would wait a whole tick.
///
/// A persisted local owner stays local even when this process has lost its
/// in-memory cell: an `Owned` hint alone cannot recover an unknown cell after
/// a same-name restart.
pub(crate) async fn probe_entries(
    ownership: &BucketOwnership,
    node: &str,
    live: &LiveNodes,
    entries: Vec<(String, i64)>,
) -> Probed {
    let probed: Vec<(
        String,
        i64,
        anyhow::Result<Option<celld_logic::OwnerRecord>>,
    )> = stream::iter(entries)
        .map(|(cell, entry_ms)| async move {
            let owner = ownership.read_owner(&cell).await;
            (cell, entry_ms, owner)
        })
        .buffer_unordered(ELECTED_PROBE_CONCURRENCY)
        .collect()
        .await;
    let mut failed = 0usize;
    let mut sorted = Probed::default();
    for (cell, entry_ms, owner) in probed {
        let owner = match owner {
            Ok(record) => record.and_then(|record| record.node),
            Err(_) => {
                failed += 1;
                None
            }
        };
        let live_owner = owner
            .as_deref()
            .and_then(|name| Some((name, live.get(name)?)));
        match (
            elected_hint_needed(owner.as_deref(), node, live_owner.is_some()),
            live_owner,
        ) {
            (false, Some((owner, addr))) => sorted
                .pushed
                .entry(owner.to_string())
                .or_insert_with(|| OwnerGroup {
                    addr: addr.clone(),
                    entries: Vec::new(),
                })
                .entries
                .push((cell, entry_ms)),
            _ => sorted.local.push((cell, entry_ms)),
        }
    }
    if failed > 0 {
        warn!(
            failed,
            "wake probe owner reads failed; this node hints those entries"
        );
    }
    sorted.local.sort();
    for group in sorted.pushed.values_mut() {
        group.entries.sort();
    }
    sorted
}

/// How the waker reaches a live owner: this node's peer client and signer.
#[derive(Clone)]
struct PeerLink {
    http: reqwest::Client,
    auth: Arc<PeerAuth>,
}

/// The requests that carry one owner's entries, at most `PUSH_ENTRIES_MAX` each.
fn push_bodies(entries: &[(String, i64)], maintain: bool) -> impl Iterator<Item = WakePush> + '_ {
    entries.chunks(PUSH_ENTRIES_MAX).map(move |chunk| WakePush {
        entries: chunk
            .iter()
            .map(|(cell, entry_ms)| PushedEntry {
                cell: cell.clone(),
                entry_ms: *entry_ms,
                maintain,
            })
            .collect(),
    })
}

/// Push each live owner its entries: one signed request per owner per pass
/// (more only past `PUSH_ENTRIES_MAX`), `PUSH_CONCURRENCY` owners at once.
///
/// A failed push only warns, once for the owner. Nothing is lost: the entry is
/// still due or unretired, so the next tick pushes it again, and if the owner
/// has died its lease expires and the waker hints the entry itself as an
/// orphan. The timeout is one tick, so a frozen owner cannot hold the waker
/// past the next election.
///
/// ponytail: one attempt per owner per tick, no backoff and no state between
/// ticks. An owner that stays unreachable warns every tick until its lease
/// expires (a node lease is ten seconds by default). Add per-owner backoff if
/// that noise ever matters.
async fn push_to_owners(
    link: &PeerLink,
    pushed: BTreeMap<String, OwnerGroup>,
    maintain: bool,
    tick_ms: u64,
) {
    let timeout = Duration::from_millis(tick_ms);
    stream::iter(pushed)
        .for_each_concurrent(PUSH_CONCURRENCY, |(owner, group)| async move {
            if let Err(error) = push_one(link, &owner, &group, maintain, timeout).await {
                warn!(
                    %owner, %error, entries = group.entries.len(),
                    "wake push failed; the next tick pushes it again"
                );
            }
        })
        .await;
}

async fn push_one(
    link: &PeerLink,
    owner: &str,
    group: &OwnerGroup,
    maintain: bool,
    timeout: Duration,
) -> anyhow::Result<()> {
    const PATH: &str = "/peer/wake";
    for push in push_bodies(&group.entries, maintain) {
        let body = serde_json::to_vec(&push)?;
        let request = link.http.post(format!("http://{}{PATH}", group.addr));
        let response = link
            .auth
            .sign(request.timeout(timeout), "POST", PATH, &body, owner)?
            .body(body)
            .send()
            .await?;
        anyhow::ensure!(
            response.status() == reqwest::StatusCode::OK,
            "{owner} answered HTTP {}",
            response.status()
        );
        crate::peer_auth::validate_response(response.headers())?;
    }
    Ok(())
}

/// One due entry as the Actor takes it: a refresh entry first re-checks its
/// retirement, then the hint. `false` once the Actor has stopped.
///
/// The scope is `Fleet`, not `Owned`. A probe read the owner record and found
/// this node in it (or no live owner), and `Fleet` also admits a persisted
/// local owner absent from RAM, such as a cell from an ownership-only handoff
/// or a same-name restart. An `Owned` hint ignores a cell the map does not
/// already hold dormant. A `Fleet` hint for a cell that is no longer ours
/// resolves to its owner and takes nothing.
fn send_hint(
    actor: &tokio::sync::mpsc::UnboundedSender<crate::actor::Message>,
    cell: String,
    entry_ms: i64,
    maintain: bool,
) -> bool {
    if maintain
        && actor
            .send(crate::actor::Message::MaintainWake { cell: cell.clone() })
            .is_err()
    {
        return false;
    }
    actor
        .send(crate::actor::Message::WakeHint {
            cell,
            entry_ms,
            scope: celld_logic::WakeHintScope::Fleet,
        })
        .is_ok()
}

/// Take a push from the elected waker exactly as this node's own scan used to
/// take its entries. `false` once the Actor has stopped.
///
/// A name the wake index would never list is skipped: this route is a second
/// door to the same hint, and the name becomes a storage key.
pub fn deliver_push(
    actor: &tokio::sync::mpsc::UnboundedSender<crate::actor::Message>,
    push: WakePush,
) -> bool {
    for entry in push.entries {
        if celld_logic::cell::valid_cell_scope(&entry.cell)
            && !send_hint(actor, entry.cell, entry.entry_ms, entry.maintain)
        {
            return false;
        }
    }
    true
}

/// Run the periodic wake scan after node authority and the boot scan complete.
/// The interval, election, store reads, and Actor hints stay in one path so a
/// caller cannot run a scan without delivering the entries it discovers.
pub async fn run_periodic(
    bucket: Bucket,
    ownership: Arc<BucketOwnership>,
    node: String,
    actor: tokio::sync::mpsc::UnboundedSender<crate::actor::Message>,
    tick_ms: u64,
    http: reqwest::Client,
    auth: Arc<PeerAuth>,
) {
    let link = PeerLink { http, auth };
    // One election per tick. The due loop takes it and publishes the live
    // nodes it saw; cleanup runs once per publication. Each loop used to read
    // the waker lease itself, and cleanup listed a page before it knew whether
    // it was elected. Now a node that is not elected reads the lease once a
    // tick and lists nothing. A watch rather than a timestamp: cleanup acts on
    // an election in the tick that took it, never on a stale one, and a
    // cleanup pass that overruns a tick picks up the latest and skips the rest.
    let (elected, election) = tokio::sync::watch::channel(LiveNodes::new());
    tokio::join!(
        run_due_scans(
            bucket.clone(),
            ownership.clone(),
            node.clone(),
            actor.clone(),
            tick_ms,
            link.clone(),
            elected,
        ),
        run_cleanup(bucket, ownership, node, actor, tick_ms, link, election),
    );
}

async fn run_cleanup(
    bucket: Bucket,
    ownership: Arc<BucketOwnership>,
    node: String,
    actor: tokio::sync::mpsc::UnboundedSender<crate::actor::Message>,
    tick_ms: u64,
    link: PeerLink,
    mut election: tokio::sync::watch::Receiver<LiveNodes>,
) {
    // ponytail: one cursor, held only by the waker, so a failover restarts the
    // sweep at the head of the index. A sweep takes index size / 128 ticks,
    // as it did on each node before. Raise the page size if that is too slow.
    let mut collector = Collector::default();
    // Woken by an election, so this node lists nothing unless it holds the
    // waker role. The sender is gone once the due loop has returned.
    while election.changed().await.is_ok() {
        let live = election.borrow_and_update().clone();
        let refresh = match collector.pass(&bucket).await {
            Ok(refresh) => refresh,
            Err(error) => {
                warn!(%error, "wake cleanup page failed");
                continue;
            }
        };
        let probed = probe_entries(&ownership, &node, &live, refresh).await;
        for (cell, entry_ms) in probed.local {
            if !send_hint(&actor, cell, entry_ms, true) {
                return;
            }
        }
        push_to_owners(&link, probed.pushed, true, tick_ms).await;
        if actor.is_closed() {
            return;
        }
    }
}

async fn run_due_scans(
    bucket: Bucket,
    ownership: Arc<BucketOwnership>,
    node: String,
    actor: tokio::sync::mpsc::UnboundedSender<crate::actor::Message>,
    tick_ms: u64,
    link: PeerLink,
    elected: tokio::sync::watch::Sender<LiveNodes>,
) {
    let mut tick = crate::asyncrt::interval(Duration::from_millis(tick_ms));
    tick.set_missed_tick_behavior(crate::asyncrt::MissedTickBehavior::Delay);
    tick.tick().await;
    let mut dead_node_gc = crate::dead_node_gc::DeadNodeGc::default();
    loop {
        tick.tick().await;
        if actor.is_closed() {
            return;
        }
        // Only the node that holds the waker role lists. A tick without the
        // role is one read of the lease, so the bucket cost of alarm wakes no
        // longer grows with the node count.
        //
        // ponytail: the role is renewed at the start of the tick and inside
        // the GC pass only. A tick whose scan, probes and pushes outrun the
        // three-tick lease lets the role lapse, and another node takes it;
        // concurrent wakers are harmless. Renew between phases if that happens.
        let Some(live) = dead_node_gc.run_elected_pass(&bucket, &node, tick_ms).await else {
            continue;
        };
        elected.send_replace(live.clone());
        let due = due_scan(&bucket, crate::asyncrt::wall_ms()).await;
        let probed = probe_entries(&ownership, &node, &live, due).await;
        for (cell, entry_ms) in probed.local {
            if !send_hint(&actor, cell, entry_ms, false) {
                return;
            }
        }
        push_to_owners(&link, probed.pushed, false, tick_ms).await;
    }
}

#[derive(serde::Deserialize)]
struct WakerRoleLease {
    node: String,
    expires_ms: i64,
}

async fn same_node_still_holds_waker(bucket: &Bucket, node: &str) -> bool {
    let Ok(Some((bytes, _))) = bucket.get("wake/waker.json").await else {
        return false;
    };
    let Ok(lease) = serde_json::from_slice::<WakerRoleLease>(&bytes) else {
        return false;
    };
    lease.node == node && lease.expires_ms > crate::asyncrt::wall_ms()
}

/// Advisory waker-role lease: one holder per fleet to avoid N nodes polling.
/// Correctness never depends on it — concurrent wakers race activation CAS
/// harmlessly — so a caller skips the tick when another node holds the role.
pub async fn try_hold_waker(bucket: &Bucket, node: &str, now_ms: i64, ttl_ms: i64) -> bool {
    const KEY: &str = "wake/waker.json";
    let body =
        |expires: i64| format!("{{\"node\":{node:?},\"expires_ms\":{expires}}}").into_bytes();
    let token = match bucket.get(KEY).await {
        // absent (or unreadable): claim if absent
        Ok(None) | Err(_) => None,
        Ok(Some((bytes, etag))) => {
            let lease = serde_json::from_slice::<WakerRoleLease>(&bytes).ok();
            let held_by_us = lease.as_ref().is_some_and(|lease| lease.node == node);
            let expires = lease.map_or(0, |lease| lease.expires_ms);
            if !celld_logic::wake::waker_may_claim(held_by_us, expires, now_ms) {
                return false;
            }
            Some(etag)
        }
    };
    match bucket
        .put_cas(KEY, body(now_ms + ttl_ms), token.as_deref())
        .await
    {
        Ok(Some(_)) => true,
        // The cleanup loop used to renew beside the due-scan loop, and a clean
        // CAS conflict could be their overlap. Keep checking the winner before
        // cancelling a GC pass and repeating its fleet-wide LIST.
        Ok(None) => same_node_still_holds_waker(bucket, node).await,
        Err(_) => false,
    }
}

/// Per-node publication and retirement caches. Durable inventory belongs to
/// LIST; an error or cancellation leaves no reservation for a later task.
#[derive(Default)]
pub struct WakeFlusher {
    pub core: Mutex<WakeCore>,
    settled: Mutex<BTreeMap<String, PublicationId>>,
    #[cfg(all(test, celld_internal_tests))]
    reuse_identity: std::sync::atomic::AtomicBool,
}

/// The alarm source and its replication proof belong to the same host.
/// Keeping this boundary together prevents cleanup from proving a position
/// through a replacement runtime that did not produce the snapshot.
pub(crate) trait AlarmHost: Sync {
    fn alarm_snapshot(&self, cell: &str) -> anyhow::Result<AlarmSnapshot>;
    fn prove_alarm<'a>(
        &'a self,
        cell: &'a str,
        epoch: u64,
        position: u64,
    ) -> impl std::future::Future<Output = anyhow::Result<(u64, celld_logic::ProofSource)>> + Send + 'a;
}

impl AlarmHost for crate::cell_host::CellHost {
    fn alarm_snapshot(&self, cell: &str) -> anyhow::Result<AlarmSnapshot> {
        Ok(self.with_alarm(cell, |alarm, _| alarm))
    }

    fn prove_alarm<'a>(
        &'a self,
        cell: &'a str,
        epoch: u64,
        position: u64,
    ) -> impl std::future::Future<Output = anyhow::Result<(u64, celld_logic::ProofSource)>> + Send + 'a
    {
        self.await_durable(cell, epoch, position)
    }
}

impl WakeFlusher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish exactly the identity captured at the committed source. An old
    /// PUT can finish after retirement, but can only recreate an obsolete key.
    pub(crate) async fn publish(
        &self,
        bucket: &Bucket,
        cell: &str,
        alarm: AlarmSnapshot,
    ) -> anyhow::Result<()> {
        if alarm.at_ms().is_none() {
            return Ok(());
        }
        anyhow::ensure!(
            alarm.source().is_some_and(|source| source.id.epoch > 0)
                && alarm.at_ms().is_some_and(|at| at >= 0),
            "armed alarm has no committed wake identity"
        );
        let publication = self.core.lock().unwrap().publication(cell, alarm);
        let Some(publication) = publication else {
            return Ok(());
        };
        #[cfg(all(test, celld_internal_tests))]
        let publication = if self
            .reuse_identity
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            celld_logic::wake::Publication {
                key: celld_logic::wake::entry_key(
                    alarm.at_ms().unwrap(),
                    cell,
                    PublicationId {
                        epoch: alarm.source().unwrap().id.epoch,
                        sequence: 1,
                    },
                ),
                alarm,
            }
        } else {
            publication
        };
        let body = serde_json::to_vec(&serde_json::json!({
            "format": 2, "cell": cell, "due_ms": alarm.at_ms(),
            "identity": alarm.source().unwrap().id.to_string(),
        }))?;
        bucket.put(&publication.key, body).await?;
        self.core.lock().unwrap().confirm(cell, alarm);
        Ok(())
    }

    /// Bind the durability wait to the original writer and source snapshot.
    /// The equality check rejects queued observations from a replaced runtime.
    /// A subsequent arm is safe: this certificate cannot retire its larger ID.
    pub(crate) async fn maintain(
        &self,
        bucket: &Bucket,
        cell: &str,
        alarm: AlarmSnapshot,
        host: &impl AlarmHost,
        ownership: &crate::actor::Ownership,
        node: &str,
    ) -> anyhow::Result<()> {
        let Some(source) = alarm.source() else {
            tracing::debug!(%cell, "wake maintenance has no resident source; awaiting discovery");
            return Ok(());
        };
        if self
            .settled
            .lock()
            .unwrap()
            .get(cell)
            .is_some_and(|id| *id >= source.id)
        {
            return Ok(());
        }
        if !host.alarm_snapshot(cell)?.same_installation(alarm) {
            return Ok(());
        }
        self.publish(bucket, cell, alarm).await?;
        let (durable, _) = host
            .prove_alarm(cell, source.id.epoch, source.position)
            .await?;
        anyhow::ensure!(durable >= source.position, "wake retirement proof is short");
        let owner = ownership
            .read_owner(cell)
            .await
            .map_err(|_| anyhow::anyhow!("wake retirement ownership read failed"))?;
        anyhow::ensure!(
            owner.is_some_and(|owner| {
                owner.epoch == source.id.epoch && owner.node.as_deref() == Some(node)
            }),
            "wake retirement lost writer authority"
        );
        let proof = RetirementProof(Retirement {
            id: source.id,
            at_ms: alarm.at_ms(),
        });
        let previous = publish_retirement(bucket, cell, &proof).await?;
        // Normal churn must not wait behind the fleet's bounded LIST cursor.
        // The same durable proof authorizes these exact immutable names. This
        // advisory batch can be lost or cancelled without blocking later arms.
        let mut obsolete: BTreeSet<_> = self
            .core
            .lock()
            .unwrap()
            .take_retired(cell, proof.0)
            .into_iter()
            .collect();
        if let Some(previous) = previous {
            if let Some(at_ms) = previous.at_ms.filter(|_| proof.0.retires(previous.id)) {
                obsolete.insert(celld_logic::wake::entry_key(at_ms, cell, previous.id));
            }
        }
        for key in obsolete {
            if let Err(error) = bucket.delete(&key).await {
                warn!(%cell, %key, %error, "wake eager entry delete failed; collector will retry");
            }
        }
        let mut settled = self.settled.lock().unwrap();
        let id = settled.entry(cell.to_string()).or_insert(source.id);
        *id = (*id).max(source.id);
        Ok(())
    }

    pub fn covered(&self, cell: &str, alarm: AlarmSnapshot) -> bool {
        self.core.lock().unwrap().covered(cell, alarm)
    }

    pub fn forget(&self, cell: &str) {
        self.core.lock().unwrap().forget(cell);
        self.settled.lock().unwrap().remove(cell);
    }

    // Reuse a retired name at the actual PUT boundary, so the independent
    // store observer must catch a later DELETE destroying acknowledged work.
    #[cfg(all(test, celld_internal_tests))]
    pub(crate) fn reuse_publication_identity_for_tooth(&self) {
        self.reuse_identity
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

// Only `maintain` constructs this after all three remote proofs. A bare
// boolean from an alarm notification cannot authorize destructive cleanup.
struct RetirementProof(Retirement);

fn retirement_key(cell: &str) -> String {
    format!("wake/retired/{cell}.json")
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RetirementRecord {
    format: u8,
    identity: String,
    at_ms: Option<i64>,
}

impl RetirementRecord {
    fn decode(bytes: &[u8]) -> anyhow::Result<Retirement> {
        let record: Self = serde_json::from_slice(bytes)?;
        anyhow::ensure!(record.format == 2, "unsupported wake retirement format");
        let id = PublicationId::parse(&record.identity)
            .ok_or_else(|| anyhow::anyhow!("invalid wake retirement identity"))?;
        anyhow::ensure!(
            id.epoch > 0 && record.at_ms.is_none_or(|at| at >= 0),
            "invalid wake retirement record"
        );
        Ok(Retirement {
            id,
            at_ms: record.at_ms,
        })
    }
}

async fn publish_retirement(
    bucket: &Bucket,
    cell: &str,
    proof: &RetirementProof,
) -> anyhow::Result<Option<Retirement>> {
    let retirement = proof.0;
    let key = retirement_key(cell);
    let body = serde_json::to_vec(&RetirementRecord {
        format: 2,
        identity: retirement.id.to_string(),
        at_ms: retirement.at_ms,
    })?;
    // Each retry retains the same source identity. An ambiguous CAS can have
    // applied; reading it back is sufficient, without reassigning its token
    // to current intent or falling back to an unconditional overwrite.
    for _ in 0..3 {
        let observed = bucket.get(&key).await?;
        let previous = observed
            .as_ref()
            .map(|(bytes, _)| RetirementRecord::decode(bytes))
            .transpose()?;
        if let Some(old) = previous {
            if old.id == retirement.id {
                anyhow::ensure!(
                    old == retirement,
                    "wake identity reused for a different alarm"
                );
                return Ok(previous);
            }
            if old.id > retirement.id {
                return Ok(previous);
            }
        }
        let token = observed.as_ref().map(|(_, token)| token.as_str());
        if bucket.put_cas(&key, body.clone(), token).await?.is_some() {
            return Ok(previous);
        }
    }
    anyhow::bail!("wake retirement CAS remained contended")
}

/// One bounded pass through the provider's opaque LIST continuation token.
/// Keep the next token only on success, and wrap after the last page: a late
/// PUT can appear behind any cursor, even after a successful DELETE.
#[derive(Default)]
pub(crate) struct Collector {
    cursor: Option<String>,
}

impl Collector {
    pub(crate) async fn pass(&mut self, bucket: &Bucket) -> anyhow::Result<Vec<(String, i64)>> {
        const PAGE_SIZE: usize = 128;
        const CONCURRENCY: usize = 8;
        let now_ms = crate::asyncrt::wall_ms();
        let page = match bucket
            .objects_page(ENTRY_PREFIX, self.cursor.clone(), PAGE_SIZE)
            .await
        {
            Ok(page) => page,
            Err(error) => {
                // Provider tokens can expire. Retrying the same invalid token
                // forever would make a finite interruption strand all cleanup.
                self.cursor = None;
                return Err(error);
            }
        };
        let mut cells: BTreeMap<String, Vec<(String, celld_logic::wake::ListedEntry)>> =
            BTreeMap::new();
        for object in page.objects {
            let key = object.location.to_string();
            if let Some(entry) = parse_entry_key(&key) {
                cells
                    .entry(entry.cell.clone())
                    .or_default()
                    .push((key, entry));
            }
        }
        let mut pending = stream::iter(cells)
            .map(|(cell, entries)| async move {
                let record = match bucket.get(&retirement_key(&cell)).await {
                    Ok(Some((bytes, _))) => match RetirementRecord::decode(&bytes) {
                        Ok(record) => Some(record),
                        Err(error) => {
                            warn!(%cell, %error, "wake retirement record rejected");
                            return None;
                        }
                    },
                    Ok(None) => None,
                    Err(error) => {
                        warn!(%cell, %error, "wake retirement read failed");
                        return None;
                    }
                };
                let mut refresh = None;
                for (key, entry) in entries {
                    if record.is_some_and(|record| record.retires(entry.id)) {
                        // No mutation completion changes eligibility. Failure,
                        // timeout, or task cancellation is retried by another LIST.
                        if let Err(error) = bucket.delete(&key).await {
                            warn!(%cell, %key, %error, "wake obsolete entry delete failed");
                        }
                    } else if entry.minute_ms <= now_ms {
                        // Even a matching certificate can trail an unreported
                        // consume. A bounded truth refresh also repairs that cut
                        // after the former owner and its callback inventory vanish.
                        // Future entries wait until their minute: refreshing them
                        // now would reactivate every sleeping cell on each sweep.
                        refresh = Some(refresh.unwrap_or(entry.minute_ms).min(entry.minute_ms));
                    }
                }
                refresh.map(|minute| (cell, minute))
            })
            .buffer_unordered(CONCURRENCY);
        let mut refresh = Vec::new();
        while let Some(cell) = pending.next().await {
            if let Some(cell) = cell {
                refresh.push(cell);
            }
        }
        self.cursor = page.page_token;
        Ok(refresh)
    }
}

// Tests drive a real socket and wall-clock ticks on their own runtime, outside
// the Actor execution domain.
#[allow(clippy::disallowed_methods)]
#[cfg(test)]
#[path = "unit_tests/wake/tests.rs"]
mod tests;
