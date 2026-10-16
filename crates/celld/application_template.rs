// Snapshot templates are staged on the host filesystem and booted as real
// Firecracker VMs, outside the Actor execution domain.
#![allow(clippy::disallowed_methods)]

//! Snapshot templates for application VMs (T26). A template is an unassigned
//! guest (kernel up, `celld-guest --template` waiting for a claim) captured
//! after a cold boot; every application VM restores from one. Templates are
//! node-local, private to this pool and gone with it. Also the jailed VMM
//! processes that both template builds and VM starts run, and the restored VMs
//! that wait for a claim (`startup_pool`). A template carries two placeholder
//! read-only drives, `handler` and `shared`, which a claim points at the
//! handler's image and the shared Pyodide runtime.

use crate::application_template_agent::{AGENT_PORT, HELLO};
use crate::application_vm::{
    copy_pinned, drain, kill_and_wait, request, trusted_executable, LaunchOptions,
};
use crate::applications::content_digest;
use anyhow::{bail, ensure, Context, Result};
use std::collections::HashMap;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::process::{Child, Command};
use tokio::time::{timeout, Instant};
use tokio_util::task::AbortOnDropHandle;

pub(crate) mod cgroup;

const TEMPLATE_FILES: [&str; 3] = ["state", "memory", "runtime.ext4"];

/// Snapshot device state changes require a distinct normal and Pyodide cache key.
const MEMORY_DEVICE_PROFILE: &str = "virtio-balloon-reporting-v1";

fn memory_profile_key(identity: &str) -> String {
    let profile = if reporting_enabled() {
        MEMORY_DEVICE_PROFILE
    } else {
        "virtio-balloon-reporting-off-v1"
    };
    content_digest(format!("{profile} {identity}").as_bytes())
}

fn reporting_enabled() -> bool {
    #[cfg(test)]
    if std::env::var_os("CELLD_APPLICATION_MEMORY_REPORTING_DISABLED").is_some() {
        return false;
    }
    true
}

fn balloon_config() -> serde_json::Value {
    serde_json::json!({
        "amount_mib": 0,
        "deflate_on_oom": false,
        "stats_polling_interval_s": 0,
        "free_page_reporting": reporting_enabled(),
        "free_page_hinting": false,
    })
}

/// One node's templates, keyed by everything a snapshot embeds: runtime image,
/// VM size, kernel, Firecracker version and memory device profile. The directory
/// also holds each VM's workspace, so files hard-link into a jail instead of copying.
/// Restored VMs that nobody has claimed yet wait here too, per template.
pub(crate) struct Templates {
    dir: tempfile::TempDir,
    memory: Option<Arc<cgroup::StableParent>>,
    /// Held while this node lives; a node killed without cleanup releases it,
    /// which is how a later node knows the directory is leaked.
    _lock: std::fs::File,
    built: Mutex<HashMap<String, Entry>>,
    // ponytail: the first Firecracker seen; one executable per node.
    firecracker_version: tokio::sync::OnceCell<String>,
    idle: Mutex<Idles>,
    hits: AtomicU64,
    misses: AtomicU64,
    /// The shared Pyodide runtime drives, by key; one build at a time.
    pyodide: tokio::sync::Mutex<HashMap<String, PyodideEntry>>,
    startup: AtomicBool,
    ready: AtomicBool,
    /// Startup CPU boost slots, in vCPUs (`Jail::boost`).
    boosts: Arc<tokio::sync::Semaphore>,
}

/// What one Pyodide runtime drive has needed so far.
#[derive(Default)]
struct PyodideEntry {
    drive: Option<PathBuf>,
    /// A failed build is not retried before this; launches fail until then.
    retry: Option<Backoff>,
    error: String,
}

/// What a template key has needed so far.
#[derive(Default)]
struct Entry {
    /// Startup captures every class; only requested classes get idle VMs.
    needed: bool,
    /// Held while a build runs: concurrent first launches wait for that one.
    build: Arc<tokio::sync::Mutex<()>>,
    template: Option<Arc<Template>>,
    /// A failed build is not retried before this; launches boot cold until then.
    build_retry: Option<Backoff>,
    /// Likewise for refilling the pool from this template.
    fill_retry: Option<Backoff>,
}

/// The wait after a failure, doubling with each further failure.
#[derive(Clone, Copy)]
struct Backoff {
    until: Instant,
    delay: Duration,
}

impl Backoff {
    const FIRST: Duration = Duration::from_secs(2);
    const LONGEST: Duration = Duration::from_secs(120);

    fn after(previous: Option<Backoff>) -> Self {
        let delay = previous.map_or(Self::FIRST, |previous| {
            (previous.delay * 2).min(Self::LONGEST)
        });
        Self {
            until: Instant::now() + delay,
            delay,
        }
    }

    fn pending(backoff: Option<Backoff>) -> bool {
        backoff.is_some_and(|backoff| Instant::now() < backoff.until)
    }
}

/// Restored VMs waiting for a claim, and what still holds capacity for them.
#[derive(Default)]
struct Idles {
    /// By template key, oldest first.
    ready: Vec<(String, Idle)>,
    /// The template keys of restores in flight.
    preparing: Vec<(String, u32)>,
    /// Evicted or broken VMs being killed and reaped.
    reaping: Vec<u32>,
    /// VMs whose reap could not be proved; their capacity stays charged.
    stuck: Vec<u32>,
}

pub(crate) struct Template {
    dir: PathBuf,
    pub(crate) key: String,
    pub(crate) runtime_sha256: String,
    pub(crate) vcpus: u8,
    pub(crate) memory_mib: u32,
    pub(crate) cpu_quota: u32,
}

impl Template {
    /// The restore needs only these files; the kernel is inside the snapshot.
    pub(crate) fn link_into(&self, root: &Path) -> Result<()> {
        for name in TEMPLATE_FILES {
            std::fs::hard_link(self.dir.join(name), root.join(name))
                .with_context(|| format!("link template {name}"))?;
        }
        Ok(())
    }
}

/// Each pool directory is `celld-vms-*` under the temporary directory.
const DIR_PREFIX: &str = "celld-vms-";

/// The drive files every jail holds before a claim swaps them, as hard links
/// of one placeholder in the pool directory.
const PLACEHOLDER: &str = "placeholder.img";
pub(crate) const HANDLER_IMAGE: &str = "handler.img";
pub(crate) const SHARED_IMAGE: &str = "shared.img";

impl Templates {
    // The jail's vsock sockets must fit a 108 byte path: keep names short.
    pub(crate) fn new() -> Result<Self> {
        let dir = tempfile::Builder::new().prefix(DIR_PREFIX).tempdir()?;
        std::fs::create_dir(dir.path().join("templates"))?;
        let lock = std::fs::File::create(dir.path().join("lock"))?;
        ensure!(lock_file(&lock), "lock pool directory");
        sweep_leaked(&std::env::temp_dir());
        // One block of zeros: any size change at the claim is then a real one.
        let placeholder = dir.path().join(PLACEHOLDER);
        std::fs::write(&placeholder, [0; 4096])?;
        std::fs::set_permissions(&placeholder, std::fs::Permissions::from_mode(0o444))?;
        Ok(Self {
            dir,
            memory: None,
            _lock: lock,
            built: Default::default(),
            firecracker_version: Default::default(),
            idle: Default::default(),
            hits: Default::default(),
            misses: Default::default(),
            pyodide: Default::default(),
            startup: Default::default(),
            ready: Default::default(),
            // ponytail: half the host's CPUs may run boosted startups; make it
            // node config if a host needs another split.
            boosts: Arc::new(tokio::sync::Semaphore::new(
                std::thread::available_parallelism().map_or(1, |cpus| (cpus.get() / 2).max(1)),
            )),
        })
    }

    pub(crate) fn initialize_memory(&mut self, budget: cgroup::Budget) -> Result<()> {
        ensure!(
            self.memory.is_none(),
            "application memory guard already initialized"
        );
        let name = self
            .dir
            .path()
            .file_name()
            .context("pool directory name")?
            .to_str()
            .context("pool directory name is not UTF-8")?;
        self.memory = Some(cgroup::StableParent::initialize(name, budget)?);
        Ok(())
    }

    pub(crate) fn memory_sample(&self) -> Result<Option<cgroup::MemorySample>> {
        self.memory
            .as_ref()
            .map(|parent| parent.sample())
            .transpose()
    }

    /// Startup builds the shared drive, keyed by Pyodide release, runtime image
    /// and memory device profile. Standalone VM checks can retry after backoff.
    pub(crate) async fn build_pyodide(
        &self,
        options: &LaunchOptions,
        app: &crate::actor::AppHandle,
    ) -> Result<PathBuf> {
        let key = memory_profile_key(&format!(
            "pyodide {} {}",
            crate::deploy::PYODIDE_VERSION,
            options.runtime.sha256,
        ));
        let mut entries = self.pyodide.lock().await;
        let entry = entries.entry(key.clone()).or_default();
        if let Some(drive) = &entry.drive {
            return Ok(drive.clone());
        }
        if Backoff::pending(entry.retry) {
            bail!(
                "the shared Pyodide runtime failed to build: {}",
                entry.error
            );
        }
        let previous = entry.retry;
        let began = Instant::now();
        let scratch = self.dir.path().join(format!("pyodide-{}", &key[..16]));
        let target = self.dir.path().join(format!("pyodide-{}.img", &key[..16]));
        let built = async {
            std::fs::create_dir(&scratch)?;
            crate::application_pyodide::build(options, app, &scratch, &target).await?;
            // Root-owned and read-only: the VMM reads it, no VM can alter it.
            std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o444))?;
            anyhow::Ok(())
        }
        .await;
        let _ = std::fs::remove_dir_all(&scratch);
        let entry = entries.get_mut(&key).expect("entry created above");
        match built {
            Ok(()) => {
                entry.drive = Some(target.clone());
                entry.retry = None;
                tracing::info!(
                    event = "pyodide_runtime_built",
                    key = &key[..12],
                    bytes = std::fs::metadata(&target).ok().map(|meta| meta.len()),
                    ms = began.elapsed().as_secs_f64() * 1000.0,
                    "shared Pyodide runtime drive built"
                );
                Ok(target)
            }
            Err(error) => {
                tracing::error!(event = "pyodide_runtime_failed", error = %format!("{error:#}"));
                entry.retry = Some(Backoff::after(previous));
                entry.error = format!("{error:#}");
                let _ = std::fs::remove_file(&target);
                Err(error)
            }
        }
    }

    /// Startup captures this approved runtime/class. Standalone VM checks can
    /// retry a failed capture after its backoff; production lookup never builds.
    pub(crate) async fn build_template(&self, options: &LaunchOptions) -> Result<Arc<Template>> {
        let key = self.key(options).await?;
        let build = {
            let mut built = self.built.lock().expect("templates poisoned");
            let entry = built.entry(key.clone()).or_default();
            if let Some(template) = &entry.template {
                return Ok(template.clone());
            }
            entry.build.clone()
        };
        let _building = build.lock().await;
        let previous = {
            let mut built = self.built.lock().expect("templates poisoned");
            let entry = built.entry(key.clone()).or_default();
            if let Some(template) = &entry.template {
                return Ok(template.clone());
            }
            if Backoff::pending(entry.build_retry) {
                bail!("startup template unavailable during retry backoff");
            }
            entry.build_retry
        };
        let built = self.build(options, &key).await;
        let mut map = self.built.lock().expect("templates poisoned");
        let entry = map.entry(key).or_default();
        match built {
            Ok(template) => {
                entry.build_retry = None;
                entry.template = Some(Arc::new(template));
                Ok(entry.template.clone().expect("published template"))
            }
            Err(error) => {
                tracing::error!(event = "vm_template_failed", error = %format!("{error:#}"));
                entry.build_retry = Some(Backoff::after(previous));
                Err(error)
            }
        }
    }

    pub(crate) fn begin_startup(&self) {
        self.startup.store(true, Ordering::Release);
    }

    pub(crate) fn finish_startup(&self) {
        self.ready.store(true, Ordering::Release);
    }

    pub(crate) fn ready(&self) -> bool {
        !self.startup.load(Ordering::Acquire) || self.ready.load(Ordering::Acquire)
    }

    pub(crate) fn production(&self) -> bool {
        self.startup.load(Ordering::Acquire)
    }

    /// Production launches only read caches prepared before node readiness.
    pub(crate) async fn get(&self, options: &LaunchOptions) -> Result<Option<Arc<Template>>> {
        let template = if !self.startup.load(Ordering::Acquire) {
            match self.build_template(options).await {
                Ok(template) => template,
                Err(error) if error.is::<crate::application_vm::ReapUnproved>() => {
                    return Err(error)
                }
                Err(_) => return Ok(None),
            }
        } else {
            let key = self.key(options).await?;
            let template = self
                .built
                .lock()
                .expect("templates poisoned")
                .get(&key)
                .and_then(|entry| entry.template.clone());
            let Some(template) = template else {
                return Ok(None);
            };
            template
        };
        self.built
            .lock()
            .expect("templates poisoned")
            .get_mut(&template.key)
            .expect("published template")
            .needed = true;
        Ok(Some(template))
    }

    pub(crate) async fn pyodide(
        &self,
        options: &LaunchOptions,
        app: &crate::actor::AppHandle,
    ) -> Result<PathBuf> {
        if !self.startup.load(Ordering::Acquire) {
            return self.build_pyodide(options, app).await;
        }
        let key = memory_profile_key(&format!(
            "pyodide {} {}",
            crate::deploy::PYODIDE_VERSION,
            options.runtime.sha256,
        ));
        self.pyodide
            .lock()
            .await
            .get(&key)
            .and_then(|entry| entry.drive.clone())
            .context("shared Pyodide runtime was not prepared at node startup")
    }

    /// The snapshot state file of every template built so far.
    #[cfg(test)]
    pub(crate) fn state_files(&self) -> Vec<PathBuf> {
        std::fs::read_dir(self.dir.path().join("templates"))
            .unwrap()
            .map(|template| template.unwrap().path().join("state"))
            .collect()
    }

    /// Drops every template and the workspace directory once the pool has
    /// reaped its VMs; a hosted pool lives in a static and is never dropped.
    pub(crate) async fn discard(&self) -> Result<()> {
        if let Some(parent) = &self.memory {
            parent
                .shutdown()
                .context(crate::application_vm::ReapUnproved)?;
        }
        self.built.lock().expect("templates poisoned").clear();
        let _ = tokio::fs::remove_dir_all(self.dir.path()).await;
        Ok(())
    }

    /// Capacity held by VMs that serve nobody yet: waiting for a claim, being
    /// restored or being reaped.
    pub(crate) fn idle_capacity(&self) -> (u32, u64) {
        let idle = self.idle.lock().expect("idle VMs poisoned");
        let memory = idle
            .ready
            .iter()
            .map(|(_, vm)| vm.memory_mib)
            .chain(idle.preparing.iter().map(|(_, memory)| *memory))
            .chain(idle.reaping.iter().copied())
            .chain(idle.stuck.iter().copied())
            .map(u64::from)
            .sum();
        (
            (idle.ready.len() + idle.preparing.len() + idle.reaping.len() + idle.stuck.len())
                as u32,
            memory,
        )
    }

    /// Idle VMs still waiting, restoring or being reaped, and those whose reap
    /// could not be proved.
    pub(crate) fn winding_down(&self) -> (u32, u32) {
        let idle = self.idle.lock().expect("idle VMs poisoned");
        (
            (idle.ready.len() + idle.preparing.len() + idle.reaping.len()) as u32,
            idle.stuck.len() as u32,
        )
    }

    /// An idle VM restored from this template, which then belongs to the
    /// caller for good; none otherwise. Counts a pool hit or miss.
    pub(crate) fn take(&self, key: &str) -> Option<Idle> {
        let mut idle = self.idle.lock().expect("idle VMs poisoned");
        let taken = idle
            .ready
            .iter()
            .position(|(ready, _)| ready == key)
            .map(|position| idle.ready.remove(position).1);
        let counter = if taken.is_some() {
            &self.hits
        } else {
            &self.misses
        };
        counter.fetch_add(1, Ordering::Relaxed);
        taken
    }

    /// The oldest idle VM, to be killed to make room. It stays charged until
    /// the caller reports its reap with `reaped`.
    pub(crate) fn evict(&self) -> Option<Idle> {
        let mut idle = self.idle.lock().expect("idle VMs poisoned");
        (!idle.ready.is_empty()).then(|| {
            let vm = idle.ready.remove(0).1;
            idle.reaping.push(vm.memory_mib);
            vm
        })
    }

    /// Idle VMs whose VMM exited or whose agent disconnected. Their template
    /// backs off from refilling, so VMs that break at once do not churn.
    pub(crate) fn evict_broken(&self) -> Vec<Idle> {
        let mut built = self.built.lock().expect("templates poisoned");
        let mut idle = self.idle.lock().expect("idle VMs poisoned");
        let mut broken = Vec::new();
        let mut at = 0;
        while at < idle.ready.len() {
            if idle.ready[at].1.broken() {
                let (key, vm) = idle.ready.remove(at);
                if let Some(entry) = built.get_mut(&key) {
                    entry.fill_retry = Some(Backoff::after(entry.fill_retry));
                }
                broken.push(vm);
            } else {
                at += 1;
            }
        }
        idle.reaping.extend(broken.iter().map(|vm| vm.memory_mib));
        broken
    }

    /// Baselines: only the template of `memory_mib` stays needed, and other
    /// idle VMs are evicted, so the next refill is that template's. A later
    /// launch of another class marks its template needed again.
    #[cfg(test)]
    pub(crate) fn focus(&self, memory_mib: u32) -> Vec<Idle> {
        for entry in self.built.lock().expect("templates poisoned").values_mut() {
            entry.needed &= entry
                .template
                .as_ref()
                .is_some_and(|template| template.memory_mib == memory_mib);
        }
        let mut idle = self.idle.lock().expect("idle VMs poisoned");
        let (kept, evicted): (Vec<_>, Vec<_>) = idle
            .ready
            .drain(..)
            .partition(|(_, vm)| vm.memory_mib == memory_mib);
        idle.ready = kept;
        let evicted: Vec<_> = evicted.into_iter().map(|(_, vm)| vm).collect();
        idle.reaping.extend(evicted.iter().map(|vm| vm.memory_mib));
        evicted
    }

    pub(crate) fn evict_all(&self) -> Vec<Idle> {
        let mut idle = self.idle.lock().expect("idle VMs poisoned");
        let all: Vec<_> = idle.ready.drain(..).map(|(_, idle)| idle).collect();
        idle.reaping.extend(all.iter().map(|vm| vm.memory_mib));
        all
    }

    pub(crate) fn reaped(&self, memory: u32, proved: bool) {
        let mut idle = self.idle.lock().expect("idle VMs poisoned");
        let position = idle
            .reaping
            .iter()
            .position(|charged| *charged == memory)
            .expect("reaping VM charged");
        idle.reaping.swap_remove(position);
        if !proved {
            idle.stuck.push(memory);
        }
    }

    /// Starts counting one more restore for a template that holds fewer than
    /// `size` idle or preparing VMs and is not backing off; `None` if none does.
    pub(crate) fn begin_fill(&self, size: u32, available_memory: u64) -> Option<Arc<Template>> {
        let built = self.built.lock().expect("templates poisoned");
        let mut idle = self.idle.lock().expect("idle VMs poisoned");
        let template = built.values().find_map(|entry| {
            if !entry.needed {
                return None;
            }
            let template = entry.template.as_ref()?;
            let held = idle
                .ready
                .iter()
                .filter(|(key, _)| *key == template.key)
                .count()
                + idle
                    .preparing
                    .iter()
                    .filter(|(key, _)| *key == template.key)
                    .count();
            (held < size as usize
                && u64::from(template.memory_mib) <= available_memory
                && !Backoff::pending(entry.fill_retry))
            .then(|| template.clone())
        })?;
        idle.preparing
            .push((template.key.clone(), template.memory_mib));
        Some(template)
    }

    /// A restore begun with `begin_fill` ended: the VM waits for a claim, or
    /// the template backs off from further restores (not at shutdown).
    pub(crate) fn end_fill(&self, key: &str, idle: Option<Idle>, back_off: bool, proved: bool) {
        let mut built = self.built.lock().expect("templates poisoned");
        let mut waiting = self.idle.lock().expect("idle VMs poisoned");
        let position = waiting
            .preparing
            .iter()
            .position(|(preparing, _)| preparing == key);
        let (_, memory) = waiting
            .preparing
            .swap_remove(position.expect("a restore in flight"));
        if !proved {
            waiting.stuck.push(memory);
        }
        if let Some(entry) = built.get_mut(key) {
            entry.fill_retry = match idle {
                Some(_) => None,
                None if back_off => Some(Backoff::after(entry.fill_retry)),
                None => entry.fill_retry,
            };
        }
        if let Some(idle) = idle {
            waiting.ready.push((key.to_owned(), idle));
        }
    }

    /// Hits, misses, idle VMs and the state of every template, for node status.
    pub(crate) fn status(&self) -> serde_json::Value {
        let built = self.built.lock().expect("templates poisoned");
        let idle = self.idle.lock().expect("idle VMs poisoned");
        let templates: Vec<_> = built
            .iter()
            .map(|(key, entry)| {
                let (state, retry) = match (&entry.template, entry.build_retry) {
                    (Some(_), _) => ("ready", None),
                    (None, Some(retry)) if Backoff::pending(Some(retry)) => ("failed", Some(retry)),
                    _ => ("building", None),
                };
                serde_json::json!({
                    "key": &key[..12],
                    "state": state,
                    "vcpus": entry.template.as_ref().map(|template| template.vcpus),
                    "memoryMib": entry.template.as_ref().map(|template| template.memory_mib),
                    "idle": idle.ready.iter().filter(|(ready, _)| ready == key).count(),
                    "preparing": idle.preparing.iter().filter(|(preparing, _)| preparing == key).count(),
                    "retryInSecs": retry.map(|retry| retry.until.saturating_duration_since(Instant::now()).as_secs()),
                })
            })
            .collect();
        serde_json::json!({
            "idle": idle.ready.len(),
            "preparing": idle.preparing.len(),
            "hits": self.hits.load(Ordering::Relaxed),
            "misses": self.misses.load(Ordering::Relaxed),
            "templates": templates,
            "ready": self.ready(),
            "reaping": idle.reaping.len(),
            "stuck": idle.stuck.len(),
        })
    }

    async fn key(&self, options: &LaunchOptions) -> Result<String> {
        let version = self
            .firecracker_version
            .get_or_try_init(|| async {
                trusted_executable(&options.firecracker)?;
                let output = Command::new(&options.firecracker)
                    .arg("--version")
                    .env_clear()
                    .stdin(Stdio::null())
                    .output()
                    .await?;
                ensure!(output.status.success(), "firecracker --version failed");
                Ok(String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .next()
                    .unwrap_or_default()
                    .to_owned())
            })
            .await?;
        Ok(memory_profile_key(&format!(
            "{} {} {} {} {version}",
            options.runtime.sha256, options.vcpus, options.memory_mib, options.kernel.sha256,
        )))
    }

    /// Cold boot in template mode, wait for the agent, pause and capture.
    async fn build(&self, options: &LaunchOptions, key: &str) -> Result<Template> {
        let began = Instant::now();
        let dir = self.dir.path().join("templates").join(key);
        tokio::fs::create_dir(&dir).await?;
        let built = async {
            // The only digest pass over the runtime image: VMs link this copy.
            copy_pinned(options.runtime.clone(), dir.join("runtime.ext4"), 1024 * 1024 * 1024).await?;
            let staged = Staged::new(self, options)?;
            copy_pinned(options.kernel.clone(), staged.root.join("vmlinux"), 128 * 1024 * 1024).await?;
            std::fs::hard_link(dir.join("runtime.ext4"), staged.root.join("runtime.ext4"))?;
            let agent = staged.listen(AGENT_PORT, options)?;
            let mut jail = staged.spawn(options)?;
            let api = jail.root.join("api.sock");
            let captured = async {
                wait_for_api(&api).await?;
                cold_boot(&api, options).await?;
                let (mut hello, _) = agent.accept().await?;
                ensure!(hello.read_u8().await? == HELLO, "template agent hello");
                request(&api, "PATCH", "/vm", serde_json::json!({"state":"Paused"})).await?;
                request(
                    &api,
                    "PUT",
                    "/snapshot/create",
                    serde_json::json!({"snapshot_type":"Full","snapshot_path":"/state","mem_file_path":"/memory"}),
                )
                .await
            };
            let captured = crate::asyncrt::select! {
                result = timeout(Duration::from_secs(180), captured) => result.context("template build timed out").and_then(|value| value),
                status = jail.child.wait() => Err(anyhow::anyhow!("template VM exited before capture: {status:?}")),
            };
            let console = jail.kill().await?;
            captured.with_context(|| format!("template VM {} failed: {console}", jail.pid))?;
            for name in ["state", "memory"] {
                std::fs::rename(jail.root.join(name), dir.join(name))?;
            }
            // Untouched guest memory is zero: a hole costs nothing to keep or map.
            let dug = Command::new("/usr/bin/fallocate")
                .arg("--dig-holes")
                .arg(dir.join("memory"))
                .status()
                .await;
            if !dug.is_ok_and(|status| status.success()) {
                tracing::warn!(event = "vm_template_dig_holes_failed");
            }
            // The VMM wrote these as its own user. Root-owned and read-only,
            // no VM can alter what later VMs restore.
            for name in TEMPLATE_FILES {
                std::os::unix::fs::chown(dir.join(name), Some(0), Some(0))?;
                std::fs::set_permissions(dir.join(name), std::fs::Permissions::from_mode(0o444))?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = built {
            let _ = tokio::fs::remove_dir_all(&dir).await;
            return Err(error);
        }
        let allocated = std::fs::metadata(dir.join("memory"))
            .map(|meta| meta.blocks() * 512)
            .ok();
        tracing::info!(
            event = "vm_template_built",
            key,
            vcpus = options.vcpus,
            memory_mib = options.memory_mib,
            memory_file_bytes = allocated,
            ms = began.elapsed().as_secs_f64() * 1000.0,
            "application VM template built"
        );
        Ok(Template {
            dir,
            key: key.into(),
            runtime_sha256: options.runtime.sha256.clone(),
            vcpus: options.vcpus,
            memory_mib: options.memory_mib,
            cpu_quota: options.cpu_quota,
        })
    }
}

/// The restore of a template into a fresh jail; the guest then redials.
pub(crate) async fn load(api: &Path) -> Result<()> {
    request(
        api,
        "PUT",
        "/snapshot/load",
        serde_json::json!({"snapshot_path":"/state","mem_backend":{"backend_type":"File","backend_path":"/memory"},"resume_vm":true}),
    )
    .await
}

/// A cold boot into template mode: only the runtime image, no application.
pub(crate) async fn cold_boot(api: &Path, options: &LaunchOptions) -> Result<()> {
    request(api, "PUT", "/machine-config", serde_json::json!({"vcpu_count":options.vcpus,"mem_size_mib":options.memory_mib,"smt":false})).await?;
    request(api, "PUT", "/boot-source", serde_json::json!({"kernel_image_path":"/vmlinux","boot_args":"console=ttyS0 reboot=k panic=1 pci=off root=/dev/vda init=/sbin/init ro devtmpfs.mount=1 celld.template=1"})).await?;
    request(api, "PUT", "/drives/runtime", serde_json::json!({"drive_id":"runtime","path_on_host":"/runtime.ext4","is_root_device":true,"is_read_only":true})).await?;
    for (id, path) in [("handler", HANDLER_IMAGE), ("shared", SHARED_IMAGE)] {
        request(api, "PUT", &format!("/drives/{id}"), serde_json::json!({"drive_id":id,"path_on_host":format!("/{path}"),"is_root_device":false,"is_read_only":true})).await?;
    }
    request(api, "PUT", "/entropy", serde_json::json!({})).await?;
    request(
        api,
        "PUT",
        "/vsock",
        serde_json::json!({"guest_cid":3,"uds_path":"/vsock"}),
    )
    .await?;
    request(api, "PUT", "/balloon", balloon_config()).await?;
    request(
        api,
        "PUT",
        "/actions",
        serde_json::json!({"action_type":"InstanceStart"}),
    )
    .await
}

pub(crate) async fn wait_for_api(api: &Path) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !api.exists() {
        ensure!(Instant::now() < deadline, "jailer API readiness timed out");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

/// A VM workspace and jail root, before its jailer starts.
pub(crate) struct Staged {
    workspace: tempfile::TempDir,
    base: PathBuf,
    pub root: PathBuf,
    id: String,
    parent: String,
    memory: Option<Arc<cgroup::StableParent>>,
}

impl Staged {
    pub(crate) fn new(templates: &Templates, options: &LaunchOptions) -> Result<Self> {
        let workspace = tempfile::Builder::new()
            .prefix("vm-")
            .tempdir_in(templates.dir.path())?;
        let base = workspace.path().join("jails");
        let executable = options
            .firecracker
            .file_name()
            .context("Firecracker executable has no filename")?;
        // The jailer's own layout: `<base>/<executable>/<id>/root`.
        let id = workspace
            .path()
            .file_name()
            .context("VM workspace name")?
            .to_string_lossy()
            .into_owned();
        let root = base.join(executable).join(&id).join("root");
        std::fs::create_dir_all(&root)?;
        for name in [HANDLER_IMAGE, SHARED_IMAGE] {
            std::fs::hard_link(templates.dir.path().join(PLACEHOLDER), root.join(name))
                .with_context(|| format!("link placeholder {name}"))?;
        }
        let parent = templates
            .dir
            .path()
            .file_name()
            .context("pool directory name")?
            .to_string_lossy()
            .into_owned();
        Ok(Self {
            workspace,
            base,
            root,
            id,
            parent,
            memory: templates.memory.clone(),
        })
    }

    /// The host end of a guest-initiated vsock port, owned by the VMM user.
    /// Bound before the guest runs, and before a restore: its connections
    /// would otherwise find nothing.
    pub(crate) fn listen(&self, port: u32, options: &LaunchOptions) -> Result<UnixListener> {
        let path = self.root.join(format!("vsock_{port}"));
        let listener = UnixListener::bind(&path)?;
        std::os::unix::fs::chown(&path, Some(options.uid), Some(options.gid))?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        Ok(listener)
    }

    pub(crate) fn spawn(self, options: &LaunchOptions) -> Result<Jail> {
        trusted_executable(&options.jailer)?;
        trusted_executable(&options.firecracker)?;
        let mut leaf = self
            .memory
            .as_ref()
            .map(|parent| parent.leaf(&self.id, options.memory_mib))
            .transpose()?;
        let cgroup = leaf.is_none().then(|| {
            Path::new("/sys/fs/cgroup")
                .join(&self.parent)
                .join(&self.id)
        });
        let boosts = options.templates.boosts.clone();
        let boost = boost_slots(&boosts, options.vcpus, options.cpu_quota);
        let quota = if boost.is_some() {
            full_vcpus(options.vcpus)
        } else {
            options.cpu_quota
        };
        let mut command = Command::new(&options.jailer);
        command
            .arg("--id")
            .arg(&self.id)
            .arg("--uid")
            .arg(options.uid.to_string())
            .arg("--gid")
            .arg(options.gid.to_string())
            .arg("--exec-file")
            .arg(&options.firecracker)
            .arg("--chroot-base-dir")
            .arg(&self.base)
            .args(["--cgroup-version", "2", "--parent-cgroup"])
            .arg(&self.parent)
            .arg("--cgroup")
            .arg(format!("cpu.max={quota} 100000"));
        #[cfg(test)]
        if leaf.is_none() && std::env::var_os("CELLD_APPLICATION_MEMORY_CHECK_ONLY").is_some() {
            // Enable accounting before attachment; this adds no memory limit.
            command.args(["--cgroup", "memory.max=max"]);
        }
        let mut child = match command
            .args(["--", "--api-sock", "/api.sock"])
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
        {
            Ok(child) => child,
            Err(error) => {
                if let Some(leaf) = &mut leaf {
                    leaf.discard_unstarted()
                        .context(crate::application_vm::ReapUnproved)?;
                }
                return Err(error).context("launch jailed application VM");
            }
        };
        if let Some(leaf) = &mut leaf {
            leaf.mark_spawned(&child)
                .context(crate::application_vm::ReapUnproved)?;
        }
        let pid = child.id().context("VM PID disappeared")?;
        let console = Arc::new(Mutex::new(Vec::new()));
        Ok(Jail {
            stdout: drain(child.stdout.take().expect("VM stdout"), console.clone()),
            stderr: drain(child.stderr.take().expect("VM stderr"), console.clone()),
            _workspace: self.workspace,
            root: self.root,
            child,
            pid,
            console,
            cgroup,
            leaf,
            memory_writer: None,
            cpu_quota: options.cpu_quota,
            vcpus: options.vcpus,
            boosts,
            boost,
        })
    }
}

/// A class quota of whole vCPUs.
fn full_vcpus(vcpus: u8) -> u32 {
    u32::from(vcpus) * 100_000
}

/// Boost slots for a VM held below its vCPUs, if they are free now; startup
/// never waits for one.
fn boost_slots(
    boosts: &Arc<tokio::sync::Semaphore>,
    vcpus: u8,
    quota: u32,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    (full_vcpus(vcpus) > quota)
        .then(|| boosts.clone().try_acquire_many_owned(vcpus.into()).ok())
        .flatten()
}

/// A running jailer/VMM and its console; the workspace lives as long as it.
pub(crate) struct Jail {
    _workspace: tempfile::TempDir,
    pub root: PathBuf,
    pub child: Child,
    pub pid: u32,
    pub console: Arc<Mutex<Vec<u8>>>,
    pub stdout: AbortOnDropHandle<()>,
    pub stderr: AbortOnDropHandle<()>,
    cgroup: Option<PathBuf>,
    leaf: Option<cgroup::Leaf>,
    /// Stored on the physical owner, never detached on a transition timeout.
    memory_writer: Option<tokio::task::JoinHandle<Result<cgroup::MemorySample>>>,
    /// The class quota; while `boost` is held the VM runs at full vCPUs.
    cpu_quota: u32,
    vcpus: u8,
    boosts: Arc<tokio::sync::Semaphore>,
    boost: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl Jail {
    /// Full vCPUs until `settle`, if boost slots are free: startup is not held
    /// to a fractional class quota. Every startup phase is bounded by its own
    /// timeout and a failed VM is killed, which frees the slots. Failing to
    /// boost leaves the class quota, which is safe.
    pub(crate) fn boost(&mut self) {
        if self.boost.is_none() {
            self.boost = boost_slots(&self.boosts, self.vcpus, self.cpu_quota)
                .filter(|_| self.set_cpu_max(full_vcpus(self.vcpus)).is_ok());
        }
    }

    /// Back to the class quota. A VM that cannot be held to it must not run.
    pub(crate) fn settle(&mut self) -> Result<()> {
        if self.boost.is_some() {
            self.set_cpu_max(self.cpu_quota)?;
            self.boost = None;
        }
        Ok(())
    }

    fn set_cpu_max(&self, quota: u32) -> Result<()> {
        let value = format!("{quota} 100000");
        match (&self.leaf, &self.cgroup) {
            (Some(leaf), _) => leaf.set_cpu_max(&value),
            (None, Some(path)) => {
                std::fs::write(path.join("cpu.max"), value).context("configure cgroup cpu.max")
            }
            (None, None) => bail!("application VM has no cgroup"),
        }
    }

    pub(crate) fn retained_target(&self, nominal_bytes: u64, margin: u64) -> Result<Option<u64>> {
        self.leaf
            .as_ref()
            .context("VM cache requires an owned memory controller")?
            .memory_limit()?
            .retained_target(nominal_bytes, margin)
    }

    async fn join_memory_writer(&mut self) -> Result<()> {
        if let Some(writer) = self.memory_writer.as_mut() {
            let result = writer.await;
            self.memory_writer.take();
            result.context("VM memory limit writer panicked")??;
        }
        Ok(())
    }

    /// The caller already holds the corresponding full reservation when raising.
    pub(crate) async fn set_memory_limit(
        &mut self,
        retained: Option<u64>,
        deadline_ms: u64,
        stop: &tokio_util::sync::CancellationToken,
        retired: &tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        ensure!(
            self.memory_writer.is_none(),
            "VM memory writer already active"
        );
        ensure!(
            crate::asyncrt::mono_ms() < deadline_ms
                && !stop.is_cancelled()
                && !retired.is_cancelled(),
            "VM memory transition expired"
        );
        let writer = self
            .leaf
            .as_ref()
            .context("VM cache controller absent")?
            .memory_limit()?;
        self.memory_writer = Some(tokio::task::spawn_blocking(move || writer.set(retained)));
        let outcome = crate::asyncrt::select! {
            result = crate::asyncrt::timeout_at(deadline_ms,
                self.memory_writer.as_mut().expect("owned memory writer")) => Some(result),
            _ = stop.cancelled() => None,
            _ = retired.cancelled() => None,
        };
        match outcome {
            Some(Ok(result)) => {
                self.memory_writer.take();
                result.context("VM memory limit writer panicked")??;
                Ok(())
            }
            _ => {
                // A blocking cgroup write cannot be cancelled. Kill, then join it
                // with the original charge held; never reap around a late writer.
                let killed = self.child.start_kill();
                let joined = self.join_memory_writer().await;
                killed?;
                joined?;
                bail!("VM memory limit transition timed out or was cancelled")
            }
        }
    }

    pub(crate) fn memory_exhausted(&self) -> Result<bool> {
        self.leaf
            .as_ref()
            .map(|leaf| {
                leaf.sample()
                    .map(|sample| sample.oom_kill > 0 || sample.oom_group_kill > 0)
            })
            .transpose()
            .map(|exhausted| exhausted.unwrap_or(false))
    }

    /// Kill, reap and return what the VM printed.
    pub(crate) async fn kill(&mut self) -> Result<String> {
        kill_and_wait(&mut self.child)
            .await
            .context(crate::application_vm::ReapUnproved)?;
        self.join_memory_writer()
            .await
            .context(crate::application_vm::ReapUnproved)?;
        let _ = timeout(Duration::from_secs(1), async {
            let _ = tokio::join!(&mut self.stdout, &mut self.stderr);
        })
        .await;
        if let Some(leaf) = &mut self.leaf {
            let sample = leaf
                .reap(&mut self.child)
                .context(crate::application_vm::ReapUnproved)?;
            tracing::info!(
                event = "application_vm_memory_reaped",
                current_bytes = sample.current_bytes,
                peak_bytes = sample.peak_bytes,
                high = sample.high,
                max = sample.max,
                oom = sample.oom,
                oom_kill = sample.oom_kill,
                oom_group_kill = sample.oom_group_kill,
                "application VM memory accounting before removal"
            );
            self.leaf = None;
        }
        if let Some(cgroup) = &self.cgroup {
            if cgroup.exists() {
                std::fs::remove_dir(cgroup).context("remove reaped VM cgroup")?;
            }
            if let Some(parent) = cgroup.parent() {
                // Unguarded legacy pools leave parent ownership to the jailer.
                let _ = std::fs::remove_dir(parent);
            }
        }
        Ok(
            String::from_utf8_lossy(&self.console.lock().expect("VM console poisoned"))
                .into_owned(),
        )
    }
}

/// Time spent reaching the agent's hello, then the prewarmed guest's, since
/// the boot began.
#[derive(Clone, Copy, Default)]
pub(crate) struct Boot {
    pub prepared: Duration,
    pub spawned: Duration,
    pub configured: Duration,
    pub hello: Duration,
    pub prewarmed: Duration,
}

/// A started VM whose prewarmed guest is connected and waits for a claim:
/// assigned to no application, holding no application data. Once claimed it serves one
/// handler and is destroyed at retirement; it never returns here.
pub(crate) struct Idle {
    pub jail: Jail,
    pub guest: UnixStream,
    pub control: UnixListener,
    pub storage: UnixListener,
    pub began: Instant,
    pub boot: Boot,
    /// `restore` or `cold`; `pool` once the VM has waited.
    pub path: &'static str,
    pub memory_mib: u32,
}

impl Idle {
    /// The VMM exited or the agent's connection closed. A live agent sends
    /// nothing while it waits, so anything but "no data yet" is a failure.
    fn broken(&mut self) -> bool {
        !matches!(self.jail.child.try_wait(), Ok(None))
            || !matches!(self.guest.try_read(&mut [0]), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    }

    /// Kill and reap the VMM; `Err` if the reap could not be proved.
    pub(crate) async fn reap(mut self) -> Result<()> {
        self.jail.kill().await.map(drop)
    }
}

fn lock_file(file: &std::fs::File) -> bool {
    use std::os::fd::AsRawFd;
    // SAFETY: flock on a descriptor the caller owns.
    unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) == 0 }
}

/// Removes the pool directories of nodes that were killed before they could
/// clean up. A live node holds a lock on its directory's `lock` file, so an
/// old directory whose lock can be taken (or that has none: an older release)
/// has no owner. A directory under a minute old is spared, which also covers a
/// node between creating its directory and locking it.
fn sweep_leaked(root: &Path) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        // The temp dir is shared: only a real (not symlinked) directory owned
        // by this process's user (root in production) is ever a candidate.
        let Ok(meta) = entry.metadata() else { continue };
        if !entry.file_name().to_string_lossy().starts_with(DIR_PREFIX)
            || !meta.is_dir()
            || meta.uid() != unsafe { libc::geteuid() }
        {
            continue;
        }
        let old = meta.modified().is_ok_and(|modified| {
            modified
                .elapsed()
                .is_ok_and(|age| age > Duration::from_secs(60))
        });
        if old && std::fs::File::open(path.join("lock")).map_or(true, |lock| lock_file(&lock)) {
            match std::fs::remove_dir_all(&path) {
                Ok(()) => {
                    tracing::warn!(event = "vm_pool_dir_swept", path = %path.display(), "removed a pool directory leaked by a killed node")
                }
                Err(error) => {
                    tracing::error!(event = "vm_pool_dir_sweep_failed", path = %path.display(), %error)
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "unit_tests/application_template/tests.rs"]
mod tests;

