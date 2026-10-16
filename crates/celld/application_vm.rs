// The VM pool and supervisor launch and reap real jailer and Firecracker processes,
// stage jail files, and run host-side sweep and warm loops. That is host process
// supervision outside the Actor execution domain.
#![allow(clippy::disallowed_methods)]

//! Owned Firecracker start of a staged application handler: a restore from a
//! node-local template, else a cold boot. No host mount of application files,
//! network device, guest ownership or proof receipt.

use crate::application_guest_transport::{Client, Dispatcher};
use crate::application_javascript_guest::native_namespace_owns_object;
use crate::application_template::{
    cold_boot, load, wait_for_api, Boot, Idle, Jail, Staged, Template, Templates,
};
use crate::application_template_agent::{claim, prewarm, AGENT_PORT, HELLO};
use crate::applications::{
    content_digest, Application, ApplicationLimits, ApplicationSizeClass, ApplicationStore,
};
use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use celld_applications::{
    ApplicationId, BindingTarget, Deployment, Handler, Language, Name, ResourceAddress,
    ResourceKind, ResourceRef, RouteDestination, RouteTable, RouteTarget,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::process::ExitStatus;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Child;
#[cfg(test)]
use tokio::process::Command;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Instant};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

#[cfg(test)]
#[path = "application_vm/memory.rs"]
mod memory_baseline;

#[cfg(test)]
#[path = "application_vm/freeze_baseline.rs"]
mod freeze_baseline;

#[cfg(test)]
#[path = "application_vm/reporting_baseline.rs"]
mod reporting_baseline;

#[cfg(test)]
mod guard_baseline;

#[cfg(test)]
mod cache_baseline;

#[cfg(test)]
mod lifecycle_baseline;

#[cfg(test)]
mod service_wake_baseline;

#[cfg(test)]
mod mixed_wake_baseline;

#[cfg(test)]
mod cpu_baseline;

#[cfg(test)]
mod cache_failure_baseline;

#[cfg(test)]
mod density_baseline;

#[cfg(test)]
mod quarantine_baseline;

#[cfg(test)]
mod working_set_baseline;

#[cfg(test)]
mod loader_background_baseline;

#[cfg(test)]
mod pressure_baseline;

#[path = "application_vm/assigned_supervisor.rs"]
mod assigned_supervisor;
pub(crate) mod lifecycle;
mod pressure;

#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApprovedFile {
    pub path: PathBuf,
    pub sha256: String,
}

#[derive(Clone)]
pub(crate) struct LaunchOptions {
    pub jailer: PathBuf,
    pub firecracker: PathBuf,
    pub kernel: ApprovedFile,
    pub runtime: ApprovedFile,
    pub adapter: Language,
    pub uid: u32,
    pub gid: u32,
    pub vcpus: u8,
    pub memory_mib: u32,
    pub cpu_quota: u32,
    /// The pool's snapshot templates; every launch restores from one.
    pub templates: Arc<Templates>,
    /// The shared read-only runtime drive (the Pyodide release of a Python
    /// handler), already built by `Templates::pyodide`; `None` for a handler
    /// that needs none.
    pub shared: Option<PathBuf>,
}

#[derive(Clone)]
pub(crate) struct Pin {
    pub application: ApplicationId,
    pub deployment: Name,
    pub manifest_sha256: String,
    pub handler: Handler,
    namespaces: Vec<(ResourceAddress, String)>,
    containers: Vec<crate::application_container::Binding>,
    limits: Option<ApplicationLimits>,
    runtime_environment: BTreeMap<Name, String>,
}

// Exit records the whole end state of a VM; callers and tests read different parts.
#[allow(dead_code)]
pub(crate) struct Exit {
    pub pid: u32,
    pub status: ExitStatus,
    pub reason: &'static str,
    pub console: String,
}

pub(crate) struct Vm {
    client: VmClient,
    task: Option<JoinHandle<Result<Exit>>>,
    /// `restore` or `cold`, as `vm_startup` reports it.
    #[cfg(test)]
    path: &'static str,
}

#[derive(Clone)]
pub(crate) struct VmClient {
    pin: Arc<Pin>,
    transport: Client,
    supervisor: Arc<assigned_supervisor::Control>,
    stop: CancellationToken,
    #[cfg(test)]
    console: Arc<Mutex<Vec<u8>>>,
    /// The `vm_startup` fields, for baselines.
    #[cfg(test)]
    pub(crate) startup: serde_json::Value,
}

impl VmClient {
    /// The guest's serial console so far, a 16 KiB ring.
    #[cfg(test)]
    pub(crate) fn console(&self) -> String {
        String::from_utf8_lossy(&self.console.lock().expect("VM console poisoned")).into_owned()
    }
    #[cfg(test)]
    pub(crate) async fn invoke(
        &self,
        app: crate::actor::AppHandle,
        resource: ResourceAddress,
        class: String,
        name: String,
        deadline: Duration,
        request: celld_applications::invocation_wire::HttpRequest,
    ) -> Result<crate::application_guest::GuestCompletion<crate::js::HttpResponse>> {
        ensure!(!self.stop.is_cancelled(), "application VM retired");
        ensure!(
            self.pin
                .namespaces
                .iter()
                .any(|(bound, declared)| bound == &resource && declared == &class),
            "resource and class are not bound to this handler"
        );
        self.transport
            .invoke(
                app,
                resource,
                class,
                name,
                self.pin.limits.map_or(deadline, |limits| {
                    deadline.min(Duration::from_millis(limits.invocation_timeout_ms))
                }),
                request,
            )
            .await
    }
}

impl VmClient {
    /// A call to an object this handler declares: the guest's own, or a peer's
    /// for an object owned here. It is never forwarded again.
    pub(crate) async fn call_object(
        &self,
        app: crate::actor::AppHandle,
        call: celld_applications::invocation_wire::ObjectCall,
        request: celld_applications::invocation_wire::HttpRequest,
    ) -> Result<crate::application_guest::GuestCompletion<crate::js::HttpResponse>> {
        ensure!(!self.stop.is_cancelled(), "application VM retired");
        self.transport.call_object(app, call, request).await
    }

    /// The pinned deployment's default Worker fetch; no object or resource.
    pub(crate) async fn invoke_stateless(
        &self,
        app: &crate::actor::AppHandle,
        deadline: Duration,
        request: celld_applications::invocation_wire::HttpRequest,
        body: Option<crate::js::HttpChunkStream>,
    ) -> Result<crate::js::HttpResponse> {
        ensure!(!self.stop.is_cancelled(), "application VM retired");
        let deadline = self.pin.limits.map_or(deadline, |limits| {
            deadline.min(Duration::from_millis(limits.invocation_timeout_ms))
        });
        self.transport
            .invoke_stateless(app, deadline, request, body)
            .await
    }

    /// An original queue consumer batch in the pinned deployment's default
    /// Worker, run as a stateless guest event. Waits for event capacity within
    /// `deadline`. Handler exceptions are an `Exception` outcome, not an error.
    pub(crate) async fn invoke_queue(
        &self,
        app: &crate::actor::AppHandle,
        deadline: Duration,
        batch: &crate::js::QueueBatch,
    ) -> Result<crate::js::QueueDispatchResult> {
        ensure!(!self.stop.is_cancelled(), "application VM retired");
        let deadline = self.pin.limits.map_or(deadline, |limits| {
            deadline.min(Duration::from_millis(limits.invocation_timeout_ms))
        });
        self.transport.invoke_queue(app, deadline, batch).await
    }

    pub(crate) async fn invoke_service(
        &self,
        app: &crate::actor::AppHandle,
        request: celld_applications::invocation_wire::HttpRequest,
        service: celld_applications::invocation_wire::ServiceInvocation,
        body: Option<crate::js::HttpChunkStream>,
        relay: Option<crate::application_guest_transport::SignalRelay>,
    ) -> Result<crate::js::HttpResponse> {
        ensure!(!self.stop.is_cancelled(), "application VM retired");
        let deadline = self.pin.limits.map_or(Duration::from_secs(30), |limits| {
            Duration::from_millis(limits.invocation_timeout_ms)
        });
        self.transport
            .invoke_service(app, deadline, request, service, body, relay)
            .await
    }
}

impl Drop for Vm {
    fn drop(&mut self) {
        self.client.stop.cancel();
        self.client.transport.retire();
    }
}

impl Vm {
    /// The caller has management authority for this application. Metadata and
    /// artifact are re-read from immutable staged storage, not supplied by guest.
    pub(crate) async fn launch(
        mut options: LaunchOptions,
        store: &ApplicationStore,
        application: &ApplicationId,
        deployment: &Name,
        handler: &Name,
    ) -> Result<Self> {
        let began = Instant::now();
        let manifest = store
            .deployment(application, deployment)
            .await?
            .context("deployment is not staged")?;
        let snapshot = store.read(application).await?;
        let app = snapshot
            .application(application)
            .context("application does not exist")?;
        let mut pin = prepare_pin(&options, manifest, handler, &app.resources)?;
        let metadata = began.elapsed();
        pin.runtime_environment = store.resolve_environment(app, &pin.handler).await?;
        let secrets = began.elapsed();
        pin.limits = app.limits;
        let selected = pin
            .limits
            .map_or(ApplicationSizeClass::default(), |limits| limits.size_class)
            .resources();
        ensure!(
            !options.templates.production()
                || (options.vcpus, options.memory_mib, options.cpu_quota) == selected,
            "application size changed during admission; retry"
        );
        if let Some(limits) = pin.limits {
            (options.vcpus, options.memory_mib, options.cpu_quota) = limits.size_class.resources();
        }
        let image = store
            .cached_artifact(application, &pin.handler.artifact_sha256)
            .await?
            .context("handler artifact is missing")?;
        let ms = |elapsed: Duration| elapsed.as_secs_f64() * 1000.0;
        tracing::info!(
            event = "vm_launch",
            metadata_ms = ms(metadata),
            secrets_ms = ms(secrets - metadata),
            artifact_ms = ms(began.elapsed() - secrets),
            "application VM inputs read"
        );
        // The shared drive goes only to a handler that declares it needs it.
        let needs_runtime = crate::application_pyodide::needs_runtime(&pin.handler)?;
        if !needs_runtime {
            options.shared = None;
        }
        ensure!(
            !needs_runtime || options.shared.is_some(),
            "the shared Pyodide runtime is not prepared"
        );
        Self::spawn(options, pin, image).await
    }

    /// A VM for a handler the node supplies itself, outside any application's
    /// catalog (the Pyodide snapshot builder). The caller vouches for `image`.
    pub(crate) async fn launch_builtin(
        options: LaunchOptions,
        handler: Handler,
        image: &Path,
    ) -> Result<Self> {
        let pin = Pin {
            application: ApplicationId {
                app: "builtin".to_owned().try_into()?,
            },
            deployment: "builtin".to_owned().try_into()?,
            manifest_sha256: content_digest(&serde_json::to_vec(&handler)?),
            handler,
            namespaces: vec![],
            containers: vec![],
            limits: None,
            runtime_environment: BTreeMap::new(),
        };
        Self::spawn(options, pin, image.to_owned()).await
    }

    async fn spawn(options: LaunchOptions, pin: Pin, image: PathBuf) -> Result<Self> {
        let (mut reply, receive) = oneshot::channel();
        // Startup is owned too: closing this frontend cannot abandon a partial
        // jailer/VMM. The task kills/waits before ending failed startup.
        tokio::spawn(async move {
            let result = start(options, Arc::new(pin), image, &mut reply).await;
            let _ = reply.send(result);
        });
        receive.await.context(ReapUnproved)?
    }
    pub(crate) fn client(&self) -> VmClient {
        self.client.clone()
    }
    pub(crate) async fn shutdown(mut self) -> Result<Exit> {
        self.client.stop.cancel();
        self.task
            .take()
            .expect("owned VM monitor")
            .await
            .context("VM monitor panicked")?
    }
    // A debugging aid for failed VM tests.
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn console(&self) -> String {
        self.client.console()
    }
    #[cfg(test)]
    pub(crate) async fn wait(mut self) -> Result<Exit> {
        self.task
            .take()
            .expect("owned VM monitor")
            .await
            .context("VM monitor panicked")?
    }
}

/// Opt-in physical memory reservations; all values are explicit host bytes.
#[derive(Clone, Copy, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MemoryGuard {
    pub max_bytes: u64,
    pub high_bytes: u64,
    pub overhead_bytes: u64,
    pub shared_cache_bytes: u64,
    #[serde(default)]
    pub idle_cache: Option<IdleCache>,
}

/// Explicit measured inputs; absent by default. Overhead stays fully charged.
#[derive(Clone, Copy, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IdleCache {
    pub max_bytes: u64,
    pub retained_margin_bytes: u64,
}

impl MemoryGuard {
    fn reservation_bytes(self, guest_mib: u64, vms: u32) -> Option<u64> {
        guest_mib
            .checked_mul(1024 * 1024)?
            .checked_add(u64::from(vms).checked_mul(self.overhead_bytes)?)?
            .checked_add(self.shared_cache_bytes)
    }

    fn validate(self) -> Result<()> {
        ensure!(
            self.high_bytes > 0 && self.high_bytes < self.max_bytes,
            "invalid application memory guard high/max"
        );
        ensure!(
            self.high_bytes > self.max_bytes - self.high_bytes,
            "application memory high must leave a positive recovery threshold"
        );
        ensure!(
            self.overhead_bytes > 0 && self.shared_cache_bytes > 0,
            "application memory guard requires measured overhead and shared cache bytes"
        );
        let largest = ApplicationSizeClass::ALL
            .iter()
            .map(|class| u64::from(class.resources().1))
            .max()
            .expect("application size classes");
        ensure!(
            self.reservation_bytes(largest, 1)
                .is_some_and(|bytes| bytes <= self.max_bytes),
            "application memory guard must fit the largest VM and host allowances"
        );
        if let Some(cache) = self.idle_cache {
            ensure!(
                cache.max_bytes > self.overhead_bytes
                    && cache.max_bytes <= self.max_bytes - self.shared_cache_bytes
                    && cache.retained_margin_bytes > 0
                    && cache.retained_margin_bytes < cache.max_bytes - self.overhead_bytes,
                "invalid retained application VM cache budget or measured margin"
            );
        }
        Ok(())
    }
}

/// Trusted node configuration: executables, the approved kernel and the
/// approved JavaScript runtime images, plus this node's VM capacity. A staged
/// handler selects a runtime image only by naming one of these digests.
#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VmHostConfig {
    pub jailer: PathBuf,
    pub firecracker: PathBuf,
    pub kernel: ApprovedFile,
    pub runtimes: Vec<ApprovedFile>,
    pub uid: u32,
    pub gid: u32,
    pub max_vms: u32,
    pub max_memory_mib: u64,
    #[serde(default)]
    pub memory_guard: Option<MemoryGuard>,
    pub idle_ttl_secs: u64,
    /// Public ingress serves handler `<route id>.<platform_domain>`; the
    /// operator points wildcard DNS and TLS at this node.
    pub platform_domain: String,
    /// The trusted `celld-fence` image Containers run once to fence the
    /// bridges. Node configuration only; no application selects it.
    #[serde(default)]
    pub container_fence_image: Option<String>,
    /// Seconds a cached handler artifact survives after its last use on this
    /// node (default 600).
    #[serde(default)]
    pub artifact_cache_ttl_secs: Option<u64>,
    #[serde(default)]
    pub metadata_cache_entries: Option<usize>,
    #[serde(default)]
    pub metadata_cache_bytes: Option<usize>,
    /// Restored VMs, per snapshot template this node has needed, kept ready
    /// for a claim (default 0: each launch restores on demand). They count
    /// against `max_vms` and `max_memory_mib`, give way to launches that need
    /// the capacity, and must leave at least one VM of room.
    #[serde(default)]
    pub startup_pool: u32,
}

impl VmHostConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        let digest = |value: &str| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        };
        ensure!(
            self.jailer.is_absolute()
                && self.firecracker.is_absolute()
                && self.kernel.path.is_absolute()
                && digest(&self.kernel.sha256),
            "VM executables and kernel must be absolute and digest-pinned"
        );
        ensure!(
            !self.runtimes.is_empty()
                && self
                    .runtimes
                    .iter()
                    .all(|runtime| { runtime.path.is_absolute() && digest(&runtime.sha256) })
                && self
                    .runtimes
                    .iter()
                    .map(|runtime| &runtime.sha256)
                    .collect::<HashSet<_>>()
                    .len()
                    == self.runtimes.len(),
            "approved runtime images must be absolute, digest-pinned and distinct"
        );
        ensure!(self.uid > 0 && self.gid > 0, "invalid host VM limits");
        ensure!(
            (1..=256).contains(&self.max_vms)
                && self.max_memory_mib >= 12_288
                && (1..=3600).contains(&self.idle_ttl_secs),
            "invalid host VM capacity"
        );
        ensure!(
            self.artifact_cache_ttl_secs
                .is_none_or(|ttl| (1..=86_400).contains(&ttl)),
            "invalid artifact cache TTL"
        );
        ensure!(
            self.metadata_cache_entries
                .is_none_or(|entries| (1..=4096).contains(&entries))
                && self
                    .metadata_cache_bytes
                    .is_none_or(|bytes| (1..=64 * 1024 * 1024).contains(&bytes)),
            "invalid application metadata cache bounds"
        );
        ensure!(
            self.startup_pool < self.max_vms
                && u64::from(self.startup_pool + 1)
                    * u64::from(ApplicationSizeClass::default().resources().1)
                    <= self.max_memory_mib,
            "startup_pool must leave room for a VM within max_vms and max_memory_mib"
        );
        if let Some(guard) = self.memory_guard {
            guard.validate()?;
        }
        // Leaves room for a 32-character route id label.
        validate_platform_domain(&self.platform_domain)?;
        if let Some(image) = &self.container_fence_image {
            crate::application_container::validate_image(image)?;
        }
        Ok(())
    }

    /// Whether `extra` more VMs fit beside the charged ones and `idle` pool VMs.
    fn room(&self, snapshot: &VmPoolSnapshot, idle: (u32, u64), extra: u32, memory: u64) -> bool {
        let Some(vms) = snapshot
            .vms
            .checked_add(idle.0)
            .and_then(|vms| vms.checked_add(extra))
        else {
            return false;
        };
        let Some(memory_mib) = snapshot
            .memory_mib
            .checked_add(idle.1)
            .and_then(|memory_mib| memory_mib.checked_add(memory))
        else {
            return false;
        };
        vms <= self.max_vms
            && memory_mib <= self.max_memory_mib
            && self.memory_guard.is_none_or(|guard| {
                snapshot
                    .physical_bytes(guard, memory_mib, vms)
                    .is_some_and(|bytes| bytes <= guard.max_bytes)
            })
    }

    /// Launch options for one approved runtime image, refused before any
    /// launch work when the digest is not approved on this node.
    fn options(&self, runtime_sha256: &str, templates: Arc<Templates>) -> Result<LaunchOptions> {
        let runtime = self
            .runtimes
            .iter()
            .find(|runtime| runtime.sha256 == runtime_sha256)
            .context("handler runtime image is not approved")?;
        Ok(LaunchOptions {
            jailer: self.jailer.clone(),
            firecracker: self.firecracker.clone(),
            kernel: self.kernel.clone(),
            runtime: runtime.clone(),
            adapter: Language::Javascript,
            uid: self.uid,
            gid: self.gid,
            vcpus: ApplicationSizeClass::default().resources().0,
            memory_mib: ApplicationSizeClass::default().resources().1,
            cpu_quota: ApplicationSizeClass::default().resources().2,
            templates,
            shared: None,
        })
    }
}

/// Refused before any VM or application code could start.
#[derive(Debug)]
pub struct CapacityExhausted;

#[derive(Debug)]
pub(crate) struct ReapUnproved;

impl std::fmt::Display for ReapUnproved {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("VM physical reap unproved")
    }
}

impl std::error::Error for ReapUnproved {}

impl std::fmt::Display for CapacityExhausted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("application VM capacity exhausted")
    }
}

impl std::error::Error for CapacityExhausted {}

/// New leases for a replaced deployment are refused; the caller resolves again.
#[derive(Debug)]
pub(crate) struct DeploymentRetiring;

impl std::fmt::Display for DeploymentRetiring {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("deployment is retiring")
    }
}

impl std::error::Error for DeploymentRetiring {}

/// One VM serves one application, deployment and handler; warm reuse never
/// crosses this identity.
type VmKey = (ApplicationId, Name, Name);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct VmPoolSnapshot {
    /// Charged VMs: starting, serving, idle or awaiting a proven reap.
    pub vms: u32,
    pub memory_mib: u64,
    pub starting: u32,
    pub ready: u32,
    /// VMs whose reap could not be proved; their capacity stays charged.
    pub quarantined: u32,
    /// Installed low ceilings, including unreaped bounded quarantine.
    pub cached_vms: u32,
    pub cached_nominal_mib: u64,
    pub retained_limit_bytes: u64,
}

impl VmPoolSnapshot {
    fn physical_bytes(self, guard: MemoryGuard, nominal_mib: u64, vms: u32) -> Option<u64> {
        if self.cached_vms > self.vms || self.cached_nominal_mib > self.memory_mib {
            return None;
        }
        let cached_bytes = self
            .retained_limit_bytes
            .checked_add(u64::from(self.cached_vms).checked_mul(guard.overhead_bytes)?)?;
        match guard.idle_cache {
            Some(cache) if cached_bytes <= cache.max_bytes => {}
            None if self.cached_vms == 0
                && self.cached_nominal_mib == 0
                && self.retained_limit_bytes == 0 => {}
            _ => return None,
        }
        guard
            .reservation_bytes(nominal_mib.checked_sub(self.cached_nominal_mib)?, vms)?
            .checked_add(self.retained_limit_bytes)
    }

    fn retain(&mut self, nominal_mib: u64, limit: u64) -> Option<()> {
        let vms = self.cached_vms.checked_add(1)?;
        let nominal = self.cached_nominal_mib.checked_add(nominal_mib)?;
        let bytes = self.retained_limit_bytes.checked_add(limit)?;
        self.cached_vms = vms;
        self.cached_nominal_mib = nominal;
        self.retained_limit_bytes = bytes;
        Some(())
    }

    fn full(&mut self, nominal_mib: u64, limit: u64) -> Option<()> {
        let vms = self.cached_vms.checked_sub(1)?;
        let nominal = self.cached_nominal_mib.checked_sub(nominal_mib)?;
        let bytes = self.retained_limit_bytes.checked_sub(limit)?;
        self.cached_vms = vms;
        self.cached_nominal_mib = nominal;
        self.retained_limit_bytes = bytes;
        Some(())
    }
}

enum Slot {
    Starting(tokio::sync::watch::Receiver<Option<Result<VmClient, String>>>),
    Ready(Ready),
}

struct Ready {
    client: VmClient,
    /// One count per live lease; idleness also requires no guest events.
    leases: Arc<()>,
    idle_since: Instant,
    gate: Option<Arc<lifecycle::Gate>>,
    /// Updated under PoolState; shared with the original reap owner.
    retained_limit: Arc<AtomicU64>,
    /// Asks the owning monitor to shut the VM down and reap it.
    retire: CancellationToken,
}

impl Ready {
    fn idle(&self) -> bool {
        Arc::strong_count(&self.leases) == 1
            && self.client.transport.pending() == 0
            && self.gate.as_ref().is_none_or(|gate| gate.known_idle())
    }

    fn ineligible(&self) -> bool {
        self.gate
            .as_ref()
            .is_some_and(|gate| gate.retire_if_expired())
    }

    fn try_lease(&self) -> Option<VmLease> {
        if self.retire.is_cancelled() || self.client.transport.is_condemned() {
            return None;
        }
        // The clone and phase check share the Gate lock with begin_pause.
        // A lease acquired first is visible to the supervisor's final check.
        let lease = match &self.gate {
            Some(gate) => gate.lease(&self.leases)?,
            None => self.leases.clone(),
        };
        Some(VmLease {
            client: self.client.clone(),
            _lease: Some(lease),
        })
    }

    fn retire_idle(&self) -> bool {
        if !self.idle()
            || self
                .gate
                .as_ref()
                .is_some_and(|gate| !gate.retire_if_idle())
        {
            return false;
        }
        self.retire.cancel();
        true
    }

    fn last_used_ms(&self) -> u64 {
        self.gate.as_ref().map_or_else(
            || {
                crate::asyncrt::mono_ms().saturating_sub(
                    self.idle_since
                        .elapsed()
                        .as_millis()
                        .try_into()
                        .unwrap_or(u64::MAX),
                )
            },
            |gate| gate.status().last_drain_ms,
        )
    }
}

/// The assigned supervisor uses this pool ledger for retained-client wakes.
struct CacheAdmission {
    state: std::sync::Weak<Mutex<PoolState>>,
    key: VmKey,
    pin: Arc<Pin>,
    retained_limit: Arc<AtomicU64>,
    nominal_mib: u64,
    config: VmHostConfig,
    templates: Arc<Templates>,
    pressure: Arc<Mutex<pressure::Guard>>,
    changed: Arc<tokio::sync::Notify>,
}

impl CacheAdmission {
    fn live(&self, state: &PoolState, gate: &Arc<lifecycle::Gate>) -> bool {
        !state.closed
            && matches!(state.slots.get(&self.key), Some(Slot::Ready(ready))
            if Arc::ptr_eq(&ready.client.pin, &self.pin)
                && Arc::ptr_eq(&ready.retained_limit, &self.retained_limit)
                && !ready.retire.is_cancelled()
                && ready.gate.as_ref().is_some_and(|owned| Arc::ptr_eq(owned, gate)))
    }

    /// Full reservation survives until enforcement and this commit succeed.
    fn cached(
        &self,
        gate: &Arc<lifecycle::Gate>,
        transition: lifecycle::Transition,
        limit: u64,
    ) -> Result<()> {
        let state = self.state.upgrade().context("VM cache pool stopped")?;
        let mut state = state.lock().expect("VM pool poisoned");
        ensure!(self.live(&state, gate), "VM cache identity retired");
        ensure!(
            self.retained_limit.load(Ordering::Relaxed) == 0
                && limit > 0
                && limit
                    < self
                        .nominal_mib
                        .checked_mul(1024 * 1024)
                        .context("VM cache nominal overflow")?,
            "VM retained ceiling grants no saving"
        );
        transition.check()?;
        let mut snapshot = state.snapshot;
        snapshot
            .retain(self.nominal_mib, limit)
            .context("VM retained accounting overflow")?;
        ensure!(
            self.config
                .room(&snapshot, self.templates.idle_capacity(), 0, 0),
            "VM retained cache budget exhausted"
        );
        // Pool -> Gate; no I/O while either is held.
        gate.confirm_pause(transition)?;
        state.snapshot = snapshot;
        self.retained_limit.store(limit, Ordering::Relaxed);
        Ok(())
    }

    /// Budget waiting leaves Cached unchanged. Only this owner raises the limit.
    fn resume(&self, gate: &Arc<lifecycle::Gate>) -> Result<Option<lifecycle::Transition>> {
        VmPool::refresh_pressure(&self.pressure, &self.templates);
        let state = self.state.upgrade().context("VM cache pool stopped")?;
        let mut state = state.lock().expect("VM pool poisoned");
        ensure!(self.live(&state, gate), "VM cache identity retired");
        let limit = self.retained_limit.load(Ordering::Relaxed);
        let mut snapshot = state.snapshot;
        if limit > 0 {
            if VmPool::under_pressure(&self.pressure) {
                return Ok(None);
            }
            snapshot
                .full(self.nominal_mib, limit)
                .context("VM cache charge missing")?;
            if !self
                .config
                .room(&snapshot, self.templates.idle_capacity(), 0, 0)
            {
                return Ok(None);
            }
        }
        let Some(transition) = gate.begin_resume() else {
            ensure!(
                gate.status().phase == lifecycle::Phase::Cached,
                "VM cache expired before admission"
            );
            return Ok(None); // the final waiter withdrew demand before promotion
        };
        state.snapshot = snapshot;
        self.retained_limit.store(0, Ordering::Relaxed);
        Ok(Some(transition))
    }

    fn evict_for_resume(&self, gate: &lifecycle::Gate) {
        if !gate.status().resume_requested {
            return;
        }
        if let Some(idle) = self.templates.evict() {
            VmPool::reap_idle(idle, self.templates.clone(), self.changed.clone());
            return;
        }
        if self.templates.winding_down().0 > 0 {
            return;
        }
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let state = state.lock().expect("VM pool poisoned");
        if state.closed
            || state
                .slots
                .values()
                .any(|slot| matches!(slot, Slot::Ready(ready) if ready.retire.is_cancelled()))
        {
            return;
        }
        if let Some(victim) = state
            .slots
            .values()
            .filter_map(|slot| match slot {
                Slot::Ready(ready)
                    if !Arc::ptr_eq(&ready.retained_limit, &self.retained_limit)
                        && ready.retained_limit.load(Ordering::Relaxed) > 0
                        && ready.idle() =>
                {
                    Some(ready)
                }
                _ => None,
            })
            .min_by_key(|ready| ready.last_used_ms())
        {
            victim.retire_idle();
        }
    }
}
#[derive(Default)]
struct PoolState {
    slots: HashMap<VmKey, Slot>,
    /// Deployments being replaced: no new leases, shut down once idle.
    retiring: HashSet<(ApplicationId, Name)>,
    snapshot: VmPoolSnapshot,
    closed: bool,
}

/// A leased VM. Live work prevents expiry; an unused lease does not extend retention.
pub(crate) struct VmLease {
    client: VmClient,
    _lease: Option<Arc<()>>,
}

impl Drop for VmLease {
    fn drop(&mut self) {
        drop(self._lease.take());
        self.client.supervisor.lease_dropped();
    }
}

impl std::ops::Deref for VmLease {
    type Target = VmClient;
    fn deref(&self) -> &VmClient {
        &self.client
    }
}

/// Cold launch, warm reuse, idle/LRU eviction, deployment retirement and
/// shutdown for one node. Capacity is reserved before launch and released
/// only after the VM's jailer/VMM is reaped; an unproved reap keeps it charged.
pub(crate) struct VmPool {
    service_routes: std::sync::OnceLock<std::sync::Weak<Routes>>,
    config: VmHostConfig,
    store: Arc<ApplicationStore>,
    state: Arc<Mutex<PoolState>>,
    changed: Arc<tokio::sync::Notify>,
    templates: Arc<Templates>,
    /// Cancelled at shutdown: ends the refill and any restore in flight.
    stop: CancellationToken,
    pressure: Arc<Mutex<pressure::Guard>>,
    _sweeper: AbortOnDropHandle<()>,
    /// Replaces broken idle VMs and refills the pool; only with `startup_pool`.
    _warmer: Option<AbortOnDropHandle<()>>,
}

/// An owned startup task holds capacity through cancellation or panic.
struct StartupCharge {
    state: Arc<Mutex<PoolState>>,
    changed: Arc<tokio::sync::Notify>,
    memory: u32,
    proved: bool,
}

impl StartupCharge {
    fn finish(mut self, proved: bool) {
        self.proved = proved;
    }
}

impl Drop for StartupCharge {
    fn drop(&mut self) {
        VmPool::release_startup(&self.state, self.memory, self.proved);
        self.changed.notify_waiters();
    }
}

impl VmPool {
    /// Startup is sequential and charged like a handler VM until its reap.
    async fn prepare(&self, app: &crate::actor::AppHandle) -> Result<()> {
        self.templates.begin_startup();
        let began = Instant::now();
        for runtime in &self.config.runtimes {
            for class in ApplicationSizeClass::ALL {
                let mut options = self
                    .config
                    .options(&runtime.sha256, self.templates.clone())?;
                (options.vcpus, options.memory_mib, options.cpu_quota) = class.resources();
                let charge = self.charge_startup(options.memory_mib).await?;
                let templates = self.templates.clone();
                // The owner finishes the capture and reap even if startup is cancelled.
                tokio::spawn(async move {
                    let built = templates.build_template(&options).await;
                    charge.finish(
                        !built
                            .as_ref()
                            .is_err_and(|error| error.is::<ReapUnproved>()),
                    );
                    built.map(drop)
                })
                .await
                .context(ReapUnproved)?
                .with_context(|| {
                    format!(
                        "startup template failed for {class:?} runtime {}",
                        runtime.sha256
                    )
                })?;
            }
            // Always prepare Python: later activation must never build on a request.
            let options = self
                .config
                .options(&runtime.sha256, self.templates.clone())?;
            let charge = self.charge_startup(options.memory_mib).await?;
            let (templates, app) = (self.templates.clone(), app.clone());
            tokio::spawn(async move {
                let built = templates.build_pyodide(&options, &app).await;
                charge.finish(
                    !built
                        .as_ref()
                        .is_err_and(|error| error.is::<ReapUnproved>()),
                );
                built.map(drop)
            })
            .await
            .context(ReapUnproved)??;
        }
        ensure!(
            !self.stop.is_cancelled(),
            "application startup was shut down"
        );
        self.templates.finish_startup();
        tracing::info!(
            event = "application_caches_ready",
            ms = began.elapsed().as_secs_f64() * 1000.0,
            runtimes = self.config.runtimes.len(),
            classes = ApplicationSizeClass::ALL.len(),
            "application startup caches ready"
        );
        Ok(())
    }

    async fn charge_startup(&self, memory: u32) -> Result<StartupCharge> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            Self::refresh_pressure(&self.pressure, &self.templates);
            {
                let mut state = self.state.lock().expect("VM pool poisoned");
                ensure!(
                    !state.closed && !self.stop.is_cancelled(),
                    "application startup was shut down"
                );
                if !Self::under_pressure(&self.pressure) {
                    ensure!(
                        self.config.room(
                            &state.snapshot,
                            self.templates.idle_capacity(),
                            1,
                            u64::from(memory)
                        ),
                        "startup cache VM exceeds node capacity"
                    );
                    state.snapshot.vms += 1;
                    state.snapshot.starting += 1;
                    state.snapshot.memory_mib += u64::from(memory);
                    return Ok(StartupCharge {
                        state: self.state.clone(),
                        changed: self.changed.clone(),
                        memory,
                        proved: false,
                    });
                }
            }
            crate::asyncrt::select! {
                result = tokio::time::timeout_at(deadline, &mut changed) => {
                    result.context("application startup memory pressure deadline exceeded")?;
                },
                _ = self.stop.cancelled() => anyhow::bail!("application startup was shut down"),
            }
        }
    }

    fn release_startup(state: &Mutex<PoolState>, memory: u32, proved: bool) {
        let mut state = state.lock().expect("VM pool poisoned");
        state.snapshot.starting -= 1;
        if proved {
            state.snapshot.vms -= 1;
            state.snapshot.memory_mib -= u64::from(memory);
        } else {
            state.snapshot.quarantined += 1;
        }
    }

    fn settled(&self) -> bool {
        let state = self.state.lock().expect("VM pool poisoned");
        state.slots.values().all(|slot| match slot {
            Slot::Ready(ready) => ready.idle(),
            Slot::Starting(_) => false,
        })
    }

    pub(crate) fn new(config: VmHostConfig, store: Arc<ApplicationStore>) -> Result<Self> {
        config.validate()?;
        store.enable_artifact_cache(crate::applications::ArtifactCache::new(
            Duration::from_secs(config.artifact_cache_ttl_secs.unwrap_or(600)),
        )?);
        let state = Arc::new(Mutex::new(PoolState::default()));
        let changed = Arc::new(tokio::sync::Notify::new());
        let ttl = Duration::from_secs(config.idle_ttl_secs);
        let poll = (ttl / 4).clamp(Duration::from_millis(250), Duration::from_secs(1));
        let pressure = Arc::new(Mutex::new(pressure::Guard::new(
            config
                .memory_guard
                .map(|guard| (guard.high_bytes, guard.max_bytes)),
            poll,
        )));
        let mut templates = Templates::new()?;
        if let Some(guard) = config.memory_guard {
            templates.initialize_memory(crate::application_template::cgroup::Budget {
                high_bytes: guard.high_bytes,
                max_bytes: guard.max_bytes,
                overhead_bytes: guard.overhead_bytes,
            })?;
        }
        let templates = Arc::new(templates);
        let sweeper = {
            let state = Arc::downgrade(&state);
            let changed = changed.clone();
            let store = store.clone();
            let templates = templates.clone();
            let pressure = pressure.clone();
            AbortOnDropHandle::new(tokio::spawn(async move {
                loop {
                    // A retiring deployment's VM leaves once idle, so the tick stays short.
                    // ponytail: a pool-wide poll; a lease-drop wakeup if many VMs make it costly.
                    tokio::time::sleep(poll).await;
                    let Some(state) = state.upgrade() else { return };
                    Self::refresh_pressure(&pressure, &templates);
                    Self::evict_pressure(&state, &templates, &changed, &pressure);
                    Self::sweep(&mut state.lock().expect("VM pool poisoned"), ttl);
                    changed.notify_waiters();
                    store.sweep_artifact_cache().await;
                }
            }))
        };
        let stop = CancellationToken::new();
        let warmer = (config.startup_pool > 0).then(|| {
            AbortOnDropHandle::new(tokio::spawn(Self::warm(
                config.clone(),
                Arc::downgrade(&state),
                changed.clone(),
                templates.clone(),
                pressure.clone(),
                stop.clone(),
            )))
        });
        Ok(Self {
            config,
            service_routes: Default::default(),
            store,
            state,
            changed,
            templates,
            stop,
            pressure,
            _sweeper: sweeper,
            _warmer: warmer,
        })
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> VmPoolSnapshot {
        self.state.lock().expect("VM pool poisoned").snapshot
    }

    /// What node status reports: the capacity charged to handler VMs, and the
    /// startup pool with its hits and misses and each template's state.
    pub(crate) fn status(&self) -> serde_json::Value {
        let memory_guard =
            self.config
                .memory_guard
                .map(|limits| match self.templates.memory_sample() {
                    Ok(Some(sample)) => serde_json::json!({"limits": limits, "sample": sample}),
                    Ok(None) | Err(_) => {
                        serde_json::json!({"limits": limits, "sampleFailed": true})
                    }
                });
        let (snapshot, phases) = {
            let state = self.state.lock().expect("VM pool poisoned");
            let mut phases = [0u32; 5];
            for slot in state.slots.values() {
                if let Slot::Ready(ready) = slot {
                    let phase = ready
                        .gate
                        .as_ref()
                        .map_or(lifecycle::Phase::Running, |gate| gate.status().phase);
                    let index = match phase {
                        lifecycle::Phase::Running => 0,
                        lifecycle::Phase::Pausing => 1,
                        lifecycle::Phase::Cached => 2,
                        lifecycle::Phase::Resuming => 3,
                        lifecycle::Phase::Retiring => 4,
                    };
                    phases[index] += 1;
                }
            }
            (state.snapshot, phases)
        };
        let idle = self.templates.idle_capacity();
        serde_json::json!({
            "vms": snapshot.vms + idle.0,
            "memoryMib": snapshot.memory_mib + idle.1,
            "memoryGuard": memory_guard,
            "memoryReservations": {
                "fullExecutionMib": snapshot.memory_mib + idle.1 - snapshot.cached_nominal_mib,
                "retainedLimitBytes": snapshot.retained_limit_bytes,
                "retainedVms": snapshot.cached_vms,
                "cachedNominalMib": snapshot.cached_nominal_mib,
                "retainedOverheadBytes": self.config.memory_guard.map(|guard|
                    u64::from(snapshot.cached_vms) * guard.overhead_bytes),
                "physicalBytes": self.config.memory_guard.and_then(|guard|
                    snapshot.physical_bytes(guard, snapshot.memory_mib + idle.1, snapshot.vms + idle.0)),
            },
            "memoryPressureBlocked": Self::under_pressure(&self.pressure),
            "lifecycle": {"running": phases[0], "pausing": phases[1], "cached": phases[2],
                "resuming": phases[3], "retiring": phases[4]},
            "starting": snapshot.starting,
            "ready": snapshot.ready,
            "quarantined": snapshot.quarantined,
            "startupPool": {"size": self.config.startup_pool, "pool": self.templates.status()},
        })
    }

    /// Kill and reap an idle VM in the background; its capacity returns when
    /// the reap is proved and stays charged otherwise.
    fn reap(&self, idle: Idle) {
        Self::reap_idle(idle, self.templates.clone(), self.changed.clone());
    }

    fn reap_idle(idle: Idle, templates: Arc<Templates>, changed: Arc<tokio::sync::Notify>) {
        tokio::spawn(async move {
            let memory = idle.memory_mib;
            let reaped = idle.reap().await;
            if let Err(error) = &reaped {
                tracing::error!(error = %format!("{error:#}"), "idle application VM reap unproved; capacity quarantined");
            }
            templates.reaped(memory, reaped.is_ok());
            changed.notify_waiters();
        });
    }

    fn under_pressure(pressure: &Mutex<pressure::Guard>) -> bool {
        pressure
            .lock()
            .expect("VM pressure guard poisoned")
            .blocked(Instant::now())
    }

    /// Cgroup and proc reads must precede acquiring PoolState.
    fn refresh_pressure(pressure: &Mutex<pressure::Guard>, templates: &Templates) {
        if !pressure
            .lock()
            .expect("VM pressure guard poisoned")
            .enabled()
        {
            return;
        }
        let sample = templates
            .memory_sample()
            .and_then(|sample| sample.context("guarded VM pool memory sampler absent"))
            .and_then(|memory| {
                Ok(pressure::Sample {
                    memory,
                    available_bytes: pressure::host_available()?,
                })
            });
        pressure
            .lock()
            .expect("VM pressure guard poisoned")
            .observe(Instant::now(), sample.ok());
    }

    fn evict_pressure(
        state: &Mutex<PoolState>,
        templates: &Arc<Templates>,
        changed: &Arc<tokio::sync::Notify>,
        pressure: &Mutex<pressure::Guard>,
    ) {
        if !Self::under_pressure(pressure) {
            return;
        }
        // Startup guests go first; their existing charge lasts through proven reap.
        for idle in templates.evict_all() {
            Self::reap_idle(idle, templates.clone(), changed.clone());
        }
        if templates.winding_down().0 > 0 {
            return;
        }
        let state = state.lock().expect("VM pool poisoned");
        if state.closed
            || state
                .slots
                .values()
                .any(|slot| matches!(slot, Slot::Ready(ready) if ready.retire.is_cancelled()))
        {
            return;
        }
        if let Some(victim) = state
            .slots
            .values()
            .filter_map(|slot| match slot {
                Slot::Ready(ready)
                    if ready.idle()
                        && ready
                            .gate
                            .as_ref()
                            .is_none_or(|gate| !gate.status().resume_requested) =>
                {
                    Some(ready)
                }
                _ => None,
            })
            .min_by_key(|ready| ready.last_used_ms())
        {
            // Gate closes admission atomically; a raced retained client is protected.
            victim.retire_idle();
        }
    }

    /// Every tick: reap idle VMs that broke (VMM exit, agent disconnect), then
    /// restore replacements for templates below `startup_pool`, one VM of room
    /// always left to launches. A failed restore backs its template off.
    // ponytail: a poll like the sweeper; wake on `changed` if the delay to refill matters.
    async fn warm(
        config: VmHostConfig,
        state: std::sync::Weak<Mutex<PoolState>>,
        changed: Arc<tokio::sync::Notify>,
        templates: Arc<Templates>,
        pressure: Arc<Mutex<pressure::Guard>>,
        stop: CancellationToken,
    ) {
        let reserved_memory = u64::from(ApplicationSizeClass::default().resources().1);
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let Some(state) = state.upgrade() else { return };
            Self::refresh_pressure(&pressure, &templates);
            for idle in templates.evict_broken() {
                tracing::warn!(
                    event = "vm_pool_idle_broken",
                    pid = idle.jail.pid,
                    "idle application VM broke; replacing it"
                );
                Self::reap_idle(idle, templates.clone(), changed.clone());
            }
            loop {
                let template = {
                    let state = state.lock().expect("VM pool poisoned");
                    // This VM and one more for a launch.
                    if state.closed
                        || !templates.ready()
                        || Self::under_pressure(&pressure)
                        || !config.room(
                            &state.snapshot,
                            templates.idle_capacity(),
                            2,
                            reserved_memory,
                        )
                    {
                        break;
                    }
                    let Some(template) = templates.begin_fill(
                        config.startup_pool,
                        config.max_memory_mib
                            - state.snapshot.memory_mib
                            - templates.idle_capacity().1
                            - reserved_memory,
                    ) else {
                        break;
                    };
                    template
                };
                let (config, templates, changed, stop, pressure) = (
                    config.clone(),
                    templates.clone(),
                    changed.clone(),
                    stop.clone(),
                    pressure.clone(),
                );
                tokio::spawn(async move {
                    let filled = async {
                        Self::refresh_pressure(&pressure, &templates);
                        ensure!(
                            !Self::under_pressure(&pressure),
                            "application VM memory pressure blocks refill"
                        );
                        let mut options =
                            config.options(&template.runtime_sha256, templates.clone())?;
                        (options.vcpus, options.memory_mib, options.cpu_quota) =
                            (template.vcpus, template.memory_mib, template.cpu_quota);
                        boot(&options, Some(&template), stop.cancelled()).await
                    }
                    .await;
                    if let Err(error) = &filled {
                        tracing::error!(event = "vm_pool_fill_failed", error = %format!("{error:#}"), "idle application VM restore failed");
                    }
                    let proved = !filled
                        .as_ref()
                        .is_err_and(|error| error.is::<ReapUnproved>());
                    templates.end_fill(&template.key, filled.ok(), !stop.is_cancelled(), proved);
                    changed.notify_waiters();
                });
            }
        }
    }

    /// Retire idle VMs past their TTL, idle VMs of retiring deployments and
    /// idle VMs whose guest condemned itself.
    fn sweep(state: &mut PoolState, ttl: Duration) {
        state.retiring.retain(|(app, deployment)| {
            state
                .slots
                .keys()
                .any(|key| &key.0 == app && &key.1 == deployment)
        });
        let now = Instant::now();
        let retiring = state.retiring.clone();
        let live: HashSet<_> = state
            .slots
            .iter()
            .filter(|&(_, slot)| matches!(slot, Slot::Ready(ready) if !ready.idle()))
            .map(|((app, deployment, _), _)| (app.clone(), deployment.clone()))
            .collect();
        for ((application, deployment, _), slot) in &mut state.slots {
            let Slot::Ready(ready) = slot else { continue };
            let idle = ready.idle();
            if ready.gate.is_none() && !idle {
                // Older guests retain the existing running-only idle policy.
                ready.idle_since = now;
            }
            let expired = if ready.gate.is_some() {
                ready.ineligible()
            } else {
                idle && now.duration_since(ready.idle_since) >= ttl
            };
            if expired {
                ready.retire.cancel();
            } else if idle
                && (ready.client.transport.is_condemned()
                    || (retiring.contains(&(application.clone(), deployment.clone()))
                        && !live.contains(&(application.clone(), deployment.clone()))))
            {
                ready.retire_idle();
            }
        }
    }

    /// A lease on this pinned handler's VM. The caller has already resolved
    /// application, deployment and handler; the pool cannot choose another.
    pub(crate) async fn acquire(
        &self,
        app: &crate::actor::AppHandle,
        application: &ApplicationId,
        deployment: &Name,
        handler: &Name,
    ) -> Result<VmLease> {
        self.acquire_inner(app, application, deployment, handler, false)
            .await
    }

    async fn acquire_inner(
        &self,
        app: &crate::actor::AppHandle,
        application: &ApplicationId,
        deployment: &Name,
        handler: &Name,
        pinned_service: bool,
    ) -> Result<VmLease> {
        ensure!(!app.is_draining(), "node is draining");
        ensure!(
            self.templates.ready(),
            "application startup caches are not ready"
        );
        let snapshot = self.store.read(application).await?;
        let current = snapshot
            .application(application)
            .context("application does not exist")?;
        if !pinned_service && current.active.as_ref().map(|active| &active.id) != Some(deployment) {
            return Err(DeploymentRetiring.into());
        }
        let limits = current.limits;
        let size_class = limits.map_or(ApplicationSizeClass::default(), |limits| limits.size_class);
        let memory = u64::from(size_class.resources().1);
        let key: VmKey = (application.clone(), deployment.clone(), handler.clone());
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if !pinned_service {
                let latest = self.store.read(application).await?;
                let current = latest
                    .application(application)
                    .context("application does not exist")?;
                if current.active.as_ref().map(|active| &active.id) != Some(deployment) {
                    return Err(DeploymentRetiring.into());
                }
                ensure!(
                    current
                        .limits
                        .map_or(ApplicationSizeClass::default(), |limits| limits.size_class)
                        == size_class,
                    "application size changed during admission; retry"
                );
            }
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            Self::refresh_pressure(&self.pressure, &self.templates);
            let waiting = {
                let mut state = self.state.lock().expect("VM pool poisoned");
                ensure!(!state.closed, "application VM pool is shut down");
                if !pinned_service
                    && state
                        .retiring
                        .contains(&(application.clone(), deployment.clone()))
                {
                    return Err(DeploymentRetiring.into());
                }
                match state.slots.get_mut(&key) {
                    // A condemned VM leases nothing more; the sweep retires it
                    // once idle and the key then launches a fresh one.
                    Some(Slot::Ready(ready)) => {
                        if let Some(lease) = ready.try_lease() {
                            if ready.gate.is_none() {
                                ready.idle_since = Instant::now();
                            }
                            return Ok(lease);
                        }
                        // A retiring VM is reaped before its key launches again.
                        // Pausing/Resuming wait for the owner to commit or retire.
                        if ready.ineligible() {
                            ready.retire.cancel();
                        }
                        None
                    }
                    Some(Slot::Starting(startup)) => Some(startup.clone()),
                    None if Self::under_pressure(&self.pressure) => None,
                    None => {
                        // Idle pool VMs hold capacity until they are claimed or reaped.
                        if self.config.room(
                            &state.snapshot,
                            self.templates.idle_capacity(),
                            1,
                            memory,
                        ) {
                            state.snapshot.vms += 1;
                            state.snapshot.memory_mib += memory;
                            state.snapshot.starting += 1;
                            let (publish, startup) = tokio::sync::watch::channel(None);
                            state
                                .slots
                                .insert(key.clone(), Slot::Starting(startup.clone()));
                            tokio::spawn(Self::start(
                                app.clone(),
                                self.config.clone(),
                                self.store.clone(),
                                self.state.clone(),
                                self.changed.clone(),
                                self.templates.clone(),
                                self.pressure.clone(),
                                key.clone(),
                                publish,
                                self.service_routes.get().cloned().unwrap_or_default(),
                                pinned_service,
                                size_class,
                            ));
                            Some(startup)
                        } else if let Some(idle) = self.templates.evict() {
                            // Idle pool VMs go before any VM that has served. ponytail: a
                            // full node restores for this launch instead of claiming one;
                            // claim it in place if pool hits at capacity matter.
                            self.reap(idle);
                            None
                        } else {
                            // Least recently used idle VM first. Its capacity
                            // returns only after its reap.
                            let victim = state
                                .slots
                                .values()
                                .filter_map(|slot| match slot {
                                    Slot::Ready(ready) if ready.idle() => Some(ready),
                                    _ => None,
                                })
                                .min_by_key(|ready| ready.last_used_ms());
                            let retiring = state.slots.values().any(|slot| {
                                matches!(slot, Slot::Ready(ready) if ready.retire.is_cancelled())
                            }) || self.templates.winding_down().0 > 0;
                            match victim {
                                Some(victim) => {
                                    victim.retire_idle();
                                }
                                None if retiring => {}
                                None => return Err(CapacityExhausted.into()),
                            }
                            None
                        }
                    }
                }
            };
            match waiting {
                Some(mut startup) => {
                    let result = timeout(
                        deadline.saturating_duration_since(Instant::now()),
                        startup.wait_for(Option::is_some),
                    )
                    .await
                    .context("application VM startup timed out")?
                    .map_err(|_| anyhow::anyhow!("application VM startup stopped"))?
                    .clone()
                    .expect("published startup");
                    let client = result.map_err(anyhow::Error::msg)?;
                    {
                        let state = self.state.lock().expect("VM pool poisoned");
                        if let Some(Slot::Ready(ready)) = state.slots.get(&key) {
                            if Arc::ptr_eq(&ready.client.pin, &client.pin) {
                                if let Some(lease) = ready.try_lease() {
                                    return Ok(lease);
                                }
                            }
                        }
                    }
                    // A startup may publish while its owner is Pausing. Do not
                    // spin on the already-populated watch value.
                    timeout(deadline.saturating_duration_since(Instant::now()), changed)
                        .await
                        .context("application VM lifecycle did not settle in time")?;
                }
                None => {
                    timeout(deadline.saturating_duration_since(Instant::now()), changed)
                        .await
                        .context("application VM capacity did not free in time")?;
                }
            }
        }
    }

    /// The startup and monitor task owns the VM from launch until its reap,
    /// so a cancelled caller cannot abandon a partial jailer/VMM.
    // Each parameter names one independent input of a VM startup.
    #[allow(clippy::too_many_arguments)]
    async fn start(
        app: crate::actor::AppHandle,
        config: VmHostConfig,
        store: Arc<ApplicationStore>,
        state: Arc<Mutex<PoolState>>,
        changed: Arc<tokio::sync::Notify>,
        templates: Arc<Templates>,
        pressure: Arc<Mutex<pressure::Guard>>,
        key: VmKey,
        publish: tokio::sync::watch::Sender<Option<Result<VmClient, String>>>,
        service_routes: std::sync::Weak<Routes>,
        pinned_service: bool,
        size_class: ApplicationSizeClass,
    ) {
        let memory = u64::from(size_class.resources().1);
        let began = Instant::now();
        let (mut checked, mut launched_at) = (Duration::ZERO, Duration::ZERO);
        let launched = async {
            let manifest = store
                .deployment(&key.0, &key.1)
                .await?
                .context("deployment is not staged")?;
            let handler = manifest
                .handlers
                .iter()
                .find(|handler| handler.id == key.2)
                .context("handler is not in staged deployment")?;
            let mut options = config.options(&handler.runtime_image_sha256, templates.clone())?;
            let catalog = store.read(&key.0).await?;
            let current = catalog
                .application(&key.0)
                .context("application does not exist")?;
            if !pinned_service && current.active.as_ref().map(|active| &active.id) != Some(&key.1) {
                return Err(DeploymentRetiring.into());
            }
            let actual = current
                .limits
                .map_or(ApplicationSizeClass::default(), |limits| limits.size_class);
            ensure!(
                actual == size_class,
                "application size changed during admission; retry"
            );
            (options.vcpus, options.memory_mib, options.cpu_quota) = size_class.resources();
            // A Python handler uses only the startup-prepared shared drive.
            if crate::application_pyodide::needs_runtime(handler)? {
                options.shared = Some(templates.pyodide(&options, &app).await?);
            }
            checked = began.elapsed();
            let vm = Vm::launch(options, &store, &key.0, &key.1, &key.2).await?;
            launched_at = began.elapsed();
            let leases = Arc::new(());
            let client = vm.client();
            let configured = (|| -> Result<_> {
                if !assigned_supervisor::enabled(&client.transport) {
                    return Ok(None);
                }
                let gate = lifecycle::Gate::new(Duration::from_secs(config.idle_ttl_secs))?;
                client.transport.install_execution_gate(gate.clone())?;

                Ok(Some(gate))
            })();
            match configured {
                Ok(gate) => Ok((vm, gate, leases)),
                Err(error) => {
                    vm.shutdown().await.context(ReapUnproved)?;
                    Err(error)
                }
            }
        }
        .await;
        let (vm, gate, leases) = match launched {
            Ok(vm) => vm,
            Err(error) => {
                tracing::error!(error = %format!("{error:#}"), "application VM launch failed");
                // A failed launch has already killed and waited for its VMM.
                let mut state = state.lock().expect("VM pool poisoned");
                state.slots.remove(&key);
                if error.is::<ReapUnproved>() {
                    state.snapshot.quarantined += 1;
                } else {
                    state.snapshot.vms -= 1;
                    state.snapshot.memory_mib -= memory;
                }
                state.snapshot.starting -= 1;
                drop(state);
                let _ = publish.send(Some(Err(format!("{error:#}"))));
                changed.notify_waiters();
                return;
            }
        };
        let client = vm.client();
        client
            .transport
            .bind_service_routes(service_routes)
            .expect("a fresh service router is unbound");
        let retire = CancellationToken::new();
        let retained_limit = Arc::new(AtomicU64::new(0));
        let assignment = gate.as_ref().map(|gate| assigned_supervisor::Assignment {
            gate: gate.clone(),
            leases: Arc::downgrade(&leases),
            changed: changed.clone(),
            cache: config
                .memory_guard
                .and_then(|guard| guard.idle_cache)
                .map(|_| {
                    Box::new(CacheAdmission {
                        state: Arc::downgrade(&state),
                        key: key.clone(),
                        pin: client.pin.clone(),
                        retained_limit: retained_limit.clone(),
                        nominal_mib: memory,
                        config: config.clone(),
                        templates: templates.clone(),
                        pressure,
                        changed: changed.clone(),
                    })
                }),
        });
        {
            let mut state = state.lock().expect("VM pool poisoned");
            state.snapshot.starting -= 1;
            state.snapshot.ready += 1;
            // Only a trusted source-binding acquisition may publish a cold
            // callee in an old deployment, while that source's lease/event is
            // still alive. Public acquisition remains refused by acquire().
            let live_source = state
                .slots
                .iter()
                .any(|((application, deployment, _), slot)| {
                    application == &key.0
                        && deployment == &key.1
                        && matches!(slot, Slot::Ready(ready) if !ready.idle())
                });
            if state.closed
                || (state.retiring.contains(&(key.0.clone(), key.1.clone()))
                    && !(pinned_service && live_source))
            {
                retire.cancel();
            }
            state.slots.insert(
                key.clone(),
                Slot::Ready(Ready {
                    client: client.clone(),
                    leases,
                    idle_since: Instant::now(),
                    gate,
                    retained_limit: retained_limit.clone(),
                    retire: retire.clone(),
                }),
            );
        }
        // The budget record exists before the supervisor can grant cache credit.
        if let Some(assignment) = assignment {
            if let Err(error) = client.supervisor.install(assignment) {
                tracing::warn!(error=%format!("{error:#}"), "VM lifecycle assignment failed");
                client.transport.retire();
                retire.cancel();
            }
        }
        let _ = publish.send(Some(Ok(client.clone())));
        changed.notify_waiters();
        let ms = |elapsed: Duration| elapsed.as_secs_f64() * 1000.0;
        tracing::info!(
            event = "vm_acquire",
            check_ms = ms(checked),
            launch_ms = ms(launched_at - checked),
            assign_ms = ms(began.elapsed() - launched_at),
            "application VM published"
        );
        crate::asyncrt::select! {
            _ = retire.cancelled() => {}
            _ = client.stop.cancelled() => {}
        }
        retire.cancel();
        let reaped = vm.shutdown().await;
        let mut state = state.lock().expect("VM pool poisoned");
        if matches!(state.slots.get(&key), Some(Slot::Ready(ready)) if Arc::ptr_eq(&ready.client.pin, &client.pin))
        {
            state.slots.remove(&key);
        }
        state.snapshot.ready -= 1;
        match reaped {
            Ok(_) => {
                let limit = retained_limit.load(Ordering::Relaxed);
                if limit > 0 {
                    state
                        .snapshot
                        .full(memory, limit)
                        .expect("owned VM cache charge");
                }
                state.snapshot.vms -= 1;
                state.snapshot.memory_mib -= memory;
            }
            Err(error) => {
                tracing::error!(error = %format!("{error:#}"), "application VM reap unproved; capacity quarantined");
                state.snapshot.quarantined += 1;
            }
        }
        drop(state);
        changed.notify_waiters();
    }

    /// Replacement or deactivation: no new leases for this deployment; each of
    /// its VMs shuts down once its leases and guest events settle.
    pub(crate) async fn retire_deployment(&self, application: &ApplicationId, deployment: &Name) {
        {
            let mut state = self.state.lock().expect("VM pool poisoned");
            state
                .retiring
                .insert((application.clone(), deployment.clone()));
            Self::sweep(&mut state, Duration::from_secs(self.config.idle_ttl_secs));
        }
        self.changed.notify_waiters();
    }

    /// Rollback: a deployment that is active again may lease and launch.
    pub(crate) fn readmit_deployment(&self, application: &ApplicationId, deployment: &Name) {
        self.state
            .lock()
            .expect("VM pool poisoned")
            .retiring
            .remove(&(application.clone(), deployment.clone()));
    }

    /// Refuse new leases, stop every VM and wait for each reap. Fails if any
    /// reap could not be proved.
    pub(crate) async fn shutdown(&self) -> Result<()> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let snapshot = {
                let mut state = self.state.lock().expect("VM pool poisoned");
                state.closed = true;
                for slot in state.slots.values() {
                    if let Slot::Ready(ready) = slot {
                        ready.retire.cancel();
                    }
                }
                state.snapshot
            };
            // No more refill; idle VMs, including any that finish restoring
            // meanwhile, are reaped.
            self.stop.cancel();
            for idle in self.templates.evict_all() {
                self.reap(idle);
            }
            let (idle, stuck) = self.templates.winding_down();
            if snapshot.vms == snapshot.quarantined && idle == 0 {
                ensure!(
                    snapshot.quarantined + stuck == 0,
                    "application VM reap unproved for {} VMs",
                    snapshot.quarantined + stuck
                );
                self.templates.discard().await?;
                return Ok(());
            }
            changed.await;
        }
    }
}

const RECONCILE_PERIOD: Duration = Duration::from_secs(1);

/// The guest harness's reserved entrypoint for a scheduled event. A leading `.`
/// cannot start a JavaScript class name, so no export collides with it.
const SCHEDULED_ENTRYPOINT: &str = ".scheduled";

/// What a trusted application `__Queue` cell may know about itself. The
/// catalog is keyed by the cell's scope, which authenticates the application
/// and the queue resource, so same-named queues of two applications never share
/// an entry.
pub(crate) struct QueueEntry {
    pub application: ApplicationId,
    /// The active deployment this entry was built from; a batch runs in it.
    pub deployment: Name,
    /// The consuming handler of the active deployment and its original
    /// settings, absent for a queue nothing consumes (producer or DLQ only).
    pub consumer: Option<(Name, crate::protocol::QueueConsumerConfig)>,
    /// The namespace key of the dead-letter queue's resource.
    pub dead_letter_key: Option<String>,
}

impl QueueEntry {
    /// The consumer settings the harness reads, with the dead-letter key.
    pub(crate) fn consumer_wire(&self) -> Option<serde_json::Value> {
        let (handler, config) = self.consumer.as_ref()?;
        Some(crate::queue_policy::queue_consumer_wire(
            handler.as_str(),
            config,
            self.dead_letter_key.as_deref(),
        ))
    }
}

/// An entry for every registered Queue resource of an application with an active
/// deployment: a dead-letter-only queue has no bound handler but still needs one.
/// An invalid or contested consumer declaration leaves its queue unconsumed.
fn queue_catalog<'a>(
    applications: impl Iterator<Item = &'a Application>,
) -> HashMap<String, Arc<QueueEntry>> {
    let mut catalog = HashMap::new();
    for app in applications {
        let Some(deployment) = &app.active else {
            continue;
        };
        let registered: HashSet<_> = app
            .resources
            .iter()
            .map(|item| item.resource.clone())
            .collect();
        let mut consumers = HashMap::new();
        let mut invalid = HashSet::new();
        for handler in &deployment.handlers {
            match crate::application_javascript_guest::queue_consumers(handler, &registered) {
                Ok(declared) => {
                    for config in declared {
                        // Two handlers claiming one queue: neither consumes it.
                        let queue = config.queue.clone();
                        let claim = Some((handler.id.clone(), config));
                        if consumers.insert(queue.clone(), claim).is_some() {
                            consumers.insert(queue, None);
                        }
                    }
                }
                Err(error) => {
                    invalid.insert(handler.id.clone());
                    tracing::error!(
                        application = ?app.id, handler = handler.id.as_str(),
                        error = %format!("{error:#}"), "invalid queue consumers; handler consumes nothing"
                    )
                }
            }
        }
        // A queue trigger picks the consuming handler. Its settings are that
        // handler's own `queue_consumers` entry, else the original defaults;
        // a second claimant of the queue, by either route, contests it.
        for trigger in &deployment.triggers {
            let celld_applications::Trigger::Queue { queue, handler } = trigger else {
                continue;
            };
            match consumers.get(queue.as_str()) {
                Some(Some((owner, _))) if owner == handler => {}
                Some(_) => {
                    consumers.insert(queue.as_str().to_owned(), None);
                }
                None if !invalid.contains(handler) => {
                    let config = crate::protocol::QueueConsumerConfig {
                        queue: queue.as_str().to_owned(),
                        max_batch_size: celld_logic::queue::DEFAULT_MAX_BATCH_SIZE,
                        max_batch_timeout: celld_logic::queue::DEFAULT_MAX_BATCH_TIMEOUT_SECONDS,
                        max_retries: celld_logic::queue::DEFAULT_MAX_RETRIES,
                        dead_letter_queue: None,
                        max_concurrency: Some(
                            crate::application_guest_transport::QUEUE_MAX_CONCURRENCY,
                        ),
                        retry_delay: None,
                    };
                    consumers.insert(config.queue.clone(), Some((handler.clone(), config)));
                }
                None => {}
            }
        }
        for item in app
            .resources
            .iter()
            .filter(|item| item.resource.kind == ResourceKind::Queue)
        {
            let address = ResourceAddress {
                application: app.id.clone(),
                resource: item.resource.clone(),
            };
            let name = celld_logic::queue::cell_name(item.resource.id.as_str());
            let Ok(scope) = address.cell_scope(celld_logic::queue::RESERVED_CLASS, name) else {
                continue;
            };
            let consumer = consumers.get(name).cloned().flatten();
            let dead_letter_key = consumer
                .as_ref()
                .and_then(|(_, config)| config.dead_letter_queue.clone())
                .and_then(|queue| Name::try_from(queue).ok())
                .map(|id| {
                    ResourceAddress {
                        application: app.id.clone(),
                        resource: ResourceRef {
                            kind: ResourceKind::Queue,
                            id,
                        },
                    }
                    .namespace_key(celld_logic::queue::RESERVED_CLASS)
                });
            catalog.insert(
                scope,
                Arc::new(QueueEntry {
                    application: app.id.clone(),
                    deployment: deployment.id.clone(),
                    consumer,
                    dead_letter_key,
                }),
            );
        }
    }
    catalog
}

/// What a trusted application `.cron` cell may know about itself, keyed by the
/// cell's scope as `QueueEntry` is. Every registered Cron resource has an entry,
/// so a cell whose trigger was removed still starts, sees no schedule and retires.
pub(crate) struct CronEntry {
    pub application: ApplicationId,
    /// The trigger of the active deployment; absent once removed or deactivated.
    pub schedule: Option<CronSchedule>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CronSchedule {
    pub deployment: Name,
    pub handler: Name,
    pub expression: String,
}

fn cron_catalog<'a>(
    applications: impl Iterator<Item = &'a Application>,
) -> HashMap<String, Arc<CronEntry>> {
    let mut catalog = HashMap::new();
    for app in applications {
        for item in app
            .resources
            .iter()
            .filter(|item| item.resource.kind == ResourceKind::Cron)
        {
            let address = ResourceAddress {
                application: app.id.clone(),
                resource: item.resource.clone(),
            };
            let Ok(scope) =
                address.cell_scope(celld_logic::cron::RESERVED_CLASS, item.resource.id.as_str())
            else {
                continue;
            };
            let schedule = app.active.as_ref().and_then(|deployment| {
                deployment
                    .triggers
                    .iter()
                    .find_map(|trigger| match trigger {
                        celld_applications::Trigger::Cron {
                            id,
                            expression,
                            handler,
                        } if id == &item.resource.id => Some(CronSchedule {
                            deployment: deployment.id.clone(),
                            handler: handler.clone(),
                            expression: expression.clone(),
                        }),
                        _ => None,
                    })
            });
            catalog.insert(
                scope,
                Arc::new(CronEntry {
                    application: app.id.clone(),
                    schedule,
                }),
            );
        }
    }
    catalog
}

#[cfg(test)]
#[path = "unit_tests/application_vm/tests.rs"]
pub(crate) mod tests;

/// Ownership remains application storage when its executable deployment is absent.
pub(crate) struct ScopeMetadata {
    pub scope: String,
    pub address: ResourceAddress,
    pub compat: crate::js::Compat,
    pub queue: Option<Arc<QueueEntry>>,
    pub cron: Option<Arc<CronEntry>>,
    pub object: Option<(
        RouteTarget,
        celld_applications::invocation_wire::ObjectTarget,
    )>,
}

const METADATA_CACHE_ENTRIES: usize = 256;
const METADATA_CACHE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Default)]
struct ScopeCache {
    entries: HashMap<String, (Instant, usize, Arc<ScopeMetadata>)>,
    bytes: usize,
    max_entries: usize,
    max_bytes: usize,
}

impl ScopeCache {
    fn get(&mut self, scope: &str) -> Option<Arc<ScopeMetadata>> {
        self.entries
            .retain(|_, (at, _, _)| at.elapsed() < RECONCILE_PERIOD);
        self.bytes = self.entries.values().map(|(_, size, _)| size).sum();
        self.entries.get(scope).map(|(_, _, value)| value.clone())
    }

    fn insert(&mut self, scope: String, size: usize, value: Arc<ScopeMetadata>) {
        if size > self.max_bytes {
            return;
        }
        if let Some((_, size, _)) = self.entries.remove(&scope) {
            self.bytes -= size;
        }
        while self.entries.len() >= self.max_entries || self.bytes + size > self.max_bytes {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, (at, _, _))| *at)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            let (_, size, _) = self
                .entries
                .remove(&oldest)
                .expect("selected cache entry exists");
            self.bytes -= size;
        }
        self.bytes += size;
        self.entries.insert(scope, (Instant::now(), size, value));
    }
}

pub(crate) struct Routes {
    store: Arc<ApplicationStore>,
    pool: Option<VmPool>,
    domain: String,
    scopes: Mutex<ScopeCache>,
}

impl Routes {
    fn pool(&self) -> Result<&VmPool> {
        self.pool
            .as_ref()
            .context("application VM execution is not configured on this node")
    }

    pub(crate) async fn asset_resolver(
        &self,
        application: &ApplicationId,
        deployment: &Name,
        handler: &Name,
    ) -> Result<Option<crate::assets::AssetResolver>> {
        let manifest = self
            .store
            .deployment(application, deployment)
            .await?
            .context("deployment is not staged")?;
        let declared = manifest
            .handlers
            .iter()
            .find(|candidate| &candidate.id == handler)
            .context("deployment has no such handler")?;
        let resolver = match crate::application_javascript_guest::asset_binding(declared)? {
            Some((alias, reference)) => Some(
                self.store
                    .asset_resolver(application, &reference, Some(&alias))
                    .await?,
            ),
            None => None,
        };
        Ok(resolver)
    }

    /// The source transport has resolved its immutable binding. Keep the
    /// caller's exact application/deployment when acquiring the callee VM.
    // Each parameter names one independent input of a service call.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn call_service<'a>(
        &'a self,
        app: &'a crate::actor::AppHandle,
        application: &'a ApplicationId,
        deployment: &'a Name,
        handler: &'a Name,
        request: celld_applications::invocation_wire::HttpRequest,
        service: celld_applications::invocation_wire::ServiceInvocation,
        body: Option<crate::js::HttpChunkStream>,
        relay: Option<crate::application_guest_transport::SignalRelay>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<crate::js::HttpResponse>> + Send + 'a>,
    > {
        Box::pin(async move {
            // Only a live source event's broker can take this route. New HTTP
            // ingress still uses acquire(), which refuses retiring deployments.
            let lease = self
                .pool()?
                .acquire_inner(app, application, deployment, handler, true)
                .await?;
            lease
                .invoke_service(app, request, service, body, relay)
                .await
        })
    }
    async fn reconcile(&self) -> Result<()> {
        if self.pool.is_none() {
            return Ok(());
        }
        *self.scopes.lock().expect("scope cache poisoned") = ScopeCache {
            max_entries: self
                .pool()?
                .config
                .metadata_cache_entries
                .unwrap_or(METADATA_CACHE_ENTRIES),
            max_bytes: self
                .pool()?
                .config
                .metadata_cache_bytes
                .unwrap_or(METADATA_CACHE_BYTES),
            ..Default::default()
        };
        let residents: BTreeSet<_> = self
            .pool()?
            .state
            .lock()
            .expect("VM pool poisoned")
            .slots
            .keys()
            .map(|(app, deployment, _)| (app.clone(), deployment.clone()))
            .collect();
        let applications: BTreeSet<_> = residents.iter().map(|(app, _)| app.clone()).collect();
        for application in applications {
            let snapshot = self.store.read(&application).await?;
            let active = snapshot
                .application(&application)
                .and_then(|app| app.active.as_ref())
                .map(|deployment| &deployment.id);
            for (_, deployment) in residents.iter().filter(|(app, _)| app == &application) {
                if active == Some(deployment) {
                    self.pool()?.readmit_deployment(&application, deployment);
                } else {
                    self.pool()?
                        .retire_deployment(&application, deployment)
                        .await;
                }
            }
        }
        Ok(())
    }

    async fn scope(&self, scope: &str) -> Result<Option<Arc<ScopeMetadata>>> {
        if let Some(found) = self.scopes.lock().expect("scope cache poisoned").get(scope) {
            return Ok(Some(found));
        }
        let owner = self.store.cell_owner(scope).await?;
        let bytes = owner
            .as_ref()
            .map(|owner| serde_json::to_vec(&owner.application).map(|body| body.len()))
            .transpose()?
            .unwrap_or(0)
            + 2 * scope.len()
            + 1024;
        let value = owner.map(|owner| {
            let application = owner.application;
            let deployments = application
                .active
                .clone()
                .map(|deployment| HashMap::from([(application.id.clone(), deployment)]))
                .unwrap_or_default();
            let compat = storage_handler(&deployments, scope)
                .map(|handler| crate::worker_compat(&handler.worker_config))
                .unwrap_or_default();
            Arc::new(ScopeMetadata {
                scope: scope.to_owned(),
                address: owner.locator.address,
                compat,
                queue: queue_catalog(std::iter::once(&application)).remove(scope),
                cron: cron_catalog(std::iter::once(&application)).remove(scope),
                object: alarm_target(&deployments, scope),
            })
        });
        // An absent read can race another node's first cell-index publication.
        if let Some(metadata) = &value {
            self.scopes.lock().expect("scope cache poisoned").insert(
                scope.to_owned(),
                bytes,
                metadata.clone(),
            );
        }
        Ok(value)
    }
}

/// Routes HTTP requests to the pinned handlers of the committed catalog. The
/// route table only ever comes from a committed revision, so a staged but
/// unactivated deployment is unreachable. Reconciliation swaps the table and
/// retires replaced or deactivated deployments from the pool.
pub struct RouteDispatcher {
    routes: Arc<Routes>,
    alarm_host: std::sync::OnceLock<crate::actor::AppHandle>,
    _reconciler: Option<AbortOnDropHandle<()>>,
}

impl RouteDispatcher {
    /// Storage activation is allowed only for authenticated namespaces of
    /// committed application resources. This admits no Worker code.
    pub async fn owns_storage_cell(&self, cell: &str) -> Result<bool> {
        Ok(self.prepare_scope(cell).await?.is_some())
    }

    pub(crate) async fn prepare_scope(&self, cell: &str) -> Result<Option<Arc<ScopeMetadata>>> {
        self.routes.scope(cell).await
    }

    pub(crate) async fn register_cell(
        &self,
        address: &ResourceAddress,
        object: &celld_applications::invocation_wire::ObjectTarget,
    ) -> Result<()> {
        self.routes.store.register_cell(address, object).await?;
        // An absent lookup before registration must not survive the durable publication.
        let mut cache = self.routes.scopes.lock().expect("scope cache poisoned");
        let scope = format!("{}:{}", object.class, object.id);
        if let Some((_, size, _)) = cache.entries.remove(&scope) {
            cache.bytes -= size;
        }
        Ok(())
    }

    /// The application cron the cell scope names, in the committed catalog.
    pub(crate) async fn cron_entry(&self, scope: &str) -> Result<Option<Arc<CronEntry>>> {
        Ok(self
            .prepare_scope(scope)
            .await?
            .and_then(|metadata| metadata.cron.clone()))
    }

    /// Cron cells whose schedule is not yet armed on this node, as the first
    /// call after boot or a catalog change sees them. Like the original arm,
    /// every node asks and ownership decides which cell answers.
    pub async fn cron_candidates(
        &self,
        cursor: Option<String>,
    ) -> Result<crate::applications::CronCandidatePage> {
        self.routes.store.cron_candidates(32, cursor).await
    }

    pub async fn cron_candidate_ready(&self, scope: &str) -> Result<bool> {
        Ok(self
            .cron_entry(scope)
            .await?
            .is_some_and(|entry| entry.schedule.is_some()))
    }

    /// One scheduled event of an application cron, run in the VM of the trigger's
    /// pinned deployment and handler. The target comes from the host catalog by
    /// the cell's scope, never from the cell; a trigger that changed since the
    /// cell read it is refused and the cell retries it. Returns the guest's JSON
    /// `{outcome, noRetry, error}` for the cell to apply.
    pub(crate) async fn dispatch_scheduled(
        &self,
        scope: &str,
        cron: &str,
        scheduled_ms: i64,
    ) -> Result<String> {
        let entry = self
            .cron_entry(scope)
            .await?
            .context("not an application cron")?;
        let schedule = entry
            .schedule
            .as_ref()
            .context("cron trigger is not active")?;
        ensure!(schedule.expression == cron, "cron trigger changed");
        let app = self
            .alarm_host
            .get()
            .context("application alarm host is unavailable")?;
        let lease = self
            .routes
            .pool()?
            .acquire(
                app,
                &entry.application,
                &schedule.deployment,
                &schedule.handler,
            )
            .await?;
        let response = lease
            .invoke_service(
                app,
                celld_applications::invocation_wire::HttpRequest {
                    method: "POST".into(),
                    url: "https://scheduled.celld.internal/".into(),
                    headers: vec![],
                    body: serde_json::json!({"scheduledTime": scheduled_ms, "cron": cron})
                        .to_string()
                        .into_bytes(),
                },
                celld_applications::invocation_wire::ServiceInvocation {
                    entrypoint: Some(SCHEDULED_ENTRYPOINT.into()),
                    props: vec![],
                    rpc: None,
                    get: false,
                    signals: vec![],
                    signal_owner: None,
                },
                None,
                None,
            )
            .await?;
        ensure!(
            response.status == 200 && response.stream.is_none(),
            "guest scheduled handler answered status {}",
            response.status
        );
        String::from_utf8(response.body).context("guest scheduled result is not UTF-8")
    }

    /// The application queue the cell scope names, in the active catalog.
    pub(crate) async fn queue_entry(&self, scope: &str) -> Result<Option<Arc<QueueEntry>>> {
        Ok(self
            .prepare_scope(scope)
            .await?
            .and_then(|metadata| metadata.queue.clone()))
    }

    /// Whether the scope is an application queue cell, not a legacy one.
    pub async fn owns_queue(&self, scope: &str) -> Result<bool> {
        Ok(self
            .prepare_scope(scope)
            .await?
            .is_some_and(|metadata| metadata.address.resource.kind == ResourceKind::Queue))
    }

    /// Serves the committed catalog from its first request.
    pub(crate) async fn new(config: VmHostConfig, store: Arc<ApplicationStore>) -> Result<Self> {
        let fence = config.container_fence_image.clone();
        let domain = config.platform_domain.clone();
        let scopes = ScopeCache {
            max_entries: config
                .metadata_cache_entries
                .unwrap_or(METADATA_CACHE_ENTRIES),
            max_bytes: config.metadata_cache_bytes.unwrap_or(METADATA_CACHE_BYTES),
            ..Default::default()
        };
        let routes = Arc::new(Routes {
            pool: Some(VmPool::new(config, store.clone())?),
            store,
            domain,
            scopes: Mutex::new(scopes),
        });
        routes
            .pool()?
            .service_routes
            .set(Arc::downgrade(&routes))
            .expect("a fresh service router is unbound");
        crate::container::install_application_fence(fence);
        routes.store.validate_format().await?;
        let weak = Arc::downgrade(&routes);
        let reconciler = AbortOnDropHandle::new(tokio::spawn(async move {
            loop {
                tokio::time::sleep(RECONCILE_PERIOD).await;
                let Some(routes) = weak.upgrade() else { return };
                if let Err(error) = routes.reconcile().await {
                    tracing::error!(error = %format!("{error:#}"), "application route reconciliation failed");
                }
            }
        }));
        Ok(Self {
            routes,
            alarm_host: Default::default(),
            _reconciler: Some(reconciler),
        })
    }

    pub(crate) async fn static_host(domain: String, store: Arc<ApplicationStore>) -> Result<Self> {
        validate_platform_domain(&domain)?;
        store.validate_format().await?;
        Ok(Self {
            routes: Arc::new(Routes {
                store,
                pool: None,
                domain,
                scopes: Mutex::new(ScopeCache {
                    max_entries: METADATA_CACHE_ENTRIES,
                    max_bytes: METADATA_CACHE_BYTES,
                    ..Default::default()
                }),
            }),
            alarm_host: Default::default(),
            _reconciler: None,
        })
    }

    #[cfg(test)]
    pub(crate) async fn reconcile(&self) -> Result<()> {
        self.routes.reconcile().await
    }

    async fn resolve(&self, url: &str) -> Result<Option<RouteTarget>> {
        let url = url::Url::parse(url)?;
        let host = url.host_str().unwrap_or_default();
        // The same Host parsing the in-memory table uses: one owner for the
        // port and case rules, so production runs the rules the tests assert.
        let Some(label) = celld_applications::route_label(host, &self.routes.domain) else {
            return Ok(None);
        };
        let Ok(id) = celld_applications::RouteId::try_from(label) else {
            return Ok(None);
        };
        self.routes.store.resolve_route(&id).await
    }

    /// The static assets of the handler a committed route claims for `url`.
    pub async fn asset_resolver_for(
        &self,
        url: &str,
    ) -> Result<Option<crate::assets::AssetResolver>> {
        let Some(target) = self.resolve(url).await? else {
            return Ok(None);
        };
        match target.destination {
            RouteDestination::Handler(handler) => {
                self.routes
                    .asset_resolver(&target.application, &target.deployment, &handler)
                    .await
            }
            RouteDestination::Assets => {
                let manifest = self
                    .routes
                    .store
                    .deployment(&target.application, &target.deployment)
                    .await?
                    .context("asset deployment is not staged")?;
                let reference = manifest
                    .assets
                    .as_ref()
                    .context("asset route has no assets")?;
                self.routes
                    .store
                    .asset_resolver(&target.application, reference, None)
                    .await
                    .map(Some)
            }
        }
    }

    /// Whether this URL is inside the platform domain, before any body is
    /// read. Every such host belongs to applications: an unknown or inactive
    /// route id is a 404, never the legacy Worker.
    pub fn claims(&self, url: &str) -> bool {
        url::Url::parse(url).is_ok_and(|url| {
            RouteTable::is_platform_host(url.host_str().unwrap_or_default(), &self.routes.domain)
        })
    }

    /// `None` when no committed route claims the request URL.
    pub async fn dispatch(
        &self,
        app: &crate::actor::AppHandle,
        deadline: Duration,
        request: celld_applications::invocation_wire::HttpRequest,
        body: Option<crate::js::HttpChunkStream>,
    ) -> Result<Option<crate::js::HttpResponse>> {
        // Replaced between resolving and leasing: routes are published before a
        // deployment retires, so resolving again finds its successor.
        // ponytail: loops only while replacements keep racing this request.
        let lease = loop {
            let Some(target) = self.resolve(&request.url).await? else {
                return Ok(None);
            };
            if target.destination == RouteDestination::Assets {
                return Ok(None);
            }
            match self
                .routes
                .pool()?
                .acquire(
                    app,
                    &target.application,
                    &target.deployment,
                    target.handler()?,
                )
                .await
            {
                Err(error) if error.is::<DeploymentRetiring>() => {}
                lease => break lease?,
            }
        };
        lease
            .invoke_stateless(app, deadline, request, body)
            .await
            .map(Some)
    }

    /// An application queue's batch, run in this node's VM for the consumer
    /// handler of the application's active deployment. The consumer comes from
    /// the host catalog by the cell's scope, never from the cell. An unconsumed
    /// queue, an inactive application or a failed event is an error: the lease
    /// expires and the batch is redelivered, as in the original.
    pub async fn dispatch_queue(
        &self,
        app: &crate::actor::AppHandle,
        scope: &str,
        batch: &crate::js::QueueBatch,
    ) -> Result<crate::js::QueueDispatchResult> {
        let entry = self
            .queue_entry(scope)
            .await?
            .context("not an application queue")?;
        let (handler, _) = entry.consumer.as_ref().context("queue has no consumer")?;
        let lease = self
            .routes
            .pool()?
            .acquire(app, &entry.application, &entry.deployment, handler)
            .await?;
        // The cell's lease also covers admission and settlement.
        lease
            .invoke_queue(app, crate::js::handler_budget(), batch)
            .await
    }

    pub async fn shutdown(&self) -> Result<()> {
        match &self.routes.pool {
            Some(pool) => pool.shutdown().await,
            None => Ok(()),
        }
    }

    /// Handler VM capacity and the startup pool, for the node's `/state`.
    pub fn pool_status(&self) -> serde_json::Value {
        self.routes.pool.as_ref().map_or_else(
            || {
                serde_json::json!({
                    "vms": 0, "memoryMib": 0, "starting": 0, "ready": 0, "quarantined": 0,
                    "startupPool": {"size": 0, "pool": []}
                })
            },
            VmPool::status,
        )
    }

    /// Whether every VM's accepted work, including `waitUntil` retained after
    /// a reply, has drained. A node's drain waits for this before it hands
    /// off the cells that work still writes.
    pub fn settled(&self) -> bool {
        self.routes.pool.as_ref().is_none_or(VmPool::settled)
    }

    pub fn install_alarm_host(&self, app: crate::actor::AppHandle) -> Result<()> {
        ensure!(
            self.alarm_host.set(app).is_ok(),
            "application alarm host already installed"
        );
        Ok(())
    }

    /// Resolve the alarm from the authenticated cell id in the active catalog.
    /// Native KV/Queue/D1 service cells keep their original host dispatch.
    pub(crate) async fn alarm_target_exists(&self, scope: &str) -> Result<bool> {
        let Some(metadata) = self.prepare_scope(scope).await? else {
            return Ok(false);
        };
        if matches!(
            metadata.address.resource.kind,
            ResourceKind::DurableObject | ResourceKind::Workflow
        ) {
            ensure!(
                metadata.object.is_some(),
                "indexed application object has no active handler"
            );
            return Ok(true);
        }
        Ok(false)
    }

    async fn alarm_target(
        &self,
        scope: &str,
    ) -> Result<
        Option<(
            RouteTarget,
            celld_applications::invocation_wire::ObjectTarget,
        )>,
    > {
        Ok(self
            .prepare_scope(scope)
            .await?
            .and_then(|metadata| metadata.object.clone()))
    }

    pub(crate) async fn dispatch_alarm(
        &self,
        runtime: &crate::runtime::RuntimeManager,
        scope: &str,
        request_id: crate::js::RequestId,
        scheduled_ms: i64,
        cancelled: CancellationToken,
    ) -> Result<Option<(celld_logic::wake::AlarmSnapshot, bool, Option<u64>)>> {
        let Some((target, object)) = self.alarm_target(scope).await? else {
            return Ok(None);
        };
        let app = self
            .alarm_host
            .get()
            .context("application alarm host is unavailable")?;
        let lease = crate::asyncrt::select_biased! {
            "a cancellation wins a tie with VM admission so a cancelled alarm never dispatches";
            _ = cancelled.cancelled() => anyhow::bail!("alarm cancelled before VM admission"),
            lease = self.routes.pool()?.acquire(app, &target.application, &target.deployment, target.handler()?) => lease?,
        };
        let completion = lease
            .transport
            .invoke_alarm(app.clone(), object, request_id, scheduled_ms, cancelled)
            .await?;
        // A joined handler error already closed its native claim and proved the
        // retry/explicit re-arm. Returning the old firing would resurrect it.
        if let Err(error) = completion.result {
            tracing::warn!(%scope, %error, "application alarm handler failed");
        }
        Ok(Some(runtime.with_alarm_snapshot(scope, |alarm| {
            (
                alarm,
                runtime.alarm_covered(scope, alarm),
                completion.write_position,
            )
        })))
    }

    /// How the node frees a peer's parked WebSocket 101 that no client took.
    pub fn install_parked_release(release: fn(u64)) {
        crate::application_guest_transport::install_parked_release(release);
    }

    /// Whether this scope is a Durable Object of an active application, whose
    /// hibernatable sockets the guest serves.
    pub async fn owns_socket_scope(&self, scope: &str) -> Result<bool> {
        Ok(self.prepare_scope(scope).await?.is_some())
    }

    /// A message or close of a hibernatable socket, run in this node's VM for
    /// the handler of the object's application. As for an alarm, the target
    /// comes from the active catalog by the authenticated scope, so a
    /// hibernated socket wakes whichever deployment is active now; the
    /// object reads its tags and attachment back from the host registry.
    pub async fn dispatch_socket(
        &self,
        app: &crate::actor::AppHandle,
        scope: &str,
        ws: u64,
        event: celld_applications::invocation_wire::SocketEvent,
        body: Vec<u8>,
    ) -> Result<()> {
        let (target, object) = self
            .alarm_target(scope)
            .await?
            .context("socket scope is not an active application object")?;
        let lease = self
            .routes
            .pool()?
            .acquire(
                app,
                &target.application,
                &target.deployment,
                target.handler()?,
            )
            .await?;
        let completion = lease
            .transport
            .invoke_socket(app.clone(), object, ws, event, body)
            .await?;
        completion.result.map(|_| ())
    }

    /// An object call from a guest, here or on another node, run in this
    /// node's VM for the handler of the application's active deployment that
    /// declares the object's class and binds its namespace. `None` when this
    /// node no longer owns the object.
    pub async fn call_object(
        &self,
        app: &crate::actor::AppHandle,
        application: &ApplicationId,
        object: celld_applications::invocation_wire::ObjectTarget,
        rpc: Option<String>,
        request: celld_applications::invocation_wire::HttpRequest,
    ) -> Result<Option<crate::js::HttpResponse>> {
        // As in `dispatch`: a deployment replaced between resolving and leasing
        // is resolved again.
        let lease = loop {
            let (deployment, handler, resource) = {
                let snapshot = self.routes.store.read(application).await?;
                let deployment = snapshot
                    .application(application)
                    .and_then(|app| app.active.as_ref())
                    .context("application has no active deployment")?;
                let handler = deployment
                    .handlers
                    .iter()
                    .find(|handler| handler_owns_object(application, handler, &object))
                    .context("no active handler declares and binds this object")?;
                let resource =
                    crate::application_javascript_guest::object_namespaces(application, handler)?
                        .into_iter()
                        .find(|(_, resource, class)| {
                            crate::application_javascript_guest::namespace_owns_object(
                                resource, class, &object,
                            )
                        })
                        .context("object binding disappeared")?
                        .1;
                (deployment.id.clone(), handler.id.clone(), resource)
            };
            if matches!(
                resource.resource.kind,
                ResourceKind::Kv | ResourceKind::Queue
            ) {
                let call = celld_applications::invocation_wire::ObjectCall { object, rpc };
                return match crate::application_guest_transport::native_call(
                    app.clone(),
                    resource,
                    call,
                    request,
                    Duration::from_millis(app.operation_deadline_ms),
                )
                .await
                {
                    Ok(response) => Ok(Some(response)),
                    Err(error) if error.is::<crate::application_guest::RemoteOwner>() => Ok(None),
                    Err(error) => Err(error),
                };
            }
            match self
                .routes
                .pool()?
                .acquire(app, application, &deployment, &handler)
                .await
            {
                Err(error) if error.is::<DeploymentRetiring>() => {}
                lease => break lease?,
            }
        };
        let call = celld_applications::invocation_wire::ObjectCall { object, rpc };
        match lease.call_object(app.clone(), call, request).await {
            Ok(mut completion) => {
                std::mem::replace(&mut completion.result, Err(anyhow::anyhow!("taken"))).map(Some)
            }
            Err(error) if error.is::<crate::application_guest::RemoteOwner>() => Ok(None),
            Err(error) => Err(error),
        }
    }
}

/// The handler of an active deployment whose bound namespace authenticates the
/// cell's id. The catalog, not the cell, says which application owns it.
fn storage_handler<'a>(
    deployments: &'a HashMap<ApplicationId, Deployment>,
    cell: &str,
) -> Option<&'a Handler> {
    let (class, id) = cell.split_once(':')?;
    let object = celld_applications::invocation_wire::ObjectTarget {
        class: class.into(),
        id: id.into(),
        name: None,
    };
    object.validate().ok()?;
    deployments.iter().find_map(|(application, deployment)| {
        deployment.handlers.iter().find(|handler| {
            crate::application_javascript_guest::object_namespaces(application, handler).is_ok_and(
                |namespaces| {
                    namespaces.iter().any(|(_, resource, declared)| {
                        native_namespace_owns_object(resource, declared, &object)
                    })
                },
            )
        })
    })
}

fn handler_owns_object(
    application: &ApplicationId,
    handler: &Handler,
    object: &celld_applications::invocation_wire::ObjectTarget,
) -> bool {
    crate::application_javascript_guest::object_namespaces(application, handler).is_ok_and(
        |namespaces| {
            namespaces.iter().any(|(_, resource, class)| {
                crate::application_javascript_guest::namespace_owns_object(resource, class, object)
            })
        },
    )
}

/// Only guest-owned namespaces can turn a native alarm into application code.
/// The active catalog supplies both application and deployment; the cell id
/// authenticates the exact resource and class, including reserved workflows.
fn alarm_target(
    deployments: &HashMap<ApplicationId, Deployment>,
    scope: &str,
) -> Option<(
    RouteTarget,
    celld_applications::invocation_wire::ObjectTarget,
)> {
    let (class, id) = scope.split_once(':')?;
    let object = celld_applications::invocation_wire::ObjectTarget {
        class: class.into(),
        id: id.into(),
        name: None,
    };
    object.validate().ok()?;
    deployments.iter().find_map(|(application, deployment)| {
        deployment
            .handlers
            .iter()
            .find(|handler| {
                crate::application_javascript_guest::object_namespaces(application, handler)
                    .is_ok_and(|namespaces| {
                        namespaces.iter().any(|(_, resource, declared)| {
                            matches!(
                                resource.resource.kind,
                                ResourceKind::DurableObject | ResourceKind::Workflow
                            ) && native_namespace_owns_object(resource, declared, &object)
                        })
                    })
            })
            .map(|handler| {
                (
                    RouteTarget {
                        application: application.clone(),
                        deployment: deployment.id.clone(),
                        destination: RouteDestination::Handler(handler.id.clone()),
                    },
                    object.clone(),
                )
            })
    })
}

pub fn validate_platform_domain(domain: &str) -> Result<()> {
    ensure!(
        celld_applications::valid_hostname(domain) && domain.contains('.') && domain.len() <= 200,
        "platform_domain must be a lowercase DNS name with at least two labels"
    );
    Ok(())
}

static HOSTED: std::sync::OnceLock<RouteDispatcher> = std::sync::OnceLock::new();

/// Node startup with a trusted configuration file: serve the committed
/// application catalog's routes from this node's VMs.
pub async fn host(
    config: &Path,
    bucket: crate::bucket::Bucket,
    app: &crate::actor::AppHandle,
) -> Result<()> {
    ensure!(HOSTED.get().is_none(), "application VMs are already hosted");
    let config = tokio::fs::read(config)
        .await
        .context("read application VM configuration")?;
    let config = serde_json::from_slice(&config).context("invalid application VM configuration")?;
    let dispatcher = RouteDispatcher::new(config, Arc::new(ApplicationStore::new(bucket))).await?;
    if let Err(error) = dispatcher.routes.pool()?.prepare(app).await {
        let cleanup = dispatcher.shutdown().await;
        return Err(error.context(format!("application startup failed; cleanup: {cleanup:?}")));
    }
    ensure!(
        HOSTED.set(dispatcher).is_ok(),
        "application VMs are already hosted"
    );
    Ok(())
}

pub async fn host_static(domain: String, bucket: crate::bucket::Bucket) -> Result<()> {
    let dispatcher =
        RouteDispatcher::static_host(domain, Arc::new(ApplicationStore::new(bucket))).await?;
    ensure!(
        HOSTED.set(dispatcher).is_ok(),
        "applications are already hosted"
    );
    Ok(())
}

/// `None` unless this node was started with an application platform domain or VM configuration.
pub fn hosted() -> Option<&'static RouteDispatcher> {
    HOSTED.get()
}

fn prepare_pin(
    options: &LaunchOptions,
    manifest: Deployment,
    handler: &Name,
    resources: &[crate::applications::Resource],
) -> Result<Pin> {
    manifest.validate()?;
    let registered: HashSet<ResourceRef> =
        resources.iter().map(|item| item.resource.clone()).collect();
    let selected = manifest
        .handlers
        .iter()
        .find(|item| &item.id == handler)
        .context("handler is not in staged deployment")?
        .clone();
    ensure!(
        selected.language == options.adapter,
        "runtime adapter is not supported or does not match handler"
    );
    ensure!(
        selected.runtime_image_sha256 == options.runtime.sha256,
        "handler runtime image is not approved"
    );
    ensure!(
        options.uid > 0
            && options.gid > 0
            && options.vcpus > 0
            && options.vcpus <= 64
            && (256..=12_288).contains(&options.memory_mib),
        "invalid host VM limits"
    );
    for target in selected.bindings.values() {
        match target {
            BindingTarget::Resource { resource } => {
                ensure!(
                    registered.contains(resource),
                    "handler binding resource is not registered"
                );
            }
            BindingTarget::Handler { handler } => {
                ensure!(
                    manifest.handlers.iter().any(|target| &target.id == handler),
                    "service target is not in the pinned deployment"
                );
            }
        }
    }
    crate::application_javascript_guest::queue_consumers(&selected, &registered)?;
    // Validate namespace/class grants before spawning a VMM. The exact same
    // resolver constructs the guest's env bindings and authorizes peer calls.
    let namespaces =
        crate::application_javascript_guest::object_namespaces(&manifest.application, &selected)?
            .into_iter()
            .map(|(_, resource, class)| (resource, class))
            .collect::<Vec<_>>();
    let containers = crate::application_container::bindings(
        &manifest.application,
        &selected,
        resources,
        &namespaces,
    )?;
    Ok(Pin {
        application: manifest.application.clone(),
        deployment: manifest.id.clone(),
        manifest_sha256: content_digest(&serde_json::to_vec(&manifest)?),
        runtime_environment: selected.environment.clone(),
        handler: selected,
        namespaces,
        containers,
        limits: None,
    })
}

// Reuses the reference launcher's executable trust rule. Digest pinning alone
// cannot protect a root-executed jailer supplied from a writable/symlink path.
pub(crate) fn trusted_executable(path: &Path) -> Result<()> {
    ensure!(path.is_absolute(), "VM executable must be absolute");
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir | Component::Normal(_) => current.push(component),
            _ => anyhow::bail!("VM executable path is not canonical"),
        }
        let meta = std::fs::symlink_metadata(&current)?;
        ensure!(
            !meta.file_type().is_symlink() && meta.uid() == 0 && meta.mode() & 0o022 == 0,
            "VM executable path must be root-owned and not writable by others"
        );
    }
    ensure!(
        std::fs::metadata(path)?.is_file(),
        "VM executable is not a regular file"
    );
    Ok(())
}

pub(crate) async fn copy_pinned(source: ApprovedFile, target: PathBuf, limit: u64) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        use sha2::{Digest, Sha256};
        ensure!(
            source.path.is_absolute(),
            "approved image path must be absolute"
        );
        let mut input = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&source.path)?;
        ensure!(
            input.metadata()?.is_file()
                && input.metadata()?.len() > 0
                && input.metadata()?.len() <= limit,
            "approved image size/type is invalid"
        );
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&target)?;
        let mut digest = Sha256::new();
        let mut total = 0u64;
        let mut buffer = [0; 65536];
        loop {
            let count = input.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            total += count as u64;
            ensure!(total <= limit, "approved image grew beyond limit");
            digest.update(&buffer[..count]);
            output.write_all(&buffer[..count])?;
        }
        ensure!(
            format!("{:x}", digest.finalize()) == source.sha256,
            "approved image digest mismatch"
        );
        output.sync_all()?;
        output.set_permissions(std::fs::Permissions::from_mode(0o444))?;
        Ok(())
    })
    .await
    .context("image copy task panicked")?
}

pub(crate) fn drain<R: AsyncRead + Unpin + Send + 'static>(
    mut reader: R,
    console: Arc<Mutex<Vec<u8>>>,
) -> AbortOnDropHandle<()> {
    AbortOnDropHandle::new(tokio::spawn(async move {
        let mut buffer = [0; 4096];
        while let Ok(count) = reader.read(&mut buffer).await {
            if count == 0 {
                break;
            }
            let mut saved = console.lock().expect("VM console poisoned");
            saved.extend_from_slice(&buffer[..count]);
            let excess = saved.len().saturating_sub(16384);
            saved.drain(..excess);
        }
    }))
}

pub(crate) async fn request(
    api: &Path,
    method: &str,
    path: &str,
    value: serde_json::Value,
) -> Result<()> {
    use http_body_util::{BodyExt, Full, Limited};
    use hyper_util::rt::TokioIo;
    // Full startup snapshots write up to 12 GiB before returning.
    let deadline = Duration::from_secs(if path == "/snapshot/create" { 180 } else { 10 });
    timeout(deadline, async {
        #[cfg(test)]
        let api = assigned_supervisor::resume_ack_test::route(api, method, path, &value)?;
        let stream = tokio::net::UnixStream::connect(api).await?;
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
        let _connection = AbortOnDropHandle::new(tokio::spawn(connection));
        let request = hyper::Request::builder()
            .method(method)
            .uri(path)
            .header("host", "firecracker")
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(serde_json::to_vec(&value)?)))?;
        let response = sender.send_request(request).await?;
        let status = response.status();
        let body = Limited::new(response.into_body(), 65536)
            .collect()
            .await
            .map_err(|error| anyhow::anyhow!("Firecracker API body: {error}"))?
            .to_bytes();
        ensure!(
            status == 204 || status == 200,
            "Firecracker API refused {path}: {status}: {}",
            String::from_utf8_lossy(&body)
        );
        Ok(())
    })
    .await
    .context("Firecracker API timed out")?
}

pub(crate) async fn kill_and_wait(child: &mut Child) -> Result<ExitStatus> {
    if let Some(status) = child.try_wait()? {
        return Ok(status);
    }
    child.start_kill()?;
    child.wait().await.context("reap application VM")
}

/// A started VM: assigned, with its guest ready and its dispatcher connected.
struct Started {
    jail: Jail,
    dispatcher: Dispatcher,
    path: &'static str,
    began: Instant,
    boot: Boot,
    /// Since `began`: the claim was accepted, the application guest was ready.
    claimed: Duration,
    booted: Duration,
}

/// A restore from `template`, else a cold boot into template mode, then the
/// prewarm, until the prewarmed guest says hello: a VM that no application has
/// claimed. `abandoned`
/// ends the boot early. A failed boot has killed and reaped its VMM.
async fn boot(
    options: &LaunchOptions,
    template: Option<&Template>,
    abandoned: impl std::future::Future<Output = ()>,
) -> Result<Idle> {
    let began = Instant::now();
    let staged = Staged::new(&options.templates, options)?;
    match template {
        Some(template) => template.link_into(&staged.root)?,
        None => {
            copy_pinned(
                options.kernel.clone(),
                staged.root.join("vmlinux"),
                128 * 1024 * 1024,
            )
            .await?;
            copy_pinned(
                options.runtime.clone(),
                staged.root.join("runtime.ext4"),
                1024 * 1024 * 1024,
            )
            .await?;
        }
    }
    let agent = staged.listen(AGENT_PORT, options)?;
    let control = staged.listen(5002, options)?;
    let storage = staged.listen(5001, options)?;
    let prepared = began.elapsed();
    let mut jail = staged.spawn(options)?;
    let (pid, api) = (jail.pid, jail.root.join("api.sock"));
    let startup = async {
        wait_for_api(&api).await?;
        let spawned = began.elapsed();
        match template {
            Some(_) => load(&api).await?,
            None => cold_boot(&api, options).await?,
        }
        let configured = began.elapsed();
        // A restore resets the agent's connection and it dials again, so the
        // host never sees the template's own.
        let (mut agent_stream, _) = agent.accept().await?;
        ensure!(agent_stream.read_u8().await? == HELLO, "guest agent hello");
        let hello = began.elapsed();
        // Fresh entropy for this VM alone; the agent then execs the prewarmed
        // guest, which starts the generic runtime and dials again.
        agent_stream.write_all(&prewarm()?).await?;
        let (mut guest, _) = agent.accept().await?;
        ensure!(guest.read_u8().await? == HELLO, "prewarmed guest hello");
        let boot = Boot {
            prepared,
            spawned,
            configured,
            hello,
            prewarmed: began.elapsed(),
        };
        Ok((guest, boot))
    };
    let result = {
        tokio::pin!(startup);
        crate::asyncrt::select! {
            result = timeout(Duration::from_secs(30), &mut startup) => result.context("application VM startup timed out").and_then(|value| value),
            _ = abandoned => Err(anyhow::anyhow!("VM startup abandoned")),
            status = jail.child.wait() => match status {
                Ok(status) => Err(anyhow::anyhow!("application VM exited before readiness: {status}")),
                Err(error) => Err(error.into()),
            },
        }
    };
    // Boosted since spawn; an unclaimed VM waits at its class quota.
    let result = result.and_then(|started| jail.settle().map(|()| started));
    match result {
        Ok((guest, boot)) => Ok(Idle {
            jail,
            guest,
            control,
            storage,
            began,
            boot,
            path: if template.is_some() {
                "restore"
            } else {
                "cold"
            },
            memory_mib: options.memory_mib,
        }),
        Err(error) => {
            let console = jail.kill().await?;
            Err(error.context(format!("application VM {pid} bootstrap failed: {console}")))
        }
    }
}

/// Points one of the template's placeholder drives at `source`, which is
/// hard-linked into the jail (copied if the cache is on another filesystem),
/// and returns its size. The VM resumes with the old size until the guest is
/// told to wait for the new one.
async fn point_drive(api: &Path, root: &Path, id: &str, source: &Path) -> Result<u64> {
    let name = format!("{id}-claimed.img");
    let target = root.join(&name);
    if std::fs::hard_link(source, &target).is_err() {
        tokio::fs::copy(source, &target)
            .await
            .context("copy image into the jail")?;
    }
    request(
        api,
        "PATCH",
        &format!("/drives/{id}"),
        serde_json::json!({"drive_id": id, "path_on_host": format!("/{name}")}),
    )
    .await?;
    Ok(tokio::fs::metadata(&target).await?.len())
}

/// The claim (drives, reseed, clock, bootstrap) and the application guest's
/// readiness. A failed claim has killed and reaped the VMM.
async fn claim_vm(
    idle: Idle,
    pin: &Arc<Pin>,
    bootstrap: &[u8],
    image: &Path,
    shared: Option<&Path>,
    reply: &mut oneshot::Sender<Result<Vm>>,
) -> Result<Started> {
    let Idle {
        mut jail,
        mut guest,
        control,
        storage,
        began,
        boot,
        path,
        memory_mib: _,
    } = idle;
    let pid = jail.pid;
    let (api, root) = (jail.root.join("api.sock"), jail.root.clone());
    // Loading the application runs at full vCPUs until its Ready frame.
    jail.boost();
    let startup = async {
        let image_bytes = point_drive(&api, &root, "handler", image).await?;
        let shared_bytes = match shared {
            Some(shared) => point_drive(&api, &root, "shared", shared).await?,
            None => 0,
        };
        guest
            .write_all(&claim(bootstrap, image_bytes, shared_bytes)?)
            .await?;
        if guest.read_u8().await? != 0 {
            let mut reason = String::new();
            let _ = (&mut guest).take(4096).read_to_string(&mut reason).await;
            anyhow::bail!("guest refused the claim: {reason}");
        }
        let claimed = began.elapsed();
        let (socket, _) = control.accept().await?;
        let dispatcher = Dispatcher::connect(
            pin.application.clone(),
            pin.handler.language,
            socket,
            storage,
            Some(pid),
        )
        .await?;
        Ok((dispatcher, claimed))
    };
    let result = {
        tokio::pin!(startup);
        crate::asyncrt::select! {
            result = timeout(Duration::from_secs(30), &mut startup) => result.context("application VM startup timed out").and_then(|value| value),
            _ = reply.closed() => Err(anyhow::anyhow!("VM startup frontend closed")),
            status = jail.child.wait() => match status {
                Ok(status) => Err(anyhow::anyhow!("application VM exited before readiness: {status}")),
                Err(error) => Err(error.into()),
            },
        }
    };
    let result = result.and_then(|ready| jail.settle().map(|()| ready));
    match result {
        Ok((dispatcher, claimed)) => Ok(Started {
            jail,
            dispatcher,
            path,
            began,
            boot,
            claimed,
            booted: began.elapsed(),
        }),
        Err(error) => {
            let console = jail.kill().await?;
            Err(error.context(format!("application VM {pid} bootstrap failed: {console}")))
        }
    }
}

/// One start attempt: boot from `template` (else cold) and claim.
async fn attempt(
    options: &LaunchOptions,
    pin: &Arc<Pin>,
    bootstrap: &[u8],
    image: &Path,
    template: Option<&Template>,
    reply: &mut oneshot::Sender<Result<Vm>>,
) -> Result<Started> {
    ensure!(
        !reply.is_closed(),
        "VM startup frontend closed before launch"
    );
    let idle = boot(options, template, reply.closed()).await?;
    claim_vm(
        idle,
        pin,
        bootstrap,
        image,
        options.shared.as_deref(),
        reply,
    )
    .await
}

async fn start(
    options: LaunchOptions,
    pin: Arc<Pin>,
    image: PathBuf,
    reply: &mut oneshot::Sender<Result<Vm>>,
) -> Result<Vm> {
    let entered = Instant::now();
    ensure!(
        unsafe { libc::geteuid() } == 0,
        "jailed VM launch requires the trusted root supervisor"
    );
    trusted_executable(&options.jailer)?;
    trusted_executable(&options.firecracker)?;
    // The cache verified the digest; a file that is no image never boots a VM,
    // restored or cold.
    crate::application_image::check(&image)?;
    // Only this private guest copy contains values. Catalog, staged manifest
    // and public pin metadata retain the original application-local references.
    let mut guest_handler = pin.handler.clone();
    guest_handler.environment = pin.runtime_environment.clone();
    guest_handler.secrets.clear();
    let bootstrap = celld_applications::bootstrap::Bootstrap {
        version: celld_applications::bootstrap::VERSION,
        application: pin.application.clone(),
        deployment: pin.deployment.clone(),
        manifest_sha256: pin.manifest_sha256.clone(),
        handler: guest_handler,
    }
    .encode_disk()?;
    let mut restored = None;
    if let Some(template) = options.templates.get(&options).await? {
        // A VM that already waits for a claim; a broken one fails its claim
        // and is reaped, and the launch restores one of its own.
        if let Some(mut idle) = options.templates.take(&template.key) {
            (idle.path, idle.began, idle.boot) = ("pool", Instant::now(), Boot::default());
            match claim_vm(
                idle,
                &pin,
                &bootstrap,
                &image,
                options.shared.as_deref(),
                reply,
            )
            .await
            {
                Ok(started) => restored = Some(started),
                Err(error) => {
                    if error.is::<ReapUnproved>() {
                        return Err(error);
                    }
                    tracing::error!(event = "vm_pool_claim_failed", error = %format!("{error:#}"), "idle application VM claim failed; restoring");
                }
            }
        }
        if restored.is_none() {
            match attempt(&options, &pin, &bootstrap, &image, Some(&template), reply).await {
                Ok(started) => restored = Some(started),
                Err(error) => {
                    if error.is::<ReapUnproved>() {
                        return Err(error);
                    }
                    tracing::error!(event = "vm_restore_failed", error = %format!("{error:#}"), "application VM restore failed");
                }
            }
        }
    }
    ensure!(
        restored.is_some() || !options.templates.production(),
        "startup template restore failed; request refused"
    );
    let started = match restored {
        Some(started) => started,
        None => attempt(&options, &pin, &bootstrap, &image, None, reply).await?,
    };
    let Started {
        mut jail,
        dispatcher,
        path,
        began,
        boot: phases,
        claimed,
        booted,
    } = started;
    let transport = dispatcher.client();
    transport
        .bind_services(pin.deployment.clone(), &pin.handler)
        .expect("a fresh transport is unbound");
    // The handler's own bindings are the only namespaces its guest may call.
    // ponytail: fixed object-call deadline when the application sets no limit.
    transport
        .bind(
            pin.namespaces.clone(),
            pin.limits.map_or(Duration::from_secs(30), |limits| {
                Duration::from_millis(limits.invocation_timeout_ms)
            }),
        )
        .expect("a fresh transport is unbound");
    transport
        .bind_containers(pin.containers.clone())
        .expect("a fresh transport is unbound");
    let ms = |elapsed: Duration| elapsed.as_secs_f64() * 1000.0;
    tracing::info!(
        event = "vm_startup",
        path,
        pid = jail.pid,
        // Checks, bootstrap and template lookup before the boot or the claim.
        admission_ms = ms(began.saturating_duration_since(entered)),
        // A pool VM was prepared earlier: its time starts at the claim.
        prepare_ms = ms(phases.prepared),
        spawn_ms = ms(phases.spawned - phases.prepared),
        // Cold: configure and boot the VMM; restore: load the snapshot and resume.
        configure_ms = ms(phases.configured - phases.spawned),
        // Until the guest agent is connected (cold: the boot; restore: the redial).
        agent_ms = ms(phases.hello - phases.configured),
        // Reseed, exec and the generic runtime, until the prewarmed guest dials.
        prewarm_ms = ms(phases.prewarmed - phases.hello),
        claim_ms = ms(claimed - phases.prewarmed),
        // The application, loaded by the claim, until its Ready frame.
        guest_ready_ms = ms(booted - claimed),
        deployment_ms = ms(began.elapsed() - booted),
        ms = ms(began.elapsed()),
        "application VM started"
    );
    let retired = dispatcher.retirement();
    let stop = CancellationToken::new();
    let (supervisor, assignment) = assigned_supervisor::Control::new();
    let client = VmClient {
        pin,
        transport: transport.clone(),
        supervisor: supervisor.clone(),
        stop: stop.clone(),
        #[cfg(test)]
        console: jail.console.clone(),
        #[cfg(test)]
        startup: serde_json::json!({
            "path": path,
            "admissionMs": ms(began.saturating_duration_since(entered)),
            "prepareMs": ms(phases.prepared),
            "spawnMs": ms(phases.spawned - phases.prepared),
            "configureMs": ms(phases.configured - phases.spawned),
            "agentMs": ms(phases.hello - phases.configured),
            "prewarmMs": ms(phases.prewarmed - phases.hello),
            "claimMs": ms(claimed - phases.prewarmed),
            "guestReadyMs": ms(booted - claimed),
            "deploymentMs": ms(began.elapsed() - booted),
            "ms": ms(began.elapsed()),
        }),
    };
    let pid = jail.pid;
    let task = tokio::spawn(async move {
        let (reason, status) = assigned_supervisor::wait(
            &mut jail,
            dispatcher,
            &transport,
            &stop,
            &retired,
            &supervisor,
            assignment,
        )
        .await?;
        transport.retire();
        stop.cancel();
        let status = match status {
            Some(status) => status,
            None => kill_and_wait(&mut jail.child).await?,
        };
        transport.confirm_vm_reaped(pid, &mut jail.child)?;
        transport.close_read_cursors().await;
        let exhausted = jail.memory_exhausted();
        let console = jail.kill().await?;
        let reason = if exhausted.context(ReapUnproved)? {
            "memory limit exhausted"
        } else {
            reason
        };
        Ok(Exit {
            pid,
            status,
            reason,
            console,
        })
    });
    Ok(Vm {
        client,
        task: Some(task),
        #[cfg(test)]
        path,
    })
}

#[cfg(test)]
#[path = "unit_tests/application_vm/memory_guard_tests.rs"]
mod memory_guard_tests;
