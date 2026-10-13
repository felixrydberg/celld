// Application management metadata and the artifact cache are operator
// control-plane state on the host, outside the Actor execution domain.
#![allow(clippy::disallowed_methods)]

//! Application management metadata over Celld's existing conditional bucket
//! adapter. Executing an application remains the runtime/Actor's responsibility.

use crate::bucket::Bucket;
use anyhow::{bail, ensure, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use bytes::Bytes;
use celld_applications::{
    ApplicationId, BindingTarget, Deployment, Name, ResourceAddress, ResourceKind, ResourceRef,
    RouteDestination, RouteId, RouteTable, RouteTarget, Trigger,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const LEGACY_CATALOG_KEY: &str = "applications/v1/catalog.json";
const METADATA_PREFIX: &str = "applications/v2/metadata";
const CRON_PREFIX: &str = "applications/v2/cron-candidates";
const METADATA_VERSION: u16 = 2;
const MAX_METADATA_BYTES: u64 = 8 * 1024 * 1024;
const MAX_INDEX_BYTES: u64 = 4096;
pub const MAX_APPLICATION_PAGE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_APPLICATION_PAGE: usize = 256;
pub const MAX_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_SECRET_BYTES: usize = 16 * 1024;

/// Application sizes are fixed; container compatibility aliases do not apply.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ApplicationSizeClass {
    Lite,
    #[default]
    Basic,
    #[serde(rename = "standard-1")]
    Standard1,
    #[serde(rename = "standard-2")]
    Standard2,
    #[serde(rename = "standard-3")]
    Standard3,
    #[serde(rename = "standard-4")]
    Standard4,
}

impl ApplicationSizeClass {
    pub const ALL: [Self; 6] = [
        Self::Lite,
        Self::Basic,
        Self::Standard1,
        Self::Standard2,
        Self::Standard3,
        Self::Standard4,
    ];

    pub fn resources(self) -> (u8, u32, u32) {
        let name = match self {
            Self::Lite => "lite",
            Self::Basic => "basic",
            Self::Standard1 => "standard-1",
            Self::Standard2 => "standard-2",
            Self::Standard3 => "standard-3",
            Self::Standard4 => "standard-4",
        };
        let (cpu, bytes) =
            crate::container::instance_resources(name).expect("fixed application class");
        (
            cpu.ceil() as u8,
            (bytes / (1024 * 1024)) as u32,
            (cpu * 100_000.0) as u32,
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationLimits {
    #[serde(default)]
    pub size_class: ApplicationSizeClass,
    pub invocation_timeout_ms: u64,
}

impl ApplicationLimits {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=300_000).contains(&self.invocation_timeout_ms),
            "invocation_timeout_ms must be between 1 and 300000"
        );
        Ok(())
    }
}

/// An opaque random version, not a digest of a potentially guessable value.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretVersion(String);

impl SecretVersion {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.0.len() == 64
                && self
                    .0
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "invalid secret version"
        );
        Ok(())
    }
}

fn secret_key(application: &ApplicationId, name: &Name, version: &SecretVersion) -> Result<String> {
    version.validate()?;
    Ok(format!(
        "{}secrets/{}/{}",
        application.storage_root(),
        name.as_str(),
        version.0
    ))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretValue {
    application: ApplicationId,
    name: Name,
    version: SecretVersion,
    value: String,
}

pub fn content_digest(body: &[u8]) -> String {
    Sha256::digest(body)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

const STATUS_PREFIX: &str = "runtime/";
const MAX_STATUS_RECORDS: usize = 64;

fn status_key(application: &ApplicationId, node: &str) -> String {
    format!(
        "{}{STATUS_PREFIX}{}.json",
        application.storage_root(),
        content_digest(node.as_bytes())
    )
}

fn artifact_key(application: &ApplicationId, digest: &str) -> Result<String> {
    ensure!(
        digest.len() == 64
            && digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid artifact digest"
    );
    Ok(format!("{}artifacts/{digest}", application.storage_root()))
}

async fn write_cache_file(path: &Path, body: &[u8]) -> Result<()> {
    let parent = path.parent().context("artifact cache path has no parent")?;
    tokio::fs::create_dir_all(parent).await?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        std::process::id(),
        rand::random::<u64>()
    ));
    let written = async {
        let mut file = tokio::fs::File::create(&temporary).await?;
        tokio::io::AsyncWriteExt::write_all(&mut file, body).await?;
        file.sync_all().await
    }
    .await;
    match written {
        Ok(()) => Ok(tokio::fs::rename(&temporary, path).await?),
        Err(error) => {
            let _ = tokio::fs::remove_file(&temporary).await;
            Err(error.into())
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resource {
    pub resource: ResourceRef,
    #[serde(default = "empty_object")]
    pub configuration: serde_json::Value,
}

fn empty_object() -> serde_json::Value {
    serde_json::json!({})
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Application {
    pub id: ApplicationId,
    pub resources: Vec<Resource>,
    pub active: Option<Deployment>,
    #[serde(default)]
    pub secrets: BTreeMap<Name, SecretVersion>,
    #[serde(default)]
    pub limits: Option<ApplicationLimits>,
    /// Public route id of each handler, assigned by the host when the handler
    /// first appears in an activated deployment. Never changed, removed or
    /// reused, even after the handler or its deployment is gone.
    #[serde(default)]
    pub route_ids: BTreeMap<Name, RouteId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asset_route_id: Option<RouteId>,
}

impl Application {
    fn validate(&self) -> Result<()> {
        let mut route_ids = BTreeSet::new();
        ensure!(
            self.route_ids.values().chain(self.asset_route_id.iter()).all(|id| route_ids.insert(id)),
            "route id belongs to multiple application handlers"
        );
        ensure!(
            self.secrets.len() <= 256,
            "application secret count exceeds 256"
        );
        for version in self.secrets.values() {
            version.validate()?;
        }
        if let Some(limits) = self.limits {
            limits.validate()?;
        }
        let mut resources = BTreeSet::new();
        for resource in &self.resources {
            ensure!(
                resource.configuration.is_object(),
                "resource configuration must be an object"
            );
            ensure!(
                resources.insert(&resource.resource),
                "duplicate application resource"
            );
        }
        if let Some(deployment) = &self.active {
            ensure!(
                deployment.application == self.id,
                "deployment belongs to another application"
            );
            deployment.validate()?;
            for handler in &deployment.handlers {
                ensure!(
                    handler
                        .secrets
                        .values()
                        .all(|name| self.secrets.contains_key(name)),
                    "handler names an unregistered application secret"
                );
                for binding in handler.bindings.values() {
                    if let BindingTarget::Resource { resource } = binding {
                        ensure!(
                            resources.contains(resource),
                            "binding names an unregistered application resource"
                        );
                    }
                }
            }
            for trigger in &deployment.triggers {
                let resource = match trigger {
                    Trigger::Cron { id, .. } => ResourceRef {
                        kind: ResourceKind::Cron,
                        id: id.clone(),
                    },
                    Trigger::Queue { queue, .. } => ResourceRef {
                        kind: ResourceKind::Queue,
                        id: queue.clone(),
                    },
                };
                ensure!(
                    resources.contains(&resource),
                    "trigger names an unregistered application resource"
                );
            }
        }
        Ok(())
    }
}

/// What a node's runtime observed for one pinned handler. The catalog says what
/// is meant to run; only this record says what a node actually launched.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum HandlerState {
    Launching,
    Serving,
    Draining,
    /// A fixed, host-chosen reason; causes and guest output stay in host logs.
    Failed {
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HandlerStatus {
    pub deployment: Name,
    pub handler: Name,
    #[serde(flatten)]
    pub state: HandlerState,
}

/// One overwritten record per application and node, bounded by that node's
/// tracked handlers. The timestamp lets readers judge freshness: a crashed
/// node cannot retract its last record.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeStatus {
    pub node: String,
    pub observed_at_unix_secs: u64,
    pub handlers: Vec<HandlerStatus>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataRecord {
    metadata_version: u16,
    application: Application,
}

fn applications_routes<'a>(
    applications: impl Iterator<Item = &'a Application>,
) -> Result<RouteTable> {
    RouteTable::from_deployments(
        applications.filter_map(|app| Some((app.active.as_ref()?, &app.route_ids, app.asset_route_id.as_ref()))),
    )
}

fn metadata_key(id: &ApplicationId) -> String {
    format!("{METADATA_PREFIX}/{}.json", id.app.as_str())
}

/// An application's own provider revision; an absent record uses create-CAS.
pub struct Snapshot {
    id: ApplicationId,
    application: Option<Application>,
    token: Option<String>,
}

impl Snapshot {
    pub fn revision(&self) -> Option<&str> {
        self.token.as_deref()
    }

    pub fn application(&self, id: &ApplicationId) -> Option<&Application> {
        (&self.id == id)
            .then_some(self.application.as_ref())
            .flatten()
    }

    fn application_mut(&mut self, id: &ApplicationId) -> Result<&mut Application> {
        ensure!(&self.id == id, "snapshot belongs to another application");
        self.application
            .as_mut()
            .context("application does not exist")
    }

    pub fn create(&mut self, id: ApplicationId) -> Result<()> {
        ensure!(self.id == id, "snapshot belongs to another application");
        ensure!(self.application.is_none(), "application already exists");
        self.application = Some(Application {
            id,
            resources: Vec::new(),
            active: None,
            secrets: BTreeMap::new(),
            limits: None,
            route_ids: BTreeMap::new(),
            asset_route_id: None,
        });
        Ok(())
    }

    pub fn register_resource(&mut self, id: &ApplicationId, resource: Resource) -> Result<()> {
        ensure!(
            resource.configuration.is_object(),
            "resource configuration must be an object"
        );
        let app = self.application_mut(id)?;
        if let Some(existing) = app
            .resources
            .iter()
            .find(|item| item.resource == resource.resource)
        {
            ensure!(
                existing == &resource,
                "resource identity already has another configuration"
            );
        } else {
            app.resources.push(resource);
        }
        Ok(())
    }

    pub fn set_limits(
        &mut self,
        id: &ApplicationId,
        limits: Option<ApplicationLimits>,
    ) -> Result<()> {
        if let Some(limits) = limits {
            limits.validate()?;
        }
        self.application_mut(id)?.limits = limits;
        Ok(())
    }

    pub fn set_secret(
        &mut self,
        id: &ApplicationId,
        name: Name,
        version: SecretVersion,
    ) -> Result<()> {
        version.validate()?;
        let app = self.application_mut(id)?;
        ensure!(
            app.secrets.contains_key(&name) || app.secrets.len() < 256,
            "application secret count exceeds 256"
        );
        app.secrets.insert(name, version);
        Ok(())
    }

    pub fn remove_secret(&mut self, id: &ApplicationId, name: &Name) -> Result<()> {
        let app = self.application_mut(id)?;
        ensure!(
            !app.active.as_ref().is_some_and(|deployment| deployment
                .handlers
                .iter()
                .any(|handler| handler.secrets.values().any(|secret| secret == name))),
            "secret is referenced by the active deployment"
        );
        ensure!(
            app.secrets.remove(name).is_some(),
            "application secret does not exist"
        );
        Ok(())
    }

    /// Persistent resource identities survive handler removal and suspension.
    pub fn deactivate(&mut self, id: &ApplicationId) -> Result<()> {
        self.application_mut(id)?.active = None;
        Ok(())
    }

    pub fn routes(&self) -> Result<RouteTable> {
        applications_routes(self.application.iter())
    }
}

pub struct ApplicationListEntry {
    pub application: Application,
    pub revision: Option<String>,
}

pub struct ApplicationPage {
    pub applications: Vec<ApplicationListEntry>,
    pub next_cursor: Option<String>,
}

#[derive(Debug)]
pub struct ApplicationPageError(pub &'static str);

impl std::fmt::Display for ApplicationPageError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for ApplicationPageError {}

#[derive(Debug)]
pub struct UncommittedCellIndex;

impl std::fmt::Display for UncommittedCellIndex {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("application cell index is not committed")
    }
}

impl std::error::Error for UncommittedCellIndex {}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PageCursor {
    version: u16,
    prefix: String,
    limit: usize,
    token: String,
}

fn page_token(prefix: &str, limit: usize, cursor: Option<String>) -> Result<Option<String>> {
    ensure!(
        (1..=MAX_APPLICATION_PAGE).contains(&limit),
        ApplicationPageError("page limit must be between 1 and 256")
    );
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    ensure!(
        cursor.len() <= 16384,
        ApplicationPageError("application cursor exceeds limit")
    );
    let decoded = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| ApplicationPageError("invalid application cursor"))?;
    let cursor: PageCursor = serde_json::from_slice(&decoded)
        .map_err(|_| ApplicationPageError("invalid application cursor"))?;
    ensure!(
        cursor.version == METADATA_VERSION
            && cursor.prefix == prefix
            && cursor.limit == limit
            && !cursor.token.is_empty()
            && cursor.token.len() <= 4096,
        ApplicationPageError("application cursor does not match this listing")
    );
    Ok(Some(cursor.token))
}

fn page_cursor(prefix: &str, limit: usize, token: Option<String>) -> Result<Option<String>> {
    token
        .map(|token| {
            ensure!(
                !token.is_empty() && token.len() <= 4096,
                "provider cursor exceeds limit"
            );
            Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&PageCursor {
                version: METADATA_VERSION,
                prefix: prefix.into(),
                limit,
                token,
            })?))
        })
        .transpose()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RouteLocator {
    index_version: u16,
    application: ApplicationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    handler: Option<Name>,
}

/// A locator is not active-state authority; cell_owner checks committed metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellLocator {
    index_version: u16,
    pub scope: String,
    pub address: ResourceAddress,
}

impl CellLocator {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.index_version == METADATA_VERSION,
            "unsupported application cell index version"
        );
        ensure!(
            celld_logic::cell::valid_cell_scope(&self.scope),
            "invalid application cell scope"
        );
        let (class, id) = self
            .scope
            .split_once(':')
            .context("application cell has no class")?;
        let object = celld_applications::invocation_wire::ObjectTarget {
            class: class.into(),
            id: id.into(),
            name: None,
        };
        object.validate()?;
        let kind_matches = match self.address.resource.kind {
            ResourceKind::DurableObject => {
                class != celld_logic::cron::RESERVED_CLASS
                    && !crate::deploy::is_reserved_class(class)
            }
            ResourceKind::D1 => class == crate::deploy::D1_CLASS,
            ResourceKind::Kv => class == celld_logic::kv::RESERVED_CLASS,
            ResourceKind::Queue => class == celld_logic::queue::RESERVED_CLASS,
            ResourceKind::Cron => class == celld_logic::cron::RESERVED_CLASS,
            ResourceKind::Workflow => {
                class == crate::deploy::workflow_class(self.address.resource.id.as_str())
            }
            _ => false,
        };
        ensure!(
            kind_matches && self.address.owns(&object),
            "application cell index namespace mismatch"
        );
        let fixed_name = match self.address.resource.kind {
            ResourceKind::Kv => Some(celld_logic::kv::cell_name("namespace", 0)),
            ResourceKind::Queue | ResourceKind::Cron => {
                Some(self.address.resource.id.as_str().to_owned())
            }
            _ => None,
        };
        if let Some(name) = fixed_name {
            ensure!(
                self.address.cell_scope(class, &name)? == self.scope,
                "application index names another fixed resource cell"
            );
        }
        Ok(())
    }
}

pub struct CellOwner {
    pub locator: CellLocator,
    /// Registered resources remain typed application storage when inactive.
    pub application: Application,
    pub revision: Option<String>,
}

pub struct CronCandidatePage {
    pub candidates: Vec<CellLocator>,
    pub next_cursor: Option<String>,
}

fn cell_index_key(scope: &str) -> String {
    format!(
        "applications/v2/cells/{}.json",
        content_digest(scope.as_bytes())
    )
}

fn route_index_key(id: &RouteId) -> String {
    format!("applications/v2/routes/{}.json", id.as_str())
}

#[derive(Debug)]
pub struct Conflict;

impl std::fmt::Display for Conflict {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("application revision changed")
    }
}

impl std::error::Error for Conflict {}

#[derive(Debug)]
pub struct ImmutableConflict;

impl std::fmt::Display for ImmutableConflict {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("immutable application content differs")
    }
}

impl std::error::Error for ImmutableConflict {}

/// Node-local, content-addressed copies of handler artifacts so a cold VM start
/// does not re-download them. Entries are `<root>/<hash of the application's
/// storage root>/<sha256>`, so applications never share one; they are verified
/// on read, written by temp file plus rename, bounded in total bytes and evicted
/// `ttl` after last use (the mtime, touched on each hit).
pub struct ArtifactCache {
    root: PathBuf,
    ttl: Duration,
    max_bytes: u64,
    swept: std::sync::Mutex<Option<Instant>>,
}

impl ArtifactCache {
    pub fn new(ttl: Duration) -> Result<Self> {
        let root = std::env::var_os("CELLD_ARTIFACT_CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("celld").join("artifact-cache"));
        let max_bytes = crate::env_vars::with_default("CELLD_ARTIFACT_CACHE_BYTES", 1_u64 << 30)?;
        Ok(Self::at(root, ttl, max_bytes))
    }

    fn at(root: PathBuf, ttl: Duration, max_bytes: u64) -> Self {
        Self {
            root,
            ttl,
            max_bytes,
            swept: Default::default(),
        }
    }

    /// Evict expired and over-bound entries; at most once per quarter TTL.
    // ponytail: scans the whole cache and runs only on use or the pool tick; an index if caches get large.
    async fn sweep(&self) {
        {
            let mut swept = self.swept.lock().expect("artifact cache poisoned");
            if swept.is_some_and(|at| at.elapsed() < self.ttl / 4) {
                return;
            }
            *swept = Some(Instant::now());
        }
        let (root, max_bytes, ttl) = (self.root.clone(), self.max_bytes, self.ttl);
        let swept = tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&root)?;
            crate::assets::cleanup_cache_files(&root, max_bytes, Some(ttl))
        })
        .await;
        match swept {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(event = "artifact_cache_sweep_failed", %error),
            Err(error) => tracing::warn!(event = "artifact_cache_sweep_failed", %error),
        }
    }
}

pub struct ApplicationStore {
    bucket: Bucket,
    cache: std::sync::OnceLock<ArtifactCache>,
}

impl ApplicationStore {
    pub fn new(bucket: Bucket) -> Self {
        Self {
            bucket,
            cache: Default::default(),
        }
    }

    pub fn validate_resource(&self, resource: &ResourceRef) -> Result<()> {
        if resource.kind == ResourceKind::R2 {
            self.bucket.validate_r2_bucket_name(resource.id.as_str())?;
        }
        Ok(())
    }

    /// Turns on the node-local artifact cache for `cached_artifact` (first call wins).
    pub fn enable_artifact_cache(&self, cache: ArtifactCache) {
        let _ = self.cache.set(cache);
    }

    pub async fn sweep_artifact_cache(&self) {
        if let Some(cache) = self.cache.get() {
            cache.sweep().await;
        }
    }

    /// The path of `artifact` in the verified local cache, fetched once and
    /// cached if absent: a handler's read-only image, which a VM's drive links
    /// to. A tampered, partial or oversized cache file is removed and
    /// refetched.
    pub async fn cached_artifact(
        &self,
        application: &ApplicationId,
        digest: &str,
    ) -> Result<Option<PathBuf>> {
        artifact_key(application, digest)?;
        let cache = self.cache.get().context("artifact cache is not enabled")?;
        let scope = content_digest(application.storage_root().as_bytes());
        let path = cache.root.join(scope).join(digest);
        let hit = match tokio::fs::metadata(&path).await {
            Ok(meta) if meta.len() <= MAX_ARTIFACT_BYTES as u64 => tokio::fs::read(&path)
                .await
                .is_ok_and(|body| content_digest(&body) == digest),
            _ => false,
        };
        if hit {
            crate::assets::touch_cache_file(path.clone());
            return Ok(Some(path));
        }
        let _ = tokio::fs::remove_file(&path).await;
        let Some(body) = self.artifact(application, digest).await? else {
            return Ok(None);
        };
        // ponytail: concurrent cold starts of one digest each fetch once; a per-digest lock if it matters.
        write_cache_file(&path, &body)
            .await
            .context("write artifact cache")?;
        cache.sweep().await;
        Ok(Some(path))
    }

    /// Values live only in the credential-protected host bucket. Publication
    /// is the subsequent application CAS, not this immutable blob write.
    pub async fn stage_secret(
        &self,
        application: &ApplicationId,
        name: &Name,
        value: String,
    ) -> Result<SecretVersion> {
        ensure!(
            value.len() <= MAX_SECRET_BYTES && !value.contains('\0'),
            "secret exceeds 16 KiB or contains NUL"
        );
        let mut random = [0u8; 32];
        getrandom::fill(&mut random)
            .map_err(|_| anyhow::anyhow!("secret version entropy unavailable"))?;
        let version = SecretVersion(random.iter().map(|byte| format!("{byte:02x}")).collect());
        let key = secret_key(application, name, &version)?;
        let body = Bytes::from(serde_json::to_vec(&SecretValue {
            application: application.clone(),
            name: name.clone(),
            version: version.clone(),
            value,
        })?);
        match self.bucket.put_cas(&key, body.clone(), None).await {
            Ok(Some(_)) => Ok(version),
            outcome => {
                if let Ok(Some((observed, _))) = self.bucket.get(&key).await {
                    if observed == body {
                        return Ok(version);
                    }
                }
                match outcome {
                    Err(error) => Err(error.context("secret staging has an uncertain outcome")),
                    _ => Err(anyhow::anyhow!("secret version publication refused")),
                }
            }
        }
    }

    /// The trusted caller supplies the application's observed metadata;
    /// no guest or handler can select a bucket key or an arbitrary host env var.
    pub(crate) async fn resolve_environment(
        &self,
        application: &Application,
        handler: &celld_applications::Handler,
    ) -> Result<BTreeMap<Name, String>> {
        let mut environment = handler.environment.clone();
        for (alias, name) in &handler.secrets {
            ensure!(
                !environment.contains_key(alias) && !handler.bindings.contains_key(alias),
                "secret alias collides with environment or binding"
            );
            let version = application
                .secrets
                .get(name)
                .context("handler application secret is not registered")?;
            let key = secret_key(&application.id, name, version)?;
            if let Some((size, _)) = self.bucket.head(&key).await? {
                ensure!(
                    size <= (6 * MAX_SECRET_BYTES + 4096) as u64,
                    "stored application secret exceeds limit"
                );
            }
            let (body, _) = self
                .bucket
                .get(&key)
                .await?
                .context("application secret value is missing")?;
            ensure!(
                body.len() <= 6 * MAX_SECRET_BYTES + 4096,
                "stored application secret exceeds limit"
            );
            let secret: SecretValue = serde_json::from_slice(&body)
                .map_err(|_| anyhow::anyhow!("invalid stored application secret"))?;
            ensure!(
                secret.application == application.id
                    && &secret.name == name
                    && &secret.version == version
                    && secret.value.len() <= MAX_SECRET_BYTES
                    && !secret.value.contains('\0'),
                "stored application secret identity or value is invalid"
            );
            environment.insert(alias.clone(), secret.value);
        }
        Ok(environment)
    }

    /// Old application-platform state has no migration or compatibility reader.
    pub async fn validate_format(&self) -> Result<()> {
        ensure!(
            self.bucket.head(LEGACY_CATALOG_KEY).await?.is_none(),
            "unsupported legacy application metadata; v2 records are required"
        );
        Ok(())
    }

    async fn read_record(&self, id: &ApplicationId, budget: usize) -> Result<(Snapshot, usize)> {
        let key = metadata_key(id);
        if let Some((size, _)) = self.bucket.head(&key).await? {
            ensure!(
                size <= MAX_METADATA_BYTES,
                "application metadata exceeds individual record limit"
            );
            ensure!(
                size <= budget as u64,
                ApplicationPageError("application page exceeds 16 MiB; use a smaller limit")
            );
        }
        let (application, token, size) = match self.bucket.get(&key).await? {
            Some((bytes, token)) => {
                ensure!(
                    bytes.len() as u64 <= MAX_METADATA_BYTES,
                    "application metadata exceeds individual record limit"
                );
                ensure!(
                    bytes.len() <= budget,
                    ApplicationPageError("application page exceeds 16 MiB; use a smaller limit")
                );
                let record: MetadataRecord =
                    serde_json::from_slice(&bytes).context("invalid application metadata")?;
                ensure!(
                    record.metadata_version == METADATA_VERSION,
                    "unsupported application metadata version"
                );
                ensure!(
                    &record.application.id == id,
                    "application metadata identity mismatch"
                );
                record.application.validate()?;
                applications_routes(std::iter::once(&record.application))?;
                (Some(record.application), Some(token), bytes.len())
            }
            None => (None, None, 0),
        };
        Ok((
            Snapshot {
                id: id.clone(),
                application,
                token,
            },
            size,
        ))
    }

    pub async fn read(&self, id: &ApplicationId) -> Result<Snapshot> {
        self.validate_format().await?;
        Ok(self.read_record(id, MAX_METADATA_BYTES as usize).await?.0)
    }

    pub async fn list(&self, limit: usize, cursor: Option<String>) -> Result<ApplicationPage> {
        let token = page_token(METADATA_PREFIX, limit, cursor)?;
        self.validate_format().await?;
        let page = self
            .bucket
            .objects_page(METADATA_PREFIX, token, limit)
            .await?;
        ensure!(
            page.objects.len() <= limit,
            "provider application page exceeds limit"
        );
        let mut applications = Vec::with_capacity(page.objects.len());
        let mut bytes = 0usize;
        for object in page.objects {
            let key = object.location.as_ref();
            let name = key
                .strip_prefix(&format!("{METADATA_PREFIX}/"))
                .and_then(|name| name.strip_suffix(".json"))
                .context("invalid application metadata listing key")?;
            let id = ApplicationId {
                app: name.to_owned().try_into()?,
            };
            let remaining = MAX_APPLICATION_PAGE_BYTES - bytes;
            ensure!(
                object.size <= MAX_METADATA_BYTES,
                "application metadata exceeds individual record limit"
            );
            ensure!(
                object.size <= remaining as u64,
                ApplicationPageError("application page exceeds 16 MiB; use a smaller limit")
            );
            let (snapshot, size) = self.read_record(&id, remaining).await?;
            bytes += size;
            if let Some(application) = snapshot.application {
                applications.push(ApplicationListEntry {
                    application,
                    revision: snapshot.token,
                });
            }
        }
        Ok(ApplicationPage {
            applications,
            next_cursor: page_cursor(METADATA_PREFIX, limit, page.page_token)?,
        })
    }

    /// Locators publish before this authoritative CAS; interrupted candidates are unreachable.
    pub async fn commit(&self, snapshot: Snapshot) -> Result<String> {
        let application = snapshot.application.context("application does not exist")?;
        ensure!(
            application.id == snapshot.id,
            "application metadata identity mismatch"
        );
        application.validate()?;
        for resource in &application.resources {
            self.validate_resource(&resource.resource)?;
        }
        self.validate_format().await?;
        applications_routes(std::iter::once(&application))?;
        let key = metadata_key(&snapshot.id);
        let body = Bytes::from(serde_json::to_vec(&MetadataRecord {
            metadata_version: METADATA_VERSION,
            application: application.clone(),
        })?);
        ensure!(
            body.len() as u64 <= MAX_METADATA_BYTES,
            "application metadata exceeds individual record limit"
        );
        self.publish_indexes(&application).await?;
        match self
            .bucket
            .put_cas(&key, body.clone(), snapshot.token.as_deref())
            .await
        {
            Ok(Some(token)) => Ok(token),
            Ok(None) => Err(Conflict.into()),
            Err(error) => {
                if let Ok(Some((observed, token))) = self.bucket.get(&key).await {
                    if observed == body {
                        return Ok(token);
                    }
                }
                Err(error.context("application update has an uncertain outcome"))
            }
        }
    }

    async fn immutable_index<T: Serialize>(&self, key: &str, index: &T) -> Result<()> {
        let body = Bytes::from(serde_json::to_vec(index)?);
        ensure!(
            body.len() as u64 <= MAX_INDEX_BYTES,
            "application index exceeds limit"
        );
        match self.bucket.put_cas(key, body.clone(), None).await {
            Ok(Some(_)) => Ok(()),
            outcome => {
                if let Some(observed) = self.index_body(key).await? {
                    if observed == body {
                        return Ok(());
                    }
                    return Err(ImmutableConflict.into());
                }
                match outcome {
                    Err(error) => {
                        Err(error.context("application index publication has an uncertain outcome"))
                    }
                    _ => bail!("application index publication rejected without an existing record"),
                }
            }
        }
    }

    async fn index_body(&self, key: &str) -> Result<Option<Bytes>> {
        if let Some((size, _)) = self.bucket.head(key).await? {
            ensure!(size <= MAX_INDEX_BYTES, "application index exceeds limit");
        }
        let Some((body, _)) = self.bucket.get(key).await? else {
            return Ok(None);
        };
        ensure!(
            body.len() as u64 <= MAX_INDEX_BYTES,
            "application index exceeds limit"
        );
        Ok(Some(body))
    }

    async fn publish_indexes(&self, application: &Application) -> Result<()> {
        for (handler, id) in &application.route_ids {
            self.immutable_index(
                &route_index_key(id),
                &RouteLocator {
                    index_version: METADATA_VERSION,
                    application: application.id.clone(),
                    handler: Some(handler.clone()),
                },
            )
            .await?;
        }
        if let Some(id) = &application.asset_route_id {
            self.immutable_index(&route_index_key(id), &RouteLocator {
                index_version: METADATA_VERSION,
                application: application.id.clone(),
                handler: None,
            }).await?;
        }
        for resource in &application.resources {
            let (class, name) = match resource.resource.kind {
                ResourceKind::Queue => (
                    celld_logic::queue::RESERVED_CLASS,
                    celld_logic::queue::cell_name(resource.resource.id.as_str()),
                ),
                ResourceKind::Cron => (
                    celld_logic::cron::RESERVED_CLASS,
                    resource.resource.id.as_str(),
                ),
                _ => continue,
            };
            let address = ResourceAddress {
                application: application.id.clone(),
                resource: resource.resource.clone(),
            };
            let locator = CellLocator {
                index_version: METADATA_VERSION,
                scope: address.cell_scope(class, name)?,
                address,
            };
            locator.validate()?;
            self.immutable_index(&cell_index_key(&locator.scope), &locator)
                .await?;
            if resource.resource.kind == ResourceKind::Cron {
                let key = format!(
                    "{CRON_PREFIX}/{}.json",
                    content_digest(locator.scope.as_bytes())
                );
                self.immutable_index(&key, &locator).await?;
            }
        }
        Ok(())
    }

    /// Called after pinned host binding authorization, before the Actor's first effects.
    pub async fn register_cell(
        &self,
        address: &ResourceAddress,
        object: &celld_applications::invocation_wire::ObjectTarget,
    ) -> Result<()> {
        object.validate()?;
        ensure!(
            address.owns(object),
            "object is outside the resource namespace"
        );
        let locator = CellLocator {
            index_version: METADATA_VERSION,
            scope: format!("{}:{}", object.class, object.id),
            address: address.clone(),
        };
        locator.validate()?;
        let snapshot = self.read(&address.application).await?;
        ensure!(
            snapshot
                .application(&address.application)
                .is_some_and(|app| app
                    .resources
                    .iter()
                    .any(|resource| resource.resource == address.resource)),
            "application cell resource is not registered"
        );
        self.immutable_index(&cell_index_key(&locator.scope), &locator)
            .await
    }

    pub async fn resolve_route(&self, id: &RouteId) -> Result<Option<RouteTarget>> {
        let Some(body) = self.index_body(&route_index_key(id)).await? else {
            return Ok(None);
        };
        let locator: RouteLocator =
            serde_json::from_slice(&body).context("invalid application route index")?;
        ensure!(
            locator.index_version == METADATA_VERSION,
            "unsupported application route index version"
        );
        let snapshot = self.read(&locator.application).await?;
        let Some(app) = snapshot.application(&locator.application) else {
            return Ok(None);
        };
        let Some(deployment) = &app.active else { return Ok(None); };
        let destination = match locator.handler {
            Some(handler) => {
                if app.route_ids.get(&handler) != Some(id)
                    || !deployment.handlers.iter().any(|item| item.id == handler && item.http) {
                    return Ok(None);
                }
                RouteDestination::Handler(handler)
            }
            None => {
                if app.asset_route_id.as_ref() != Some(id) || deployment.assets.is_none() {
                    return Ok(None);
                }
                RouteDestination::Assets
            }
        };
        Ok(Some(RouteTarget {
            application: app.id.clone(),
            deployment: deployment.id.clone(),
            destination,
        }))
    }

    pub async fn cell_owner(&self, scope: &str) -> Result<Option<CellOwner>> {
        ensure!(
            celld_logic::cell::valid_cell_scope(scope),
            "invalid application cell scope"
        );
        let Some(body) = self.index_body(&cell_index_key(scope)).await? else {
            return Ok(None);
        };
        let locator: CellLocator =
            serde_json::from_slice(&body).context("invalid application cell index")?;
        ensure!(
            locator.scope == scope,
            "application cell index identity mismatch"
        );
        locator.validate()?;
        let snapshot = self.read(&locator.address.application).await?;
        let application = snapshot.application.ok_or(UncommittedCellIndex)?;
        ensure!(
            application
                .resources
                .iter()
                .any(|resource| resource.resource == locator.address.resource),
            UncommittedCellIndex
        );
        Ok(Some(CellOwner {
            locator,
            application,
            revision: snapshot.token,
        }))
    }

    /// Candidates need cell_owner/current metadata validation before dispatch.
    pub async fn cron_candidates(
        &self,
        limit: usize,
        cursor: Option<String>,
    ) -> Result<CronCandidatePage> {
        let token = page_token(CRON_PREFIX, limit, cursor)?;
        self.validate_format().await?;
        let page = self.bucket.objects_page(CRON_PREFIX, token, limit).await?;
        ensure!(
            page.objects.len() <= limit,
            "provider cron page exceeds limit"
        );
        let mut candidates = Vec::with_capacity(page.objects.len());
        for object in page.objects {
            let key = object.location.as_ref();
            let Some(body) = self.index_body(key).await? else {
                continue;
            };
            let locator: CellLocator =
                serde_json::from_slice(&body).context("invalid application cron index")?;
            locator.validate()?;
            ensure!(
                locator.address.resource.kind == ResourceKind::Cron
                    && key
                        == format!(
                            "{CRON_PREFIX}/{}.json",
                            content_digest(locator.scope.as_bytes())
                        ),
                "application cron index identity mismatch"
            );
            candidates.push(locator);
        }
        Ok(CronCandidatePage {
            candidates,
            next_cursor: page_cursor(CRON_PREFIX, limit, page.page_token)?,
        })
    }

    /// A manifest is immutable under its application/deployment identity. This
    /// does not activate code: runtime capability and artifact checks must run
    /// before the host publishes it through activate().
    pub async fn stage(&self, deployment: &Deployment) -> Result<()> {
        deployment.validate()?;
        let key = format!(
            "{}deployments/{}/manifest.json",
            deployment.application.storage_root(),
            deployment.id.as_str()
        );
        let body = Bytes::from(serde_json::to_vec(deployment)?);
        ensure!(
            body.len() as u64 <= MAX_METADATA_BYTES,
            "application manifest exceeds metadata limit"
        );
        match self.bucket.put_cas(&key, body.clone(), None).await {
            Ok(Some(_)) => Ok(()),
            outcome => {
                if let Some((observed, _)) = self.bucket.get(&key).await? {
                    if observed != body {
                        return Err(ImmutableConflict.into());
                    }
                    return Ok(());
                }
                match outcome {
                    Err(error) => Err(error.context("deployment staging has an uncertain outcome")),
                    _ => bail!("deployment publication was rejected without an existing manifest"),
                }
            }
        }
    }

    pub async fn deployment(
        &self,
        application: &ApplicationId,
        id: &celld_applications::Name,
    ) -> Result<Option<Deployment>> {
        let key = format!(
            "{}deployments/{}/manifest.json",
            application.storage_root(),
            id.as_str()
        );
        let Some((body, _)) = self.bucket.get(&key).await? else {
            return Ok(None);
        };
        ensure!(
            body.len() as u64 <= MAX_METADATA_BYTES,
            "application manifest exceeds metadata limit"
        );
        let deployment: Deployment = serde_json::from_slice(&body)?;
        deployment.validate()?;
        ensure!(
            &deployment.application == application && &deployment.id == id,
            "stored deployment identity mismatch"
        );
        Ok(Some(deployment))
    }

    /// Trusted node code only: each node overwrites its own record.
    pub async fn publish_status(
        &self,
        application: &ApplicationId,
        node: &str,
        handlers: Vec<HandlerStatus>,
    ) -> Result<()> {
        let observed_at_unix_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        let body = serde_json::to_vec(&RuntimeStatus {
            node: node.to_owned(),
            observed_at_unix_secs,
            handlers,
        })?;
        self.bucket
            .put(&status_key(application, node), Bytes::from(body))
            .await
    }

    pub async fn clear_status(&self, application: &ApplicationId, node: &str) -> Result<()> {
        self.bucket.delete(&status_key(application, node)).await
    }

    /// Every node's record; empty until some node has published. Never derived
    /// from the catalog.
    pub async fn status(&self, application: &ApplicationId) -> Result<Vec<RuntimeStatus>> {
        let prefix = format!("{}{STATUS_PREFIX}", application.storage_root());
        let records = self.bucket.list(&prefix).await?;
        ensure!(
            records.len() <= MAX_STATUS_RECORDS,
            "application has more than {MAX_STATUS_RECORDS} node status records"
        );
        let mut statuses = Vec::new();
        for record in records {
            // A node may clear its record between the listing and the read.
            let Some((body, _)) = self.bucket.get(record.location.as_ref()).await? else {
                continue;
            };
            ensure!(
                body.len() as u64 <= MAX_METADATA_BYTES,
                "application status exceeds metadata limit"
            );
            statuses.push(serde_json::from_slice(&body)?);
        }
        Ok(statuses)
    }

    /// Code is scoped to the authenticated application even when identical
    /// bytes exist elsewhere. A content-addressed upload never publishes code.
    pub async fn upload_artifact(
        &self,
        application: &ApplicationId,
        digest: &str,
        body: Bytes,
    ) -> Result<()> {
        let key = artifact_key(application, digest)?;
        ensure!(
            body.len() <= MAX_ARTIFACT_BYTES,
            "artifact exceeds upload limit"
        );
        ensure!(content_digest(&body) == digest, "artifact digest mismatch");
        match self.bucket.put_cas(&key, body.clone(), None).await {
            Ok(Some(_)) => Ok(()),
            outcome => {
                if let Some((observed, _)) = self.bucket.get(&key).await? {
                    ensure!(observed == body, "stored artifact content mismatch");
                    return Ok(());
                }
                match outcome {
                    Err(error) => Err(error.context("artifact upload has an uncertain outcome")),
                    _ => bail!("artifact upload was rejected without existing content"),
                }
            }
        }
    }

    pub async fn artifact(
        &self,
        application: &ApplicationId,
        digest: &str,
    ) -> Result<Option<Bytes>> {
        let key = artifact_key(application, digest)?;
        if let Some((size, _)) = self.bucket.head(&key).await? {
            ensure!(
                size <= MAX_ARTIFACT_BYTES as u64,
                "stored artifact exceeds upload limit"
            );
        }
        let Some((body, _)) = self.bucket.get(&key).await? else {
            return Ok(None);
        };
        ensure!(
            body.len() <= MAX_ARTIFACT_BYTES && content_digest(&body) == digest,
            "stored artifact content mismatch"
        );
        Ok(Some(body))
    }

    /// The resolver for a handler's declared static assets. The index and the
    /// bodies are this application's own artifacts, and the local body cache
    /// is private to it, so no other application can reach them.
    pub async fn asset_resolver(
        &self,
        application: &ApplicationId,
        reference: &crate::protocol::AssetManifestRef,
        alias: Option<&str>,
    ) -> Result<crate::assets::AssetResolver> {
        ensure!(
            reference.index == "assets.json",
            "unsupported asset index name: {}",
            reference.index
        );
        let root = application.storage_root();
        let resolver = crate::assets::AssetResolver::load_scoped(
            &self.bucket,
            &artifact_key(application, &reference.sha256)?,
            Some(format!("{root}artifacts/")),
            Some(&content_digest(root.as_bytes())),
            reference,
            alias.is_none(),
            None,
        )
        .await?;
        ensure!(
            alias.is_none_or(|alias| resolver.binding_name().is_none_or(|name| name == alias)),
            "asset index binding name differs from the handler's assets alias"
        );
        if alias.is_none() {
            ensure!(resolver.worker_first_disabled(), "asset-only route cannot run a worker first");
        }
        Ok(resolver)
    }

    /// Activation check: the index is valid and every body is uploaded, as
    /// the original deploy finishes every body before publishing.
    pub async fn verify_assets(
        &self,
        application: &ApplicationId,
        reference: &crate::protocol::AssetManifestRef,
        alias: Option<&str>,
    ) -> Result<()> {
        use futures_util::{StreamExt, TryStreamExt};
        let resolver = self.asset_resolver(application, reference, alias).await?;
        let bodies: BTreeMap<_, _> = resolver
            .entries()
            .map(|entry| (entry.sha256.clone(), entry.bytes))
            .collect();
        futures_util::stream::iter(bodies)
            .map(|(digest, bytes)| async move {
                let present = self.artifact(application, &digest).await?;
                ensure!(
                    present.is_some_and(|body| body.len() as u64 == bytes),
                    "asset body {digest} is not uploaded"
                );
                Ok(())
            })
            .buffer_unordered(16)
            .try_collect::<Vec<()>>()
            .await?;
        Ok(())
    }

    async fn allocate_route(
        &self,
        application: &ApplicationId,
        handler: Option<&Name>,
        mut generate: impl FnMut() -> Result<RouteId>,
    ) -> Result<RouteId> {
        let locator = RouteLocator {
            index_version: METADATA_VERSION,
            application: application.clone(),
            handler: handler.cloned(),
        };
        for _ in 0..8 {
            let id = generate()?;
            match self.immutable_index(&route_index_key(&id), &locator).await {
                Ok(()) => return Ok(id),
                Err(error) if error.is::<ImmutableConflict>() => continue,
                Err(error) => return Err(error),
            }
        }
        bail!("application route allocation exhausted collision retries")
    }

    pub async fn activate(&self, snapshot: &mut Snapshot, deployment: Deployment) -> Result<()> {
        deployment.validate()?;
        let key = format!(
            "{}deployments/{}/manifest.json",
            deployment.application.storage_root(),
            deployment.id.as_str()
        );
        let (staged, _) = self
            .bucket
            .get(&key)
            .await?
            .context("deployment was not staged")?;
        ensure!(
            staged.as_ref() == serde_json::to_vec(&deployment)?,
            "staged deployment content differs"
        );
        let app = snapshot
            .application(&deployment.application)
            .context("application does not exist")?;
        let mut candidate = Application {
            active: Some(deployment.clone()),
            ..app.clone()
        };
        candidate.validate()?;
        for resource in &candidate.resources {
            self.validate_resource(&resource.resource)?;
        }
        if deployment.assets.is_some() && candidate.asset_route_id.is_none() {
            candidate.asset_route_id = Some(self.allocate_route(&candidate.id, None, RouteId::generate).await?);
        }
        for handler in &deployment.handlers {
            if !candidate.route_ids.contains_key(&handler.id) {
                let id = self
                    .allocate_route(&candidate.id, Some(&handler.id), RouteId::generate)
                    .await?;
                candidate.route_ids.insert(handler.id.clone(), id);
            }
        }
        applications_routes(std::iter::once(&candidate))?;
        *snapshot.application_mut(&deployment.application)? = candidate;
        Ok(())
    }
}

#[cfg(test)]
#[path = "unit_tests/applications/tests.rs"]
pub(crate) mod tests;
