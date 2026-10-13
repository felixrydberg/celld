//! Application contracts shared by management, host routing and guest adapters.
//! No JavaScript engine, VM launcher or storage credentials belong here.

use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub mod bootstrap;
pub mod invocation_wire;
pub mod object_id;
pub mod resource_wire;
pub mod storage_wire;

pub const MANIFEST_VERSION: u16 = 1;

/// Validated on deserialization as well as direct construction. Names are path
/// segments, not arbitrary bucket keys supplied by a caller.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Name(String);

impl TryFrom<String> for Name {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self> {
        ensure!(
            !value.is_empty()
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_')),
            "invalid application identifier"
        );
        Ok(Self(value))
    }
}

impl From<Name> for String {
    fn from(value: Name) -> Self {
        value.0
    }
}

impl Name {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationId {
    pub app: Name,
}

impl ApplicationId {
    /// Relative to the existing fleet bucket prefix, not a replacement for it.
    pub fn storage_root(&self) -> String {
        format!("applications/v2/apps/{}/", self.app.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    DurableObject,
    D1,
    Kv,
    Queue,
    Cron,
    Workflow,
    R2,
    Container,
    WorkerLoader,
    StaticAssets,
}

impl ResourceKind {
    pub fn storage_segment(self) -> &'static str {
        match self {
            Self::DurableObject => "durable_objects",
            Self::D1 => "d1",
            Self::Kv => "kv",
            Self::Queue => "queues",
            Self::Cron => "cron",
            Self::Workflow => "workflows",
            Self::R2 => "r2",
            Self::Container => "containers",
            Self::WorkerLoader => "worker_loaders",
            Self::StaticAssets => "assets",
        }
    }
}

/// Guest binding declarations carry only an application-local reference. The
/// host attaches the authenticated application, never guest-supplied tenancy.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRef {
    pub kind: ResourceKind,
    pub id: Name,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceAddress {
    pub application: ApplicationId,
    pub resource: ResourceRef,
}

impl ResourceAddress {
    pub fn storage_root(&self) -> String {
        format!(
            "{}resources/{}/{}/",
            self.application.storage_root(),
            self.resource.kind.storage_segment(),
            self.resource.id.as_str()
        )
    }

    /// Retain the original Class:instance grammar and original node-log/LTX
    /// addressing. The digest incorporates application, namespace and instance;
    /// deployment versions and mutable binding aliases deliberately do not.
    pub fn cell_scope(&self, class: &str, instance: &str) -> Result<String> {
        ensure!(
            !class.is_empty() && !class.contains(':') && celld_logic::cell::valid_cell_scope(class),
            "invalid cell class"
        );
        let key: [u8; 32] = Sha256::digest(self.namespace_key(class).as_bytes()).into();
        let digest = object_id::for_name(&key, instance);
        let scope = format!("{class}:{}", hex::encode(digest));
        ensure!(
            celld_logic::cell::valid_cell_scope(&scope),
            "cell class is too long"
        );
        Ok(scope)
    }

    /// Whether `object` lives in this resource's namespace: its id is authentic
    /// under the namespace key, named or unique, and a name must derive it.
    pub fn owns(&self, object: &invocation_wire::ObjectTarget) -> bool {
        let key: [u8; 32] = Sha256::digest(self.namespace_key(&object.class).as_bytes()).into();
        let Some(id) = hex::decode(&object.id)
            .ok()
            .and_then(|id| <[u8; 32]>::try_from(id).ok())
            .filter(|id| hex::encode(id) == object.id)
        else {
            return false;
        };
        object_id::authentic(&key, &id)
            && object
                .name
                .as_deref()
                .is_none_or(|name| object_id::for_name(&key, name) == id)
    }

    /// The resource, rather than a handler deployment or binding alias, owns
    /// the script identity used by original Celld's namespace derivation.
    pub fn namespace_key(&self, class: &str) -> String {
        let root = self.storage_root();
        format!("cells:v1:{}:{root}:{class}", root.len())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Language {
    Javascript,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BindingTarget {
    Resource { resource: ResourceRef },
    Handler { handler: Name },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Handler {
    pub id: Name,
    pub language: Language,
    pub entrypoint: String,
    pub artifact_sha256: String,
    pub runtime_image_sha256: String,
    /// Served from public ingress at the host-assigned `<route id>.<platform
    /// domain>` while this deployment is active. Cron and queue triggers do
    /// not need it.
    #[serde(default)]
    pub http: bool,
    #[serde(default)]
    pub bindings: BTreeMap<Name, BindingTarget>,
    #[serde(default)]
    pub environment: BTreeMap<Name, String>,
    /// Application-local secret names. The management layer resolves these;
    /// callers cannot use references to arbitrary host environment variables.
    #[serde(default)]
    pub secrets: BTreeMap<Name, Name>,
    /// Original Celld Worker metadata is retained rather than translated into
    /// a reduced function API. Publication must run the original validation.
    #[serde(default = "empty_object")]
    pub worker_config: serde_json::Value,
}

fn empty_object() -> serde_json::Value {
    serde_json::json!({})
}

impl Handler {
    fn validate(&self) -> Result<()> {
        ensure!(
            !self.entrypoint.is_empty()
                && self.entrypoint.len() <= 1024
                && !self.entrypoint.contains(['\\', ':', '\0'])
                && self
                    .entrypoint
                    .split('/')
                    .all(|part| !matches!(part, "" | "." | "..")),
            "entrypoint must be a relative artifact path"
        );
        for digest in [&self.artifact_sha256, &self.runtime_image_sha256] {
            ensure!(
                digest.len() == 64
                    && digest
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                "image and artifact digests must be lowercase SHA-256"
            );
        }
        ensure!(
            self.worker_config.is_object(),
            "worker_config must be an object"
        );
        let mut names = BTreeSet::new();
        for name in self
            .bindings
            .keys()
            .chain(self.environment.keys())
            .chain(self.secrets.keys())
        {
            ensure!(
                names.insert(name),
                "binding, environment and secret names must be unique"
            );
        }
        Ok(())
    }
}

/// Host-assigned, unguessable public label of one (application, handler): 128
/// random bits as 32 lowercase hex digits, so a valid DNS label. A deployment
/// can only ask for a handler to be served (`Handler::http`); it never names one.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RouteId(String);

impl TryFrom<String> for RouteId {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self> {
        ensure!(
            value.len() == 32
                && value
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "invalid route id"
        );
        Ok(Self(value))
    }
}

impl From<RouteId> for String {
    fn from(value: RouteId) -> Self {
        value.0
    }
}

impl RouteId {
    pub fn generate() -> Result<Self> {
        let mut random = [0u8; 16];
        getrandom::fill(&mut random)
            .map_err(|_| anyhow::anyhow!("route id entropy unavailable"))?;
        Ok(Self(hex::encode(random)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Trigger {
    Cron {
        id: Name,
        expression: String,
        handler: Name,
    },
    Queue {
        queue: Name,
        handler: Name,
    },
}

/// Reference from a deploy manifest to its immutable, canonical asset index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssetManifestRef {
    pub index: String,
    pub sha256: String,
    pub file_count: u32,
    pub total_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// One immutable publication; persistent resources survive handler removal.
pub struct Deployment {
    pub manifest_version: u16,
    pub application: ApplicationId,
    pub id: Name,
    #[serde(default)]
    pub handlers: Vec<Handler>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assets: Option<AssetManifestRef>,
    /// Removed: tenants no longer choose hostnames. Kept only so `validate`
    /// can refuse old manifests with a clear error.
    #[serde(default, skip_serializing)]
    pub http_routes: Option<serde_json::Value>,
    #[serde(default)]
    pub triggers: Vec<Trigger>,
}

impl Deployment {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.manifest_version == MANIFEST_VERSION,
            "unsupported application manifest version"
        );
        ensure!(!self.handlers.is_empty() || self.assets.is_some(), "deployment has no handlers or assets");
        if let Some(assets) = &self.assets {
            ensure!(assets.index == "assets.json", "unsupported asset index name");
            ensure!(assets.sha256.len() == 64 && assets.sha256.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)), "invalid asset index digest");
        }
        let mut handlers = BTreeSet::new();
        for handler in &self.handlers {
            handler.validate()?;
            ensure!(handlers.insert(&handler.id), "duplicate handler");
        }
        for handler in &self.handlers {
            for binding in handler.bindings.values() {
                if let BindingTarget::Handler { handler } = binding {
                    ensure!(
                        handlers.contains(handler),
                        "service binding names an unknown handler"
                    );
                }
            }
        }
        ensure!(
            self.http_routes.is_none(),
            "http_routes is no longer supported: hostnames are assigned by the host; set \"http\": true on a handler"
        );
        let mut crons = BTreeSet::new();
        let mut queues = BTreeSet::new();
        for trigger in &self.triggers {
            match trigger {
                Trigger::Cron {
                    id,
                    expression,
                    handler,
                } => {
                    ensure!(handlers.contains(handler), "cron names an unknown handler");
                    ensure!(crons.insert(id), "duplicate cron schedule");
                    celld_logic::cron::parse(expression)
                        .map_err(|error| anyhow::anyhow!("invalid cron expression: {error}"))?;
                }
                Trigger::Queue { queue, handler } => {
                    ensure!(handlers.contains(handler), "queue names an unknown handler");
                    ensure!(queues.insert(queue), "duplicate queue consumer");
                }
            }
        }
        Ok(())
    }
}

pub fn valid_hostname(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= 253
        && host
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'.'))
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label.as_bytes()[0].is_ascii_alphanumeric()
                && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
        })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RouteDestination {
    Handler(Name),
    Assets,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteTarget {
    pub application: ApplicationId,
    pub deployment: Name,
    pub destination: RouteDestination,
}

impl RouteTarget {
    pub fn handler(&self) -> Result<&Name> {
        match &self.destination {
            RouteDestination::Handler(handler) => Ok(handler),
            RouteDestination::Assets => anyhow::bail!("asset route has no executable handler"),
        }
    }
}

/// Route id to the handler of an application's active deployment. A request
/// can only reach the handler its own host label names; no claim exists to
/// contest, and nothing a tenant writes selects another tenant's id.
#[derive(Default)]
pub struct RouteTable(BTreeMap<RouteId, RouteTarget>);

impl RouteTable {
    /// Each item is an application's active deployment with the ids the host
    /// assigned to its handlers.
    pub fn from_deployments<'a>(
        applications: impl IntoIterator<Item = (&'a Deployment, &'a BTreeMap<Name, RouteId>, Option<&'a RouteId>)>,
    ) -> Result<Self> {
        let mut routes = BTreeMap::new();
        for (deployment, ids, asset_id) in applications {
            deployment.validate()?;
            if deployment.assets.is_some() {
                let id = asset_id.ok_or_else(|| anyhow::anyhow!("assets have no assigned route id"))?;
                let target = RouteTarget {
                    application: deployment.application.clone(),
                    deployment: deployment.id.clone(),
                    destination: RouteDestination::Assets,
                };
                ensure!(routes.insert(id.clone(), target).is_none(), "route id belongs to multiple targets");
            }
            for handler in deployment.handlers.iter().filter(|handler| handler.http) {
                let id = ids
                    .get(&handler.id)
                    .ok_or_else(|| anyhow::anyhow!("HTTP handler has no assigned route id"))?;
                let target = RouteTarget {
                    application: deployment.application.clone(),
                    deployment: deployment.id.clone(),
                    destination: RouteDestination::Handler(handler.id.clone()),
                };
                ensure!(
                    routes.insert(id.clone(), target).is_none(),
                    "route id belongs to multiple handlers"
                );
            }
        }
        Ok(Self(routes))
    }

    /// `hostname` is a request Host (optional port). Only `<route id>.<domain>`
    /// can resolve.
    pub fn resolve(&self, hostname: &str, domain: &str) -> Option<&RouteTarget> {
        let label = route_label(hostname, domain)?;
        self.0.get(&RouteId::try_from(label).ok()?)
    }

    /// Whether `hostname` is inside the platform domain, whatever its label.
    pub fn is_platform_host(hostname: &str, domain: &str) -> bool {
        let name = hostname.split_once(':').map_or(hostname, |(name, _)| name);
        name.to_ascii_lowercase()
            .strip_suffix(domain)
            .is_some_and(|label| label.ends_with('.'))
    }
}

/// The route label of a request Host, from the one place that parses a Host for
/// routing. `Some("<route id>")` only for a `<route id>.<domain>` host whose port
/// is a non-zero number; the label is case-folded, the domain must be lowercase
/// as configured.
///
/// Both routing paths derive their label here: `RouteTable::resolve` for the
/// in-memory table and `application_vm::resolve` for the store lookup a node
/// actually serves from. Keeping one owner is what makes the port and case rules
/// the rules that run, rather than rules only a test exercised.
pub fn route_label(hostname: &str, domain: &str) -> Option<String> {
    let hostname = match hostname.split_once(':') {
        Some((name, port)) if port.parse::<u16>().is_ok_and(|port| port > 0) => name,
        Some(_) => return None,
        None => hostname,
    }
    .to_ascii_lowercase();
    let label = hostname.strip_suffix(domain)?.strip_suffix('.')?;
    Some(label.to_owned())
}
