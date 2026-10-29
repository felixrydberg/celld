// The management listener is an operator control-plane server outside the Actor
// execution domain; its files and tasks belong to the host.
#![allow(clippy::disallowed_methods)]

//! Authenticated application management. This listener has no peer/operator
//! routes and never hands bucket credentials to clients or application code.

use crate::applications::{
    content_digest, ApplicationLimits, ApplicationStore, Conflict, ImmutableConflict, Resource,
    Snapshot, MAX_ARTIFACT_BYTES, MAX_SECRET_BYTES,
};
use anyhow::{ensure, Context};
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Extension, Path, Query, State};
use axum::http::{header, Method, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::{Json, Router};
use celld_applications::{ApplicationId, Deployment, Name};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Access {
    Read,
    Manage,
}

#[derive(Clone, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case", deny_unknown_fields)]
pub enum Permission {
    Operator {},
    Application {
        application: ApplicationId,
        access: Access,
    },
}

impl Permission {
    fn allows(&self, application: &ApplicationId, write: bool) -> bool {
        let access = match self {
            Self::Operator {} => return true,
            Self::Application {
                application: allowed,
                access,
            } if allowed == application => access,
            _ => return false,
        };
        !write || matches!(access, Access::Manage)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    /// Lowercase SHA-256 of a random bearer token. Raw tokens never enter
    /// catalog metadata, guests, error messages or server configuration.
    pub token_sha256: String,
    pub permission: Permission,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Authorization {
    pub credentials: Vec<Credential>,
}

impl Authorization {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.credentials.is_empty() && self.credentials.len() <= 1024,
            "authorization requires 1 to 1024 credentials"
        );
        let mut tokens = BTreeSet::new();
        for credential in &self.credentials {
            ensure!(
                credential.token_sha256.len() == 64
                    && credential
                        .token_sha256
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                "invalid credential SHA-256"
            );
            ensure!(
                tokens.insert(&credential.token_sha256),
                "duplicate credential"
            );
        }
        Ok(())
    }

    fn authenticate(&self, headers: &axum::http::HeaderMap) -> Option<Permission> {
        let mut values = headers.get_all(header::AUTHORIZATION).iter();
        let value = values.next()?;
        if values.next().is_some() {
            return None;
        }
        let token = value.to_str().ok()?.strip_prefix("Bearer ")?;
        if !(32..=4096).contains(&token.len()) || token.bytes().any(|b| !b.is_ascii_graphic()) {
            return None;
        }
        let digest = content_digest(token.as_bytes());
        self.credentials
            .iter()
            .find(|credential| {
                bool::from(digest.as_bytes().ct_eq(credential.token_sha256.as_bytes()))
            })
            .map(|credential| credential.permission.clone())
    }
}

struct ApiState {
    store: ApplicationStore,
    authorization: Authorization,
    /// The nodes' `platform_domain`; used only to show handler hostnames.
    platform_domain: String,
    // ponytail: eight concurrent management bodies bound upload memory to
    // 512 MiB; stream artifacts to disk if larger bundles become necessary.
    requests: Semaphore,
    /// How long one request may hold its permit, including body extraction.
    request_timeout: Duration,
}

/// How long one management request may hold its `requests` permit. This is a
/// management API on a trusted network, not the public listener, so it is
/// generous: it bounds a stalled or hostile client without refusing a slow
/// large upload. The 64 MiB artifact ceiling is the largest legitimate body.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

type ApiError = (StatusCode, String);
type ApiResult<T> = Result<T, ApiError>;

fn bad_request(error: impl std::fmt::Display) -> ApiError {
    (StatusCode::BAD_REQUEST, error.to_string())
}
fn internal(error: anyhow::Error) -> ApiError {
    tracing::error!(%error, "application management operation failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "application management operation failed".into(),
    )
}
fn commit_error(error: anyhow::Error) -> ApiError {
    if error.is::<ImmutableConflict>() {
        return (
            StatusCode::CONFLICT,
            "deployment identity already holds different content".into(),
        );
    }
    if error.is::<Conflict>() {
        (
            StatusCode::CONFLICT,
            "application revision changed; read again before editing".into(),
        )
    } else {
        internal(error)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Revision {
    pub token: Option<String>,
}

impl Revision {
    fn of(snapshot: &Snapshot) -> Self {
        Self {
            token: snapshot.revision().map(str::to_owned),
        }
    }
    fn check(&self, snapshot: &Snapshot) -> ApiResult<()> {
        if self.token.as_deref() != snapshot.revision() {
            return Err((
                StatusCode::CONFLICT,
                "application revision changed; read again before editing".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Edit {
    revision: Revision,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Activate {
    revision: Revision,
    deployment: Name,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterResource {
    revision: Revision,
    resource: Resource,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SetSecret {
    revision: Revision,
    value: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SetLimits {
    revision: Revision,
    limits: ApplicationLimits,
}

fn application(app: String) -> ApiResult<ApplicationId> {
    Ok(ApplicationId { app: app.try_into().map_err(bad_request)? })
}

async fn authenticate(
    State(state): State<Arc<ApiState>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let Some(permission) = state.authorization.authenticate(request.headers()) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Ok(_permit) = state.requests.try_acquire() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    request.extensions_mut().insert(permission);
    // The permit is held across body extraction and the handler. A client can
    // send a request head and never send the body, so without a deadline eight
    // such requests hold every permit forever and every other route, including
    // GET, answers 503. Bound each hold; the permit drops with the future.
    match crate::asyncrt::timeout(state.request_timeout, next.run(request)).await {
        Ok(response) => response,
        Err(_) => StatusCode::REQUEST_TIMEOUT.into_response(),
    }
}

/// Runs before JSON/Bytes extraction. A caller cannot spend another application's
/// upload budget or reach the catalog by supplying identity fields in a body.
async fn authorize_application(
    Extension(permission): Extension<Permission>,
    Path(params): Path<HashMap<String, String>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let id = match application(params["app"].clone()) {
        Ok(id) => id,
        Err(error) => return error.into_response(),
    };
    if !permission.allows(
        &id,
        !matches!(*request.method(), Method::GET | Method::HEAD),
    ) {
        return StatusCode::FORBIDDEN.into_response();
    }
    next.run(request).await
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Listing {
    limit: Option<usize>,
    cursor: Option<String>,
}

async fn list(
    State(state): State<Arc<ApiState>>,
    Extension(permission): Extension<Permission>,
    Query(page): Query<Listing>,
) -> ApiResult<Json<serde_json::Value>> {
    let limit = page.limit.unwrap_or(100);
    if !(1..=crate::applications::MAX_APPLICATION_PAGE).contains(&limit) {
        return Err(bad_request("page limit must be between 1 and 256"));
    }
    if page.cursor.as_ref().is_some_and(|cursor| cursor.len() > 16384) {
        return Err(bad_request("application cursor exceeds limit"));
    }
    let (applications, next_cursor) = match permission {
        Permission::Operator {} => {
            let page = state.store.list(limit, page.cursor).await.map_err(|error| {
                if let Some(error) = error.downcast_ref::<crate::applications::ApplicationPageError>() {
                    bad_request(error)
                } else {
                    internal(error)
                }
            })?;
            let entries: Vec<_> = page.applications.into_iter().map(|entry|
                json!({"application": entry.application, "revision": Revision { token: entry.revision }})
            ).collect();
            (entries, page.next_cursor)
        }
        Permission::Application { application, .. } => {
            if page.cursor.is_some() {
                return Err(bad_request("application-scoped listing does not accept a cursor"));
            }
            let snapshot = state.store.read(&application).await.map_err(internal)?;
            let entries: Vec<_> = snapshot.application(&application).into_iter().map(|app|
                json!({"application": app, "revision": Revision::of(&snapshot)})
            ).collect();
            (entries, None)
        }
    };
    let body = json!({"applications": applications, "next_cursor": next_cursor});
    if serde_json::to_vec(&body).map_err(|error| internal(error.into()))?.len()
        > crate::applications::MAX_APPLICATION_PAGE_BYTES {
        return Err(bad_request("application page exceeds 16 MiB; use a smaller limit"));
    }
    Ok(Json(body))
}

async fn inspect(
    State(state): State<Arc<ApiState>>,
    Path(app): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let id = application(app)?;
    let snapshot = state.store.read(&id).await.map_err(internal)?;
    let app = snapshot
        .application(&id)
        .ok_or((StatusCode::NOT_FOUND, "application does not exist".into()))?;
    // "application" is catalog metadata (resources are registered, not
    // provisioned).
    // Public hostname of each served handler of the active deployment.
    let hostnames: std::collections::BTreeMap<_, _> = app
        .active
        .iter()
        .flat_map(|deployment| &deployment.handlers)
        .filter(|handler| handler.http)
        .filter_map(|handler| {
            let id = app.route_ids.get(&handler.id)?;
            Some((&handler.id, format!("{}.{}", id.as_str(), state.platform_domain)))
        })
        .collect();
    let asset_hostname = app.active.as_ref().filter(|deployment| deployment.assets.is_some())
        .and(app.asset_route_id.as_ref()).map(|id| format!("{}.{}", id.as_str(), state.platform_domain));
    Ok(Json(
        json!({"revision": Revision::of(&snapshot), "application": app, "hostnames": hostnames, "asset_hostname": asset_hostname}),
    ))
}

async fn create(
    State(state): State<Arc<ApiState>>,
    Path(app): Path<String>,
    Json(edit): Json<Edit>,
) -> ApiResult<Json<serde_json::Value>> {
    let id = application(app)?;
    let mut snapshot = state.store.read(&id).await.map_err(internal)?;
    edit.revision.check(&snapshot)?;
    if snapshot.application(&id).is_some() {
        return Err((StatusCode::CONFLICT, "application already exists".into()));
    }
    snapshot.create(id.clone()).map_err(bad_request)?;
    let token = state.store.commit(snapshot).await.map_err(commit_error)?;
    Ok(Json(
        json!({"revision": Revision { token: Some(token) }, "application": id}),
    ))
}

async fn resource(
    State(state): State<Arc<ApiState>>,
    Path(app): Path<String>,
    Json(edit): Json<RegisterResource>,
) -> ApiResult<Json<Revision>> {
    state
        .store
        .validate_resource(&edit.resource.resource)
        .map_err(bad_request)?;
    let id = application(app)?;
    let mut snapshot = state.store.read(&id).await.map_err(internal)?;
    edit.revision.check(&snapshot)?;
    if snapshot.application(&id).is_none() {
        return Err((StatusCode::NOT_FOUND, "application does not exist".into()));
    }
    snapshot
        .register_resource(&id, edit.resource)
        .map_err(bad_request)?;
    let token = state.store.commit(snapshot).await.map_err(commit_error)?;
    Ok(Json(Revision { token: Some(token) }))
}

/// Publishes a staged deployment (also rollback: any staged version). The
/// catalog commit is the only acknowledgement; refusals publish nothing.
async fn activate(
    State(state): State<Arc<ApiState>>,
    Path(app): Path<String>,
    Json(edit): Json<Activate>,
) -> ApiResult<Json<Revision>> {
    let id = application(app)?;
    let mut snapshot = state.store.read(&id).await.map_err(internal)?;
    edit.revision.check(&snapshot)?;
    if snapshot.application(&id).is_none() {
        return Err((StatusCode::NOT_FOUND, "application does not exist".into()));
    }
    let deployment = state
        .store
        .deployment(&id, &edit.deployment)
        .await
        .map_err(internal)?
        .ok_or((StatusCode::NOT_FOUND, "deployment is not staged".into()))?;
    for handler in &deployment.handlers {
        // artifact() also proves the stored bytes match the digest.
        if state
            .store
            .artifact(&id, &handler.artifact_sha256)
            .await
            .map_err(internal)?
            .is_none()
        {
            return Err((
                StatusCode::CONFLICT,
                "handler artifact is not uploaded".into(),
            ));
        }
    }
    for handler in &deployment.handlers {
        if let Some((alias, reference)) =
            crate::application_javascript_guest::asset_binding(handler)
                .map_err(|error| (StatusCode::CONFLICT, error.to_string()))?
        {
            state
                .store
                .verify_assets(&id, &reference, Some(&alias))
                .await
                .map_err(|error| (StatusCode::CONFLICT, error.to_string()))?;
        }
    }
    if let Some(reference) = &deployment.assets {
        state.store.verify_assets(&id, reference, None).await
            .map_err(|error| (StatusCode::CONFLICT, error.to_string()))?;
    }
    // Route conflicts and unregistered secrets/resources refuse here.
    state
        .store
        .activate(&mut snapshot, deployment)
        .await
        .map_err(|error| (StatusCode::CONFLICT, error.to_string()))?;
    let token = state.store.commit(snapshot).await.map_err(commit_error)?;
    Ok(Json(Revision { token: Some(token) }))
}

async fn deactivate(
    State(state): State<Arc<ApiState>>,
    Path(app): Path<String>,
    Json(edit): Json<Edit>,
) -> ApiResult<Json<Revision>> {
    let id = application(app)?;
    let mut snapshot = state.store.read(&id).await.map_err(internal)?;
    edit.revision.check(&snapshot)?;
    if snapshot.application(&id).is_none() {
        return Err((StatusCode::NOT_FOUND, "application does not exist".into()));
    }
    snapshot.deactivate(&id).map_err(bad_request)?;
    let token = state.store.commit(snapshot).await.map_err(commit_error)?;
    Ok(Json(Revision { token: Some(token) }))
}

async fn secrets(
    State(state): State<Arc<ApiState>>,
    Path(app): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let id = application(app)?;
    let snapshot = state.store.read(&id).await.map_err(internal)?;
    let app = snapshot
        .application(&id)
        .ok_or((StatusCode::NOT_FOUND, "application does not exist".into()))?;
    Ok(Json(
        json!({"revision": Revision::of(&snapshot), "secrets": app.secrets}),
    ))
}

async fn set_secret(
    State(state): State<Arc<ApiState>>,
    Path((app, name)): Path<(String, String)>,
    Json(edit): Json<SetSecret>,
) -> ApiResult<Json<Revision>> {
    let id = application(app)?;
    let name: Name = name.try_into().map_err(bad_request)?;
    if edit.value.len() > MAX_SECRET_BYTES || edit.value.contains('\0') {
        return Err(bad_request("secret exceeds 16 KiB or contains NUL"));
    }
    let mut snapshot = state.store.read(&id).await.map_err(internal)?;
    edit.revision.check(&snapshot)?;
    let app = snapshot
        .application(&id)
        .ok_or((StatusCode::NOT_FOUND, "application does not exist".into()))?;
    if !app.secrets.contains_key(&name) && app.secrets.len() >= 256 {
        return Err(bad_request("application secret count exceeds 256"));
    }
    let version = state
        .store
        .stage_secret(&id, &name, edit.value)
        .await
        .map_err(internal)?;
    snapshot
        .set_secret(&id, name, version)
        .map_err(bad_request)?;
    let token = state.store.commit(snapshot).await.map_err(commit_error)?;
    Ok(Json(Revision { token: Some(token) }))
}

async fn delete_secret(
    State(state): State<Arc<ApiState>>,
    Path((app, name)): Path<(String, String)>,
    Json(edit): Json<Edit>,
) -> ApiResult<Json<Revision>> {
    let id = application(app)?;
    let name: Name = name.try_into().map_err(bad_request)?;
    let mut snapshot = state.store.read(&id).await.map_err(internal)?;
    edit.revision.check(&snapshot)?;
    let app = snapshot
        .application(&id)
        .ok_or((StatusCode::NOT_FOUND, "application does not exist".into()))?;
    if !app.secrets.contains_key(&name) {
        return Err((
            StatusCode::NOT_FOUND,
            "application secret does not exist".into(),
        ));
    }
    snapshot.remove_secret(&id, &name).map_err(|_| {
        (
            StatusCode::CONFLICT,
            "secret is referenced by the active deployment".into(),
        )
    })?;
    let token = state.store.commit(snapshot).await.map_err(commit_error)?;
    Ok(Json(Revision { token: Some(token) }))
}

async fn set_limits(
    State(state): State<Arc<ApiState>>,
    Path(app): Path<String>,
    Json(edit): Json<SetLimits>,
) -> ApiResult<Json<Revision>> {
    edit.limits.validate().map_err(bad_request)?;
    edit_limits(
        &state,
        application(app)?,
        edit.revision,
        Some(edit.limits),
    )
    .await
}

async fn reset_limits(
    State(state): State<Arc<ApiState>>,
    Path(app): Path<String>,
    Json(edit): Json<Edit>,
) -> ApiResult<Json<Revision>> {
    edit_limits(&state, application(app)?, edit.revision, None).await
}

async fn edit_limits(
    state: &ApiState,
    id: ApplicationId,
    revision: Revision,
    limits: Option<ApplicationLimits>,
) -> ApiResult<Json<Revision>> {
    let mut snapshot = state.store.read(&id).await.map_err(internal)?;
    revision.check(&snapshot)?;
    if snapshot.application(&id).is_none() {
        return Err((StatusCode::NOT_FOUND, "application does not exist".into()));
    }
    snapshot.set_limits(&id, limits).map_err(bad_request)?;
    let token = state.store.commit(snapshot).await.map_err(commit_error)?;
    Ok(Json(Revision { token: Some(token) }))
}

async fn stage(
    State(state): State<Arc<ApiState>>,
    Path((app, version)): Path<(String, String)>,
    Json(deployment): Json<Deployment>,
) -> ApiResult<StatusCode> {
    let id = application(app)?;
    let version: Name = version.try_into().map_err(bad_request)?;
    if deployment.application != id || deployment.id != version {
        return Err(bad_request("deployment does not match request path"));
    }
    deployment.validate().map_err(bad_request)?;
    let snapshot = state.store.read(&id).await.map_err(internal)?;
    if snapshot.application(&id).is_none() {
        return Err((StatusCode::NOT_FOUND, "application does not exist".into()));
    }
    if let Some(existing) = state
        .store
        .deployment(&id, &version)
        .await
        .map_err(internal)?
    {
        if existing != deployment {
            return Err((
                StatusCode::CONFLICT,
                "deployment identity already holds different content".into(),
            ));
        }
    }
    state.store.stage(&deployment).await.map_err(commit_error)?;
    Ok(StatusCode::CREATED)
}

async fn manifest(
    State(state): State<Arc<ApiState>>,
    Path((app, version)): Path<(String, String)>,
) -> ApiResult<Json<Deployment>> {
    let id = application(app)?;
    let version = version.try_into().map_err(bad_request)?;
    state
        .store
        .deployment(&id, &version)
        .await
        .map_err(internal)?
        .map(Json)
        .ok_or((StatusCode::NOT_FOUND, "deployment does not exist".into()))
}

async fn upload(
    State(state): State<Arc<ApiState>>,
    Path((app, digest)): Path<(String, String)>,
    body: Bytes,
) -> ApiResult<StatusCode> {
    let id = application(app)?;
    if content_digest(&body) != digest {
        return Err(bad_request("artifact digest mismatch"));
    }
    let snapshot = state.store.read(&id).await.map_err(internal)?;
    if snapshot.application(&id).is_none() {
        return Err((StatusCode::NOT_FOUND, "application does not exist".into()));
    }
    state
        .store
        .upload_artifact(&id, &digest, body)
        .await
        .map_err(internal)?;
    Ok(StatusCode::CREATED)
}

/// The management router with the shipped permit deadline. Callers that need a
/// short deadline (tests) use an explicit `router_with` so the bound is
/// exercised without waiting for the real one.
pub fn router(
    store: ApplicationStore,
    authorization: Authorization,
    platform_domain: String,
) -> anyhow::Result<Router> {
    router_with(store, authorization, platform_domain, REQUEST_TIMEOUT)
}

pub(crate) fn router_with(
    store: ApplicationStore,
    authorization: Authorization,
    platform_domain: String,
    request_timeout: Duration,
) -> anyhow::Result<Router> {
    authorization.validate()?;
    ensure!(
        celld_applications::valid_hostname(&platform_domain) && platform_domain.contains('.'),
        "platform domain must be a lowercase DNS name with at least two labels"
    );
    let state = Arc::new(ApiState {
        store,
        authorization,
        platform_domain,
        requests: Semaphore::new(8),
        request_timeout,
    });
    let applications = Router::new()
        .route("/v1/applications/:app", get(inspect).post(create))
        .route("/v1/applications/:app/resources", put(resource))
        .route("/v1/applications/:app/secrets", get(secrets))
        .route(
            "/v1/applications/:app/secrets/:name",
            put(set_secret).delete(delete_secret),
        )
        .route(
            "/v1/applications/:app/limits",
            put(set_limits).delete(reset_limits),
        )
        .route(
            "/v1/applications/:app/active",
            put(activate).delete(deactivate),
        )
        .route(
            "/v1/applications/:app/deployments/:version",
            get(manifest).put(stage),
        )
        .route(
            "/v1/applications/:app/artifacts/:digest",
            put(upload).layer(DefaultBodyLimit::max(MAX_ARTIFACT_BYTES)),
        )
        .route_layer(middleware::from_fn(authorize_application));
    Ok(Router::new()
        .route("/v1/applications", get(list))
        .merge(applications)
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .layer(middleware::from_fn_with_state(state.clone(), authenticate))
        .with_state(state))
}

/// A management-only process uses the same production provider/CAS adapter as
/// nodes. Startup validates authorization before contacting the bucket.
pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let mut fleet = crate::cli_options::FleetFlags::default();
    let mut listen = "127.0.0.1:8090".to_owned();
    let mut authorization = None;
    let mut platform_domain = None;
    let mut args = arguments.into_iter();
    while let Some(arg) = args.next() {
        if arg == "--help" || arg == "-h" {
            crate::note!(
                "celld application-api --auth-file FILE --platform-domain DOMAIN [--listen IP:PORT]\n  --platform-domain  the nodes' platform_domain; handlers are served at <id>.DOMAIN\n{}",
                crate::cli_options::FLEET_HELP
            );
            return Ok(());
        }
        let mut value = |flag: &str| {
            args.next()
                .with_context(|| format!("{flag} requires a value"))
        };
        if fleet.consume(&arg, &mut value)? {
            continue;
        }
        match arg.as_str() {
            "--listen" => listen = value("--listen")?,
            "--auth-file" => authorization = Some(value("--auth-file")?),
            "--platform-domain" => platform_domain = Some(value("--platform-domain")?),
            _ => anyhow::bail!("unknown application-api option; run celld application-api --help"),
        }
    }
    let file = authorization.context("application-api requires --auth-file")?;
    let platform_domain = platform_domain.context("application-api requires --platform-domain")?;
    let bytes = tokio::fs::read(file)
        .await
        .context("read application authorization file")?;
    let authorization: Authorization =
        serde_json::from_slice(&bytes).context("invalid application authorization file")?;
    authorization.validate()?;
    let listen: std::net::SocketAddr = listen.parse().context("listen requires an IP:PORT")?;
    let store = ApplicationStore::new(fleet.resolve("celld application-api")?.open().await?);
    let app = router(store, authorization, platform_domain)?;
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(address = %listener.local_addr()?, "application management listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

#[cfg(test)]
#[path = "unit_tests/application_api/tests.rs"]
mod tests;
