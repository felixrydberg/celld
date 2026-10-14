//! `celld application` transports management requests. It never opens the
//! fleet bucket or implements a second catalog/storage path.
// The management CLI is an operator command that reads local files and runs
// outside the Actor execution domain.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Context};
use celld_applications::{ApplicationId, Name};
use reqwest::{Client, Method, RequestBuilder};
use serde_json::{json, Value};
use std::io::Write;
use std::time::Duration;

const HELP: &str =
    "celld application list|create|get|resource|stage|manifest|artifact|assets|python|activate|deactivate|secrets|secret-put|secret-delete|limits|limits-reset [OPTIONS]
  --api URL           Management URL (or CELLD_APPLICATION_API)
  --app NAME          Application (required except list)
  --limit COUNT       Listing page size (default 100, maximum 256)
  --cursor TOKEN      Resume one listing page
  --file FILE         Resource/deployment/limits JSON, an artifact image, or raw UTF-8 secret;
                      for assets, an optional project config JSON (assets, compatibility_*);
                      for python, the project's wrangler.jsonc (needs esbuild and mkfs.erofs;
                      builds the Python Worker here and uploads it; prints entrypoint and worker_config)
  --directory DIR     Static asset directory to upload with assets; prints assets (top-level for an asset-only deployment, or worker_config.assets)
  --secret NAME       Application secret name for secret-put/secret-delete
  --deployment NAME   Deployment to inspect with manifest or publish with activate
  CELLD_APPLICATION_TOKEN is the bearer credential; use HTTPS for remote APIs.

Edits read the application revision once; create uses an absent revision. A conflict stops the command without retry.
Staging/uploading does not activate handlers; activate publishes a staged
deployment (activate an older one to roll back; stage and activate one without
a handler to remove it). Nodes started with --application-domain serve assets; --application-vms serves executable
deployments.
get shows metadata under application (resources are registered, not
provisioned) and node-observed handler state under runtime (one record per node,
empty until a node reports; check observed_at_unix_secs for freshness).";

async fn response(request: RequestBuilder) -> anyhow::Result<Value> {
    let mut response = request
        .send()
        .await
        .context("application API request failed")?;
    let status = response.status();
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len().saturating_add(chunk.len()) <= 16 * 1024 * 1024,
            "application API response exceeds limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    ensure!(
        status.is_success(),
        "application API returned {status}: {}",
        String::from_utf8_lossy(&bytes)
    );
    if bytes.is_empty() {
        return Ok(json!({"status": status.as_u16()}));
    }
    serde_json::from_slice(&bytes).context("application API returned invalid JSON")
}

/// A Python project built into a handler artifact by upstream's
/// `deploy::build`: a read-only image of the bundle (`index.js`, with the
/// handler's code and the Workers SDK) and the `worker_config` that has it load
/// the interpreter from the node's shared Pyodide drive. The guest runs it in
/// V8 like any JavaScript handler.
pub(crate) fn python_artifact(config: std::path::PathBuf) -> anyhow::Result<(Vec<u8>, Value)> {
    let built = crate::deploy::build(&crate::deploy::Options {
        config: Some(config),
        bucket: None,
        endpoint: None,
        region: None,
        dry_run: true,
        json: false,
        vars: Default::default(),
        local_images: true,
        shared_python_runtime: true,
    })?;
    ensure!(
        built
            .manifest
            .required_features
            .iter()
            .any(|feature| feature == crate::protocol::FEATURE_PYTHON_WORKERS_V1),
        "`main` of the project config must be a .py file"
    );
    ensure!(
        built.assets.is_none(),
        "upload static assets with `celld application assets`, not in a python project config"
    );
    let mut worker_config = crate::application_pyodide::handler_config();
    for key in ["compatibility_date", "compatibility_flags"] {
        if let Some(value) = built.manifest.raw_metadata.get(key) {
            worker_config[key] = value.clone();
        }
    }
    Ok((crate::application_image::build(&built.modules)?, worker_config))
}

pub(crate) async fn execute(arguments: Vec<String>, token: &str) -> anyhow::Result<Value> {
    ensure!(
        (32..=4096).contains(&token.len()) && token.bytes().all(|b| b.is_ascii_graphic()),
        "CELLD_APPLICATION_TOKEN must contain 32 to 4096 printable non-space bytes"
    );
    let mut args = arguments.into_iter();
    let command = args.next().context("application command required")?;
    ensure!(
        matches!(
            command.as_str(),
            "list"
                | "create"
                | "get"
                | "resource"
                | "stage"
                | "manifest"
                | "artifact"
                | "assets"
                | "python"
                | "activate"
                | "deactivate"
                | "secrets"
                | "secret-put"
                | "secret-delete"
                | "limits"
                | "limits-reset"
        ),
        "unknown application command"
    );
    let mut api = std::env::var("CELLD_APPLICATION_API").ok();
    let (mut app, mut file, mut deployment) = (None, None, None);
    let (mut limit, mut cursor) = (None, None);
    let mut directory = None;
    let mut secret = None;
    while let Some(arg) = args.next() {
        let value = args.next().context("application option requires a value")?;
        match arg.as_str() {
            "--api" => api = Some(value),
            "--app" => app = Some(value),
            "--limit" => limit = Some(value),
            "--cursor" => cursor = Some(value),
            "--file" => file = Some(value),
            "--directory" => directory = Some(value),
            "--deployment" => deployment = Some(value),
            "--secret" => secret = Some(value),
            _ => anyhow::bail!("unknown application option"),
        }
    }
    let api = url::Url::parse(&api.context("--api or CELLD_APPLICATION_API is required")?)?;
    let loopback = match api.host() {
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        Some(url::Host::Domain("localhost")) => true,
        _ => false,
    };
    ensure!(
        api.scheme() == "https" || (api.scheme() == "http" && loopback),
        "application API requires HTTPS except on loopback"
    );
    ensure!(
        api.username().is_empty()
            && api.password().is_none()
            && api.query().is_none()
            && api.fragment().is_none()
            && api.path() == "/",
        "application API URL must be an origin without credentials, query or fragment"
    );
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(60))
        .build()?;
    let request = |method, path: &str| -> anyhow::Result<RequestBuilder> {
        Ok(client.request(method, api.join(path)?).bearer_auth(token))
    };
    if command == "list" {
        ensure!(
            app.is_none()
                && file.is_none()
                && directory.is_none()
                && deployment.is_none()
                && secret.is_none(),
            "list takes only --api, --limit and --cursor"
        );
        let mut params = Vec::new();
        if let Some(limit) = limit {
            let count: usize = limit.parse().context("--limit requires an integer")?;
            ensure!((1..=256).contains(&count), "--limit must be between 1 and 256");
            params.push(("limit", count.to_string()));
        }
        if let Some(cursor) = cursor {
            ensure!(cursor.len() <= 16384, "--cursor exceeds limit");
            params.push(("cursor", cursor));
        }
        return response(request(Method::GET, "v1/applications")?.query(&params)).await;
    }
    ensure!(limit.is_none() && cursor.is_none(), "--limit and --cursor are only for list");
    let id = ApplicationId { app: app.context("--app is required")?.try_into()? };
    let path = format!("v1/applications/{}", id.app.as_str());
    let file_required = matches!(
        command.as_str(),
        "resource" | "stage" | "artifact" | "python" | "secret-put" | "limits"
    );
    ensure!(
        file.is_some() == file_required || command == "assets",
        "--file is required only for resource, stage, artifact, python, secret-put and limits"
    );
    ensure!(
        directory.is_some() == (command == "assets"),
        "--directory is required only for assets"
    );
    ensure!(
        deployment.is_some() == matches!(command.as_str(), "manifest" | "activate"),
        "--deployment is required only for manifest and activate"
    );
    ensure!(
        secret.is_some() == matches!(command.as_str(), "secret-put" | "secret-delete"),
        "--secret is required only for secret-put and secret-delete"
    );
    let secret: Option<Name> = secret.map(|value: String| value.try_into()).transpose()?;
    if command == "python" {
        // Built here, where upstream's `celld deploy` builds it: the node never
        // runs esbuild or fetches Pyodide, and the artifact is an ordinary
        // digest-addressed module bundle.
        let config = std::path::PathBuf::from(file.unwrap());
        let (bundle, worker_config) =
            tokio::task::spawn_blocking(move || python_artifact(config)).await??;
        let digest = crate::applications::content_digest(&bundle);
        let bytes = bundle.len();
        let outcome =
            response(request(Method::PUT, &format!("{path}/artifacts/{digest}"))?.body(bundle))
                .await?;
        return Ok(json!({
            "artifact_sha256": digest, "bytes": bytes, "entrypoint": "index.js",
            "worker_config": worker_config, "result": outcome,
        }));
    }
    let bytes = if let Some(file) = file {
        let limit = if command == "artifact" {
            crate::applications::MAX_ARTIFACT_BYTES
        } else if command == "secret-put" {
            crate::applications::MAX_SECRET_BYTES
        } else {
            1024 * 1024
        };
        ensure!(
            tokio::fs::metadata(&file).await?.len() <= limit as u64,
            "management input exceeds limit"
        );
        let bytes = tokio::fs::read(file).await?;
        ensure!(bytes.len() <= limit, "management input exceeds limit");
        Some(bytes)
    } else {
        None
    };
    match command.as_str() {
        "get" => response(request(Method::GET, &path)?).await,
        "secrets" => response(request(Method::GET, &format!("{path}/secrets"))?).await,
        "manifest" => {
            let version: Name = deployment.unwrap().try_into()?;
            response(request(
                Method::GET,
                &format!("{path}/deployments/{}", version.as_str()),
            )?)
            .await
        }
        "stage" => {
            let manifest: celld_applications::Deployment =
                serde_json::from_slice(bytes.as_ref().unwrap())?;
            manifest.validate()?;
            ensure!(
                manifest.application == id,
                "manifest does not match selected application"
            );
            response(
                request(
                    Method::PUT,
                    &format!("{path}/deployments/{}", manifest.id.as_str()),
                )?
                .json(&manifest),
            )
            .await
        }
        "assets" => {
            // The original deploy's asset build, uploaded as this application's
            // artifacts: every body first, then the index that names them.
            let project: Value = match &bytes {
                Some(bytes) => serde_json::from_slice(bytes)?,
                None => json!({}),
            };
            let project = project
                .as_object()
                .context("asset config must be an object")?;
            let directory = std::fs::canonicalize(directory.unwrap())?;
            let mut assets = project.get("assets").cloned().unwrap_or_else(|| json!({}));
            assets
                .as_object_mut()
                .context("config `assets` must be an object")?
                .insert(
                    "directory".into(),
                    directory
                        .file_name()
                        .and_then(|name| name.to_str())
                        .context("asset directory needs a UTF-8 name")?
                        .into(),
                );
            let root = directory.parent().context("asset directory has no parent")?;
            let built = crate::deploy::build_assets(&crate::deploy::read_asset_project(
                &assets, root, project,
            )?)?;
            let reference = crate::protocol::AssetManifestRef {
                index: "assets.json".into(),
                sha256: crate::applications::content_digest(&built.index),
                file_count: built.file_count,
                total_bytes: built.total_bytes,
            };
            let uploads = built
                .blobs
                .into_iter()
                .chain([(reference.sha256.clone(), built.index)])
                .map(|(digest, body)| {
                    let upload = request(Method::PUT, &format!("{path}/artifacts/{digest}"));
                    async move { response(upload?.body(body)).await }
                });
            futures_util::TryStreamExt::try_collect::<Vec<_>>(
                futures_util::StreamExt::buffer_unordered(futures_util::stream::iter(uploads), 8),
            )
            .await?;
            Ok(json!({"assets": reference}))
        }
        "artifact" => {
            let bytes = bytes.unwrap();
            let digest = crate::applications::content_digest(&bytes);
            let outcome =
                response(request(Method::PUT, &format!("{path}/artifacts/{digest}"))?.body(bytes))
                    .await?;
            Ok(json!({"artifact_sha256": digest, "result": outcome}))
        }
        "create" | "resource" | "activate" | "deactivate" | "secret-put" | "secret-delete"
        | "limits" | "limits-reset" => {
            let revision = if command == "create" {
                json!({"token": null})
            } else {
                response(request(Method::GET, &path)?).await?
                    .get("revision").context("API response lacks revision")?.clone()
            };
            let (method, target, body) = match command.as_str() {
                "create" => (Method::POST, path, json!({"revision": revision})),
                "activate" => {
                    let version: Name = deployment.unwrap().try_into()?;
                    (
                        Method::PUT,
                        format!("{path}/active"),
                        json!({"revision": revision, "deployment": version}),
                    )
                }
                "deactivate" => (
                    Method::DELETE,
                    format!("{path}/active"),
                    json!({"revision": revision}),
                ),
                "secret-put" => {
                    let value =
                        String::from_utf8(bytes.unwrap()).context("secret file must be UTF-8")?;
                    ensure!(!value.contains('\0'), "secret contains NUL");
                    (
                        Method::PUT,
                        format!("{path}/secrets/{}", secret.unwrap().as_str()),
                        json!({"revision": revision, "value": value}),
                    )
                }
                "secret-delete" => (
                    Method::DELETE,
                    format!("{path}/secrets/{}", secret.unwrap().as_str()),
                    json!({"revision": revision}),
                ),
                "limits" => {
                    let limits: crate::applications::ApplicationLimits =
                        serde_json::from_slice(bytes.as_ref().unwrap())?;
                    limits.validate()?;
                    (
                        Method::PUT,
                        format!("{path}/limits"),
                        json!({"revision": revision, "limits": limits}),
                    )
                }
                "limits-reset" => (
                    Method::DELETE,
                    format!("{path}/limits"),
                    json!({"revision": revision}),
                ),
                _ => {
                    let resource: crate::applications::Resource =
                        serde_json::from_slice(bytes.as_ref().unwrap())?;
                    (
                        Method::PUT,
                        format!("{path}/resources"),
                        json!({"revision": revision, "resource": resource}),
                    )
                }
            };
            response(request(method, &target)?.json(&body)).await
        }
        _ => unreachable!(),
    }
}

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    if arguments.is_empty()
        || arguments
            .iter()
            .any(|arg| matches!(arg.as_str(), "--help" | "-h"))
    {
        crate::note!("{HELP}");
        return Ok(());
    }
    let token =
        std::env::var("CELLD_APPLICATION_TOKEN").context("CELLD_APPLICATION_TOKEN is required")?;
    let result = execute(arguments, &token).await?;
    let mut output = std::io::stdout().lock();
    serde_json::to_writer_pretty(&mut output, &result)?;
    writeln!(output)?;
    Ok(())
}

#[cfg(test)]
#[path = "unit_tests/application_cli/tests.rs"]
mod tests;
