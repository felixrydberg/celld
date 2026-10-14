// The node builds this drive on the host filesystem and boots a VM of its own
// to do it, outside the Actor execution domain.
#![allow(clippy::disallowed_methods)]

//! The shared Pyodide runtime drive (T28). A Python handler's image holds only
//! its code and the Workers SDK. The interpreter (Wasm, glue, standard library)
//! and a base memory snapshot of it, booted before any handler import, live on
//! one read-only drive per Pyodide release that every Python VM shares:
//! hard-linked into each jail, read through the host page cache, and never
//! counted as handler data. The drive is tenant-agnostic by construction: it is
//! built on the node from the pinned, hash-verified Pyodide files and a builder
//! the node supplies, and keyed by the Pyodide release, runtime image and
//! snapshot memory-device profile used by the builder.
//!
//! Every approved runtime's drive is built before node readiness. Python
//! activation then reads this cache without building on the request path.

use crate::application_image;
use crate::application_vm::{LaunchOptions, Vm};
use crate::deploy::{runtime_files, sentinel_bytes, PYODIDE_VERSION};
use anyhow::{bail, ensure, Context, Result};
use celld_applications::Handler;
use futures_util::StreamExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The snapshot's name on the drive, as `python/worker.js` imports it.
const SNAPSHOT: &str = "pyodide-snapshot.bin";
/// The drive may hold the whole interpreter and its snapshot.
const DRIVE_BYTES: u64 = 256 * 1024 * 1024;
/// The snapshot is about 20 MiB; the builder's answer is bounded.
const SNAPSHOT_BYTES: usize = 128 * 1024 * 1024;

const BUILDER: &str = include_str!("python/snapshot.mjs");
const SHIMS: &str = include_str!("python/shims.js");

/// The drive files a Python handler's bundle imports as modules, with their
/// kind. `pyodide.asm.js` imports `shims.js` and, through it, the rest.
const HANDLER_MODULES: [(&str, &str); 7] = [
    ("pyodide.asm.js", "es_module"),
    ("shims.js", "es_module"),
    ("config.js", "es_module"),
    ("pyodide.asm.wasm", "wasm"),
    ("sentinel.wasm", "wasm"),
    ("python_stdlib.zip", "data"),
    (SNAPSHOT, "data"),
];

/// What the snapshot builder imports from the bare drive: the loader itself,
/// and no snapshot.
const BUILDER_MODULES: [(&str, &str); 7] = [
    ("pyodide.asm.js", "es_module"),
    ("pyodide.mjs", "es_module"),
    ("shims.js", "es_module"),
    ("config.js", "es_module"),
    ("pyodide.asm.wasm", "wasm"),
    ("sentinel.wasm", "wasm"),
    ("python_stdlib.zip", "data"),
];

fn shared(modules: &[(&str, &str)]) -> Vec<serde_json::Value> {
    modules
        .iter()
        .map(|(name, kind)| serde_json::json!({"name": name, "path": name, "kind": kind, "shared": true}))
        .collect()
}

/// The worker configuration a Python handler is staged with: the Pyodide
/// release it was built for, and the modules it loads from the shared drive.
pub(crate) fn handler_config() -> serde_json::Value {
    serde_json::json!({"pyodide": PYODIDE_VERSION, "modules": shared(&HANDLER_MODULES)})
}

/// A pinned Pyodide script, evaluated with the names that esbuild defines
/// (`process`, `location`) or injects (`python/shims.js`) for a copy bundled
/// into a Python Worker (`deploy/python.rs`). Kept as separate modules on the
/// shared drive, such a copy would otherwise see the isolate's globals.
fn in_bundle_scope(script: &[u8]) -> Result<Vec<u8>> {
    const HEADER: &str = concat!(
        "import { RUNTIME_URL, WebAssembly, fetch, self, importScripts, WorkerGlobalScope }",
        " from \"./shims.js\";
",
        "const process = undefined;
",
        "const location = RUNTIME_URL;
",
    );
    Ok([HEADER.as_bytes(), std::str::from_utf8(script)?.as_bytes()].concat())
}

/// Whether this handler needs the shared drive: it names the Pyodide release
/// its bundle was built for, which must be the one this node serves.
pub(crate) fn needs_runtime(handler: &Handler) -> Result<bool> {
    match handler.worker_config.get("pyodide") {
        None => Ok(false),
        Some(release) if release == PYODIDE_VERSION => Ok(true),
        Some(release) => {
            bail!("handler needs Pyodide {release}; this node serves {PYODIDE_VERSION}")
        }
    }
}

/// Builds the drive at `target`: the interpreter's files first, then a VM that
/// boots the interpreter from them and answers with its snapshot, then the
/// files and the snapshot together. `scratch` holds the intermediate files.
pub(crate) async fn build(
    options: &LaunchOptions,
    app: &crate::actor::AppHandle,
    scratch: &Path,
    target: &Path,
) -> Result<()> {
    let runtime = tokio::task::spawn_blocking(runtime_files).await??;
    let sentinel = sentinel_bytes(&runtime["pyodide.mjs"])?;
    let lock: serde_json::Value = serde_json::from_slice(&runtime["pyodide-lock.json"])?;
    // What `deploy/python.rs` writes into a bundle's `config.js`, less the
    // handler's own entries.
    let config = format!(
        "export const SENTINEL_BYTES = new Uint8Array({});
export const LOCK = {};
",
        serde_json::to_string(&sentinel)?,
        serde_json::json!({"info": lock["info"], "packages": {}}),
    );
    let mut files: Vec<(String, Vec<u8>)> = vec![
        ("pyodide.asm.js".into(), in_bundle_scope(&runtime["pyodide.asm.js"])?),
        ("pyodide.mjs".into(), in_bundle_scope(&runtime["pyodide.mjs"])?),
        ("shims.js".into(), SHIMS.as_bytes().to_vec()),
        ("config.js".into(), config.into_bytes()),
        ("pyodide.asm.wasm".into(), runtime["pyodide.asm.wasm"].clone()),
        ("sentinel.wasm".into(), sentinel),
        ("python_stdlib.zip".into(), runtime["python_stdlib.zip"].clone()),
    ];
    // The builder boots from the drive as it is before the snapshot exists.
    let bare = scratch.join("bare.img");
    let (staged, path) = (files.clone(), bare.clone());
    let image = move || application_image::build_file(&staged, &path, DRIVE_BYTES);
    tokio::task::spawn_blocking(image).await??;
    let builder = [("main.mjs".to_owned(), BUILDER.as_bytes().to_vec())];
    let builder = tokio::task::spawn_blocking(move || application_image::build(&builder)).await??;
    files.push((SNAPSHOT.into(), snapshot(options, app, scratch, bare.clone(), builder).await?));
    let (path, drive) = (target.to_owned(), files);
    tokio::task::spawn_blocking(move || application_image::build_file(&drive, &path, DRIVE_BYTES)).await??;
    let _ = std::fs::remove_file(bare);
    Ok(())
}

/// Boots the interpreter in a VM that has only the bare drive and the builder,
/// and returns the memory snapshot it serves.
async fn snapshot(
    options: &LaunchOptions,
    app: &crate::actor::AppHandle,
    scratch: &Path,
    bare: PathBuf,
    builder: Vec<u8>,
) -> Result<Vec<u8>> {
    use celld_applications::invocation_wire::HttpRequest;
    let image = scratch.join("builder.img");
    tokio::fs::write(&image, builder).await?;
    let modules = shared(&BUILDER_MODULES);
    let handler: Handler = serde_json::from_value(serde_json::json!({
        "id": "snapshot",
        "language": "javascript",
        "entrypoint": "main.mjs",
        "artifact_sha256": "0".repeat(64),
        "runtime_image_sha256": options.runtime.sha256,
        "worker_config": {"compatibility_date": "2026-03-01", "pyodide": PYODIDE_VERSION, "modules": modules},
    }))?;
    let mut options = options.clone();
    options.shared = Some(bare);
    let vm = Vm::launch_builtin(options, handler, &image).await?;
    let answer = tokio::time::timeout(Duration::from_secs(180), async {
        let response = vm
            .client()
            .invoke_stateless(
                app,
                Duration::from_secs(120),
                HttpRequest {
                    method: "GET".into(),
                    url: "http://snapshot.celld.internal/".into(),
                    headers: vec![],
                    body: vec![],
                },
                None,
            )
            .await?;
        let mut body = response.body;
        if let Some(mut stream) = response.stream {
            while let Some(chunk) = stream.next().await {
                body.extend(chunk.map_err(anyhow::Error::msg)?);
                ensure!(body.len() <= SNAPSHOT_BYTES, "the Pyodide snapshot is larger than expected");
            }
        }
        ensure!(response.status == 200 && !body.is_empty(), "the snapshot builder answered {}", response.status);
        Ok(body)
    })
    .await
    .map_err(|_| anyhow::anyhow!("Pyodide snapshot response timed out"))
    .and_then(|answer| answer);
    let exit = vm.shutdown().await;
    match answer {
        Ok(body) => {
            exit.context(crate::application_vm::ReapUnproved)?;
            Ok(body)
        }
        Err(error) => {
            let exit = match exit.context(crate::application_vm::ReapUnproved) {
                Ok(exit) => exit,
                Err(reap_error) => return Err(reap_error.context(error)),
            };
            Err(error.context(format!(
                "Pyodide snapshot builder failed: {}",
                exit.console
            )))
        }
    }
}

#[cfg(test)]
#[path = "unit_tests/application_pyodide/tests.rs"]
mod tests;
