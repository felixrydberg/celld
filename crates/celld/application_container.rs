//! Application admission over the original native Container engine.
use anyhow::{Context, Result, ensure};
use celld_applications::{ApplicationId, BindingTarget, Handler, ResourceAddress, ResourceKind};
use std::sync::Arc;

/// The guest-visible declaration: which Durable Object class owns the
/// container bound as `binding`. Nothing privileged is declared here.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Declaration {
    pub binding: String,
    pub class_name: String,
}

/// The registered Container resource's configuration: the sole source of the
/// image and every other privileged setting.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    image: String,
    #[serde(default)]
    instance_type: Option<String>,
    #[serde(default)]
    max_instances: Option<u64>,
    #[serde(default)]
    runtime: Option<String>,
}

#[derive(Clone)]
pub(crate) struct Binding {
    pub address: ResourceAddress,
    pub class: String,
    pub objects: Vec<ResourceAddress>,
    pub spec: Arc<crate::container::ContainerSpec>,
}

pub(crate) fn declarations(handler: &Handler) -> Result<Vec<Declaration>> {
    let declarations: Vec<Declaration> = serde_json::from_value(
        handler
            .worker_config
            .get("containers")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])),
    )
    .context("invalid Container declarations")?;
    ensure!(declarations.len() <= 128, "too many Container declarations");
    let object_classes: Vec<String> = serde_json::from_value(
        handler
            .worker_config
            .get("do_classes")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])),
    )
    .context("invalid Durable Object classes")?;
    let mut classes = std::collections::HashSet::new();
    let mut aliases = std::collections::HashSet::new();
    for item in &declarations {
        ensure!(
            classes.insert(&item.class_name) && aliases.insert(&item.binding),
            "duplicate Container declaration"
        );
        ensure!(
            celld_logic::cell::valid_cell_scope(&item.class_name)
                && !item.class_name.contains(':')
                && !crate::deploy::is_reserved_class(&item.class_name),
            "invalid Container class"
        );
        ensure!(handler.bindings.iter().any(|(alias, target)| alias.as_str() == item.binding
            && matches!(target, BindingTarget::Resource { resource } if resource.kind == ResourceKind::Container)), "Container binding is not declared");
        ensure!(
            object_classes.contains(&item.class_name),
            "Container class is not a declared Durable Object class"
        );
    }
    for (alias, target) in &handler.bindings {
        if matches!(target, BindingTarget::Resource { resource } if resource.kind == ResourceKind::Container)
        {
            ensure!(
                declarations
                    .iter()
                    .any(|item| item.binding == alias.as_str()),
                "Container resource has no declaration"
            );
        }
    }
    Ok(declarations)
}

pub(crate) fn validate_image(image: &str) -> Result<()> {
    let key = image
        .strip_prefix("celld-image:")
        .context("Container image must be content addressed")?;
    ensure!(
        key.len() == 64
            && key
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "invalid Container image digest"
    );
    Ok(())
}

pub(crate) fn bindings(
    application: &ApplicationId,
    handler: &Handler,
    resources: &[crate::applications::Resource],
    namespaces: &[(ResourceAddress, String)],
) -> Result<Vec<Binding>> {
    declarations(handler)?
        .into_iter()
        .map(|declaration| {
            let (_, target) = handler
                .bindings
                .iter()
                .find(|(alias, _)| alias.as_str() == declaration.binding)
                .context("unbound Container")?;
            let BindingTarget::Resource { resource } = target else {
                anyhow::bail!("Container is not a resource")
            };
            let registered = resources
                .iter()
                .find(|item| item.resource == *resource)
                .context("Container resource is not registered")?;
            let configuration: Configuration =
                serde_json::from_value(registered.configuration.clone())
                    .context("invalid registered Container specification")?;
            validate_image(&configuration.image)?;
            ensure!(
                configuration
                    .instance_type
                    .as_deref()
                    .is_none_or(|kind| crate::container::instance_resources(kind).is_some()),
                "invalid Container instance type"
            );
            ensure!(
                configuration.max_instances.is_none_or(|limit| limit > 0),
                "invalid Container instance ceiling"
            );
            ensure!(
                configuration
                    .runtime
                    .as_ref()
                    .is_none_or(|runtime| !runtime.is_empty() && runtime.len() <= 128),
                "invalid Container runtime"
            );
            let objects: Vec<_> = namespaces
                .iter()
                .filter(|(address, class)| {
                    address.application == *application
                        && class == &declaration.class_name
                        && address.resource.kind == ResourceKind::DurableObject
                })
                .map(|(address, _)| address.clone())
                .collect();
            ensure!(
                !objects.is_empty(),
                "Container class requires an authenticated Durable Object namespace"
            );
            let address = ResourceAddress {
                application: application.clone(),
                resource: resource.clone(),
            };
            // Original per-class quota accounting uses the registered resource key.
            let spec = crate::container::ContainerSpec {
                class_name: address.storage_root(),
                image: configuration.image,
                instance_type: configuration.instance_type,
                max_instances: configuration.max_instances,
                runtime: configuration.runtime,
            };
            Ok(Binding {
                address,
                class: declaration.class_name,
                objects,
                spec: Arc::new(spec),
            })
        })
        .collect()
}

/// Native container mutations (start, destroy, signal, exec) owned by one guest
/// event. Cancelling the caller never cancels a begun effect, and settlement of
/// the event's storage joins every one, so none can finish after its owner is
/// released and leave a container or process behind. Joining closes the set.
#[derive(Clone)]
pub(crate) struct Effects(Arc<std::sync::Mutex<Option<Vec<tokio::sync::oneshot::Receiver<()>>>>>);

impl Default for Effects {
    fn default() -> Self {
        Self(Arc::new(std::sync::Mutex::new(Some(Vec::new()))))
    }
}

impl Effects {
    // Container effects run the V8 arm's engine calls (`js/container.rs`), which
    // stays outside the Actor execution domain.
    #[allow(clippy::disallowed_methods)]
    pub(crate) fn spawn<T: Send + 'static>(
        &self,
        work: impl std::future::Future<Output = T> + Send + 'static,
    ) -> Result<tokio::task::JoinHandle<T>> {
        let mut open = self.0.lock().expect("container effects poisoned");
        let joined = open
            .as_mut()
            .context("guest event is settling; container effects are closed")?;
        let (done, receive) = tokio::sync::oneshot::channel();
        joined.push(receive);
        Ok(tokio::spawn(async move {
            let output = work.await;
            let _ = done.send(());
            output
        }))
    }

    pub(crate) async fn join(&self) {
        let pending = self
            .0
            .lock()
            .expect("container effects poisoned")
            .take()
            .unwrap_or_default();
        for done in pending {
            // A panicked effect drops its sender; there is nothing left to wait for.
            let _ = done.await;
        }
    }
}

/// Sockets a guest event opened to its container. They belong to that event:
/// every id is checked against it, and settlement (or the last drop) frees
/// them all, as the original request context frees its sockets.
#[derive(Clone)]
pub(crate) struct Sockets(Arc<SocketIds>);

struct SocketIds(std::sync::Mutex<Option<Vec<u64>>>);

impl Default for Sockets {
    fn default() -> Self {
        Self(Arc::new(SocketIds(std::sync::Mutex::new(Some(Vec::new())))))
    }
}

impl SocketIds {
    fn free(&self) {
        let ids = self.0.lock().expect("container sockets poisoned").take();
        for id in ids.unwrap_or_default() {
            crate::js::tcp::free(id);
        }
    }
}

impl Drop for SocketIds {
    fn drop(&mut self) {
        self.free();
    }
}

impl Sockets {
    pub(crate) fn register(&self, id: u64) -> Result<()> {
        let mut open = self.0 .0.lock().expect("container sockets poisoned");
        open.as_mut()
            .context("guest event is settling; container sockets are closed")?
            .push(id);
        Ok(())
    }

    pub(crate) fn owns(&self, id: u64) -> Result<()> {
        let open = self.0 .0.lock().expect("container sockets poisoned");
        ensure!(
            open.as_ref().is_some_and(|ids| ids.contains(&id)),
            "the socket is closed"
        );
        Ok(())
    }

    pub(crate) fn forget(&self, id: u64) {
        if let Some(ids) = self.0 .0.lock().expect("container sockets poisoned").as_mut() {
            ids.retain(|owned| *owned != id);
        }
    }

    /// Free every socket and refuse new ones.
    pub(crate) fn close(&self) {
        self.0.free();
    }
}

impl Binding {
    pub(crate) fn authorizes(
        &self,
        application: &ApplicationId,
        object: &celld_applications::invocation_wire::ObjectTarget,
    ) -> bool {
        &self.address.application == application
            && self.class == object.class
            && self
                .objects
                .iter()
                .any(|resource| resource.application == *application && resource.owns(object))
    }
}

// Tests use real Tokio time and tasks on their own runtime.
#[allow(clippy::disallowed_methods)]
#[cfg(test)]
#[path = "unit_tests/application_container/tests.rs"]
mod tests;

/// Real Docker: the host broker against a native daemon, with tiny local
/// images. As root: `DOCKER_HOST=unix:///run/celld-docker.sock cargo test -p
/// celld --lib application_container::docker -- --ignored`. Images are built
/// from inline Dockerfiles and tagged `celld-image:<hex>`, which the engine
/// finds in the daemon without a bucket.
// Tests drive a real Docker daemon with real Tokio time and tasks.
#[allow(clippy::disallowed_methods)]
#[cfg(test)]
#[path = "unit_tests/application_container/docker.rs"]
pub(crate) mod docker;
