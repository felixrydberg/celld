//! Host-owned binding lifetimes. Revocation and admission use one lock so a
//! cancelled guest cannot start new work while accepted effects escape drain.

use crate::{ApplicationId, BindingTarget, Deployment, Name, ResourceAddress, ResourceKind};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub name: Name,
    pub handle: String,
    pub adapter: BindingAdapter,
}

/// Public type information lets each language build the right binding API.
/// It carries no address or authority; only the host resolves the handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum BindingAdapter {
    Resource { kind: ResourceKind },
    Handler {},
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Target {
    Resource(ResourceAddress),
    Handler {
        application: ApplicationId,
        deployment: Name,
        handler: Name,
    },
}

struct State {
    revoked: bool,
    active: usize,
    targets: HashMap<String, Target>,
}

struct Inner {
    state: Mutex<State>,
    drained: Notify,
}

/// Construct a fresh session per event. Never carry handles from one guest,
/// generation or event into another. An event's session is host context, not
/// chosen by application/deployment fields in a guest request.
pub struct Session {
    grants: Vec<Grant>,
    inner: Arc<Inner>,
}

impl Session {
    pub fn new(deployment: &Deployment, handler: &Name) -> Result<Self> {
        deployment.validate()?;
        let handler = deployment
            .handlers
            .iter()
            .find(|item| &item.id == handler)
            .context("unknown handler")?;
        let mut grants = Vec::new();
        let mut targets = HashMap::new();
        for (name, binding) in &handler.bindings {
            let mut bytes = [0u8; 32];
            getrandom::fill(&mut bytes)
                .map_err(|error| anyhow::anyhow!("binding entropy failed: {error}"))?;
            let handle = hex::encode(bytes);
            let target = match binding {
                BindingTarget::Resource { resource } => Target::Resource(ResourceAddress {
                    application: deployment.application.clone(),
                    resource: resource.clone(),
                }),
                BindingTarget::Handler { handler } => Target::Handler {
                    application: deployment.application.clone(),
                    deployment: deployment.id.clone(),
                    handler: handler.clone(),
                },
            };
            ensure!(
                targets.insert(handle.clone(), target).is_none(),
                "binding handle collision"
            );
            grants.push(Grant {
                name: name.clone(),
                handle,
                adapter: match binding {
                    BindingTarget::Resource { resource } => BindingAdapter::Resource {
                        kind: resource.kind,
                    },
                    BindingTarget::Handler { .. } => BindingAdapter::Handler {},
                },
            });
        }
        Ok(Self {
            grants,
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    revoked: false,
                    active: 0,
                    targets,
                }),
                drained: Notify::new(),
            }),
        })
    }

    /// This list is the only binding metadata sent to a guest. Resource roots
    /// and other applications' identities stay in host context.
    pub fn grants(&self) -> &[Grant] {
        &self.grants
    }

    pub fn admit(&self, handle: &str) -> Result<AcceptedCall> {
        let mut state = self.inner.state.lock().unwrap();
        ensure!(!state.revoked, "binding session was revoked");
        let target = state
            .targets
            .get(handle)
            .context("unknown binding handle")?
            .clone();
        state.active = state
            .active
            .checked_add(1)
            .context("binding call count exhausted")?;
        Ok(AcceptedCall {
            target,
            inner: self.inner.clone(),
        })
    }

    pub fn revoke(&self) {
        let mut state = self.inner.state.lock().unwrap();
        state.revoked = true;
        state.targets.clear();
        if state.active == 0 {
            self.inner.drained.notify_waiters();
        }
    }

    /// Revokes before waiting, so completing the last accepted call cannot
    /// race a new admission. The call guard belongs to the host effect.
    pub async fn revoke_and_drain(&self) {
        self.revoke();
        loop {
            let drained = self.inner.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self.inner.state.lock().unwrap().active == 0 {
                return;
            }
            drained.await;
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.revoke();
    }
}

pub struct AcceptedCall {
    target: Target,
    inner: Arc<Inner>,
}

impl AcceptedCall {
    pub fn target(&self) -> &Target {
        &self.target
    }
}

impl Drop for AcceptedCall {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        state.active -= 1;
        if state.active == 0 {
            self.inner.drained.notify_waiters();
        }
    }
}
