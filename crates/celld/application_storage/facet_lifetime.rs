use std::collections::HashMap;
use std::sync::{atomic::AtomicBool, Arc, Mutex, MutexGuard, OnceLock};

type Key = (usize, String);

#[derive(Default)]
struct Registry {
    next_id: u64,
    leases: HashMap<Key, (u64, usize)>,
}

pub(super) struct Lease {
    root: usize,
    id: u64,
    pub(super) scope: String,
}

static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();

fn registry() -> MutexGuard<'static, Registry> {
    REGISTRY
        .get_or_init(|| Mutex::new(Registry::default()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn root_id(root: &Arc<AtomicBool>) -> usize {
    Arc::as_ptr(root) as usize
}

/// Keep a facet connection open while any event for this root holds it.
pub(super) fn retain(root: &Arc<AtomicBool>, scope: &str) -> Lease {
    let root = root_id(root);
    let mut registry = registry();
    let key = (root, scope.to_owned());
    let id = if let Some((id, count)) = registry.leases.get_mut(&key) {
        *count += 1;
        *id
    } else {
        registry.next_id = registry.next_id.wrapping_add(1).max(1);
        let id = registry.next_id;
        registry.leases.insert(key, (id, 1));
        id
    };
    Lease {
        root,
        id,
        scope: scope.to_owned(),
    }
}

/// Release a session's lease; return true when it was the last live user.
pub(super) fn release(lease: &Lease) -> bool {
    let mut registry = registry();
    let key = (lease.root, lease.scope.clone());
    let last = {
        let Some((id, count)) = registry.leases.get_mut(&key) else {
            return false;
        };
        if *id != lease.id || *count == 0 {
            return false;
        }
        *count -= 1;
        *count == 0
    };
    if last {
        registry.leases.remove(&key);
    }
    last
}

/// Explicit deletion or abort invalidates leases held by every event.
pub(super) fn invalidate(root: &Arc<AtomicBool>, scope: &str) {
    registry().leases.remove(&(root_id(root), scope.to_owned()));
}
