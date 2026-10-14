// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

static ENABLED: AtomicBool = AtomicBool::new(false);
static ACTIVE: AtomicUsize = AtomicUsize::new(0);

/// Enable before loading the root worker, including its module initialization.
/// Loaded descendants may not carry the root's guest storage marker.
pub(crate) fn enable() {
    ENABLED.store(true, Ordering::Release);
}

pub(crate) fn enabled() -> bool {
    ENABLED.load(Ordering::Acquire)
}

pub(crate) fn idle() -> bool {
    enabled() && ACTIVE.load(Ordering::Acquire) == 0
}

pub(crate) struct Hold(Option<&'static AtomicUsize>);

impl Hold {
    fn counted(counter: &'static AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self(Some(counter))
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        if let Some(counter) = self.0 {
            assert!(counter.fetch_sub(1, Ordering::AcqRel) > 0);
        }
    }
}

pub(crate) fn hold() -> Hold {
    if ENABLED.load(Ordering::Acquire) {
        Hold::counted(&ACTIVE)
    } else {
        Hold(None)
    }
}

/// Acquire before queueing; aborting a waiter cannot release a blocking job.
pub(crate) fn blocking<T>(job: impl FnOnce() -> T) -> impl FnOnce() -> T {
    let activity = hold();
    move || {
        let _activity = activity;
        job()
    }
}

pub(crate) fn track<F: Future>(future: F) -> impl Future<Output = F::Output> {
    let activity = hold();
    async move {
        let _activity = activity;
        future.await
    }
}

#[cfg(test)]
#[path = "../unit_tests/js/guest_activity/tests.rs"]
mod tests;
