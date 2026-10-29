//! Concurrent callers of one key share one fetch's result. Sharing the result,
//! not a lock around the fetch, is what keeps a failing source cheap under a
//! stampede: the failure reaches every waiter at once and each moves on (to the
//! next source), instead of retrying in turn behind a lock and each waiting out
//! its own timeout.

use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

type Flight<V> = Arc<OnceCell<Result<V, Arc<anyhow::Error>>>>;

pub(crate) struct SingleFlight<K, V> {
    flights: Mutex<HashMap<K, Flight<V>>>,
}

impl<K, V> Default for SingleFlight<K, V> {
    fn default() -> Self {
        Self {
            flights: Default::default(),
        }
    }
}

impl<K: Eq + Hash + Clone, V: Clone> SingleFlight<K, V> {
    /// The result of one `fetch` for `key`: the first caller's runs, and the
    /// callers that arrive meanwhile get its value or its error. A caller
    /// arriving after it finished starts a new one.
    // ponytail: the shared error is rebuilt from its text, so downcasts are lost;
    // a flight every caller dropped stays until its key runs again.
    pub(crate) async fn run<F, Fut>(&self, key: K, fetch: F) -> Result<V>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V>>,
    {
        let flight = self
            .flights
            .lock()
            .expect("single flight poisoned")
            .entry(key.clone())
            .or_default()
            .clone();
        // If the caller running `fetch` is dropped, the cell hands the
        // initializer to another waiter, so a cancelled caller strands nobody.
        let shared = flight
            .get_or_init(|| async move { fetch().await.map_err(Arc::new) })
            .await
            .clone();
        let mut flights = self.flights.lock().expect("single flight poisoned");
        // The map holds only what is in flight: a newer flight is not ours to remove.
        if flights
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, &flight))
        {
            flights.remove(&key);
        }
        shared.map_err(|error| anyhow!("{error:#}"))
    }
}

#[cfg(test)]
#[path = "unit_tests/single_flight/tests.rs"]
mod tests;
