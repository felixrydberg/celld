use super::*;
use anyhow::Context;
use futures_util::future::join_all;
use futures_util::FutureExt;
use std::sync::atomic::{AtomicUsize, Ordering};

const CALLERS: usize = 8;

#[tokio::test]
async fn concurrent_callers_share_one_fetch() {
    let flight = SingleFlight::<u8, usize>::default();
    let fetches = AtomicUsize::new(0);
    let fetch = || async {
        // Pending once, so every caller has joined before it finishes.
        tokio::task::yield_now().await;
        Ok(fetches.fetch_add(1, Ordering::SeqCst) + 100)
    };
    let values = join_all((0..CALLERS).map(|_| flight.run(1, fetch))).await;
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
    assert!(values.into_iter().all(|value| value.unwrap() == 100));
    assert!(
        flight.flights.lock().unwrap().is_empty(),
        "only what is in flight is held"
    );

    // Done: the next caller fetches again.
    assert_eq!(flight.run(1, fetch).await.unwrap(), 101);
    assert_eq!(fetches.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_failure_reaches_every_caller_from_one_fetch() {
    let flight = SingleFlight::<u8, ()>::default();
    let fetches = AtomicUsize::new(0);
    let fetch = || async {
        tokio::task::yield_now().await;
        fetches.fetch_add(1, Ordering::SeqCst);
        Err(anyhow!("upstream down")).context("read key")
    };
    let results = join_all((0..CALLERS).map(|_| flight.run(1, fetch))).await;
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
    assert_eq!(results.len(), CALLERS);
    for result in results {
        assert_eq!(
            format!("{:#}", result.unwrap_err()),
            "read key: upstream down"
        );
    }
    // A failure is not remembered either.
    assert!(flight.run(1, || async { Ok(()) }).await.is_ok());
}

#[tokio::test]
async fn cancelling_the_first_caller_does_not_strand_the_others() {
    let flight = SingleFlight::<u8, u8>::default();
    let mut first = Box::pin(flight.run(1, std::future::pending));
    assert!(
        first.as_mut().now_or_never().is_none(),
        "the first fetch never ends"
    );
    let mut second = Box::pin(flight.run(1, || async { Ok(7) }));
    assert!(
        second.as_mut().now_or_never().is_none(),
        "the second waits on the first"
    );
    drop(first);
    assert_eq!(second.await.unwrap(), 7);
}
