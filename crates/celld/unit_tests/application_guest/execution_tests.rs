use super::*;

#[test]
fn original_deadline_is_bounded_and_cleanup_releases_the_shared_slot() {
    assert_eq!(deadline_after(100, Duration::from_millis(20)).unwrap(), 120);
    assert!(deadline_after(100, Duration::ZERO).is_err());
    assert!(deadline_after(u64::MAX, Duration::from_millis(1)).is_err());
    let finished = tokio_util::sync::CancellationToken::new();
    let slot = Arc::new(std::sync::Mutex::new(None));
    let escaped = slot.clone();
    drop(Finished {
        finished: finished.clone(),
        execution: slot,
    });
    assert!(finished.is_cancelled());
    assert!(escaped.lock().unwrap().is_none());
}
