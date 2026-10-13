use super::*;

#[test]
fn queued_and_independent_blocking_owners_must_all_drop() {
    static COUNT: AtomicUsize = AtomicUsize::new(0);
    let queued = Hold::counted(&COUNT);
    let blocking = Hold::counted(&COUNT);
    drop(queued);
    assert_eq!(COUNT.load(Ordering::Acquire), 1);
    drop(blocking);
    assert_eq!(COUNT.load(Ordering::Acquire), 0);
    let queued = Hold::counted(&COUNT);
    let cancelled_before_poll = async move {
        let _queued = queued;
    };
    assert_eq!(COUNT.load(Ordering::Acquire), 1);
    drop(cancelled_before_poll);
    assert_eq!(COUNT.load(Ordering::Acquire), 0);
}
