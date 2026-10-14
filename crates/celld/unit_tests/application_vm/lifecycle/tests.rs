use super::*;
use std::sync::atomic::AtomicU64;

fn new_gate() -> (Arc<Gate>, Arc<AtomicU64>) {
    let clock = Arc::new(AtomicU64::new(100));
    let mut gate = Gate::at(30, 100);
    Arc::get_mut(&mut gate).unwrap().clock = Some(clock.clone());
    (gate, clock)
}

fn admitted(gate: &Arc<Gate>) -> EventHold {
    let hold = gate
        .try_enter(1000, &CancellationToken::new())
        .unwrap()
        .unwrap();
    hold.mark_admitted();
    hold
}

#[test]
fn clones_count_once_and_final_drop_records_exact_drain() {
    let (gate, clock) = new_gate();
    let first = admitted(&gate);
    let retained = first.clone();
    assert_eq!(gate.status().active_events, 1);
    clock.store(110, Ordering::Release);
    drop(first);
    assert_eq!(gate.status().active_events, 1);
    assert_eq!(gate.status().last_drain_ms, 100);
    assert!(gate.begin_pause().is_none());
    clock.store(120, Ordering::Release);
    drop(retained);
    assert_eq!(gate.status().active_events, 0);
    assert_eq!(gate.status().last_drain_ms, 120);
    assert_eq!(gate.status().idle_deadline_ms, 150);
}

#[test]
fn queries_and_unadmitted_preparation_never_refresh_idle() {
    let (gate, clock) = new_gate();
    let event = admitted(&gate);
    let preparation = gate
        .try_enter(1000, &CancellationToken::new())
        .unwrap()
        .unwrap();
    clock.store(110, Ordering::Release);
    drop(event);
    clock.store(120, Ordering::Release);
    drop(preparation);
    assert_eq!(gate.status().last_drain_ms, 110);
    assert_eq!(gate.status().idle_deadline_ms, 140);
    let before = gate.status();
    clock.store(125, Ordering::Release);
    assert_eq!(gate.status(), before);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(gate.try_enter(1000, &cancelled).is_err());
    assert!(gate.try_enter(125, &CancellationToken::new()).is_err());
    assert_eq!(gate.status(), before);
    let preparation = gate
        .try_enter(1000, &CancellationToken::new())
        .unwrap()
        .unwrap();
    clock.store(126, Ordering::Release);
    drop(preparation);
    assert_eq!(gate.status(), before);
}

#[test]
fn concurrent_wakes_share_resume_and_waiter_cancellation_does_not_own_it() {
    let (gate, clock) = new_gate();
    let _waiter = AdmissionWaiter::new(&gate).unwrap();
    let mut pause = gate.begin_pause().unwrap();
    assert!(gate.begin_pause().is_none());
    assert!(gate
        .try_enter(1000, &CancellationToken::new())
        .unwrap()
        .is_none());
    assert!(gate.status().resume_requested);
    pause.confirm_idle().unwrap();
    gate.confirm_pause(pause).unwrap();
    let _waiter = AdmissionWaiter::new(&gate).unwrap();
    assert!(gate
        .try_enter(1000, &CancellationToken::new())
        .unwrap()
        .is_none());
    let resume = gate.begin_resume().unwrap();
    assert!(gate.begin_resume().is_none());
    assert!(gate
        .try_enter(1000, &CancellationToken::new())
        .unwrap()
        .is_none());
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(gate.try_enter(1000, &cancelled).is_err());
    assert_eq!(gate.status().phase, Phase::Resuming);
    resume.check().unwrap();
    gate.confirm_resume(resume).unwrap();
    assert_eq!(gate.status().idle_deadline_ms, 130);
    let first = admitted(&gate);
    let second = admitted(&gate);
    assert_eq!(gate.status().active_events, 2);
    clock.store(140, Ordering::Release);
    drop(first);
    assert_eq!(gate.status().last_drain_ms, 100);
    clock.store(150, Ordering::Release);
    drop(second);
    assert_eq!(gate.status().last_drain_ms, 150);
}

#[test]
fn cancelled_and_expired_waiters_withdraw_only_their_own_demand() {
    let (gate, clock) = new_gate();
    let mut pause = gate.begin_pause().unwrap();
    pause.confirm_idle().unwrap();
    gate.confirm_pause(pause).unwrap();
    let original = gate.status();
    let first = AdmissionWaiter::new(&gate).unwrap();
    let second = AdmissionWaiter::new(&gate).unwrap();
    let cancel = CancellationToken::new();
    assert!(gate.try_enter(120, &cancel).unwrap().is_none());
    cancel.cancel();
    assert!(gate.try_enter(120, &cancel).is_err());
    drop(first);
    assert!(gate.status().resume_requested);
    clock.store(120, Ordering::Release);
    assert!(gate.try_enter(120, &CancellationToken::new()).is_err());
    drop(second);
    assert!(!gate.status().resume_requested);
    assert_eq!(gate.status().phase, Phase::Cached);
    assert_eq!(gate.status().last_drain_ms, original.last_drain_ms);
    assert_eq!(gate.status().idle_deadline_ms, original.idle_deadline_ms);
    assert!(gate.begin_resume().is_none());
}

#[test]
fn last_waiter_drop_does_not_cancel_an_owned_resume() {
    let (gate, _) = new_gate();
    let mut pause = gate.begin_pause().unwrap();
    pause.confirm_idle().unwrap();
    gate.confirm_pause(pause).unwrap();
    let waiter = AdmissionWaiter::new(&gate).unwrap();
    assert!(gate
        .try_enter(1000, &CancellationToken::new())
        .unwrap()
        .is_none());
    let resume = gate.begin_resume().unwrap();
    drop(waiter);
    assert_eq!(gate.status().phase, Phase::Resuming);
    resume.check().unwrap();
    gate.confirm_resume(resume).unwrap();
    assert_eq!(gate.status().phase, Phase::Running);
    assert_eq!(gate.status().idle_deadline_ms, 130);
}
#[test]
fn expiry_cannot_revive_cached_or_transitioning_guests() {
    let (gate, clock) = new_gate();
    let mut pause = gate.begin_pause().unwrap();
    pause.confirm_idle().unwrap();
    gate.confirm_pause(pause).unwrap();
    clock.store(130, Ordering::Release);
    assert!(gate.begin_resume().is_none());
    assert_eq!(gate.status().phase, Phase::Retiring);
    assert!(gate.try_enter(1000, &CancellationToken::new()).is_err());
    assert!(gate.begin_pause().is_none());
    let (gate, clock) = new_gate();
    let mut pause = gate.begin_pause().unwrap();
    pause.confirm_idle().unwrap();
    gate.confirm_pause(pause).unwrap();
    let _waiter = AdmissionWaiter::new(&gate).unwrap();
    assert!(gate
        .try_enter(1000, &CancellationToken::new())
        .unwrap()
        .is_none());
    let resume = gate.begin_resume().unwrap();
    clock.store(130, Ordering::Release);
    assert!(resume.check().is_err());
    assert!(gate.confirm_resume(resume).is_err());
    assert_eq!(gate.status().phase, Phase::Retiring);
}

#[test]
fn lost_acknowledgements_and_explicit_retirement_keep_transitions_closed() {
    let (gate, _) = new_gate();
    drop(gate.begin_pause().unwrap());
    assert_eq!(gate.status().phase, Phase::Retiring);
    let (gate, _) = new_gate();
    let mut pause = gate.begin_pause().unwrap();
    pause.confirm_idle().unwrap();
    gate.confirm_pause(pause).unwrap();
    let _waiter = AdmissionWaiter::new(&gate).unwrap();
    assert!(gate
        .try_enter(1000, &CancellationToken::new())
        .unwrap()
        .is_none());
    drop(gate.begin_resume().unwrap());
    assert_eq!(gate.status().phase, Phase::Retiring);
    let (gate, _) = new_gate();
    let pause = gate.begin_pause().unwrap();
    gate.retire();
    assert!(gate.confirm_pause(pause).is_err());
    assert_eq!(gate.status().phase, Phase::Retiring);
}
#[test]
fn expired_acknowledgements_and_initialized_idle_vms_are_ineligible() {
    let (gate, clock) = new_gate();
    let mut pause = gate.begin_pause().unwrap();
    clock.store(130, Ordering::Release);
    assert!(pause.confirm_idle().is_err());
    assert_eq!(gate.status().phase, Phase::Retiring);
    let (gate, clock) = new_gate();
    clock.store(130, Ordering::Release);
    assert!(gate
        .try_enter(1000, &CancellationToken::new())
        .unwrap()
        .is_none());
    let mut pause = gate.begin_pause().unwrap();
    assert!(pause.confirm_idle().is_err());
    assert_eq!(gate.status().phase, Phase::Retiring);
}

#[test]
fn guest_busy_does_not_pause_retire_or_refresh_idle_deadline() {
    let (gate, clock) = new_gate();
    let before = gate.status();
    clock.store(140, Ordering::Release);
    gate.begin_pause().unwrap().cancel_pause().unwrap();
    assert_eq!(gate.status().phase, Phase::Running);
    assert_eq!(gate.status().last_drain_ms, before.last_drain_ms);
    assert_eq!(gate.status().idle_deadline_ms, before.idle_deadline_ms);
    assert!(!gate.known_idle());
    let preparation = gate
        .try_enter(1000, &CancellationToken::new())
        .unwrap()
        .unwrap();
    drop(preparation);
    let mut pause = gate.begin_pause().unwrap();
    assert!(pause.confirm_idle().is_err());
    assert_eq!(gate.status().phase, Phase::Retiring);
}

#[test]
fn an_unused_lease_cannot_extend_verified_idle() {
    let (gate, clock) = new_gate();
    let mut probe = gate.begin_pause().unwrap();
    probe.confirm_idle().unwrap();
    probe.cancel_pause_idle().unwrap();
    assert!(gate.known_idle());
    clock.store(130, Ordering::Release);
    assert!(gate.try_enter(1000, &CancellationToken::new()).is_err());
    assert_eq!(gate.status().phase, Phase::Retiring);
}

#[test]
fn lease_clone_is_closed_during_transitions_and_does_not_refresh_ttl() {
    let (gate, clock) = new_gate();
    let leases = Arc::new(());
    let before = gate.status();
    let lease = gate.lease(&leases).unwrap();
    assert_eq!(Arc::strong_count(&leases), 2);
    assert_eq!(gate.status(), before);
    let mut pause = gate.begin_pause().unwrap();
    assert!(gate.lease(&leases).is_none());
    pause.confirm_idle().unwrap();
    gate.confirm_pause(pause).unwrap();
    drop(lease);
    let lease = gate.lease(&leases).unwrap();
    assert_eq!(gate.status().idle_deadline_ms, before.idle_deadline_ms);
    clock.store(before.idle_deadline_ms, Ordering::Release);
    assert!(gate.lease(&leases).is_none());
    assert_eq!(gate.status().phase, Phase::Retiring);
    drop(lease);
}

#[test]
fn eviction_and_expiry_cannot_retire_a_new_execution_owner() {
    let (gate, clock) = new_gate();
    let mut probe = gate.begin_pause().unwrap();
    probe.confirm_idle().unwrap();
    probe.cancel_pause_idle().unwrap();
    let event = admitted(&gate);
    clock.store(140, Ordering::Release);
    assert!(!gate.retire_if_expired());
    assert!(!gate.retire_if_idle());
    assert_eq!(gate.status().phase, Phase::Running);
    drop(event);
    let mut probe = gate.begin_pause().unwrap();
    probe.confirm_idle().unwrap();
    probe.cancel_pause_idle().unwrap();
    assert!(gate.retire_if_idle());
}
