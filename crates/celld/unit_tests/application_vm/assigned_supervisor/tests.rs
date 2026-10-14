use super::*;
fn running() -> Status {
    Status {
        phase: Phase::Running,
        active_events: 0,
        last_drain_ms: 10,
        idle_deadline_ms: 40,
        resume_requested: false,
        epoch: 0,
    }
}
#[test]
fn unused_leases_do_not_delay_expiry_probe_but_active_work_does() {
    let mut status = running();
    assert!(!should_probe(status, 2, 0, 39));
    assert!(should_probe(status, 2, 0, 40));
    assert!(should_probe(status, 1, 0, 20));
    assert!(!should_probe(status, 1, 1, 40));
    status.active_events = 1;
    assert!(!should_probe(status, 1, 0, 40));
    status.active_events = 0;
    status.phase = Phase::Cached;
    assert!(!should_probe(status, 1, 0, 40));
}
#[test]
fn physical_transition_cannot_extend_the_original_retention_deadline() {
    assert_eq!(transition_deadline(running(), 39), 40);
    let mut status = running();
    status.idle_deadline_ms = 5000;
    assert_eq!(transition_deadline(status, 100), 2100);
}
