use super::*;

fn sample(current_bytes: u64) -> Sample {
    Sample {
        memory: MemorySample {
            current_bytes,
            ..MemorySample::default()
        },
        available_bytes: 20,
    }
}

#[test]
fn configured_gap_and_stable_poll_control_recovery() {
    let now = Instant::now();
    let poll = Duration::from_secs(1);
    let mut guard = Guard::new(Some((80, 100)), poll);
    assert!(guard.blocked(now));
    guard.observe(now, Some(sample(79)));
    assert!(!guard.blocked(now));
    guard.observe(now, Some(sample(80)));
    assert!(guard.blocked(now));
    guard.observe(now, Some(sample(60)));
    guard.observe(now + poll, Some(sample(60)));
    assert!(guard.blocked(now + poll)); // strictly below high-gap
    guard.observe(now + poll, Some(sample(59)));
    guard.observe(now + poll + poll / 2, Some(sample(59)));
    assert!(guard.blocked(now + poll + poll / 2));
    guard.observe(now + poll * 2, Some(sample(59)));
    assert!(!guard.blocked(now + poll * 2));
    assert!(guard.blocked(now + poll * 5)); // missing fresh sample
    guard.observe(now + poll * 5, Some(sample(59)));
    assert!(guard.blocked(now + poll * 5)); // no recovery across an unsampled gap
    guard.observe(now + poll * 6, Some(sample(59)));
    assert!(!guard.blocked(now + poll * 6));
}

#[test]
fn every_signal_delta_reset_and_host_headroom_fail_closed() {
    let now = Instant::now();
    let poll = Duration::from_secs(1);
    for index in 0..7 {
        let mut guard = Guard::new(Some((80, 100)), poll);
        guard.observe(now, Some(sample(1)));
        let mut changed = sample(1);
        match index {
            0 => changed.memory.high = 1,
            1 => changed.memory.max = 1,
            2 => changed.memory.oom = 1,
            3 => changed.memory.oom_kill = 1,
            4 => changed.memory.oom_group_kill = 1,
            5 => changed.memory.some_total_us = 1,
            6 => changed.memory.full_total_us = 1,
            _ => unreachable!(),
        }
        guard.observe(now, Some(changed));
        assert!(guard.blocked(now));
        guard.observe(now, Some(changed));
        guard.observe(now + poll, Some(changed));
        assert!(!guard.blocked(now + poll));
        guard.observe(now + poll, Some(sample(1))); // reset
        assert!(guard.blocked(now + poll));
    }
    let mut guard = Guard::new(Some((80, 100)), poll);
    guard.observe(
        now,
        Some(Sample {
            available_bytes: 19,
            ..sample(1)
        }),
    );
    assert!(guard.blocked(now));
    guard.observe(now, None);
    guard.observe(now, Some(sample(1)));
    assert!(guard.blocked(now));
    guard.observe(now + poll, Some(sample(1)));
    assert!(!guard.blocked(now + poll));
    assert!(!Guard::new(None, poll).blocked(now));
    let mut invalid = Guard::new(Some((80, 80)), poll);
    invalid.observe(now, Some(sample(1)));
    assert!(invalid.blocked(now));
}

#[test]
fn host_available_requires_one_bounded_unit_checked_scalar() {
    // The parser is shared with the isolate memory diagnostics (`js`), so it is
    // asserted where it lives. The guard wraps its refusal as host_available's
    // error above.
    assert_eq!(
        crate::machine::meminfo_available_bytes("MemTotal: 50 kB\nMemAvailable: 2 kB\n"),
        Some(2048)
    );
    for text in [
        "",
        "MemAvailable: 2 MB",
        "MemAvailable: 2 kB extra",
        "MemAvailable: 2 kB\nMemAvailable: 2 kB",
        "MemAvailable: 18446744073709551615 kB",
    ] {
        assert_eq!(crate::machine::meminfo_available_bytes(text), None, "{text:?}");
    }
}
