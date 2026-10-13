use super::*;
const MIB: u64 = 1024 * 1024;

fn config() -> VmHostConfig {
    serde_json::from_value(serde_json::json!({
        "jailer": "/usr/bin/jailer",
        "firecracker": "/usr/bin/firecracker",
        "kernel": {"path": "/kernel", "sha256": "0".repeat(64)},
        "runtimes": [{"path": "/runtime", "sha256": "1".repeat(64)}],
        "uid": 1000, "gid": 1000, "max_vms": 32,
        "max_memory_mib": 65536, "idle_ttl_secs": 30,
        "platform_domain": "applications.example.com"
    }))
    .unwrap()
}

fn guard() -> MemoryGuard {
    MemoryGuard {
        max_bytes: 16384 * MIB,
        high_bytes: 15360 * MIB,
        overhead_bytes: 32 * MIB,
        shared_cache_bytes: 512 * MIB,
        idle_cache: None,
    }
}

#[test]
fn explicit_guard_validation_and_legacy_default() {
    let config = config();
    assert!(config.memory_guard.is_none());
    assert!(config.validate().is_ok());
    assert!(guard().validate().is_ok());
    for broken in [
        MemoryGuard {
            high_bytes: 0,
            ..guard()
        },
        MemoryGuard {
            high_bytes: guard().max_bytes,
            ..guard()
        },
        MemoryGuard {
            high_bytes: guard().max_bytes / 2,
            ..guard()
        },
        MemoryGuard {
            max_bytes: 1,
            ..guard()
        },
        MemoryGuard {
            overhead_bytes: 0,
            ..guard()
        },
        MemoryGuard {
            shared_cache_bytes: 0,
            ..guard()
        },
        MemoryGuard {
            shared_cache_bytes: u64::MAX,
            ..guard()
        },
        MemoryGuard {
            max_bytes: 12288 * MIB,
            high_bytes: 12288 * MIB,
            ..guard()
        },
    ] {
        assert!(broken.validate().is_err());
    }
    assert!(guard().reservation_bytes(u64::MAX, 1).is_none());
    assert!(MemoryGuard {
        overhead_bytes: u64::MAX,
        ..guard()
    }
    .reservation_bytes(1, 2)
    .is_none());
    assert!(serde_json::from_value::<MemoryGuard>(serde_json::json!({
        "max_bytes": 1, "high_bytes": 1, "overhead_bytes": 1,
        "shared_cache_bytes": 1, "unknown": 1
    }))
    .is_err());
}

#[test]
fn retained_cache_is_explicit_and_validated() {
    assert!(guard().idle_cache.is_none());
    let cache = IdleCache {
        max_bytes: 1536 * MIB,
        retained_margin_bytes: 64 * MIB,
    };
    assert!(MemoryGuard {
        idle_cache: Some(cache),
        ..guard()
    }
    .validate()
    .is_ok());
    for cache in [
        IdleCache {
            max_bytes: guard().overhead_bytes,
            ..cache
        },
        IdleCache {
            max_bytes: u64::MAX,
            ..cache
        },
        IdleCache {
            retained_margin_bytes: 0,
            ..cache
        },
        IdleCache {
            retained_margin_bytes: cache.max_bytes,
            ..cache
        },
    ] {
        assert!(MemoryGuard {
            idle_cache: Some(cache),
            ..guard()
        }
        .validate()
        .is_err());
    }
}

#[test]
fn retained_charge_preserves_nominal_overhead_and_requires_full_readmission() {
    let mut config = config();
    config.max_memory_mib = 24576;
    let policy = MemoryGuard {
        idle_cache: Some(IdleCache {
            max_bytes: 1536 * MIB,
            retained_margin_bytes: 64 * MIB,
        }),
        ..guard()
    };
    config.memory_guard = Some(policy);
    let mut snapshot = VmPoolSnapshot {
        vms: 2,
        memory_mib: 24576,
        ..Default::default()
    };
    let full = snapshot;
    assert!(!config.room(&snapshot, (0, 0), 0, 0));
    snapshot.retain(12288, 1024 * MIB).unwrap();
    assert_eq!(snapshot.memory_mib, full.memory_mib);
    assert_eq!(snapshot.vms, full.vms);
    assert_eq!(
        snapshot.physical_bytes(policy, snapshot.memory_mib, snapshot.vms),
        Some((12288 + 1024 + 64 + 512) * MIB)
    );
    assert!(config.room(&snapshot, (0, 0), 0, 0));
    assert!(!config.room(&snapshot, (0, 0), 1, 1)); // unchanged nominal ceiling
    assert!(snapshot
        .physical_bytes(guard(), snapshot.memory_mib, snapshot.vms)
        .is_none());
    let mut promoted = snapshot;
    promoted.full(12288, 1024 * MIB).unwrap();
    assert_eq!(promoted, full);
    assert!(!config.room(&promoted, (0, 0), 0, 0));
    // Failed reap keeps snapshot unchanged: no credit can be spent.
    let quarantined = VmPoolSnapshot {
        quarantined: 1,
        ..snapshot
    };
    assert_eq!(
        quarantined.physical_bytes(policy, quarantined.memory_mib, quarantined.vms),
        snapshot.physical_bytes(policy, snapshot.memory_mib, snapshot.vms)
    );
    assert!(snapshot
        .physical_bytes(
            MemoryGuard {
                idle_cache: Some(IdleCache {
                    max_bytes: 1056 * MIB - 1,
                    retained_margin_bytes: MIB,
                }),
                ..guard()
            },
            snapshot.memory_mib,
            snapshot.vms
        )
        .is_none());
}

#[test]
fn cache_counter_failures_do_not_partially_release_capacity() {
    let mut snapshot = VmPoolSnapshot {
        cached_vms: 1,
        cached_nominal_mib: 1,
        retained_limit_bytes: 1,
        ..Default::default()
    };
    let before = snapshot;
    assert!(snapshot.full(2, 1).is_none());
    assert_eq!(snapshot, before);
    snapshot.retained_limit_bytes = u64::MAX;
    let before = snapshot;
    assert!(snapshot.retain(1, 1).is_none());
    assert_eq!(snapshot, before);
    assert!(snapshot.physical_bytes(guard(), 1, 0).is_none());
}
#[test]
fn room_keeps_full_starting_quarantined_and_idle_reservations() {
    let mut config = config();
    let snapshot = VmPoolSnapshot {
        vms: 2,
        memory_mib: 8192,
        starting: 1,
        quarantined: 1,
        ..Default::default()
    };
    let reservation = guard().reservation_bytes(13312, 5).unwrap();
    assert_eq!(reservation, 13984 * MIB);
    config.memory_guard = Some(MemoryGuard {
        max_bytes: reservation,
        high_bytes: reservation - 1024 * MIB,
        ..guard()
    });
    assert!(config.validate().is_ok());
    assert!(config.room(&snapshot, (2, 4096), 1, 1024));
    assert!(!config.room(&snapshot, (2, 4096), 1, 1025));
    config.memory_guard = None;
    assert!(config.room(&snapshot, (2, 4096), 1, 16384));
    assert!(!config.room(&snapshot, (0, 0), 1, config.max_memory_mib));
    assert!(!config.room(
        &VmPoolSnapshot {
            vms: u32::MAX,
            ..snapshot
        },
        (1, 0),
        1,
        0
    ));
    assert!(!config.room(
        &VmPoolSnapshot {
            memory_mib: u64::MAX,
            ..snapshot
        },
        (0, 1),
        0,
        0
    ));
}
