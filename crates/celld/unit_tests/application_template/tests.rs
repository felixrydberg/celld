use super::*;

#[test]
fn reporting_device_profile_versions_all_snapshot_identities() {
    assert_eq!(
        balloon_config(),
        serde_json::json!({
            "amount_mib": 0,
            "deflate_on_oom": false,
            "stats_polling_interval_s": 0,
            "free_page_reporting": true,
            "free_page_hinting": false,
        })
    );
    for identity in [
        "runtime 1 256 kernel Firecracker v1.17.0",
        "pyodide 0.28.3 runtime",
    ] {
        assert_eq!(
            memory_profile_key(identity),
            content_digest(format!("{MEMORY_DEVICE_PROFILE} {identity}").as_bytes())
        );
        assert_ne!(
            memory_profile_key(identity),
            content_digest(identity.as_bytes())
        );
        assert_ne!(
            memory_profile_key(identity),
            content_digest(format!("virtio-balloon-reporting-v2 {identity}").as_bytes())
        );
    }
}

#[test]
fn pool_charges_actual_sizes_through_preparation_and_unproved_reap() {
    let templates = Templates::new().unwrap();
    {
        let mut idle = templates.idle.lock().unwrap();
        idle.preparing = vec![("lite".into(), 256), ("standard-4".into(), 12288)];
        idle.reaping = vec![4096];
    }
    assert_eq!(templates.idle_capacity(), (3, 16640));
    templates.reaped(4096, false);
    assert_eq!(templates.idle_capacity(), (3, 16640));
    assert_eq!(templates.winding_down(), (2, 1));
    templates.end_fill("lite", None, false, true);
    assert_eq!(templates.idle_capacity(), (2, 16384));
    templates.end_fill("standard-4", None, false, false);
    assert_eq!(templates.idle_capacity(), (2, 16384));
    assert_eq!(templates.winding_down(), (0, 2));
}

#[test]
fn refill_skips_classes_that_do_not_fit_and_gates_startup_readiness() {
    let templates = Templates::new().unwrap();
    templates.begin_startup();
    assert!(!templates.ready());
    for (key, memory) in [("large", 12288), ("small", 256)] {
        templates.built.lock().unwrap().insert(
            key.into(),
            Entry {
                needed: true,
                template: Some(Arc::new(Template {
                    dir: templates.dir.path().into(),
                    key: key.into(),
                    runtime_sha256: "a".repeat(64),
                    vcpus: 1,
                    memory_mib: memory,
                    cpu_quota: 6250,
                })),
                ..Entry::default()
            },
        );
    }
    assert_eq!(templates.begin_fill(1, 256).unwrap().key, "small");
    assert!(templates.begin_fill(1, 256).is_none());
    assert_eq!(templates.idle_capacity(), (1, 256));
    templates.finish_startup();
    assert!(templates.ready());
}

#[test]
fn sweep_removes_only_old_directories_without_a_live_owner() {
    let root = tempfile::tempdir().unwrap();
    let dir = |name: &str, locked: bool, old: bool| {
        let dir = root.path().join(name);
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("memory"), b"x").unwrap();
        if locked {
            std::fs::write(dir.join("lock"), b"").unwrap();
        }
        if old {
            let aged = std::fs::File::open(&dir).unwrap();
            aged.set_modified(std::time::SystemTime::now() - Duration::from_secs(120))
                .unwrap();
        }
        dir
    };
    let leaked = dir("celld-vms-leaked", true, true);
    let live = dir("celld-vms-live", true, true);
    let older_release = dir("celld-vms-older", false, true);
    let starting = dir("celld-vms-starting", true, false);
    let other = dir("unrelated", false, true);
    // A running node holds its lock; the killed one's is free.
    let held = std::fs::File::open(live.join("lock")).unwrap();
    assert!(lock_file(&held));
    sweep_leaked(root.path());
    assert!(!leaked.exists() && !older_release.exists());
    assert!(live.exists() && starting.exists() && other.exists());
}

#[cfg(test)]
#[test]
fn pure_templates_do_not_initialize_host_memory_limits() {
    let templates = Templates::new().unwrap();
    assert!(templates.memory.is_none());
    assert!(templates.memory_sample().unwrap().is_none());
}
