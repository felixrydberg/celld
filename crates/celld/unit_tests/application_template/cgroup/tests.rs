use super::*;
use std::os::unix::fs::symlink;

#[test]
fn parent_registry_preserves_intent_and_created_identity() {
    let fixture = tempfile::tempdir().unwrap();
    let dir = File::from(open(fixture.path(), DIR_FLAGS, Mode::empty()).unwrap());
    let file = File::from(
        openat(
            &dir,
            "cgroups.jsonl",
            OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_APPEND | OFlag::O_NOFOLLOW,
            Mode::from_bits_truncate(0o600),
        )
        .unwrap(),
    );
    let mut registration = ParentRegistration {
        file,
        record: serde_json::json!({
            "runId": "run-1", "name": "celld-vms-test", "ownerPid": 42,
            "ownerStartTicks": 73, "created": false,
        }),
    };
    registration.append(None).unwrap();
    let identity = fstat(&dir).unwrap();
    registration.append(Some(&identity)).unwrap();
    let records: Vec<serde_json::Value> = read(&dir, "cgroups.jsonl")
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["created"], false);
    assert!(records[0].get("ino").is_none());
    assert_eq!(records[1]["created"], true);
    assert_eq!(records[1]["dev"], identity.st_dev);
    assert_eq!(records[1]["ino"], identity.st_ino);
    assert_eq!(records[1]["ownerStartTicks"], 73);
    registration.record["runId"] = serde_json::json!("x".repeat(LIMIT as usize));
    assert!(registration.append(None).is_err());
}
#[test]
fn budgets_and_scalar_bounds() {
    let budget = Budget {
        high_bytes: 900_000_000,
        max_bytes: 1_000_000_000,
        overhead_bytes: 1_000_000,
    };
    assert!(budget.validate().is_ok());
    assert_eq!(budget.leaf_bytes(256).unwrap(), 269_435_456);
    assert!(budget.leaf_bytes(1024).is_err());
    assert!(Budget {
        overhead_bytes: u64::MAX,
        ..budget
    }
    .leaf_bytes(1)
    .is_err());
    assert!(Budget {
        high_bytes: 0,
        ..budget
    }
    .validate()
    .is_err());
    for name in ["", ".", "..", "x/y", "/x", "x\0"] {
        assert!(valid_name(name).is_err());
    }
    assert!(valid_name("celld-vms-abCD_01").is_ok());
    assert_eq!(value("populated 0\nfrozen 0\n", "populated").unwrap(), 0);
    assert!(value("populated 0\npopulated 1", "populated").is_err());
    assert!(value("populated 0 extra", "populated").is_err());
    assert!(value("populated -1", "populated").is_err());
    assert_eq!(
        pressure_total("some avg10=0.00 total=42", "some").unwrap(),
        42
    );
    assert!(pressure_total("some total=42 total=43", "some").is_err());
}

#[test]
fn descriptor_reads_refuse_symlinks_and_oversized_files() {
    let fixture = tempfile::tempdir().unwrap();
    let dir = File::from(open(fixture.path(), DIR_FLAGS, Mode::empty()).unwrap());
    std::fs::write(fixture.path().join("scalar"), "1").unwrap();
    symlink("scalar", fixture.path().join("link")).unwrap();
    assert!(read(&dir, "link").is_err());
    symlink(fixture.path(), fixture.path().join("directory-link")).unwrap();
    assert!(directory(&dir, "directory-link").is_err());
    std::fs::write(fixture.path().join("large"), vec![b'x'; LIMIT as usize + 1]).unwrap();
    assert!(read(&dir, "large").is_err());
    assert!(same_directory(&dir, "directory-link", &dir).is_err());
    for (name, text) in [
        ("memory.current", "7"),
        ("memory.peak", "9"),
        (
            "memory.events.local",
            "high 1\nmax 2\noom 3\noom_kill 4\noom_group_kill 5\n",
        ),
        (
            "memory.pressure",
            "some avg10=0 total=11\nfull avg10=0 total=13\n",
        ),
    ] {
        std::fs::write(fixture.path().join(name), text).unwrap();
    }
    assert_eq!(
        sample(&dir, "memory.events.local").unwrap(),
        MemorySample {
            current_bytes: 7,
            peak_bytes: 9,
            high: 1,
            max: 2,
            oom: 3,
            oom_kill: 4,
            oom_group_kill: 5,
            some_total_us: 11,
            full_total_us: 13,
        }
    );
}

#[test]
fn failed_cleanup_keeps_claim_and_parent() {
    let fixture = tempfile::tempdir().unwrap();
    std::fs::create_dir(fixture.path().join("parent")).unwrap();
    let root = File::from(open(fixture.path(), DIR_FLAGS, Mode::empty()).unwrap());
    let parent = Arc::new(StableParent {
        directory: directory(&root, "parent").unwrap(),
        root,
        name: "parent".into(),
        budget: Budget {
            high_bytes: 1,
            max_bytes: 2,
            overhead_bytes: 0,
        },
        ownership: Mutex::new(Ownership::default()),
    });
    std::fs::create_dir(fixture.path().join("parent/leaf")).unwrap();
    std::fs::write(fixture.path().join("parent/leaf/cgroup.procs"), "").unwrap();
    std::fs::write(
        fixture.path().join("parent/leaf/cgroup.events"),
        "populated 1",
    )
    .unwrap();
    parent
        .ownership
        .lock()
        .unwrap()
        .claims
        .insert("leaf".into());
    let mut leaf = Leaf {
        directory: directory(&parent.directory, "leaf").unwrap(),
        parent: parent.clone(),
        id: "leaf".into(),
        full_limit: 4096,
        spawned: false,
        removed: false,
    };
    assert!(leaf.discard_unstarted().is_err());
    leaf.spawned = true;
    assert!(leaf.discard_unstarted().is_err());
    leaf.spawned = false;
    std::fs::write(
        fixture.path().join("parent/leaf/cgroup.events"),
        "populated 0",
    )
    .unwrap();
    assert!(
        leaf.discard_unstarted().is_err(),
        "nonrecursive removal must fail with files"
    );
    drop(leaf);
    assert!(parent.ownership.lock().unwrap().claims.contains("leaf"));
    assert!(parent.shutdown().is_err());
    assert!(fixture.path().join("parent/leaf").is_dir());
}
