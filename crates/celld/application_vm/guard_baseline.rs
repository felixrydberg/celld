//! Actual-VM hardware guard qualification; policy eviction is qualified separately.
use super::memory_baseline as memory;
use super::*;
use anyhow::{ensure, Context};
use celld_applications::invocation_wire::HttpRequest;
use memory::{config, name, no_vms, request, stage, Recorder};
use nix::fcntl::{open, openat, AtFlags, OFlag};
use nix::sys::stat::{fstat, fstatat, Mode};
use nix::sys::statfs::{fstatfs, CGROUP2_SUPER_MAGIC};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::fs::File;
use std::io::{Read, Write};
use tokio::time::timeout;

const MIB: u64 = 1024 * 1024;
const DIRECTORY: OFlag = OFlag::O_RDONLY
    .union(OFlag::O_DIRECTORY)
    .union(OFlag::O_NOFOLLOW)
    .union(OFlag::O_CLOEXEC);

fn component(value: &str, prefix: &str) -> bool {
    value.starts_with(prefix)
        && value.len() > prefix.len()
        && value.len() <= 255
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
}

fn process(pid: u32) -> Result<(u32, u64)> {
    let value = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let tail = value
        .rsplit_once(')')
        .context("invalid owned process stat")?
        .1;
    let fields: Vec<_> = tail.split_whitespace().collect();
    ensure!(fields.len() > 19, "short owned process stat");
    Ok((fields[1].parse()?, fields[19].parse()?))
}

fn membership(pid: u32) -> Result<Option<(String, String)>> {
    let value = std::fs::read_to_string(format!("/proc/{pid}/cgroup"))?;
    let path = value
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .context("owned process lacks cgroup v2 membership")?;
    let parts: Vec<_> = path
        .strip_prefix('/')
        .context("invalid cgroup path")?
        .split('/')
        .collect();
    // A jailer can be observed before attachment. Never use that membership for control writes.
    if parts.len() != 2 || !component(parts[0], "celld-vms-") {
        return Ok(None);
    }
    ensure!(component(parts[1], "vm-"), "invalid owned VM leaf");
    Ok(Some((parts[0].into(), parts[1].into())))
}

pub(super) struct Group {
    parent: File,
    pub(super) directory: File,
    name: String,
    path: PathBuf,
    device: u64,
    inode: u64,
    pub(super) member: Option<(u32, u64)>,
}

impl Group {
    fn root() -> Result<File> {
        let mut directory = File::from(open("/", DIRECTORY, Mode::empty())?);
        for part in ["sys", "fs", "cgroup"] {
            directory = File::from(openat(&directory, part, DIRECTORY, Mode::empty())?);
        }
        ensure!(
            fstatfs(&directory)?.filesystem_type() == CGROUP2_SUPER_MAGIC,
            "guard fixture requires cgroup v2"
        );
        Ok(directory)
    }

    fn open(parent: File, name: &str, path: PathBuf, member: Option<(u32, u64)>) -> Result<Self> {
        let directory = File::from(openat(&parent, name, DIRECTORY, Mode::empty())?);
        let stat = fstat(&directory)?;
        ensure!(stat.st_uid == 0, "guard cgroup must be owned by root");
        let group = Self {
            parent,
            directory,
            name: name.into(),
            path,
            device: stat.st_dev,
            inode: stat.st_ino,
            member,
        };
        ensure!(group.exists()?, "owned cgroup vanished");
        Ok(group)
    }

    pub(super) fn exists(&self) -> Result<bool> {
        match fstatat(
            &self.parent,
            self.name.as_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        ) {
            Ok(stat) => {
                ensure!(
                    stat.st_dev == self.device && stat.st_ino == self.inode,
                    "owned cgroup identity changed"
                );
                Ok(true)
            }
            Err(nix::errno::Errno::ENOENT) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn event(&self, control: &str, field: &str) -> Result<u64> {
        let file = File::from(openat(
            &self.directory,
            control,
            OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
            Mode::empty(),
        )?);
        let mut value = String::new();
        file.take(4097).read_to_string(&mut value)?;
        ensure!(value.len() <= 4096, "oversized cgroup event file");
        let mut matches = value.lines().filter_map(|line| {
            let (key, count) = line.split_once(' ')?;
            (key == field).then_some(count)
        });
        let count = matches.next().context("missing cgroup event field")?;
        ensure!(matches.next().is_none(), "duplicate cgroup event field");
        Ok(count.parse()?)
    }

    pub(super) fn scalar(&self, control: &str) -> Result<u64> {
        let file = File::from(openat(
            &self.directory,
            control,
            OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
            Mode::empty(),
        )?);
        let mut value = String::new();
        file.take(4097).read_to_string(&mut value)?;
        ensure!(value.len() <= 4096, "oversized cgroup scalar");
        Ok(value.trim().parse()?)
    }

    pub(super) fn verify_member(&self, owned: fn() -> HashSet<u32>) -> Result<()> {
        let (pid, ticks) = self
            .member
            .context("leaf write requires a process identity")?;
        ensure!(owned().contains(&pid), "VM PID no longer owned by fixture");
        ensure!(
            process(pid)? == (std::process::id(), ticks),
            "VM process identity changed"
        );
        let (parent, leaf) = membership(pid)?.context("VM no longer attached to owned leaf")?;
        ensure!(
            self.path == Path::new("/sys/fs/cgroup").join(parent).join(leaf),
            "VM cgroup membership changed"
        );
        ensure!(self.exists()?, "VM leaf disappeared before write");
        Ok(())
    }

    fn write_max(&self, value: u64) -> Result<()> {
        ensure!(
            self.member.is_some(),
            "hard-breach writes require a VM leaf"
        );
        ensure!(self.exists()?, "owned leaf removed before control write");
        let mut file = File::from(openat(
            &self.directory,
            "memory.max",
            OFlag::O_WRONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
            Mode::empty(),
        )?);
        file.write_all(value.to_string().as_bytes())?;
        Ok(())
    }

    pub(super) fn leaf(
        pid: u32,
        expected_parent: &Group,
        owned: fn() -> HashSet<u32>,
    ) -> Result<Option<Self>> {
        ensure!(owned().contains(&pid), "process not owned by fixture");
        let (ppid, ticks) = process(pid)?;
        ensure!(ppid == std::process::id(), "VM is not fixture child");
        let Some((parent, leaf)) = membership(pid)? else {
            return Ok(None);
        };
        ensure!(
            parent == expected_parent.name,
            "owned VM attached to another parent"
        );
        let root = Self::root()?;
        let directory = File::from(openat(&root, parent.as_str(), DIRECTORY, Mode::empty())?);
        let stat = fstat(&directory)?;
        ensure!(
            stat.st_dev == expected_parent.device && stat.st_ino == expected_parent.inode,
            "owned parent identity changed"
        );
        let group = Self::open(
            directory,
            &leaf,
            Path::new("/sys/fs/cgroup").join(parent).join(&leaf),
            Some((pid, ticks)),
        )?;
        group.verify_member(owned)?;
        Ok(Some(group))
    }

    pub(super) fn registered_parent() -> Result<Self> {
        let run = std::env::var("CELLD_APPLICATION_RUN_ID")?;
        let artifacts = PathBuf::from(
            std::env::var_os("CELLD_APPLICATION_NODE_ARTIFACTS")
                .context("guard fixture requires cgroup registry")?,
        );
        let (_, ticks) = process(std::process::id())?;
        let mut registry = String::new();
        File::open(artifacts.join("cgroups.jsonl"))?
            .take(65537)
            .read_to_string(&mut registry)?;
        ensure!(registry.len() <= 65536, "oversized fixture cgroup registry");
        let entries: Vec<Value> = registry
            .lines()
            .map(serde_json::from_str)
            .collect::<std::result::Result<_, _>>()?;
        let entries: Vec<_> = entries
            .iter()
            .filter(|entry| {
                entry["runId"] == run
                    && entry["ownerPid"] == std::process::id()
                    && entry["ownerStartTicks"] == ticks
                    && entry["created"] == true
            })
            .collect();
        ensure!(entries.len() == 1, "expected one owned guarded pool parent");
        let entry = entries[0];
        let name = entry["name"]
            .as_str()
            .context("missing guarded parent name")?;
        ensure!(component(name, "celld-vms-"), "invalid guarded parent name");
        let group = Self::open(
            Self::root()?,
            name,
            Path::new("/sys/fs/cgroup").join(name),
            None,
        )?;
        ensure!(
            entry["dev"].as_u64() == Some(group.device)
                && entry["ino"].as_u64() == Some(group.inode),
            "parent registry identity mismatch"
        );
        Ok(group)
    }

    fn leaf_limits(&self, guest_mib: u64) -> Result<()> {
        ensure!(
            self.scalar("memory.max")? == (guest_mib + 256) * MIB,
            "VM leaf max differs from full guest ceiling plus explicit overhead"
        );
        ensure!(self.scalar("memory.swap.max")? == 0, "VM leaf permits swap");
        ensure!(
            self.scalar("memory.oom.group")? == 1,
            "VM leaf OOM group disabled"
        );
        Ok(())
    }
}

// Restores the original control after an assertion fails, but never writes to a replacement leaf.
struct MaxOverride {
    group: Arc<Group>,
    original: u64,
    active: bool,
}
impl MaxOverride {
    fn restore(&mut self) -> Result<()> {
        if self.active {
            if self.group.exists()? {
                self.group.write_max(self.original)?;
            }
            self.active = false;
        }
        Ok(())
    }
}
impl Drop for MaxOverride {
    fn drop(&mut self) {
        if let Err(error) = self.restore() {
            eprintln!("guard fixture restore failed: {error}");
        }
    }
}

async fn one_leaf(parent: &Group, before: HashSet<u32>, owned: fn() -> HashSet<u32>) -> Arc<Group> {
    let after = owned();
    let added: Vec<_> = after.difference(&before).copied().collect();
    assert_eq!(added.len(), 1, "acquire must add exactly one assigned VM");
    Arc::new(
        Group::leaf(added[0], parent, owned)
            .unwrap()
            .expect("VM not attached"),
    )
}

fn assert_parent(parent: &Group) {
    assert!(parent.exists().unwrap());
    assert_eq!(parent.scalar("memory.max").unwrap(), 14 * 1024 * MIB);
    assert_eq!(parent.scalar("memory.high").unwrap(), 13 * 1024 * MIB);
    assert_eq!(parent.scalar("memory.oom.group").unwrap(), 0);
}

pub(super) async fn run(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    _directory: &Path,
    owned: fn() -> HashSet<u32>,
) {
    assert!(owned().is_empty(), "pre-existing fixture VM");
    let output = PathBuf::from(
        std::env::var_os("CELLD_APPLICATION_MEMORY_OUTPUT")
            .expect("guard fixture requires a new memory JSONL output"),
    );
    let recorder = Recorder::new(&output, "guard-template-bootstrap");
    let mut config = config(options, 0, 120);
    let guard = MemoryGuard {
        max_bytes: 14 * 1024 * MIB,
        high_bytes: 13 * 1024 * MIB,
        overhead_bytes: 256 * MIB,
        shared_cache_bytes: 256 * MIB,
        idle_cache: None,
    };
    config.memory_guard = Some(guard);
    let pool = Arc::new(VmPool::new(config, store.clone(), "guard-node".into()).unwrap());
    let parent = Arc::new(Group::registered_parent().unwrap());
    assert_parent(&parent);
    assert_eq!(parent.scalar("memory.current").unwrap(), 0);
    assert_eq!(parent.event("cgroup.events", "populated").unwrap(), 0);
    assert_eq!(pool.snapshot().vms, 0);
    assert_eq!(pool.templates.idle_capacity(), (0, 0));
    recorder.emit(json!({"kind":"guardInputs","limits":guard,
        "provenance":"explicit fixture stress inputs; not production defaults",
        "parentDevice":parent.device,"parentInode":parent.inode}));
    let (stop, sampler) = recorder.start_sampler(&pool, owned);

    // Observe every actual builder while prepare owns and charges it. Retain no VM descriptor.
    let audit_stop = CancellationToken::new();
    let audit_signal = audit_stop.clone();
    let audit_parent = parent.clone();
    let audit_pool = pool.clone();
    let audit = tokio::spawn(async move {
        let mut classes = BTreeSet::new();
        while !audit_signal.is_cancelled() {
            for pid in owned() {
                match Group::leaf(pid, &audit_parent, owned) {
                    Ok(Some(group)) => {
                        let observed: Result<u64> = (|| {
                            let max = group.scalar("memory.max")?;
                            ensure!(max >= 256 * MIB && max % MIB == 0, "invalid leaf ceiling");
                            let guest = max / MIB - 256;
                            group.leaf_limits(guest)?;
                            ensure!(
                                ApplicationSizeClass::ALL
                                    .iter()
                                    .any(|class| u64::from(class.resources().1) == guest),
                                "unknown builder class"
                            );
                            let charged = audit_pool.snapshot();
                            group.verify_member(owned)?;
                            ensure!(
                                charged.vms > 0 && charged.memory_mib >= guest,
                                "builder leaf exists without its full charge"
                            );
                            ensure!(
                                guard
                                    .reservation_bytes(charged.memory_mib, charged.vms)
                                    .is_some_and(|bytes| bytes <= guard.max_bytes),
                                "builder exceeds ledger guard"
                            );
                            Ok(guest)
                        })();
                        match observed {
                            Ok(guest) => {
                                classes.insert(guest);
                            }
                            Err(error) => {
                                if owned().contains(&pid) {
                                    panic!("builder guard sample failed: {error}");
                                }
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        // Process exit and leaf removal can race this observer.
                        if owned().contains(&pid) {
                            panic!("builder guard audit failed: {error}");
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        classes
    });
    timeout(Duration::from_secs(1500), pool.prepare(app))
        .await
        .expect("guard template preparation timed out")
        .unwrap();
    no_vms(&pool, owned).await;
    audit_stop.cancel();
    let seen = audit.await.unwrap();
    assert_eq!(
        seen,
        ApplicationSizeClass::ALL
            .iter()
            .map(|class| u64::from(class.resources().1))
            .collect()
    );
    assert_parent(&parent);
    assert_eq!(parent.event("cgroup.events", "populated").unwrap(), 0);
    recorder.emit(json!({"kind":"guardBuilders","guestMib":seen,
        "emptyParentPersists":true,"ledger":pool.status()}));

    let basic = stage(
        store,
        options,
        "memory-guard-basic",
        ApplicationSizeClass::Basic,
    )
    .await;
    let lite = stage(
        store,
        options,
        "memory-guard-lite",
        ApplicationSizeClass::Lite,
    )
    .await;
    recorder.context("guard-assigned", None, 0);
    let before = owned();
    let basic_lease = timeout(
        Duration::from_secs(60),
        pool.acquire(app, &basic, &name("memory"), &name("memory")),
    )
    .await
    .unwrap()
    .unwrap();
    let basic_group = one_leaf(&parent, before, owned).await;
    basic_group.leaf_limits(1024).unwrap();
    let before = owned();
    let lite_lease = timeout(
        Duration::from_secs(60),
        pool.acquire(app, &lite, &name("memory"), &name("memory")),
    )
    .await
    .unwrap()
    .unwrap();
    let lite_group = one_leaf(&parent, before, owned).await;
    lite_group.leaf_limits(256).unwrap();
    assert_ne!(basic_group.inode, lite_group.inode);
    let _basic_held = request(
        app,
        &basic_lease,
        "/retain",
        &recorder,
        ApplicationSizeClass::Basic,
    )
    .await;
    let lite_held = request(
        app,
        &lite_lease,
        "/retain",
        &recorder,
        ApplicationSizeClass::Lite,
    )
    .await;
    let charged = pool.snapshot();
    assert_eq!(
        (charged.vms, charged.memory_mib, charged.quarantined),
        (2, 1280, 0)
    );
    assert_eq!(pool.templates.idle_capacity(), (0, 0));
    assert_eq!(
        guard.reservation_bytes(charged.memory_mib, charged.vms),
        Some(2048 * MIB)
    );
    assert_parent(&parent);
    recorder.emit(json!({"kind":"guardAssigned","ledger":pool.status(),
        "reservationBytes":2048*MIB,"guestCeilingsMib":[1024,256]}));

    // Deliberately breach only the Basic VM's identity-bound leaf, keeping both leases alive.
    recorder.context("guard-leaf-oom", Some(ApplicationSizeClass::Basic), 0);
    basic_group.verify_member(owned).unwrap();
    assert!(basic_group.scalar("memory.current").unwrap() > 4096);
    let before_oom = pool.templates.memory_sample().unwrap().unwrap();
    let mut override_max = MaxOverride {
        group: basic_group.clone(),
        original: basic_group.scalar("memory.max").unwrap(),
        active: true,
    };
    let victim = basic_group.clone();
    timeout(
        Duration::from_secs(15),
        tokio::task::spawn_blocking(move || {
            victim.verify_member(owned)?;
            victim.write_max(4096)
        }),
    )
    .await
    .expect("owned leaf hard-breach write timed out")
    .unwrap()
    .unwrap();
    let failed = timeout(
        Duration::from_secs(10),
        basic_lease.invoke_stateless(
            app,
            Duration::from_secs(5),
            HttpRequest {
                method: "GET".into(),
                url: "http://memory/status".into(),
                headers: vec![],
                body: vec![],
            },
            None,
        ),
    )
    .await
    .expect("OOM request did not settle within its bound");
    assert!(
        failed.is_err(),
        "original Basic client unexpectedly survived leaf OOM"
    );
    timeout(Duration::from_secs(30), async {
        loop {
            let charged = pool.snapshot();
            if charged.starting == 0
                && charged.ready == 1
                && (charged.vms == 1 || charged.quarantined == 1)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("OOM VM neither reaped nor quarantined");
    override_max.restore().unwrap();
    let charged = pool.snapshot();
    let reason = if charged.quarantined == 0 {
        assert_eq!((charged.vms, charged.memory_mib), (1, 256));
        assert!(!basic_group.exists().unwrap());
        assert!(!owned().contains(&basic_group.member.unwrap().0));
        "memory limit exhausted"
    } else {
        assert_eq!(
            (charged.vms, charged.memory_mib, charged.quarantined),
            (2, 1280, 1)
        );
        "VM reap unproved; capacity quarantined"
    };
    let states = store.status(&basic).await.unwrap();
    assert!(
        states
            .iter()
            .flat_map(|state| state.handlers.iter())
            .any(|handler| handler.deployment == name("memory")
                && handler.handler == name("memory")
                && handler.state
                    == HandlerState::Failed {
                        reason: reason.into()
                    }),
        "OOM status did not preserve its fixed failure reason"
    );
    let after_oom = pool.templates.memory_sample().unwrap().unwrap();
    assert!(
        after_oom.oom_kill > before_oom.oom_kill
            || after_oom.oom_group_kill > before_oom.oom_group_kill,
        "no kernel OOM kill counter increased"
    );
    lite_group.verify_member(owned).unwrap();
    let lite_after = request(
        app,
        &lite_lease,
        "/status",
        &recorder,
        ApplicationSizeClass::Lite,
    )
    .await;
    assert_eq!(
        lite_after, lite_held,
        "unrelated VM lost retained worker state"
    );
    let host = timeout(Duration::from_secs(3), app.snapshot())
        .await
        .expect("host actor did not answer after leaf OOM");
    let host: Value = serde_json::from_str(&host).unwrap();
    assert!(
        host.get("error").is_none(),
        "host actor failed after leaf OOM"
    );
    assert_parent(&parent);
    recorder.emit(json!({"kind":"guardIsolation","kernelBefore":before_oom,
        "kernelAfter":after_oom,"failureReason":reason,"ledger":pool.status(),
        "unrelatedVmRetainedState":true,"hostActorResponded":true}));

    drop(basic_lease);
    drop(lite_lease);
    pool.retire_deployment(&lite, &name("memory")).await;
    no_vms(&pool, owned).await; // A quarantine cannot receive a passing cleanup marker.
    assert_parent(&parent);
    assert_eq!(parent.event("cgroup.events", "populated").unwrap(), 0);
    recorder.emit(json!({"kind":"guardEmpty","stableParentPersists":true,"ledger":pool.status()}));
    stop.cancel();
    sampler.await.unwrap();
    timeout(Duration::from_secs(30), pool.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !parent.exists().unwrap(),
        "guard parent leaked after proven shutdown"
    );
    let leaked: Vec<_> = recorder
        .groups
        .lock()
        .unwrap()
        .iter()
        .filter(|path| path.exists())
        .cloned()
        .collect();
    assert!(
        owned().is_empty() && leaked.is_empty(),
        "guard fixture resource leak"
    );
    recorder.emit(
        json!({"kind":"cleanup","ownedPids":owned(),"remainingCgroups":leaked,
        "parentRemoved":true,"pending":["parent.high throttling","pressure victim ordering",
            "16GiB density and repeat trials"]}),
    );
    eprintln!(
        "T29_RESULT {}",
        json!({"scenario":"t33::memory_guard","status":"passed",
        "duration_ms":recorder.began.elapsed().as_millis(),"output":output,
        "scope":"actual kernel leaf ceilings, OOM isolation, stable parent and proven cleanup"})
    );
}

#[tokio::test]
#[ignore = "requires diagnostic JavaScript image, Linux KVM and explicit guarded memory run"]
async fn t33_memory_guard() {
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_GUARD_CHECK_ONLY").is_some());
    memory::bridge_check("memory-guard").await;
}

#[test]
fn guarded_component_refuses_unowned_paths() {
    for value in [
        "celld-vms-",
        "celld-vms-../x",
        "../celld-vms-a",
        "celld-vms-a/x",
        "celld-vms-a\\x",
        "celld-vms-.a",
        "vm-a",
    ] {
        assert!(!component(value, "celld-vms-"));
    }
    assert!(component("celld-vms-123-a", "celld-vms-"));
    assert!(component("vm-a", "vm-"));
}
