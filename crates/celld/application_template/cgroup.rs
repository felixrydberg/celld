//! Pool-owned cgroups: capacity remains claimed until physical cleanup succeeds.
use anyhow::{ensure, Context, Result};
use nix::fcntl::{open, openat, AtFlags, OFlag};
use nix::sys::stat::{fstat, fstatat, mkdirat, Mode};
use nix::sys::statfs::{fstatfs, CGROUP2_SUPER_MAGIC};
use nix::unistd::{unlinkat, UnlinkatFlags};
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use tokio::process::Child;

const LIMIT: u64 = 4096;
const DIR_FLAGS: OFlag = OFlag::O_RDONLY
    .union(OFlag::O_DIRECTORY)
    .union(OFlag::O_NOFOLLOW)
    .union(OFlag::O_CLOEXEC);

#[derive(Clone, Copy)]
pub(crate) struct Budget {
    pub high_bytes: u64,
    pub max_bytes: u64,
    pub overhead_bytes: u64,
}

impl Budget {
    fn validate(self) -> Result<()> {
        ensure!(
            self.high_bytes > 0 && self.high_bytes <= self.max_bytes,
            "invalid pool memory high/max"
        );
        ensure!(
            self.overhead_bytes < self.max_bytes,
            "invalid VM memory overhead"
        );
        Ok(())
    }

    fn leaf_bytes(self, guest_mib: u32) -> Result<u64> {
        ensure!(guest_mib > 0, "empty VM memory reservation");
        let bytes = u64::from(guest_mib)
            .checked_mul(1024 * 1024)
            .and_then(|bytes| bytes.checked_add(self.overhead_bytes))
            .context("VM memory reservation overflow")?;
        ensure!(
            bytes <= self.max_bytes,
            "VM reservation exceeds pool memory max"
        );
        Ok(bytes)
    }
}

#[derive(Default)]
struct Ownership {
    claims: HashSet<String>,
    closed: bool,
}

pub(crate) struct StableParent {
    root: File,
    directory: File,
    name: String,
    budget: Budget,
    ownership: Mutex<Ownership>,
}

impl StableParent {
    /// Call during privileged startup, separately from pure Templates::new.
    pub(crate) fn initialize(name: &str, budget: Budget) -> Result<Arc<Self>> {
        valid_name(name)?;
        budget.validate()?;
        let mut root = File::from(open("/", DIR_FLAGS, Mode::empty())?);
        for part in ["sys", "fs", "cgroup"] {
            root = directory(&root, part)?;
        }
        ensure!(
            fstatfs(&root)?.filesystem_type() == CGROUP2_SUPER_MAGIC,
            "application VM cgroups require cgroup v2"
        );
        delegate(&root)?;
        #[cfg(test)]
        let mut registration = ParentRegistration::begin(name)?;
        mkdirat(&root, name, Mode::from_bits_truncate(0o700))
            .context("create application VM cgroup parent")?;
        let mut identity = None;
        let configured: Result<File> = (|| {
            let directory = directory(&root, name)?;
            let metadata = fstat(&directory)?;
            identity = Some((metadata.st_dev, metadata.st_ino));
            #[cfg(test)]
            if let Some(registration) = &mut registration {
                registration.append(Some(&metadata))?;
            }
            ensure!(
                read(&directory, "cgroup.type")?.trim() == "domain",
                "application VM parent is not a domain cgroup"
            );
            empty(&directory)?;
            write(&directory, "memory.max", &budget.max_bytes.to_string())?;
            write(&directory, "memory.high", &budget.high_bytes.to_string())?;
            write(&directory, "memory.oom.group", "0")?;
            delegate(&directory)?;
            sample(&directory, "memory.events")?;
            Ok(directory)
        })();
        let directory = match configured {
            Ok(directory) => directory,
            Err(error) => {
                // No VM can have attached: this parent was created exclusively above.
                if let Err(cleanup) = unlinkat(&root, name, UnlinkatFlags::RemoveDir) {
                    return Err(error.context(format!(
                        "application VM cgroup parent cleanup unproved: name={name} identity={identity:?} cleanup={cleanup}"
                    )).context(crate::application_vm::ReapUnproved));
                }
                return Err(error);
            }
        };
        Ok(Arc::new(Self {
            root,
            directory,
            name: name.into(),
            budget,
            ownership: Mutex::new(Ownership::default()),
        }))
    }

    pub(crate) fn leaf(self: &Arc<Self>, id: &str, guest_mib: u32) -> Result<Leaf> {
        valid_name(id)?;
        let bytes = self.budget.leaf_bytes(guest_mib)?;
        let mut ownership = self.ownership.lock().expect("cgroup ownership poisoned");
        ensure!(!ownership.closed, "application VM cgroup parent is closed");
        ensure!(!ownership.claims.contains(id), "VM cgroup already claimed");
        mkdirat(&self.directory, id, Mode::from_bits_truncate(0o700))?;
        // Record the claim before any fallible configuration or attachment.
        ownership.claims.insert(id.into());
        let configured: Result<(File, u64)> = (|| {
            let directory = directory(&self.directory, id)?;
            empty(&directory)?;
            write(&directory, "memory.max", &bytes.to_string())?;
            write(&directory, "memory.swap.max", "0")?;
            write(&directory, "memory.oom.group", "1")?;
            sample(&directory, "memory.events.local")?;
            let full_limit = read(&directory, "memory.max")?.trim().parse()?;
            Ok((directory, full_limit))
        })();
        match configured {
            Ok((directory, full_limit)) => Ok(Leaf {
                parent: self.clone(),
                directory,
                id: id.into(),
                full_limit,
                spawned: false,
                removed: false,
            }),
            Err(error) => {
                if unlinkat(&self.directory, id, UnlinkatFlags::RemoveDir).is_ok() {
                    ownership.claims.remove(id);
                } else {
                    return Err(error.context(crate::application_vm::ReapUnproved));
                }
                Err(error)
            }
        }
    }

    pub(crate) fn sample(&self) -> Result<MemorySample> {
        sample(&self.directory, "memory.events")
    }

    pub(crate) fn shutdown(&self) -> Result<()> {
        let mut ownership = self.ownership.lock().expect("cgroup ownership poisoned");
        if ownership.closed {
            return Ok(());
        }
        ensure!(ownership.claims.is_empty(), "VM cgroup claims remain");
        empty(&self.directory)?;
        same_directory(&self.root, &self.name, &self.directory)?;
        unlinkat(&self.root, self.name.as_str(), UnlinkatFlags::RemoveDir)
            .context("remove empty application VM cgroup parent")?;
        ownership.closed = true;
        Ok(())
    }
}

pub(crate) struct Leaf {
    parent: Arc<StableParent>,
    directory: File,
    id: String,
    full_limit: u64,
    spawned: bool,
    removed: bool,
}

impl Leaf {
    /// Call immediately after spawn succeeds, before any other fallible step.
    pub(crate) fn mark_spawned(&mut self, child: &Child) -> Result<()> {
        ensure!(
            !self.spawned && !self.removed && child.id().is_some(),
            "invalid VM cgroup spawn ownership"
        );
        self.spawned = true;
        Ok(())
    }

    pub(crate) fn sample(&self) -> Result<MemorySample> {
        ensure!(!self.removed, "VM cgroup removed");
        sample(&self.directory, "memory.events.local")
    }

    /// A descriptor-owned writer; the Jail retains and joins its blocking task.
    pub(crate) fn memory_limit(&self) -> Result<MemoryLimit> {
        ensure!(self.spawned && !self.removed, "VM cgroup not live");
        Ok(MemoryLimit {
            directory: self.directory.try_clone()?,
            full_limit: self.full_limit,
        })
    }

    /// The caller must retain its reservation if this fails.
    pub(crate) fn reap(&mut self, child: &mut Child) -> Result<MemorySample> {
        ensure!(self.spawned && !self.removed, "invalid VM cgroup reap");
        ensure!(child.try_wait()?.is_some(), "VM child exit is unproved");
        empty(&self.directory)?;
        let final_sample = self.sample()?;
        self.remove()?;
        Ok(final_sample)
    }

    /// Only valid when Command::spawn failed; never use after successful spawn.
    pub(crate) fn discard_unstarted(&mut self) -> Result<()> {
        ensure!(!self.spawned && !self.removed, "VM cgroup already started");
        empty(&self.directory)?;
        self.remove()
    }

    fn remove(&mut self) -> Result<()> {
        let mut ownership = self
            .parent
            .ownership
            .lock()
            .expect("cgroup ownership poisoned");
        same_directory(&self.parent.directory, &self.id, &self.directory)?;
        unlinkat(
            &self.parent.directory,
            self.id.as_str(),
            UnlinkatFlags::RemoveDir,
        )
        .context("remove reaped VM cgroup")?;
        ownership.claims.remove(&self.id);
        self.removed = true;
        Ok(())
    }
}

/// Refers to the same inode throughout lowering, promotion and cleanup.
pub(crate) struct MemoryLimit {
    directory: File,
    full_limit: u64,
}

impl MemoryLimit {
    pub(crate) fn retained_target(&self, nominal_bytes: u64, margin: u64) -> Result<Option<u64>> {
        let sample = sample(&self.directory, "memory.events.local")?;
        let target = page_aligned(
            sample
                .current_bytes
                .checked_add(margin)
                .context("VM retained ceiling overflow")?,
        )?;
        // Overhead remains an extra reservation, so a real saving requires this.
        Ok((target < nominal_bytes && target < self.full_limit).then_some(target))
    }

    pub(crate) fn set(self, retained: Option<u64>) -> Result<MemorySample> {
        let target = retained.unwrap_or(self.full_limit);
        ensure!(
            target > 0 && target <= self.full_limit,
            "invalid owned VM memory limit"
        );
        let before = sample(&self.directory, "memory.events.local")?;
        ensure!(
            before.oom == 0 && before.oom_kill == 0 && before.oom_group_kill == 0,
            "VM memory limit owner already encountered OOM"
        );
        if retained.is_some() {
            ensure!(
                before.current_bytes <= target,
                "VM retained target became stale"
            );
        }
        write(&self.directory, "memory.max", &target.to_string())?;
        let actual: u64 = read(&self.directory, "memory.max")?.trim().parse()?;
        ensure!(
            actual == page_aligned(target)?,
            "VM memory limit readback mismatch"
        );
        let after = sample(&self.directory, "memory.events.local")?;
        ensure!(
            after.current_bytes <= actual,
            "VM retained ceiling unsettled"
        );
        ensure!(
            (after.oom, after.oom_kill, after.oom_group_kill)
                == (before.oom, before.oom_kill, before.oom_group_kill),
            "VM memory limit caused OOM"
        );
        Ok(after)
    }
}

fn page_aligned(bytes: u64) -> Result<u64> {
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let page = u64::try_from(page).context("host page size absent")?;
    ensure!(page > 0, "invalid host page size");
    bytes
        .checked_add(page - 1)
        .and_then(|bytes| bytes.checked_div(page))
        .and_then(|pages| pages.checked_mul(page))
        .context("VM limit alignment overflow")
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct MemorySample {
    pub current_bytes: u64,
    pub peak_bytes: u64,
    pub high: u64,
    pub max: u64,
    pub oom: u64,
    pub oom_kill: u64,
    pub oom_group_kill: u64,
    pub some_total_us: u64,
    pub full_total_us: u64,
}

fn sample(dir: &File, events_file: &str) -> Result<MemorySample> {
    let events = read(dir, events_file)?;
    let pressure = read(dir, "memory.pressure")?;
    Ok(MemorySample {
        current_bytes: read(dir, "memory.current")?.trim().parse()?,
        peak_bytes: read(dir, "memory.peak")?.trim().parse()?,
        high: value(&events, "high")?,
        max: value(&events, "max")?,
        oom: value(&events, "oom")?,
        oom_kill: value(&events, "oom_kill")?,
        oom_group_kill: value(&events, "oom_group_kill")?,
        some_total_us: pressure_total(&pressure, "some")?,
        full_total_us: pressure_total(&pressure, "full")?,
    })
}

fn valid_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty()
            && name.len() <= 255
            && name != "."
            && name != ".."
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte)),
        "invalid cgroup component"
    );
    Ok(())
}

fn directory(parent: &File, name: &str) -> Result<File> {
    Ok(File::from(openat(parent, name, DIR_FLAGS, Mode::empty())?))
}

fn same_directory(parent: &File, name: &str, directory: &File) -> Result<()> {
    let owned = fstat(directory)?;
    let named = fstatat(parent, name, AtFlags::AT_SYMLINK_NOFOLLOW)?;
    ensure!(
        owned.st_dev == named.st_dev && owned.st_ino == named.st_ino,
        "cgroup directory ownership changed"
    );
    Ok(())
}

fn read(dir: &File, name: &str) -> Result<String> {
    let file = File::from(openat(
        dir,
        name,
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )?);
    let mut bytes = Vec::new();
    file.take(LIMIT + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= LIMIT,
        "cgroup scalar file exceeds bound"
    );
    Ok(String::from_utf8(bytes)?)
}

fn write(dir: &File, name: &str, value: &str) -> Result<()> {
    let mut file = File::from(openat(
        dir,
        name,
        OFlag::O_WRONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )?);
    file.write_all(value.as_bytes())
        .with_context(|| format!("configure cgroup {name}"))
}

fn delegate(dir: &File) -> Result<()> {
    let available = read(dir, "cgroup.controllers")?;
    ensure!(
        ["cpu", "memory"]
            .iter()
            .all(|controller| available.split_whitespace().any(|word| word == *controller)),
        "cpu and memory cgroup controllers are unavailable"
    );
    write(dir, "cgroup.subtree_control", "+cpu +memory")?;
    let enabled = read(dir, "cgroup.subtree_control")?;
    ensure!(
        ["cpu", "memory"]
            .iter()
            .all(|controller| enabled.split_whitespace().any(|word| word == *controller)),
        "cpu and memory cgroup controllers are not delegated"
    );
    Ok(())
}

fn empty(dir: &File) -> Result<()> {
    ensure!(
        read(dir, "cgroup.procs")?.trim().is_empty(),
        "cgroup has processes"
    );
    ensure!(
        value(&read(dir, "cgroup.events")?, "populated")? == 0,
        "cgroup remains populated"
    );
    Ok(())
}

fn value(text: &str, key: &str) -> Result<u64> {
    let mut result = None;
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        if fields.next() == Some(key) {
            ensure!(result.is_none(), "duplicate cgroup scalar");
            result = Some(fields.next().context("missing cgroup scalar")?.parse()?);
            ensure!(fields.next().is_none(), "invalid cgroup scalar");
        }
    }
    result.context("missing cgroup counter")
}

fn pressure_total(text: &str, key: &str) -> Result<u64> {
    let mut result = None;
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        if fields.next() == Some(key) {
            ensure!(result.is_none(), "duplicate cgroup pressure line");
            let mut total = None;
            for field in fields {
                if let Some(number) = field.strip_prefix("total=") {
                    ensure!(total.is_none(), "duplicate cgroup pressure total");
                    total = Some(number.parse()?);
                }
            }
            result = Some(total.context("missing cgroup pressure total")?);
        }
    }
    result.context("missing cgroup pressure line")
}

#[cfg(test)]
struct ParentRegistration {
    file: File,
    record: serde_json::Value,
}

#[cfg(test)]
impl ParentRegistration {
    fn begin(name: &str) -> Result<Option<Self>> {
        use nix::sys::stat::SFlag;
        use std::path::{Component, Path};
        let artifacts = std::env::var_os("CELLD_APPLICATION_NODE_ARTIFACTS");
        let run_id = std::env::var_os("CELLD_APPLICATION_RUN_ID");
        let (artifacts, run_id) = match (artifacts, run_id) {
            (None, None) => return Ok(None),
            (Some(artifacts), Some(run_id)) => (artifacts, run_id),
            _ => anyhow::bail!("cgroup registry requires artifact directory and run ID"),
        };
        let run_id = run_id
            .to_str()
            .context("cgroup registry run ID is not UTF-8")?;
        valid_name(run_id)?;
        ensure!(name.starts_with("celld-vms-"), "foreign cgroup parent name");
        let mut directory = File::from(open("/", DIR_FLAGS, Mode::empty())?);
        let mut components = Path::new(&artifacts).components();
        ensure!(
            matches!(components.next(), Some(Component::RootDir)),
            "cgroup registry artifact directory must be absolute"
        );
        for component in components {
            let Component::Normal(component) = component else {
                anyhow::bail!("invalid cgroup registry artifact directory");
            };
            directory = File::from(openat(&directory, component, DIR_FLAGS, Mode::empty())?);
        }
        let metadata = fstat(&directory)?;
        ensure!(
            metadata.st_uid == 0 && metadata.st_mode & 0o022 == 0,
            "cgroup registry artifact directory must be owned and writable only by root"
        );
        let file = File::from(openat(
            &directory,
            "cgroups.jsonl",
            OFlag::O_WRONLY
                | OFlag::O_APPEND
                | OFlag::O_CREAT
                | OFlag::O_NOFOLLOW
                | OFlag::O_CLOEXEC,
            Mode::from_bits_truncate(0o600),
        )?);
        let metadata = fstat(&file)?;
        ensure!(
            SFlag::from_bits_truncate(metadata.st_mode) == SFlag::S_IFREG
                && metadata.st_uid == 0
                && metadata.st_nlink == 1
                && metadata.st_mode & 0o022 == 0,
            "cgroup registry must be a root-owned regular file without links"
        );
        let pid = std::process::id();
        let proc = File::from(open("/proc", DIR_FLAGS, Mode::empty())?);
        let owner = File::from(openat(
            &proc,
            pid.to_string().as_str(),
            DIR_FLAGS,
            Mode::empty(),
        )?);
        let stat = read(&owner, "stat")?;
        let start_ticks: u64 = stat
            .rsplit_once(") ")
            .context("invalid owner process stat")?
            .1
            .split_whitespace()
            .nth(19)
            .context("missing owner process start ticks")?
            .parse()?;
        let mut registration = Self {
            file,
            record: serde_json::json!({
                "runId": run_id, "name": name, "ownerPid": pid,
                "ownerStartTicks": start_ticks, "created": false,
            }),
        };
        registration.append(None)?;
        Ok(Some(registration))
    }

    fn append(&mut self, identity: Option<&nix::sys::stat::FileStat>) -> Result<()> {
        if let Some(identity) = identity {
            self.record["created"] = serde_json::json!(true);
            self.record["dev"] = serde_json::json!(identity.st_dev);
            self.record["ino"] = serde_json::json!(identity.st_ino);
        }
        let mut bytes = serde_json::to_vec(&self.record)?;
        bytes.push(b'\n');
        ensure!(
            bytes.len() as u64 <= LIMIT,
            "cgroup registry record exceeds bound"
        );
        self.file.lock()?;
        let written = self.file.write_all(&bytes);
        let unlocked = self.file.unlock();
        written?;
        unlocked?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "../unit_tests/application_template/cgroup/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../unit_tests/application_template/cgroup/retained_limit_tests.rs"]
mod retained_limit_tests;
