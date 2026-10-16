//! Actual unproved leaf reap: one owned extra child keeps a dead VMM's leaf populated.
use super::cache_failure_baseline as failure;
use super::guard_baseline::Group;
use super::memory_baseline::{activate_bundle, bridge_check, config, name, stage, Recorder};
use super::*;
use nix::fcntl::{openat, OFlag};
use nix::sys::stat::{fstat, Mode};
use serde_json::{json, Value};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::os::unix::process::CommandExt;

const SOURCE: &str = include_str!("../tests/fixtures/t33-memory/freeze.mjs");
const WRAPPER: &str =
    "\nexport default {fetch(request, env, ctx) {return original.fetch(request, env, ctx);}};\n";

fn read_at(group: &Group, name: &str) -> Result<String> {
    let file = File::from(openat(
        &group.directory,
        name,
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )?);
    let mut value = String::new();
    file.take(4097).read_to_string(&mut value)?;
    ensure!(value.len() <= 4096, "oversized fixture cgroup value");
    Ok(value)
}
fn process(pid: u32) -> Result<(u32, u64)> {
    let mut stat = String::new();
    File::open(format!("/proc/{pid}/stat"))?
        .take(4097)
        .read_to_string(&mut stat)?;
    ensure!(stat.len() <= 4096, "oversized fixture process stat");
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .context("invalid fixture process stat")?
        .1
        .split_whitespace()
        .collect();
    ensure!(fields.len() > 19, "short fixture process stat");
    Ok((fields[1].parse()?, fields[19].parse()?))
}
fn directory(group: &Group) -> Value {
    assert!(group.exists().unwrap());
    let stat = fstat(&group.directory).unwrap();
    let path =
        std::fs::read_link(format!("/proc/self/fd/{}", group.directory.as_raw_fd())).unwrap();
    assert!(path.starts_with("/sys/fs/cgroup"));
    assert_eq!(stat.st_uid, 0);
    json!({"path":path,"device":stat.st_dev,"inode":stat.st_ino})
}

struct Helper {
    child: Option<std::process::Child>,
    identity: Option<(u32, u64)>,
}
impl Helper {
    fn spawn(group: &Group, owned: fn() -> HashSet<u32>) -> Result<Self> {
        group.verify_member(owned)?;
        let vmm = group.member.context("missing original VMM")?.0;
        let membership = std::fs::read_to_string(format!("/proc/{vmm}/cgroup"))?;
        let control = File::from(openat(
            &group.directory,
            "cgroup.procs",
            OFlag::O_WRONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
            Mode::empty(),
        )?);
        let executable = Path::new("/usr/bin/sleep");
        trusted_executable(executable)?;
        let mut command = std::process::Command::new(executable);
        command
            .arg("300")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        // The inherited original leaf descriptor moves only this child before exec.
        unsafe {
            command.pre_exec(move || {
                let written = libc::write(control.as_raw_fd(), b"0".as_ptr().cast(), 1);
                if written == 1 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
        let mut helper = Self {
            child: Some(command.spawn()?),
            identity: None,
        };
        let pid = helper.child.as_ref().unwrap().id();
        let (parent, ticks) = process(pid)?;
        ensure!(
            parent == std::process::id(),
            "helper is not an original direct child"
        );
        helper.identity = Some((pid, ticks));
        group.verify_member(owned)?;
        ensure!(
            std::fs::read_to_string(format!("/proc/{pid}/cgroup"))? == membership,
            "helper did not enter the original VM leaf"
        );
        let artifacts = PathBuf::from(
            std::env::var_os("CELLD_APPLICATION_NODE_ARTIFACTS")
                .context("helper process registry required")?,
        );
        let mut registry = OpenOptions::new()
            .append(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(artifacts.join("processes.jsonl"))?;
        ensure!(
            registry.metadata()?.is_file() && registry.metadata()?.uid() == 0,
            "helper registry is not an owned regular file"
        );
        registry.lock()?;
        writeln!(
            registry,
            "{}",
            json!({"pid":pid,"start_ticks":ticks,"parent":parent,
            "kind":"quarantine-extra-child"})
        )?;
        registry.sync_all()?;
        registry.unlock()?;
        Ok(helper)
    }
    fn finish(&mut self) -> Result<()> {
        let child = self.child.as_mut().context("helper already reaped")?;
        if child.try_wait()?.is_none() {
            if let Some((pid, ticks)) = self.identity {
                ensure!(
                    process(pid)? == (std::process::id(), ticks),
                    "original helper identity changed"
                );
            }
            child.kill()?;
        }
        child.wait()?;
        self.child = None;
        Ok(())
    }
}
impl Drop for Helper {
    fn drop(&mut self) {
        if self.child.is_some() {
            let _ = self.finish();
        }
    }
}

fn claim(pool: &VmPool, previous: VmPoolSnapshot) -> VmPoolSnapshot {
    let now = pool.snapshot();
    assert_eq!(
        (
            now.vms,
            now.memory_mib,
            now.cached_vms,
            now.cached_nominal_mib,
            now.retained_limit_bytes
        ),
        (
            previous.vms,
            previous.memory_mib,
            previous.cached_vms,
            previous.cached_nominal_mib,
            previous.retained_limit_bytes
        ),
        "unproved reap released the last safe reservation"
    );
    assert_eq!((now.starting, now.ready, now.quarantined), (0, 0, 1));
    assert_eq!(
        now.physical_bytes(failure::guard(), now.memory_mib, now.vms),
        previous.physical_bytes(failure::guard(), previous.memory_mib, previous.vms)
    );
    now
}
async fn pressure_clear(pool: &VmPool) {
    timeout(Duration::from_secs(8), async {
        loop {
            VmPool::refresh_pressure(&pool.pressure, &pool.templates);
            if !VmPool::under_pressure(&pool.pressure) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("quarantine fixture pressure did not recover");
}
async fn refused(
    pool: &VmPool,
    app: &crate::actor::AppHandle,
    application: &ApplicationId,
    previous: VmPoolSnapshot,
    owned: fn() -> HashSet<u32>,
) -> Value {
    pressure_clear(pool).await;
    let admission_began = Instant::now();
    let outcome = timeout(
        Duration::from_secs(3),
        pool.acquire(app, application, &name("failure"), &name("memory")),
    )
    .await
    .expect("quarantined admission did not refuse within three seconds");
    assert!(
        matches!(outcome, Err(ref error) if error.is::<CapacityExhausted>()),
        "quarantined capacity admitted a replacement or failed for another reason"
    );
    let admission_ms = admission_began.elapsed().as_millis();
    claim(pool, previous);
    assert!(owned().is_empty(), "refused admission created a VMM");
    let actor_began = Instant::now();
    timeout(Duration::from_secs(3), app.snapshot())
        .await
        .expect("host Actor became unresponsive");
    json!({"admissionRefused":true,"capacityExhausted":true,"admissionMs":admission_ms,
        "actorResponsive":true,"actorMs":actor_began.elapsed().as_millis()})
}

pub(super) async fn run(
    app: &crate::actor::AppHandle,
    store: &Arc<ApplicationStore>,
    options: &LaunchOptions,
    _directory: &Path,
    owned: fn() -> HashSet<u32>,
) {
    assert!(owned().is_empty());
    let output = PathBuf::from(
        std::env::var_os("CELLD_APPLICATION_MEMORY_OUTPUT")
            .expect("fresh quarantine JSONL required"),
    );
    let recorder = Recorder::new(&output, "failure-quarantine-bootstrap");
    let mut configuration = config(options, 0, 30);
    configuration.max_vms = 1;
    configuration.memory_guard = Some(failure::guard());
    let pool = Arc::new(VmPool::new(configuration, store.clone()).unwrap());
    let parent = Group::registered_parent().unwrap();
    timeout(Duration::from_secs(1500), pool.prepare(app))
        .await
        .unwrap()
        .unwrap();
    pressure_clear(&pool).await;
    let application = stage(
        store,
        options,
        "memory-quarantine",
        ApplicationSizeClass::Basic,
    )
    .await;
    assert_eq!(SOURCE.matches("export default {").count(), 1);
    let source = SOURCE.replacen("export default {", "const original = {", 1) + WRAPPER;
    let bundle =
        crate::application_image::build(&[("main.mjs".into(), source.into_bytes())]).unwrap();
    activate_bundle(
        store,
        options,
        &application,
        "failure",
        bundle,
        (
            "main.mjs",
            json!({"compatibility_date":"2026-03-01"}),
            json!({}),
        ),
    )
    .await;
    let (lease, group, original) = failure::acquire(&pool, app, &application, &parent, owned).await;
    let client = lease.client.clone();
    drop(lease);
    let (_, limit) = failure::cached(&pool, &client, &group, owned, &recorder, "Quarantine").await;
    let leaf_identity = directory(&group);
    let parent_identity = directory(&parent);
    let before = pool.snapshot();
    assert_eq!(
        (
            before.vms,
            before.memory_mib,
            before.cached_vms,
            before.cached_nominal_mib
        ),
        (1, 1024, 1, 1024)
    );
    assert_eq!(before.retained_limit_bytes, limit);
    let mut helper = Helper::spawn(&group, owned).unwrap();
    let (helper_pid, helper_ticks) = helper.identity.unwrap();
    let vmm = group.member.unwrap();
    let mut members: Vec<u32> = read_at(&group, "cgroup.procs")
        .unwrap()
        .split_whitespace()
        .map(|pid| pid.parse().unwrap())
        .collect();
    members.sort_unstable();
    let mut expected = vec![vmm.0, helper_pid];
    expected.sort_unstable();
    assert_eq!(members, expected, "unexpected occupant in original leaf");
    recorder.emit(json!({"kind":"quarantineInputs","guard":failure::guard(),"kernelSha256":options.kernel.sha256,
        "runtimeSha256":options.runtime.sha256,"sourceSha256":content_digest(SOURCE.as_bytes()),
        "firecrackerSha256":content_digest(&std::fs::read(&options.firecracker).unwrap()),
        "helperExecutable":"/usr/bin/sleep","helperExecutableSha256":content_digest(&std::fs::read("/usr/bin/sleep").unwrap()),
        "wrapperSha256":content_digest(WRAPPER.as_bytes()),"originalIdentity":original["identity"],
        "vmmPid":vmm.0,"vmmStartTicks":vmm.1,"helperPid":helper_pid,"helperStartTicks":helper_ticks,
        "leaf":leaf_identity,"parent":parent_identity,"lastSafeRetainedLimitBytes":limit,
        "scope":"actual original VMM reap with one real owned extra child preventing empty-leaf proof",
        "stressOnlyGuardAndMargin":true,"startupFreeze":false}));
    failure::signal(&group, owned, libc::SIGKILL);
    timeout(Duration::from_secs(25), async {
        loop {
            if pool.snapshot().quarantined == 1
                && owned().is_empty()
                && !Path::new(&format!("/proc/{}", vmm.0)).exists()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("original VMM was not physically reaped and quarantined");
    let charged = claim(&pool, before);
    assert!(group.exists().unwrap() && parent.exists().unwrap());
    assert_eq!(
        read_at(&group, "cgroup.procs").unwrap().trim(),
        helper_pid.to_string()
    );
    assert!(read_at(&group, "cgroup.events")
        .unwrap()
        .lines()
        .any(|line| line == "populated 1"));
    assert_eq!(group.scalar("memory.max").unwrap(), limit);
    assert_eq!(
        process(helper_pid).unwrap(),
        (std::process::id(), helper_ticks)
    );
    recorder.emit(json!({"kind":"unprovedReapQuarantine","vmmPid":vmm.0,"originalVmmPhysicallyReaped":true,
        "helperPid":helper_pid,"helperStartTicks":helper_ticks,"leaf":leaf_identity,"parent":parent_identity,
        "chargedVms":charged.vms,"nominalMib":charged.memory_mib,"quarantined":charged.quarantined,
        "retainedVms":charged.cached_vms,"retainedLimitBytes":charged.retained_limit_bytes,
        "physicalReservationBytes":charged.physical_bytes(failure::guard(),charged.memory_mib,charged.vms)}));
    let refusal = refused(&pool, app, &application, before, owned).await;
    recorder
        .emit(json!({"kind":"quarantineAdmissionRefused","helperReaped":false,"proof":refusal}));
    assert!(
        timeout(Duration::from_secs(7), failure::fetch(app, &client))
            .await
            .unwrap()
            .is_err(),
        "quarantined original client executed another request"
    );
    helper.finish().unwrap();
    assert!(
        !Path::new(&format!("/proc/{helper_pid}")).exists(),
        "original helper was not reaped"
    );
    assert!(group.exists().unwrap() && parent.exists().unwrap());
    assert!(read_at(&group, "cgroup.procs").unwrap().trim().is_empty());
    assert!(read_at(&group, "cgroup.events")
        .unwrap()
        .lines()
        .any(|line| line == "populated 0"));
    claim(&pool, before);
    let refusal = refused(&pool, app, &application, before, owned).await;
    recorder.emit(json!({"kind":"quarantineAdmissionRefused","helperReaped":true,"proof":refusal}));
    assert!(
        timeout(Duration::from_secs(5), pool.shutdown())
            .await
            .unwrap()
            .is_err(),
        "logical quarantine was silently forgiven after helper exit"
    );
    claim(&pool, before);
    assert!(owned().is_empty());
    assert!(group.exists().unwrap() && parent.exists().unwrap());
    let owner = std::process::id();
    let proof = json!({"schema":"t33-quarantine-cleanup-v1",
        "runId":std::env::var("CELLD_APPLICATION_RUN_ID").unwrap(),"ownerPid":owner,
        "ownerStartTicks":process(owner).unwrap().1,"parent":parent_identity,"leaf":leaf_identity});
    let mut proof_bytes = serde_json::to_vec(&proof).unwrap();
    proof_bytes.push(b'\n');
    assert!(
        proof_bytes.len() <= 4096,
        "oversized original cgroup cleanup proof"
    );
    let artifacts = PathBuf::from(
        std::env::var_os("CELLD_APPLICATION_NODE_ARTIFACTS")
            .expect("external cleanup artifact directory required"),
    );
    let mut proof_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(artifacts.join("cleanup-proof.json"))
        .unwrap();
    proof_file.write_all(&proof_bytes).unwrap();
    proof_file.sync_all().unwrap();
    recorder.emit(
        json!({"kind":"cleanup","ownedPids":owned(),"helperPid":helper_pid,
        "helperReaped":true,"originalVmmReaped":true,"quarantineRetained":true,
        "emptyOwnedLeaf":leaf_identity,"registeredOwnedParent":parent_identity,
        "externalReporterRemovalRequired":true,"externalProofFile":"cleanup-proof.json",
        "logicalQuarantineRecoveryQualified":false}),
    );
    eprintln!(
        "T29_RESULT {}",
        json!({"scenario":"t33::quarantine","status":"passed",
        "duration_ms":recorder.began.elapsed().as_millis(),"output":output,
        "scope":"real extra owned child prevents original VMM leaf reap; conservative quarantine, refusal and Actor response; final empty leaf/parent removal requires external reporter"})
    );
}

#[tokio::test]
#[ignore = "requires Linux KVM, diagnostic runtime, explicit quarantine selector and external owned-group cleanup"]
async fn t33_quarantine() {
    assert!(std::env::var_os("CELLD_APPLICATION_MEMORY_QUARANTINE_CHECK_ONLY").is_some());
    bridge_check("quarantine").await;
}
