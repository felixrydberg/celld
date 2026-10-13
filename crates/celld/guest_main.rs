//! Credential-free guest entrypoint; it never starts the trusted host shell.

#[cfg(target_os = "linux")]
// Guest PID 1 reads its own boot state before an execution runtime exists.
#[allow(clippy::disallowed_methods)]
fn main() -> anyhow::Result<()> {
    // Original console calls use tracing. Keep guest diagnostics on stderr,
    // separate from the framed invocation/storage sockets.
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .init();
    let pid_one = std::process::id() == 1;
    let boot = if pid_one && std::env::args_os().len() == 1 {
        initialize_filesystems()?;
        Some(boot_mode(&std::fs::read_to_string("/proc/cmdline")?)?)
    } else {
        None
    };
    let result = run(boot);
    if pid_one && result.is_ok() {
        if boot == Some(BootMode::Check) {
            eprintln!("CELLD_APPLICATION_JAVASCRIPT_VM_PASS");
        }
        // PID 1 must stay alive until the host reaps the VM, including after a check.
        loop {
            std::thread::park();
        }
    }
    result
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BootMode {
    Template,
    Check,
}

#[cfg(target_os = "linux")]
fn boot_mode(cmdline: &str) -> anyhow::Result<BootMode> {
    let mut mode = None;
    for argument in cmdline.split_ascii_whitespace() {
        let selected = if argument.starts_with("celld.template=") {
            anyhow::ensure!(
                argument == "celld.template=1",
                "unknown Celld template mode"
            );
            Some(BootMode::Template)
        } else if argument.starts_with("celld.check=") {
            anyhow::ensure!(
                argument == "celld.check=javascript",
                "unknown Celld check mode"
            );
            Some(BootMode::Check)
        } else {
            None
        };
        if let Some(selected) = selected {
            anyhow::ensure!(
                mode.replace(selected).is_none(),
                "multiple Celld boot modes"
            );
        }
    }
    mode.ok_or_else(|| anyhow::anyhow!("missing Celld boot mode"))
}

#[cfg(target_os = "linux")]
fn initialize_filesystems() -> anyhow::Result<()> {
    for (filesystem, target, data) in [
        (c"proc", c"/proc", None),
        (c"sysfs", c"/sys", None),
        (c"tmpfs", c"/run", Some(c"size=16m")),
        (c"tmpfs", c"/tmp", Some(c"size=16m")),
    ] {
        // SAFETY: static NUL-terminated strings and valid mount flags/data.
        anyhow::ensure!(
            unsafe {
                libc::mount(
                    filesystem.as_ptr(),
                    target.as_ptr(),
                    filesystem.as_ptr(),
                    libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
                    data.map_or(std::ptr::null(), |data| data.as_ptr().cast()),
                )
            } == 0,
            "mount {}: {}",
            target.to_string_lossy(),
            std::io::Error::last_os_error()
        );
    }
    // SAFETY: zeroed statvfs is valid and the root path is NUL-terminated.
    let read_only = unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        libc::statvfs(c"/".as_ptr(), &mut stat) == 0 && stat.f_flag & libc::ST_RDONLY != 0
    };
    anyhow::ensure!(read_only, "guest runtime root was not mounted read-only");
    Ok(())
}

#[cfg(target_os = "linux")]
fn run(boot: Option<BootMode>) -> anyhow::Result<()> {
    let mut arguments = std::env::args().skip(1);
    let path = match boot {
        Some(BootMode::Template) => "--template".to_owned(),
        Some(BootMode::Check) => "/opt/celld/javascript-check.json".to_owned(),
        None => arguments.next().ok_or_else(|| {
            anyhow::anyhow!("usage: celld-guest CONFIG.json | --application BOOTSTRAP ARTIFACT_ROOT [SHARED_ROOT] | --template")
        })?,
    };
    if path == "--template" {
        // The unassigned template VM: no tokio runtime and no V8, so a snapshot
        // holds nothing random. A claim execs the application mode.
        anyhow::ensure!(arguments.next().is_none(), "unexpected guest argument");
        return celld::application_template_agent::run();
    }
    let application = if path == "--application" {
        let bootstrap = arguments
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing bootstrap disk"))?;
        let root = arguments
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing application root"))?;
        Some((bootstrap, root, arguments.next()))
    } else {
        None
    };
    anyhow::ensure!(arguments.next().is_none(), "unexpected guest argument");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    // As in the host: one process Domain, so services (HTTP streams) never
    // split between a per-runtime instance and one installed lazily later.
    celld::asyncrt::set_host_handle(runtime.handle().clone());
    if let Some((bootstrap, root, shared)) = application {
        runtime.block_on(celld::application_javascript_guest::run_application(
            std::path::Path::new(&bootstrap),
            std::path::Path::new(&root),
            shared.as_deref().map(std::path::Path::new),
        ))
    } else {
        runtime.block_on(celld::application_javascript_guest::run(
            std::path::Path::new(&path),
        ))
    }
}

#[cfg(all(test, target_os = "linux"))]
#[test]
fn boot_mode_requires_one_exact_supported_token() {
    assert_eq!(
        boot_mode("ro console=ttyS0 celld.template=1").unwrap(),
        BootMode::Template
    );
    assert_eq!(
        boot_mode("celld.check=javascript quiet").unwrap(),
        BootMode::Check
    );
    for refused in [
        "ro quiet",
        "prefixcelld.template=1",
        "celld.template=0",
        "celld.check=python",
        "celld.template=1 celld.template=1",
        "celld.check=javascript celld.template=1",
    ] {
        assert!(boot_mode(refused).is_err(), "accepted {refused}");
    }
}

#[cfg(not(target_os = "linux"))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("celld-guest requires Linux/vsock")
}
