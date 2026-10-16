// This agent runs inside the application VM, not in the node's execution domain:
// it has no Tokio runtime and uses the guest's own clock and filesystem.
#![allow(clippy::disallowed_methods)]

//! Guest side of a snapshot start (T26). A template VM boots `celld-guest
//! --template` instead of the application guest: this agent holds no V8 and no
//! random state. It dials the host, and after a snapshot restore (which resets
//! the connection) dials again. A prewarm reseeds the kernel, sets the clock and
//! `exec`s `celld-guest --prewarm`, so V8 and Rust draw fresh randomness in a
//! fresh process of this VM alone. That process starts the generic runtime
//! before any application is known, then dials again and waits for its claim.
//! A claim reseeds once more, sets the clock and mounts the handler's read-only
//! image (and the shared Pyodide runtime, for a Python handler) that the host
//! pointed the template's placeholder drives at. The message formats live
//! here, shared with the host.

use anyhow::{ensure, Context, Result};
use celld_applications::bootstrap::DISK_BYTES;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::time::{Duration, Instant};

/// The host's agent listener; the application guest uses 5001 and 5002.
pub(crate) const AGENT_PORT: u32 = 5003;
pub(crate) const HELLO: u8 = b'R';
const CLAIM: u8 = b'C';
const PREWARM: u8 = b'W';
const BOOTSTRAP: &str = "/run/bootstrap.bin";
/// The template's placeholder drives, in the order the host attaches them.
const HANDLER_DEVICE: &str = "/dev/vdb";
const SHARED_DEVICE: &str = "/dev/vdc";
const ROOT: &str = "/application";
const SHARED_ROOT: &str = "/pyodide";
// linux/random.h: _IOW('R', 0x03, int[2]) and _IO('R', 0x07); not in libc.
const RNDADDENTROPY: libc::c_ulong = 0x4008_5203;
const RNDRESEEDCRNG: libc::c_ulong = 0x5207;
// linux/fs.h: _IOR(0x12, 114, size_t).
const BLKGETSIZE64: libc::c_ulong = 0x8008_1272;

/// A claim: `CLAIM`, 32 bytes of host entropy, the wall clock (u64 seconds, u32
/// nanoseconds), the bootstrap (u32 length), then the byte size of the handler
/// image and of the shared runtime image (u64 each, 0 for none) that the host
/// has already pointed the placeholder drives at. The guest answers one byte: 0
/// once it is assigned, else 1 and the reason.
pub(crate) fn claim(bootstrap: &[u8], image_bytes: u64, shared_bytes: u64) -> Result<Vec<u8>> {
    let mut message = fresh(CLAIM)?;
    message.extend((u32::try_from(bootstrap.len())?).to_le_bytes());
    message.extend(bootstrap);
    message.extend(image_bytes.to_le_bytes());
    message.extend(shared_bytes.to_le_bytes());
    Ok(message)
}

/// A prewarm: `PREWARM`, 32 bytes of host entropy and the wall clock, as a
/// claim begins. The guest answers by dialing again from its prewarmed process.
pub(crate) fn prewarm() -> Result<Vec<u8>> {
    fresh(PREWARM)
}

fn fresh(tag: u8) -> Result<Vec<u8>> {
    let mut entropy = [0; 32];
    getrandom::fill(&mut entropy).map_err(|error| anyhow::anyhow!("host entropy: {error}"))?;
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
    let mut message = vec![tag];
    message.extend(entropy);
    message.extend(now.as_secs().to_le_bytes());
    message.extend(now.subsec_nanos().to_le_bytes());
    Ok(message)
}

/// Runs until a prewarm `exec`s the prewarmed guest, so it returns only on error.
pub fn run() -> Result<()> {
    loop {
        let mut stream = dial();
        // Idle until told to prewarm: a reset or a close (a restore resets the
        // connection) means dial again.
        if wait(&mut stream) != Some(PREWARM) {
            continue;
        }
        reseed_and_set_clock(&mut stream)?;
        // CLOEXEC closes the agent socket; the prewarmed guest dials its own.
        let mut guest = std::process::Command::new(std::env::current_exe()?);
        return Err(guest.arg("--prewarm").exec()).context("exec prewarmed guest");
    }
}

/// The prewarmed guest, once its generic runtime is up: waits for the claim and
/// returns the bootstrap, the handler root and, for a Python handler, the
/// shared runtime root, all in place. A refused claim tells the host why.
pub fn claimed() -> Result<(&'static str, &'static str, Option<&'static str>)> {
    let mut stream = dial();
    ensure!(
        wait(&mut stream) == Some(CLAIM),
        "prewarmed guest lost its host"
    );
    match assign(&mut stream) {
        Ok(shared) => {
            stream.write_all(&[0])?;
            Ok((BOOTSTRAP, ROOT, shared.then_some(SHARED_ROOT)))
        }
        Err(error) => {
            let _ = stream.write_all(&[1]);
            let _ = stream.write_all(format!("{error:#}").as_bytes());
            Err(error)
        }
    }
}

/// A connection to the host that has said hello.
fn dial() -> std::fs::File {
    loop {
        if let Ok(mut stream) = crate::application_javascript_guest::connect(AGENT_PORT) {
            if stream.write_all(&[HELLO]).is_ok() {
                return stream;
            }
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// The host's next message tag; `None` once the connection is gone. A socket
/// timeout is not an error.
fn wait(stream: &mut impl Read) -> Option<u8> {
    let mut tag = [0];
    loop {
        match stream.read(&mut tag) {
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) => {}
            Ok(1) => return Some(tag[0]),
            _ => return None,
        }
    }
}

/// The head of both messages: credited host entropy, then the wall clock.
fn reseed_and_set_clock(stream: &mut impl Read) -> Result<()> {
    let mut entropy = [0; 32];
    stream.read_exact(&mut entropy)?;
    reseed(&entropy)?;
    let mut time = [0; 12];
    stream.read_exact(&mut time)?;
    let at = libc::timespec {
        tv_sec: i64::from_le_bytes(time[..8].try_into()?),
        tv_nsec: i64::from(u32::from_le_bytes(time[8..].try_into()?)),
    };
    // SAFETY: `at` is a live timespec.
    ensure!(
        unsafe { libc::clock_settime(libc::CLOCK_REALTIME, &at) } == 0,
        "set clock: {}",
        std::io::Error::last_os_error()
    );
    Ok(())
}

/// Whether the shared runtime was mounted too.
fn assign(stream: &mut impl Read) -> Result<bool> {
    reseed_and_set_clock(stream)?;
    let mut length = [0; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_le_bytes(length) as usize;
    ensure!(length == DISK_BYTES, "invalid bootstrap size");
    let mut bootstrap = vec![0; length];
    stream.read_exact(&mut bootstrap)?;
    std::fs::write(BOOTSTRAP, bootstrap)?;
    let mut sizes = [0; 16];
    stream.read_exact(&mut sizes)?;
    let (image, shared) = (
        u64::from_le_bytes(sizes[..8].try_into()?),
        u64::from_le_bytes(sizes[8..].try_into()?),
    );
    ensure!(image > 0, "claim has no handler image");
    mount_image(HANDLER_DEVICE, image, ROOT)?;
    if shared > 0 {
        mount_image(SHARED_DEVICE, shared, SHARED_ROOT)?;
    }
    Ok(shared > 0)
}

/// Mounts a drive the host re-pointed after the snapshot, read-only. A new
/// size reaches the guest through an asynchronous config interrupt, so wait
/// for the device to report it first.
fn mount_image(device: &str, bytes: u64, target: &str) -> Result<()> {
    let file = std::fs::File::open(device).with_context(|| format!("open {device}"))?;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut size = 0u64;
        // SAFETY: BLKGETSIZE64 writes one u64 through the pointer.
        ensure!(
            unsafe { libc::ioctl(file.as_raw_fd(), BLKGETSIZE64, &mut size) } == 0,
            "size of {device}: {}",
            std::io::Error::last_os_error()
        );
        if size == bytes {
            break;
        }
        ensure!(Instant::now() < deadline, "{device} stayed {size} bytes, not {bytes}");
        std::thread::sleep(Duration::from_millis(1));
    }
    let (source, path) = (std::ffi::CString::new(device)?, std::ffi::CString::new(target)?);
    let flags = libc::MS_RDONLY | libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC | libc::MS_NOATIME;
    // SAFETY: valid NUL-terminated strings; no filesystem data.
    ensure!(
        unsafe { libc::mount(source.as_ptr(), path.as_ptr(), c"erofs".as_ptr(), flags, std::ptr::null()) } == 0,
        "mount {device}: {}",
        std::io::Error::last_os_error()
    );
    // A handler's files are read from a read-only filesystem, never from a
    // copy in memory: a drive that came up writable fails the claim.
    // SAFETY: an all-zero statvfs is valid; `path` is NUL-terminated.
    let read_only = unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        libc::statvfs(path.as_ptr(), &mut stat) == 0 && stat.f_flag & libc::ST_RDONLY != 0
    };
    ensure!(read_only, "{device} was not mounted read-only");
    eprintln!("CELLD_APPLICATION_DRIVE_MOUNTED {{\"device\":\"{device}\",\"filesystem\":\"erofs\",\"read_only\":true,\"bytes\":{bytes},\"target\":\"{target}\"}}");
    Ok(())
}

/// Credited host entropy, then an immediate reseed of the kernel's CRNG. The
/// VM generation counter also reseeds on restore but lands a few milliseconds
/// after resume, so only this is a barrier before any random draw.
fn reseed(entropy: &[u8; 32]) -> Result<()> {
    #[repr(C)]
    struct PoolInfo {
        bits: i32,
        bytes: i32,
        buffer: [u8; 32],
    }
    let info = PoolInfo {
        bits: 256,
        bytes: 32,
        buffer: *entropy,
    };
    let random = std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/urandom")?;
    // SAFETY: both requests take the descriptor of /dev/urandom; `info` is a
    // live rand_pool_info followed by the 32 bytes it declares.
    let (added, reseeded) = unsafe {
        (
            libc::ioctl(random.as_raw_fd(), RNDADDENTROPY, &info),
            libc::ioctl(random.as_raw_fd(), RNDRESEEDCRNG),
        )
    };
    ensure!(
        added == 0 && reseeded == 0,
        "reseed kernel randomness: {}",
        std::io::Error::last_os_error()
    );
    Ok(())
}
