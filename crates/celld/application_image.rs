// Image building stages files on the host (the publish CLI or the node),
// outside the Actor execution domain.
#![allow(clippy::disallowed_methods)]

//! Read-only EROFS images for handler files and the shared Pyodide runtime.
//! Fixed metadata makes the same content produce the same approved digest.

use anyhow::{ensure, Context, Result};
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

const BLOCK: u64 = 4096;
const MKFS: [&str; 2] = ["/usr/bin/mkfs.erofs", "/usr/sbin/mkfs.erofs"];
const MAGIC: [u8; 4] = [0xe2, 0xe1, 0xf5, 0xe0];

/// Builds an uncompressed image, bounding both source bytes and image bytes.
pub(crate) fn build_file(files: &[(String, Vec<u8>)], output: &Path, limit: u64) -> Result<()> {
    ensure!(files.len() <= 4096, "application image exceeds file limit");
    let staging = tempfile::tempdir().context("stage image files")?;
    std::fs::set_permissions(staging.path(), std::fs::Permissions::from_mode(0o755))?;
    let mut total = 0u64;
    let mut names = std::collections::HashSet::new();
    for (name, body) in files {
        crate::application_javascript_guest::artifact_path(name)?;
        ensure!(names.insert(name), "duplicate application image path");
        total = total
            .checked_add(body.len() as u64)
            .context("application image size overflow")?;
        ensure!(total <= limit, "application image exceeds {limit} bytes");
        let path = staging.path().join(name);
        let parent = path.parent().context("image path has no parent")?;
        std::fs::create_dir_all(parent)?;
        for directory in parent
            .ancestors()
            .take_while(|directory| directory.starts_with(staging.path()))
        {
            std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o755))?;
        }
        std::fs::write(&path, body)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))?;
    }
    let mkfs = MKFS
        .iter()
        .find(|path| Path::new(path).is_file())
        .context("mkfs.erofs (erofs-utils) is required to build application images")?;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .context("create application image output")?;
    let checked = (|| {
        let made = Command::new(mkfs)
            .args([
                "-b",
                "4096",
                "-T",
                "1",
                "--force-uid=0",
                "--force-gid=0",
                "-U",
                "00000000-0000-0000-0000-00000000c313",
                "-x",
                "-1",
            ])
            .arg(output)
            .arg(staging.path())
            .env_clear()
            .output()
            .context("run mkfs.erofs")?;
        ensure!(
            made.status.success(),
            "mkfs.erofs failed: {}",
            String::from_utf8_lossy(&made.stderr).trim()
        );
        let size = std::fs::metadata(output)?.len();
        ensure!(
            size > 0 && size % BLOCK == 0 && size <= limit,
            "application image exceeds {limit} bytes or has invalid size"
        );
        Ok(())
    })();
    if checked.is_err() {
        let _ = std::fs::remove_file(output);
    }
    checked
}

/// `build_file` into memory, as the publish CLI uploads it.
pub(crate) fn build(files: &[(String, Vec<u8>)]) -> Result<Vec<u8>> {
    let directory = tempfile::tempdir()?;
    let output = directory.path().join("image");
    build_file(
        files,
        &output,
        crate::applications::MAX_ARTIFACT_BYTES as u64,
    )?;
    Ok(std::fs::read(output)?)
}

/// Refuses incompatible or truncated images before a VM boots. The cache
/// hashes an artifact once, at download, so this is a structural check only;
/// the guest kernel validates the filesystem on mount.
pub(crate) fn check(path: &Path) -> Result<()> {
    let mut file = std::fs::File::open(path)?;
    let length = file.metadata()?.len();
    ensure!(
        length > 0
            && length % BLOCK == 0
            && length <= crate::applications::MAX_ARTIFACT_BYTES as u64,
        "application image has an invalid size"
    );
    let mut head = [0; 1064];
    file.read_exact(&mut head)?;
    ensure!(
        head[1024..1028] == MAGIC && head[1036] == 12,
        "application artifact is not a supported EROFS image"
    );
    ensure!(
        u64::from(u32::from_le_bytes(head[1060..1064].try_into()?)) * BLOCK == length,
        "application image has an invalid block count"
    );
    Ok(())
}

#[cfg(test)]
#[path = "unit_tests/application_image/tests.rs"]
mod tests;
