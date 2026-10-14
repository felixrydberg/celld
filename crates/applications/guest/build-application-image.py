"""Build a handler artifact: a deterministic, read-only EROFS image.

Upload its bytes through the application API; artifact_sha256 pins the whole
image. The guest mounts it read-only on claim. Kernel approval is host policy.
"""
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

MAX_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_FILES = 4096
BLOCK = 4096
MAGIC = b"\xe2\xe1\xf5\xe0"


def build(source, output):
    source, output = Path(source).resolve(strict=True), Path(output).absolute()
    if not source.is_dir() or output.exists():
        raise ValueError("require a source directory and a new output")
    files, total = [], 0
    for path in sorted(source.rglob("*")):
        if path.is_symlink() or not (path.is_file() or path.is_dir()):
            raise ValueError("application bundles contain only regular files/directories")
        name = path.relative_to(source).as_posix()
        if not name.isascii() or any(character in name for character in "\\:\0") or len(name) > 1024:
            raise ValueError("unsupported file name in application bundle")
        if path.is_dir():
            continue
        files.append((path, name))
        total += path.stat().st_size
        if len(files) > MAX_FILES or total > MAX_ARTIFACT_BYTES:
            raise ValueError("application bundle exceeds file/byte limit")
    with tempfile.TemporaryDirectory(prefix="celld-handler-image-") as temporary:
        staging = Path(temporary)
        staging.chmod(0o755)
        for path, name in files:
            target = staging / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
            target.chmod(0o644)
        for path in staging.rglob("*"):
            if path.is_dir():
                path.chmod(0o755)
        with output.open("xb"):
            pass
        try:
            subprocess.run([
                "/usr/bin/mkfs.erofs", "-b", "4096", "-T", "1", "--force-uid=0", "--force-gid=0",
                "-U", "00000000-0000-0000-0000-00000000c313", "-x", "-1",
                str(output), str(staging)], check=True, env={}, capture_output=True)
            size = output.stat().st_size
            if not 0 < size <= MAX_ARTIFACT_BYTES or size % BLOCK:
                raise ValueError("application image exceeds byte limit or has invalid size")
        except BaseException:
            output.unlink(missing_ok=True)
            raise
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    output.with_suffix(".sha256").write_text(digest + "\n")
    print(f"{digest}  {output}")


def self_check():
    with tempfile.TemporaryDirectory(prefix="celld-bundle-check-") as temporary:
        root = Path(temporary)
        source = root / "source"
        source.mkdir()
        outside = root / "outside.mjs"
        outside.write_text("private host file")
        (source / "escape.mjs").symlink_to(outside)
        output = root / "application.img"
        try:
            build(source, output)
        except ValueError:
            pass
        else:
            raise AssertionError("unsafe bundle accepted")
        assert not output.exists()
        (source / "escape.mjs").unlink()
        (source / "main.mjs").write_text("export default {};")
        (source / "lib/deep").mkdir(parents=True)
        (source / "lib/deep/data").write_bytes(bytes(range(256)) * 400)
        (source / "empty").touch()
        output.write_bytes(b"keep existing bundle")
        try:
            build(source, output)
        except ValueError:
            pass
        else:
            raise AssertionError("existing bundle overwritten")
        assert output.read_bytes() == b"keep existing bundle"
        output.unlink()
        build(source, output)
        image = output.read_bytes()
        assert len(image) % BLOCK == 0 and image[1024:1028] == MAGIC
        assert image[1036] == 12
        assert int.from_bytes(image[1060:1064], "little") * BLOCK == len(image)
        for path in [source, *source.rglob("*")]:
            os.utime(path, (123456789, 123456789))
            path.chmod(0o700)
        second = root / "second.img"
        build(source, second)
        assert second.read_bytes() == image, "source metadata changed the digest"
        extracted = root / "extracted"
        subprocess.run(["/usr/bin/fsck.erofs", f"--extract={extracted}", str(output)], check=True)
        for path in source.rglob("*"):
            if path.is_file():
                assert (extracted / path.relative_to(source)).read_bytes() == path.read_bytes()
        with (source / "too-large").open("wb") as file:
            file.truncate(MAX_ARTIFACT_BYTES + 1)
        refused = root / "refused.img"
        try:
            build(source, refused)
        except ValueError:
            pass
        else:
            raise AssertionError("oversized bundle accepted")
        assert not refused.exists()
    print("Application image validation self-check passed")


if __name__ == "__main__":
    if sys.argv[1:] == ["--check"]:
        self_check()
    else:
        build(*sys.argv[1:])
