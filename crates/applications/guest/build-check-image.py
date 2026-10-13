"""Build a minimal read-only EROFS runtime from a trusted native guest ELF.

The guest is PID 1. Static builds need no libraries; dynamic builds include
only their resolved ELF dependencies. Python runs only on the build host.
"""
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

EPOCH = 1
UUID = "00000000-0000-0000-0000-00000000c311"


def reproducible_mkfs(staging, output):
    subprocess.run([
        "/usr/bin/mkfs.erofs", "-b", "4096", "-T", str(EPOCH), "--force-uid=0", "--force-gid=0",
        "-U", UUID, "-x", "-1", str(output), str(staging)], check=True, env={})


def build(output, javascript_binary):
    output = Path(output).absolute()
    if output.exists():
        raise FileExistsError(output)
    source = Path(__file__).parent.resolve()
    with tempfile.TemporaryDirectory(prefix="celld-javascript-image-") as temp:
        staging = Path(temp)
        staging.chmod(0o755)
        copied = set()

        def runtime(path, destination=None):
            path = Path(path)
            destination = destination or str(path)
            if destination in copied:
                return
            copied.add(destination)
            target = staging / destination.lstrip("/")
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
            target.chmod(0o755)
            elf = subprocess.run(["readelf", "-l", str(path)], capture_output=True, text=True)
            if elf.returncode or not re.search(r"^\s*LOAD\s", elf.stdout, re.MULTILINE):
                raise RuntimeError(f"not an executable ELF: {path}: {elf.stderr}")
            if "INTERP" not in elf.stdout:
                return
            result = subprocess.run(["ldd", str(path)], capture_output=True, text=True)
            if result.returncode or "not found" in result.stdout:
                raise RuntimeError(f"missing dependency for {path}: {result.stdout}{result.stderr}")
            for library in re.findall(r"(?:=>\s+|^\s*)(/[^\s()]+)", result.stdout, re.MULTILINE):
                runtime(library)

        for directory in ["dev", "proc", "sys", "run", "tmp", "sbin", "opt/celld", "application", "pyodide"]:
            (staging / directory).mkdir(parents=True, exist_ok=True)
        binary = staging / "stripped-guest"
        subprocess.run(["/usr/bin/strip", "--strip-all", "-o", str(binary), str(Path(javascript_binary).resolve(strict=True))], check=True)
        runtime(binary, "/opt/celld/celld-guest")
        binary.unlink()
        (staging / "sbin/init").hardlink_to(staging / "opt/celld/celld-guest")
        config = {
            "version": 1,
            "resource": {
                "application": {"app": "javascript-vm"},
                "resource": {"kind": "durable_object", "id": "counter"},
            },
            "source": (source / "javascript/check.js").read_text(),
            "do_classes": ["Counter", "StreamCounter", "QuiescenceCounter", "CallbackCounter", "Alternate"],
        }
        fixture = staging / "opt/celld/javascript-check.json"
        fixture.write_text(json.dumps(config))
        fixture.chmod(0o644)
        for directory in staging.rglob("*"):
            if directory.is_dir():
                directory.chmod(0o755)
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("xb"):
            pass
        try:
            reproducible_mkfs(staging, output)
            size = output.stat().st_size
            if not 0 < size <= 1024 * 1024 * 1024 or size % 4096:
                raise ValueError("runtime image exceeds byte limit or has invalid size")
        except BaseException:
            output.unlink(missing_ok=True)
            raise
    with output.open("rb") as image:
        digest = hashlib.file_digest(image, "sha256").hexdigest()
    output.with_suffix(".sha256").write_text(digest + "\n")
    print(f"{digest}  {output} ({size} bytes)")


if __name__ == "__main__":
    build(*sys.argv[1:])
