#!/bin/sh
# Runs one real-VM test as root on this KVM host. It builds the static guest,
# its runtime image and the lib test binary, plus the guest kernel on first
# use. Extra NAME=value arguments reach the test; paths are relative to the
# checkout. The log goes to evidence/vm-test.log.
#
#   scripts/application/vm-test.sh TEST_PATH [NAME=value...]
set -eu
test_path=${1:?usage: vm-test.sh TEST_PATH [NAME=value...]}
shift
target=${CARGO_TARGET_DIR:-$PWD/target}
kernel=${CELLD_APPLICATION_JAVASCRIPT_TEST_KERNEL:-$HOME/celld-kernel/artifacts/vmlinux}
[ -e "$kernel" ] || bash scripts/application/build-guest-kernel.sh "${kernel%/artifacts/vmlinux}"
cargo rustc --release --locked -p celld --bin celld-guest -- -C target-feature=+crt-static
# The image builder refuses to overwrite.
rm -rf "$target/application-guest"
mkdir -p "$target/application-guest" evidence
image=$target/application-guest/runtime.img
python3 -B crates/applications/guest/build-check-image.py "$image" "$target/release/celld-guest"
binary=$(cargo test --locked -p celld --lib --no-run 2>&1 | tee evidence/build.log |
    sed -n 's/.*Executable unittests lib.rs (\(.*\))/\1/p')
[ -n "$binary" ] || { tail -n 40 evidence/build.log >&2; exit 1; }
status=0
sudo env CELLD_APPLICATION_JAVASCRIPT_TEST_VM_IMAGE="$image" \
    CELLD_APPLICATION_JAVASCRIPT_TEST_KERNEL="$kernel" \
    CELLD_APPLICATION_JAVASCRIPT_TEST_KERNEL_SHA256="$(sha256sum "$kernel" | cut -d' ' -f1)" \
    "$@" "$binary" "$test_path" --exact --include-ignored --nocapture \
    > evidence/vm-test.log 2>&1 || status=$?
tail -n 40 evidence/vm-test.log
if pgrep -x firecracker || pgrep -x jailer; then
    echo 'VM processes remain after the test' >&2
    exit 1
fi
exit $status
