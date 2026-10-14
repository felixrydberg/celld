#!/usr/bin/env bash
# Build the approved guest kernel without replacing any installed kernel.
set -euo pipefail
version=6.1.186
source_sha=eeedc32bbf2448205aff50ee2760a4d87172cf8f8279c1e5930069ad36f6236e
source_url="https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-$version.tar.xz"
config="$(cd -- "$(dirname -- "$0")" && pwd)/guest-kernel.config"
root=$(realpath -m -- "${1:?usage: build-guest-kernel.sh NEW_OUTPUT_DIRECTORY}")
for tool in curl sha256sum tar make gcc flex bison bc python3; do command -v "$tool" >/dev/null; done
# objtool and the certificate helper require libelf-dev and libssl-dev.
printf '#include <libelf.h>\n#include <openssl/bio.h>\n' | gcc -x c -fsyntax-only -
# Never overwrite a kernel build or an approved artifact.
if [[ -e "$root/source" || -e "$root/build" || -e "$root/artifacts" ]]; then
  echo 'require an output directory with no source/build/artifacts' >&2
  exit 1
fi
mkdir -p "$root/source" "$root/build" "$root/artifacts"
archive="$root/source/linux-$version.tar.xz"
curl --fail --location --proto '=https' --tlsv1.2 "$source_url" -o "$archive"
printf '%s  %s\n' "$source_sha" "$archive" | sha256sum --check --strict
# The checksum above is pinned from the official published kernel.org list.
curl --fail --location --proto '=https' --tlsv1.2 https://cdn.kernel.org/pub/linux/kernel/v6.x/sha256sums.asc -o "$root/artifacts/source-checksums.asc"
grep -Fx "$source_sha  linux-$version.tar.xz" "$root/artifacts/source-checksums.asc" >/dev/null
tar --extract --xz --file "$archive" --directory "$root/source"
source="$root/source/linux-$version"
cp "$config" "$root/build/.config"
make -C "$source" O="$root/build" ARCH=x86_64 olddefconfig
python3 - "$root/build/.config" <<'PY'
import pathlib,sys
lines=set(pathlib.Path(sys.argv[1]).read_text().splitlines())
required='64BIT X86_64 SMP KVM_GUEST PARAVIRT_CLOCK ACPI PCI VMGENID VIRTIO_MMIO VIRTIO_BLK VIRTIO_BALLOON MEMORY_BALLOON PAGE_REPORTING NET VSOCKETS VIRTIO_VSOCKETS SERIAL_8250 SERIAL_8250_PNP SERIAL_8250_CONSOLE PRINTK DEVTMPFS DEVTMPFS_MOUNT PROC_FS SYSFS TMPFS SHMEM EROFS_FS EROFS_FS_ZIP EROFS_FS_ZIP_LZMA EROFS_FS_XATTR EROFS_FS_SECURITY BINFMT_ELF ADVISE_SYSCALLS MEMBARRIER MEMFD_CREATE FILE_LOCKING FUTEX EPOLL EVENTFD HIGH_RES_TIMERS IKCONFIG VMGENID RANDOM_TRUST_CPU RANDOM_TRUST_BOOTLOADER HW_RANDOM_VIRTIO PTP_1588_CLOCK_KVM X86_INTEL_MEMORY_PROTECTION_KEYS CPU_MITIGATIONS PAGE_TABLE_ISOLATION RETPOLINE RETHUNK STACKPROTECTOR_STRONG VMAP_STACK RANDOMIZE_BASE RANDOMIZE_MEMORY RANDOMIZE_KSTACK_OFFSET STRICT_KERNEL_RWX SLAB_FREELIST_RANDOM SLAB_FREELIST_HARDENED HARDENED_USERCOPY FORTIFY_SOURCE SECURITY_LANDLOCK STACK_VALIDATION SCHED_STACK_END_CHECK BUG_ON_DATA_CORRUPTION'.split()
for flag in required:
    assert 'CONFIG_'+flag+'=y' in lines, 'required kernel flag: '+flag
for flag in 'EXT4_FS SQUASHFS JBD2 FS_MBCACHE FS_ENCRYPTION NETWORK_FILESYSTEMS MODULES BLK_DEV_INITRD BINFMT_SCRIPT BINFMT_MISC DEBUG_INFO_BTF INET IPV6 PACKET NETDEVICES NETFILTER NET_SCHED BRIDGE MPLS DCB DNS_RESOLVER NETLINK_DIAG VIRTIO_NET VIRTIO_PCI VIRTIO_PMEM VIRTIO_MEM VIRTIO_CONSOLE VSOCKETS_DIAG VSOCKETS_LOOPBACK USB_SUPPORT HID INPUT SERIO SCSI ATA BLK_DEV_NVME NVME_CORE BLK_DEV_LOOP LIBNVDIMM BLK_DEV_PMEM DEV_DAX SOUND DRM MEDIA_SUPPORT I2C SPI GPIOLIB PINCTRL AUXDISPLAY THERMAL CPU_FREQ CPU_IDLE CGROUPS NAMESPACES FTRACE DEBUG_FS DAMON BPF_SYSCALL NUMA MEMORY_HOTPLUG MEMORY_HOTREMOVE SUSPEND HIBERNATION PCIEPORTBUS HOTPLUG_PCI VIRTUALIZATION SERIAL_8250_PCI SERIAL_8250_LPSS SERIAL_8250_MID SERIAL_8250_PERICOM PERF_EVENTS_INTEL_UNCORE PERF_EVENTS_INTEL_RAPL PERF_EVENTS_INTEL_CSTATE PERF_EVENTS_AMD_UNCORE HW_RANDOM_INTEL HW_RANDOM_AMD HW_RANDOM_VIA'.split():
    assert 'CONFIG_'+flag+'=y' not in lines and 'CONFIG_'+flag+'=m' not in lines, 'unexpected kernel subsystem: '+flag
assert not any(line.startswith('CONFIG_') and line.endswith('=m') for line in lines)
assert 'CONFIG_BUILD_SALT=""' in lines
assert 'CONFIG_NR_CPUS=4' in lines
PY
export KBUILD_BUILD_TIMESTAMP='1970-01-01 00:00:01 UTC'
export KBUILD_BUILD_USER=celld
export KBUILD_BUILD_HOST=builder
export KBUILD_BUILD_VERSION=1
/usr/bin/time -v make -C "$source" O="$root/build" ARCH=x86_64 -j2 vmlinux 2>&1 | tee "$root/artifacts/build.log"
cp "$root/build/vmlinux" "$root/artifacts/vmlinux"
cp "$root/build/.config" "$root/artifacts/kernel.config"
{
  printf 'source_url=%s\nsource_sha256=%s\n' "$source_url" "$source_sha"
  printf 'KBUILD_BUILD_TIMESTAMP=%s\nKBUILD_BUILD_USER=%s\nKBUILD_BUILD_HOST=%s\nKBUILD_BUILD_VERSION=%s\n' "$KBUILD_BUILD_TIMESTAMP" "$KBUILD_BUILD_USER" "$KBUILD_BUILD_HOST" "$KBUILD_BUILD_VERSION"
  gcc --version | head -1
  ld --version | head -1
  sha256sum "$config" "$root/artifacts/kernel.config" "$root/artifacts/vmlinux"
  readelf -n "$root/artifacts/vmlinux"
} > "$root/artifacts/provenance.txt"
printf 'Built candidate kernel: %s\n' "$root/artifacts/vmlinux"
