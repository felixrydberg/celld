#!/bin/sh
# One-time setup of an Ubuntu x86_64 KVM host for scripts/application/vm-test.sh:
# the build tools from docs/application-setup.md, Firecracker and jailer in
# /usr/local/bin, and Rust for the invoking user. Run it as that user.
set -eu
[ -e /dev/kvm ] || { echo 'no /dev/kvm on this host' >&2; exit 1; }
sudo apt-get update
sudo apt-get install -y build-essential python3 erofs-utils flex bison bc \
    libelf-dev libssl-dev curl
version=v1.17.0
release=firecracker-$version-x86_64
if ! /usr/local/bin/firecracker --version 2>/dev/null | grep -q "Firecracker $version"; then
    work=$(mktemp -d)
    url=https://github.com/firecracker-microvm/firecracker/releases/download/$version
    curl -fsSL -o "$work/$release.tgz" "$url/$release.tgz"
    curl -fsSL -o "$work/$release.tgz.sha256.txt" "$url/$release.tgz.sha256.txt"
    (cd "$work" && sha256sum -c "$release.tgz.sha256.txt")
    tar -xzf "$work/$release.tgz" -C "$work"
    sudo install -m 755 "$work/release-$version-x86_64/$release" /usr/local/bin/firecracker
    sudo install -m 755 "$work/release-$version-x86_64/jailer-$version-x86_64" /usr/local/bin/jailer
    rm -rf "$work"
fi
rustup_path=$(command -v rustup || :)
if [ -z "$rustup_path" ]; then
    rustup_path="$HOME/.cargo/bin/rustup"
    [ -x "$rustup_path" ] || curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal
fi
# For the pinned toolchain (rust-toolchain.toml), when run from the checkout.
"$rustup_path" component add rustfmt clippy
/usr/local/bin/firecracker --version | head -n 1
