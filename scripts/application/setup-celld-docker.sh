#!/bin/sh
# Native Docker Engine for celld's Linux container engine, beside Docker Desktop.
# Run as root in WSL Ubuntu. Docker Desktop keeps /var/run/docker.sock; this
# daemon listens only on /run/celld-docker.sock with its own state and bridges.
set -eu

# 1. Docker's official apt repository (Ubuntu noble).
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    > /etc/apt/sources.list.d/docker.list

# 2. Install without starting the stock daemon (it would claim /var/run/docker.sock,
#    which Docker Desktop's WSL integration owns).
printf '#!/bin/sh\nexit 101\n' > /usr/sbin/policy-rc.d
chmod +x /usr/sbin/policy-rc.d
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y docker-ce containerd.io
rm -f /usr/sbin/policy-rc.d
systemctl disable --now docker.service docker.socket || true
systemctl mask docker.service docker.socket

# 3. A separate daemon: own socket, data root, exec root and address pools
#    clear of WSL's 172.19.128.0/20.
mkdir -p /etc/celld-docker
cat > /etc/celld-docker/daemon.json <<'JSON'
{
  "hosts": ["unix:///run/celld-docker.sock"],
  "data-root": "/var/lib/celld-docker",
  "exec-root": "/run/celld-docker",
  "pidfile": "/run/celld-docker.pid",
  "bip": "10.213.0.1/24",
  "default-address-pools": [{"base": "10.213.0.0/16", "size": 24}]
}
JSON
cat > /etc/systemd/system/celld-docker.service <<'UNIT'
[Unit]
Description=Native Docker Engine for celld container fixtures
After=network-online.target containerd.service
Wants=containerd.service

[Service]
ExecStart=/usr/bin/dockerd --config-file /etc/celld-docker/daemon.json
Restart=on-failure

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now celld-docker.service

# 4. Qualify: reachable bridge IP from this distro.
export DOCKER_HOST=unix:///run/celld-docker.sock
docker info --format 'server={{.ServerVersion}} os={{.OperatingSystem}} runtime={{.DefaultRuntime}} cgroup={{.CgroupVersion}}'
