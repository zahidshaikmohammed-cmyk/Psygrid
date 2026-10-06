#!/usr/bin/env bash
# Idempotent host hardening for the PSYGRID service. Run with sudo from the repo root by the deploy
# workflow before `systemctl restart psygrid`. It never edits psygrid.service itself, the env files,
# the code or any data directory, and it never stops or restarts psygrid.
set -euo pipefail
cd "$(dirname "$0")/.."

install -d -m 0755 /etc/systemd/system/psygrid.service.d
install -m 0644 deploy/psygrid.service.d/10-hardening.conf /etc/systemd/system/psygrid.service.d/10-hardening.conf
install -m 0644 deploy/psygrid.service.d/20-live-core-token-share.conf /etc/systemd/system/psygrid.service.d/20-live-core-token-share.conf

install -d -m 0755 /etc/systemd/journald.conf.d
if ! cmp -s deploy/journald-psygrid.conf /etc/systemd/journald.conf.d/psygrid.conf; then
  install -m 0644 deploy/journald-psygrid.conf /etc/systemd/journald.conf.d/psygrid.conf
  systemctl restart systemd-journald
fi
journalctl --vacuum-size=300M --vacuum-time=14d >/dev/null 2>&1 || true

# Caches only: both are rebuilt on demand and hold nothing the services need.
apt-get clean >/dev/null 2>&1 || true
for home in /root /home/ubuntu; do
  rm -rf -- "${home}/.cache/pip"
done

systemctl daemon-reload
systemd-analyze verify psygrid.service 2>&1 | grep -v "^$" || true
echo "psygrid hardening installed:"
systemctl show psygrid -p Restart -p WatchdogUSec -p LimitNOFILE -p OOMScoreAdjust -p CPUWeight -p IOWeight -p MemoryLow
df -h / | tail -1
