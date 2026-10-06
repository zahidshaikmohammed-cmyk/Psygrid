#!/usr/bin/env bash
# Install or update one PSYGRID Live Core node on an Oracle E2.1.Micro VM. Idempotent.
#
# Run as root from an extracted release directory (the deploy workflow does this):
#   sudo NODE_ID=0 NODE_COUNT=2 APP_USER=ubuntu LIVE_CORE_ROOT=/home/ubuntu/psygrid-live-core \
#        PORT=10000 PEERS="1=http://10.0.0.12:10000" bash deploy/live-core/install.sh
#
#   bash deploy/live-core/install.sh --render-only   # print the systemd unit and exit
#
# It only ever manages psygrid-live-core-node<N>.service, its env/topology files, a swap file and
# the node's firewall port. It never touches psygrid.service or any full-PSYGRID file, and it
# installs no archive or data directory: the Live Core keeps market data in RAM only.
set -euo pipefail

NODE_ID="${NODE_ID:-0}"
NODE_COUNT="${NODE_COUNT:-2}"
APP_USER="${APP_USER:-ubuntu}"
LIVE_CORE_ROOT="${LIVE_CORE_ROOT:-/home/${APP_USER}/psygrid-live-core}"
PORT="${PORT:-10000}"
PEERS="${PEERS:-}"
SWAP_MB="${SWAP_MB:-1024}"
RELEASE_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
UNIT="psygrid-live-core-node${NODE_ID}.service"

case "$NODE_ID" in ''|*[!0-9]*) echo "NODE_ID must be a number" >&2; exit 2;; esac
case "$NODE_COUNT" in ''|*[!0-9]*) echo "NODE_COUNT must be a number" >&2; exit 2;; esac
if [ "$NODE_ID" -ge "$NODE_COUNT" ]; then
  echo "NODE_ID $NODE_ID must be below NODE_COUNT $NODE_COUNT" >&2
  exit 2
fi

render_unit() {
  sed -e "s|@NODE_ID@|${NODE_ID}|g" \
      -e "s|@NODE_COUNT@|${NODE_COUNT}|g" \
      -e "s|@USER@|${APP_USER}|g" \
      -e "s|@ROOT@|${LIVE_CORE_ROOT}|g" \
      "${RELEASE_DIR}/deploy/live-core/psygrid-live-core.service.in"
}

if [ "${1:-}" = "--render-only" ]; then
  render_unit
  exit 0
fi

if [ "$(id -u)" -ne 0 ]; then
  echo "install.sh must run as root" >&2
  exit 1
fi

# 1. Python >= 3.11 (the shared modules use datetime.UTC). Ubuntu 24.04 ships 3.12.
PYTHON=""
for candidate in python3.12 python3.13 python3.11 python3; do
  if command -v "$candidate" >/dev/null 2>&1 &&
     "$candidate" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)'; then
    PYTHON="$(command -v "$candidate")"
    break
  fi
done
if [ -z "$PYTHON" ]; then
  echo "No Python >= 3.11 found; use an Ubuntu 24.04 image or install python3.12" >&2
  exit 1
fi
if ! "$PYTHON" -c 'import venv, ensurepip' >/dev/null 2>&1; then
  apt-get update -qq
  apt-get install -y -qq "$(basename "$PYTHON")-venv" || apt-get install -y -qq python3-venv
fi

# 2. A small swap file so pip installs and short spikes never meet the OOM killer on 1 GB.
if [ "$SWAP_MB" -gt 0 ] && ! swapon --show=NAME --noheadings | grep -q .; then
  if [ ! -f /swapfile ]; then
    fallocate -l "${SWAP_MB}M" /swapfile || dd if=/dev/zero of=/swapfile bs=1M count="$SWAP_MB"
    chmod 600 /swapfile
    mkswap /swapfile >/dev/null
  fi
  swapon /swapfile
  grep -q '^/swapfile ' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi
sysctl -q -w vm.swappiness=10 || true

# 3. Bounded journal.
install -d -m 0755 /etc/systemd/journald.conf.d
cat > /etc/systemd/journald.conf.d/psygrid-live-core.conf <<'JOURNAL'
[Journal]
SystemMaxUse=200M
SystemMaxFileSize=25M
MaxRetentionSec=14day
JOURNAL
systemctl try-restart systemd-journald || true

# 4. Release layout: releases/<id> (this directory) -> current; one shared venv.
install -d -o "$APP_USER" -g "$APP_USER" -m 0755 "$LIVE_CORE_ROOT" "$LIVE_CORE_ROOT/releases"
chown -R "$APP_USER:$APP_USER" "$RELEASE_DIR"
if [ ! -x "$LIVE_CORE_ROOT/venv/bin/python" ]; then
  sudo -u "$APP_USER" "$PYTHON" -m venv "$LIVE_CORE_ROOT/venv"
fi
sudo -u "$APP_USER" "$LIVE_CORE_ROOT/venv/bin/pip" install -q --disable-pip-version-check --no-cache-dir \
  -r "$RELEASE_DIR/deploy/live-core/requirements.txt"
# The service cannot write bytecode (read-only filesystem), so compile it once here.
sudo -u "$APP_USER" "$LIVE_CORE_ROOT/venv/bin/python" -m compileall -q "$RELEASE_DIR" >/dev/null
# Refuse to switch to a release whose universe/partition does not validate.
(cd "$RELEASE_DIR" && sudo -u "$APP_USER" env LIVE_CORE_NODE_ID="$NODE_ID" LIVE_CORE_NODE_COUNT="$NODE_COUNT" \
  "$LIVE_CORE_ROOT/venv/bin/python" -c 'from live_core.runtime import build_runtime; r = build_runtime(); print("partition", r.partition.describe())')
ln -sfn "$RELEASE_DIR" "$LIVE_CORE_ROOT/current.new"
mv -T "$LIVE_CORE_ROOT/current.new" "$LIVE_CORE_ROOT/current"

# 5. Credentials stay on the VM; create a locked-down template the first time.
if [ ! -f /etc/psygrid-live-core.env ]; then
  install -m 0600 -o root -g root /dev/null /etc/psygrid-live-core.env
  cat > /etc/psygrid-live-core.env <<'ENVFILE'
# PSYGRID Live Core Dhan credentials (same account and data plan as the full PSYGRID).
# Shared-token mode (default): the node consumes ONE current Dhan access token and never generates
# one, so it cannot invalidate the token of the process that owns token generation.
# DHAN_CLIENT_ID=
# DHAN_ACCESS_TOKEN=
# DHAN_PIN / DHAN_TOTP_SECRET are ignored unless LIVE_CORE_TOKEN_GENERATION=1 (only for a node that
# is the account's sole token authority).
# Optional NSE holidays / special sessions (ISO dates, comma separated):
# PSYGRID_MARKET_HOLIDAYS=
# PSYGRID_SPECIAL_SESSIONS=
ENVFILE
  echo "WARNING: created /etc/psygrid-live-core.env; fill in the Dhan credentials, then restart ${UNIT}" >&2
fi
if grep -Eq '^[[:space:]]*LIVE_CORE_NODE_(ID|COUNT)=' /etc/psygrid-live-core.env; then
  echo "/etc/psygrid-live-core.env must not set LIVE_CORE_NODE_ID/COUNT (the unit sets them)" >&2
  exit 2
fi
umask 022
cat > /etc/psygrid-live-core-node.env <<TOPOLOGY
# Written by deploy/live-core/install.sh; node identity lives in ${UNIT}.
LIVE_CORE_PORT=${PORT}
LIVE_CORE_PEERS=${PEERS}
TOPOLOGY

# 6. Exactly one Live Core node per VM: retire any other node unit left on this host.
for other in /etc/systemd/system/psygrid-live-core-node*.service; do
  [ -e "$other" ] || continue
  name="$(basename "$other")"
  if [ "$name" != "$UNIT" ]; then
    systemctl disable --now "$name" || true
    rm -f "$other"
  fi
done
render_unit > "/etc/systemd/system/${UNIT}"
chmod 0644 "/etc/systemd/system/${UNIT}"
systemctl daemon-reload
systemd-analyze verify "/etc/systemd/system/${UNIT}" 2>&1 | grep -v '^$' || true

# 7. Host firewall (Oracle images reject by default). The VCN security list must also allow PORT.
if command -v iptables >/dev/null 2>&1; then
  if ! iptables -C INPUT -p tcp --dport "$PORT" -j ACCEPT 2>/dev/null; then
    iptables -I INPUT 5 -p tcp --dport "$PORT" -j ACCEPT 2>/dev/null || iptables -I INPUT -p tcp --dport "$PORT" -j ACCEPT
  fi
  if command -v netfilter-persistent >/dev/null 2>&1; then
    netfilter-persistent save >/dev/null 2>&1 || true
  fi
fi

systemctl enable "$UNIT" >/dev/null
systemctl reset-failed "$UNIT" 2>/dev/null || true
systemctl restart "$UNIT"

# 8. Keep the last three releases.
current="$(readlink -f "$LIVE_CORE_ROOT/current")"
ls -1dt "$LIVE_CORE_ROOT"/releases/*/ 2>/dev/null | tail -n +4 | while read -r old; do
  [ "$(readlink -f "$old")" = "$current" ] || rm -rf -- "$old"
done

rm -rf -- "/root/.cache/pip" "/home/${APP_USER}/.cache/pip"
echo "installed ${UNIT} (release ${current})"
systemctl show "$UNIT" -p ActiveState -p Restart -p MemoryMax -p LimitNOFILE -p WatchdogUSec
