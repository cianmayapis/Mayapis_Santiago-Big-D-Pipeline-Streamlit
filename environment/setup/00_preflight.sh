#!/usr/bin/env bash
source "$(dirname "$0")/lib/common.sh"
log "Running preflight checks"

# --- User / privileges ---
[[ $EUID -ne 0 ]] || die "Run as a normal user with sudo rights, not as root."
have sudo || die "sudo is not installed."
sudo -v   || die "sudo authentication failed."

# --- OS / architecture ---
source /etc/os-release
[[ "${ID}" == "ubuntu" ]] || die "Ubuntu required (found: ${ID})."
case "${VERSION_ID}" in
  22.04|24.04) ok "Ubuntu ${VERSION_ID} supported" ;;
  26.04) warn "Ubuntu 26.04: MongoDB packages come from the 'noble' repo (handled in 20_mongodb.sh)." ;;
  *) warn "Ubuntu ${VERSION_ID} is untested." ;;
esac
case "$(uname -m)" in
  x86_64|aarch64) ok "Architecture $(uname -m)" ;;
  *) die "Unsupported architecture $(uname -m)" ;;
esac

# --- Bootstrap curl so we can test connectivity ---
if ! have curl; then
  $SUDO apt-get update -y && $SUDO apt-get install -y curl
fi

# --- Network reachability (any HTTP response counts as reachable) ---
reach() {
  curl -sS -o /dev/null --max-time 15 "https://$1" 2>/dev/null \
    || curl -4 -sS -o /dev/null --max-time 15 "https://$1" 2>/dev/null   # retry over IPv4
}
for host in downloads.apache.org repo.mongodb.org repo.anaconda.com \
            repo1.maven.org pypi.org api.weatherapi.com; do
  if reach "$host"; then ok "Reachable: ${host}"; else die "Cannot reach ${host} (check VM network/proxy)."; fi
done
# Fallback mirror only: warn, don't abort
if reach archive.apache.org; then ok "Reachable: archive.apache.org"
else warn "archive.apache.org unreachable; fine unless a pinned version has left downloads.apache.org."; fi

# --- Resources ---
avail_gb="$(df -BG --output=avail / | tail -1 | tr -dc '0-9')"
(( avail_gb >= MIN_DISK_GB )) || die "Need >= ${MIN_DISK_GB} GB free on / (have ${avail_gb} GB)."
ok "Disk: ${avail_gb} GB free"

ram_gb="$(awk '/MemTotal/{print int($2/1024/1024)}' /proc/meminfo)"
if (( ram_gb >= MIN_RAM_GB )); then ok "RAM: ${ram_gb} GB"
else warn "Only ${ram_gb} GB RAM; ${MIN_RAM_GB}+ GB recommended."; fi

# --- Ports ---
for p in 9092 9093 27017 4040 8501; do
  if ss -ltn "( sport = :${p} )" 2>/dev/null | grep -q LISTEN; then
    warn "Port ${p} already in use (fine if it's a previous run of this stack)."
  fi
done

# --- Existing tools (informational) ---
if have java; then
  v="$(java -version 2>&1 | awk -F'[".]' '/version/{print $2; exit}')"
  if (( v >= JAVA_VERSION )); then ok "Existing Java ${v}"
  else warn "Existing Java ${v} < ${JAVA_VERSION}; will install JDK ${JAVA_VERSION}."; fi
fi
if have conda; then ok "Existing conda found"; else log "conda not found; will install Miniconda."; fi
ok "Preflight passed"