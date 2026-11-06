#!/usr/bin/env bash
# Shared helpers. Sourced by every module, never executed directly.
set -Eeo pipefail
export DEBIAN_FRONTEND=noninteractive

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${SCRIPT_ROOT}/config.env"

ENV_FILE="$HOME/.pipeline_env"
[[ -f "$ENV_FILE" ]] && source "$ENV_FILE"

C_RED=$'\e[31m'; C_GRN=$'\e[32m'; C_YEL=$'\e[33m'; C_BLU=$'\e[34m'; C_RST=$'\e[0m'
log()  { echo "${C_BLU}[INFO]${C_RST} $*"; }
ok()   { echo "${C_GRN}[ OK ]${C_RST} $*"; }
warn() { echo "${C_YEL}[WARN]${C_RST} $*" >&2; }
die()  { echo "${C_RED}[FAIL]${C_RST} $*" >&2; exit 1; }
trap 'die "Unexpected error (exit $?) at line ${LINENO} of ${BASH_SOURCE[1]:-main}"' ERR

have() { command -v "$1" >/dev/null 2>&1; }

if [[ $EUID -eq 0 ]]; then SUDO=""; else SUDO="sudo"; fi

# Install only the apt packages that are missing.
apt_install_missing() {
  local missing=() p
  for p in "$@"; do dpkg -s "$p" &>/dev/null || missing+=("$p"); done
  if ((${#missing[@]})); then
    log "apt installing: ${missing[*]}"
    $SUDO apt-get install -y --no-install-recommends "${missing[@]}"
  else
    ok "apt packages already present: $*"
  fi
}

# Idempotently persist KEY=VALUE in ~/.pipeline_env (sourced from ~/.bashrc).
# The PATH line is always re-appended LAST so it sees KAFKA_HOME / SPARK_HOME.
set_env_var() {
  local key="$1" val="$2"
  touch "$ENV_FILE"
  sed -i "/^export ${key}=/d" "$ENV_FILE"
  sed -i '/^export PATH=.*KAFKA_HOME/d' "$ENV_FILE"
  echo "export ${key}=\"${val}\"" >> "$ENV_FILE"
  echo 'export PATH="${KAFKA_HOME:+$KAFKA_HOME/bin:}${SPARK_HOME:+$SPARK_HOME/bin:}$PATH"' >> "$ENV_FILE"
  grep -q 'pipeline_env' "$HOME/.bashrc" 2>/dev/null || \
    echo '[ -f "$HOME/.pipeline_env" ] && source "$HOME/.pipeline_env"' >> "$HOME/.bashrc"
  export "${key}=${val}"
}

# Compare a file against an Apache .sha512 (handles both file formats).
verify_sha512() {
  local file="$1" sha_file="$2" raw expected actual
  raw="$(cat "$sha_file")"
  if [[ "$raw" == *:* ]]; then expected="${raw#*:}"; else expected="${raw%% *}"; fi
  expected="$(echo "$expected" | tr -d ' \n\r' | tr 'A-F' 'a-f')"
  actual="$(sha512sum "$file" | awk '{print $1}')"
  [[ -n "$expected" && "$expected" == "$actual" ]]
}

curl_retry() {
  curl -fL --retry 3 --retry-delay 2 --connect-timeout 15 "$@" \
    || curl -4 -fL --retry 3 --retry-delay 2 --connect-timeout 15 "$@"
}

download_verified() {
  local rel="$1" dest="$2" base url
  for base in "$APACHE_MIRROR" "$APACHE_ARCHIVE"; do
    url="${base}/${rel}"
    log "Downloading ${url}"
    if curl_retry -o "$dest" "$url" && curl_retry -sS -o "${dest}.sha512" "${url}.sha512"; then
      if verify_sha512 "$dest" "${dest}.sha512"; then ok "SHA-512 verified"; return 0; fi
      warn "Checksum mismatch from ${base}"
    else
      warn "Not available at ${base}"
    fi
  done
  die "Could not download a verified copy of ${rel}"
}

# ---- .env reading and MongoDB URI resolution ----
# Print the value of KEY from a dotenv file (quotes stripped). Always succeeds.
env_get() {
  [[ -f "$2" ]] || return 0
  { grep -E "^[[:space:]]*$1=" "$2" | tail -1 | cut -d= -f2- | tr -d '\r' \
      | sed -E "s/^[[:space:]]*[\"']?//; s/[\"']?[[:space:]]*\$//"; } || true
}
# Host part of MONGO_URI (safe to print: no credentials)
mongo_host() { sed -E 's#^mongodb(\+srv)?://([^@/]*@)?([^/?]+).*#\3#' <<<"${MONGO_URI:-}"; }

if [[ "${MONGO_MODE}" == "atlas" ]]; then
  MONGO_URI="$(env_get MONGO_URI "${PROJECT_ENV_FILE}")"
fi
export MONGO_MODE MONGO_URI MONGO_DB MONGO_COLLECTION