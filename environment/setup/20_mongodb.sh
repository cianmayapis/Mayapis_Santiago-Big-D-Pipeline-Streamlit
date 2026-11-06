#!/usr/bin/env bash
source "$(dirname "$0")/lib/common.sh"

if [[ "${MONGO_MODE}" == "atlas" ]]; then
  log "MONGO_MODE=atlas: skipping local MongoDB install"
  if [[ -f /lib/systemd/system/mongod.service || -f /usr/lib/systemd/system/mongod.service ]]; then
    log "Disabling the local mongod service (not used in Atlas mode)"
    $SUDO systemctl disable --now mongod >/dev/null 2>&1 || true
  fi
  ok "Atlas mode: the connection is verified in 60_verify.sh"
  exit 0
fi
exec bash "$(dirname "$0")/20_mongodb_local.sh"