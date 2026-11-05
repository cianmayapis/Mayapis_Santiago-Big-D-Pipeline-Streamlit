#!/usr/bin/env bash
source "$(dirname "$0")/lib/common.sh"
log "Setting up MongoDB ${MONGO_MAJOR}"

installed_major=""
if have mongod; then
  installed_major="$(mongod --version | awk '/db version/{print $3}' | tr -d v | cut -d. -f1,2)"
fi

if [[ "${installed_major}" == "${MONGO_MAJOR}" ]]; then
  ok "MongoDB ${installed_major} already installed"
else
  codename="$(. /etc/os-release; echo "${VERSION_CODENAME}")"
  keyring="/usr/share/keyrings/mongodb-server-${MONGO_MAJOR}.gpg"

  curl -fsSL "https://www.mongodb.org/static/pgp/server-${MONGO_MAJOR}.asc" \
    | $SUDO gpg --dearmor --yes -o "${keyring}"
  echo "deb [ arch=amd64,arm64 signed-by=${keyring} ] https://repo.mongodb.org/apt/ubuntu ${codename}/mongodb-org/${MONGO_MAJOR} multiverse" \
    | $SUDO tee "/etc/apt/sources.list.d/mongodb-org-${MONGO_MAJOR}.list" >/dev/null

  $SUDO apt-get update -y
  $SUDO apt-get install -y mongodb-org
fi

if [[ -d /run/systemd/system ]]; then
  $SUDO systemctl enable --now mongod
  for _ in {1..30}; do
    if mongosh --quiet --eval 'db.runCommand({ping:1}).ok' 2>/dev/null | grep -q 1; then
      ok "mongod is responding"; exit 0
    fi
    sleep 1
  done
  die "mongod did not become healthy; check: journalctl -u mongod"
else
  warn "systemd not available; start manually: sudo mongod --fork --logpath /var/log/mongod.log --dbpath /var/lib/mongodb"
fi