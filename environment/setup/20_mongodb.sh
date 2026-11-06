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
  repo_base="https://repo.mongodb.org/apt/ubuntu"
  # Brand-new Ubuntu releases may not have a MongoDB repo yet: fall back to noble
  if ! curl -fsI --max-time 20 "${repo_base}/dists/${codename}/mongodb-org/${MONGO_MAJOR}/InRelease" >/dev/null 2>&1; then
    warn "No MongoDB ${MONGO_MAJOR} repo for '${codename}'; using 'noble' packages instead."
    codename=noble
  fi
  keyring="/usr/share/keyrings/mongodb-server-${MONGO_MAJOR}.gpg"

  curl -fsSL "https://www.mongodb.org/static/pgp/server-${MONGO_MAJOR}.asc" \
    | $SUDO gpg --dearmor --yes -o "${keyring}"
  echo "deb [ arch=amd64,arm64 signed-by=${keyring} ] ${repo_base} ${codename}/mongodb-org/${MONGO_MAJOR} multiverse" \
    | $SUDO tee "/etc/apt/sources.list.d/mongodb-org-${MONGO_MAJOR}.list" >/dev/null

  $SUDO apt-get update -y
  if ! $SUDO apt-get install -y mongodb-org; then
    warn "Latest mongodb-org failed to install; retrying with pinned 8.0.20 packages."
    $SUDO apt-get install -y --allow-downgrades \
      mongodb-org=8.0.20 mongodb-org-database=8.0.20 mongodb-org-server=8.0.20 \
      mongodb-mongosh mongodb-org-mongos=8.0.20 mongodb-org-tools=8.0.20
  fi
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