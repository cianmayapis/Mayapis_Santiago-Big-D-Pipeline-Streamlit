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
  keyring="/usr/share/keyrings/mongodb-server-${MONGO_MAJOR}.gpg"
  list_file="/etc/apt/sources.list.d/mongodb-org-${MONGO_MAJOR}.list"

  curl -fsSL "https://www.mongodb.org/static/pgp/server-${MONGO_MAJOR}.asc" \
    | $SUDO gpg --dearmor --yes -o "${keyring}"

  add_repo() {   # $1 = Ubuntu codename to use for the MongoDB repo path
    echo "deb [ arch=amd64,arm64 signed-by=${keyring} ] ${repo_base} $1/mongodb-org/${MONGO_MAJOR} multiverse" \
      | $SUDO tee "${list_file}" >/dev/null
    $SUDO apt-get update -y || true
  }

  add_repo "${codename}"
  # A repo index can exist without packages in it (new Ubuntu releases): check for real packages
  if ! apt-cache show mongodb-org-server >/dev/null 2>&1; then
    warn "MongoDB ${MONGO_MAJOR} repo for '${codename}' has no packages; using 'noble' packages instead."
    add_repo noble
  fi
  apt-cache show mongodb-org-server >/dev/null 2>&1 \
    || die "MongoDB packages not found in ${repo_base} (tried '${codename}' and 'noble'). Check: apt-get update output."

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