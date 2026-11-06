#!/usr/bin/env bash
source "$(dirname "$0")/lib/common.sh"
log "Installing base system dependencies"

$SUDO apt-get update -y
apt_install_missing ca-certificates curl wget gnupg lsb-release tar gzip unzip jq \
                    netcat-openbsd procps "openjdk-${JAVA_VERSION}-jdk-headless"

JAVA_HOME_DIR="/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-$(dpkg --print-architecture)"
[[ -x "${JAVA_HOME_DIR}/bin/java" ]] || die "JDK ${JAVA_VERSION} not found at ${JAVA_HOME_DIR}"

major="$("${JAVA_HOME_DIR}/bin/java" -version 2>&1 | awk -F'[".]' '/version/{print $2; exit}')"
(( major >= JAVA_VERSION )) || die "Java ${major} < required ${JAVA_VERSION}"

set_env_var JAVA_HOME "${JAVA_HOME_DIR}"
mkdir -p "${DATA_ROOT}"
ok "Java ${major} ready (JAVA_HOME=${JAVA_HOME_DIR})"