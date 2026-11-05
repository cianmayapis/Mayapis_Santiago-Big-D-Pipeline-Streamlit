#!/usr/bin/env bash
source "$(dirname "$0")/lib/common.sh"
[[ -n "${JAVA_HOME:-}" ]] || die "JAVA_HOME not set. Run 10_system_deps.sh first."

KAFKA_TGZ="kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz"
KAFKA_DIR="${INSTALL_ROOT}/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}"

if [[ -x "${KAFKA_DIR}/bin/kafka-server-start.sh" ]]; then
  ok "Kafka ${KAFKA_VERSION} already installed at ${KAFKA_DIR}"
else
  tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
  download_verified "kafka/${KAFKA_VERSION}/${KAFKA_TGZ}" "${tmp}/${KAFKA_TGZ}"
  $SUDO tar -xzf "${tmp}/${KAFKA_TGZ}" -C "${INSTALL_ROOT}"
  $SUDO chown -R "$USER":"$(id -gn)" "${KAFKA_DIR}"
fi
$SUDO ln -sfn "${KAFKA_DIR}" "${INSTALL_ROOT}/kafka"
set_env_var KAFKA_HOME "${INSTALL_ROOT}/kafka"

# --- Locate the KRaft config (4.x: config/server.properties) ---
CFG=""
for f in "${KAFKA_DIR}/config/server.properties" "${KAFKA_DIR}/config/kraft/server.properties"; do
  if [[ -f "$f" ]] && grep -q '^process.roles' "$f"; then CFG="$f"; break; fi
done
[[ -n "$CFG" ]] || die "No KRaft server.properties found in ${KAFKA_DIR}/config"
set_env_var KAFKA_CONFIG "$CFG"

# --- Keep data out of /tmp so it survives reboots ---
[[ -f "${CFG}.orig" ]] || cp "$CFG" "${CFG}.orig"
mkdir -p "${KAFKA_DATA}"
sed -i "s|^log.dirs=.*|log.dirs=${KAFKA_DATA}|" "$CFG"

# --- Format storage once (single-node KRaft) ---
if [[ -f "${KAFKA_DATA}/meta.properties" ]]; then
  ok "Kafka storage already formatted"
else
  CLUSTER_ID="$("${KAFKA_DIR}/bin/kafka-storage.sh" random-uuid)"
  log "Formatting storage (cluster id ${CLUSTER_ID})"
  "${KAFKA_DIR}/bin/kafka-storage.sh" format --standalone -t "$CLUSTER_ID" -c "$CFG" \
    || "${KAFKA_DIR}/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CFG"
fi
ok "Kafka ${KAFKA_VERSION} ready"