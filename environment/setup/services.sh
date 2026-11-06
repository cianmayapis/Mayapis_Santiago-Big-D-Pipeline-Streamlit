#!/usr/bin/env bash
# Usage: ./services.sh {start|stop|status} [kafka|mongo|all]
source "$(dirname "$0")/lib/common.sh"
action="${1:?usage: services.sh start, stop or status, optionally followed by kafka, mongo or all}"
target="${2:-all}"

want() { [[ "$target" == "all" || "$target" == "$1" ]]; }
kafka_up() { nc -z localhost 9092 2>/dev/null; }

ensure_topic() {
  "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create --if-not-exists --topic "$KAFKA_TOPIC" \
    --partitions "$KAFKA_PARTITIONS" --replication-factor 1 >/dev/null
  ok "Topic '${KAFKA_TOPIC}' ready"
}

kafka_start() {
  if kafka_up; then
    ok "Kafka already running"
  else
    "${KAFKA_HOME}/bin/kafka-server-start.sh" -daemon "${KAFKA_CONFIG}"
    local i
    for i in {1..60}; do kafka_up && break; sleep 1; done
    kafka_up || die "Kafka failed to start; see ${KAFKA_HOME}/logs/server.log"
    ok "Kafka up on :9092"
  fi
  ensure_topic
}
kafka_stop() { "${KAFKA_HOME}/bin/kafka-server-stop.sh" || true; ok "Kafka stopped"; }

mongo_start() {
  if [[ "${MONGO_MODE}" == "atlas" ]]; then ok "MongoDB: Atlas (remote), nothing to start"
  else $SUDO systemctl start mongod; ok "MongoDB started"; fi
}
mongo_stop() {
  if [[ "${MONGO_MODE}" == "atlas" ]]; then ok "MongoDB: Atlas (remote), nothing to stop"
  else $SUDO systemctl stop mongod; ok "MongoDB stopped"; fi
}

status() {
  if kafka_up; then ok "Kafka: running"; else warn "Kafka: stopped"; fi
  if [[ "${MONGO_MODE}" == "atlas" ]]; then ok "MongoDB: Atlas at $(mongo_host)"
  elif systemctl is-active --quiet mongod; then ok "MongoDB: running"
  else warn "MongoDB: stopped"; fi
}

case "$action" in
  start)
    if want kafka; then kafka_start; fi
    if want mongo; then mongo_start; fi ;;
  stop)
    if want kafka; then kafka_stop; fi
    if want mongo; then mongo_stop; fi ;;
  status) status ;;
  *) die "Unknown action: $action" ;;
esac