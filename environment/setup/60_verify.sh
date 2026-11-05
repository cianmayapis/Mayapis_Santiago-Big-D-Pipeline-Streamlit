#!/usr/bin/env bash
source "$(dirname "$0")/lib/common.sh"
SMOKE="${1:-}"
fail=0
check() { if "${@:2}" >/dev/null 2>&1; then ok "$1"; else warn "MISSING/FAILED: $1"; fail=1; fi; }

log "Verifying installation"
check "Java ${JAVA_VERSION}+"        "${JAVA_HOME}/bin/java" -version
check "Kafka binaries"               test -x "${KAFKA_HOME}/bin/kafka-topics.sh"
check "Kafka storage formatted"      test -f "${KAFKA_DATA}/meta.properties"
check "Spark ${SPARK_VERSION}"       bash -c "'${SPARK_HOME}/bin/spark-submit' --version 2>&1 | grep -q 'version ${SPARK_VERSION}'"
check "Python imports (pyspark, kafka, pymongo, streamlit, plotly, pandas)" \
      "${PYSPARK_PYTHON}" -c "import pyspark, kafka, pymongo, streamlit, streamlit_autorefresh, plotly, pandas, numpy, requests"
check "pyspark == ${SPARK_VERSION}"  "${PYSPARK_PYTHON}" -c "import pyspark,sys; sys.exit(pyspark.__version__!='${SPARK_VERSION}')"
if have mongod; then
  check "MongoDB ${MONGO_MAJOR}" bash -c "mongod --version | grep -q 'v${MONGO_MAJOR}'"
else
  warn "mongod not installed (skipped?)"
fi

# --- Report key library versions ---
"${PYSPARK_PYTHON}" - <<'PY' || true
import importlib.metadata as m
for p in ("pyspark", "kafka-python", "pymongo", "streamlit", "pandas", "plotly"):
    try: print(f"  {p:<14} {m.version(p)}")
    except Exception: print(f"  {p:<14} not found")
PY

# --- Spark <-> Kafka compatibility: which kafka-clients does the connector use? ---
POM="https://repo1.maven.org/maven2/org/apache/spark/spark-parent_2.13/${SPARK_VERSION}/spark-parent_2.13-${SPARK_VERSION}.pom"
client_ver="$(curl -fsSL "$POM" 2>/dev/null | grep -oP '(?<=<kafka.version>)[^<]+' | head -1 || true)"
if [[ -n "$client_ver" ]]; then
  log "Connector ${SPARK_KAFKA_PACKAGE} uses kafka-clients ${client_ver}; broker is ${KAFKA_VERSION}"
  newest="$(printf '%s\n%s\n' "$client_ver" "$KAFKA_VERSION" | sort -V | tail -1)"
  if [[ "$newest" == "$KAFKA_VERSION" ]]; then
    ok "Client ${client_ver} <= broker ${KAFKA_VERSION}: compatible"
  else
    warn "Client ${client_ver} is NEWER than broker ${KAFKA_VERSION}; consider a newer Kafka."
  fi
else
  warn "Could not read kafka-clients version from Maven; use --smoke-test instead."
fi

# --- Optional end-to-end smoke test: produce 1 message, read it with Spark ---
if [[ "$SMOKE" == "--smoke-test" ]]; then
  log "Smoke test: Kafka -> Spark"
  bash "${SCRIPT_ROOT}/services.sh" start kafka
  TOPIC="smoke_$(date +%s)"
  "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --create --topic "$TOPIC" >/dev/null
  echo '{"ok":1}' | "${KAFKA_HOME}/bin/kafka-console-producer.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$TOPIC"

  tmp_py="$(mktemp --suffix=.py)"; tmp_out="$(mktemp)"
  cat > "$tmp_py" <<PY
from pyspark.sql import SparkSession
s = SparkSession.builder.master("local[1]").appName("smoke").getOrCreate()
df = (s.read.format("kafka")
        .option("kafka.bootstrap.servers", "${KAFKA_BOOTSTRAP}")
        .option("subscribe", "${TOPIC}")
        .option("startingOffsets", "earliest").load())
print("SMOKE_COUNT", df.count())
PY
  "${SPARK_HOME}/bin/spark-submit" --packages "${SPARK_KAFKA_PACKAGE}" "$tmp_py" > "$tmp_out" 2>&1 || true
  if grep -q "SMOKE_COUNT 1" "$tmp_out"; then
    ok "Spark read the message from Kafka (package + broker are compatible)"
  else
    warn "Smoke test FAILED. Last log lines:"; tail -n 25 "$tmp_out" >&2; fail=1
  fi
  "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --delete --topic "$TOPIC" >/dev/null || true
  rm -f "$tmp_py" "$tmp_out"
fi

if (( fail == 0 )); then ok "All checks passed"; else die "Some checks failed; see warnings above."; fi