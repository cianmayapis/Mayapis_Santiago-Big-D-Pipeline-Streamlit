#!/usr/bin/env bash
source "$(dirname "$0")/lib/common.sh"
SMOKE="${1:-}"
fail=0
check() { if "${@:2}" >/dev/null 2>&1; then ok "$1"; else warn "MISSING/FAILED: $1"; fail=1; fi; }

log "Verifying installation (MongoDB mode: ${MONGO_MODE})"
check "Java ${JAVA_VERSION}+"        "${JAVA_HOME}/bin/java" -version
check "Kafka binaries"               test -x "${KAFKA_HOME}/bin/kafka-topics.sh"
check "Kafka storage formatted"      test -f "${KAFKA_DATA}/meta.properties"
check "Spark ${SPARK_VERSION}"       bash -c "'${SPARK_HOME}/bin/spark-submit' --version 2>&1 | grep -q 'version ${SPARK_VERSION}'"
check "Python imports (pyspark, kafka, pymongo + SRV DNS, streamlit, plotly, pandas)" \
      "${PYSPARK_PYTHON}" -c "import pyspark, kafka, pymongo, dns.resolver, streamlit, streamlit_autorefresh, plotly, pandas, numpy, requests"
check "pyspark == ${SPARK_VERSION}"  "${PYSPARK_PYTHON}" -c "import pyspark,sys; sys.exit(pyspark.__version__!='${SPARK_VERSION}')"

# --- MongoDB connectivity (Atlas or local) via pymongo ---
mongo_check() {
  "${PYSPARK_PYTHON}" - <<'PY'
import os, sys
from pymongo import MongoClient
from pymongo.errors import ConfigurationError, OperationFailure, ServerSelectionTimeoutError
try:
    c = MongoClient(os.environ.get("MONGO_URI", ""), serverSelectionTimeoutMS=15000)
    c.admin.command("ping")
    print("MongoDB reachable, server version", c.server_info()["version"])
except ConfigurationError as e:
    print("MONGO_URI/DNS problem:", e); sys.exit(2)
except OperationFailure:
    print("Authentication failed: check the Atlas database user and password in .env"); sys.exit(3)
except ServerSelectionTimeoutError:
    print("Timed out: your IP is probably not in Atlas Network Access, or outbound port 27017 is blocked"); sys.exit(4)
PY
}
if [[ -z "${MONGO_URI:-}" ]]; then
  warn "MONGO_URI is empty (expected in ${PROJECT_ENV_FILE})"; fail=1
else
  log "Checking MongoDB at $(mongo_host)"
  if out="$(mongo_check 2>&1)"; then ok "$out"; else warn "$out"; fail=1; fi
fi
if [[ "${MONGO_MODE}" == "local" ]] && have mongod; then
  check "Local MongoDB ${MONGO_MAJOR}" bash -c "mongod --version | grep -q 'v${MONGO_MAJOR}'"
fi

# --- Report key library versions ---
"${PYSPARK_PYTHON}" - <<'PY' || true
import importlib.metadata as m
for p in ("pyspark", "kafka-python", "pymongo", "dnspython", "streamlit", "pandas", "plotly"):
    try: print(f"  {p:<14} {m.version(p)}")
    except Exception: print(f"  {p:<14} not found")
PY

# --- Spark <-> Kafka compatibility: which kafka-clients does the connector use? ---
POM="https://repo1.maven.org/maven2/org/apache/spark/spark-parent_2.13/${SPARK_VERSION}/spark-parent_2.13-${SPARK_VERSION}.pom"
client_ver="$(curl -fsSL "$POM" 2>/dev/null | grep -oP '(?<=<kafka.version>)[^<]+' | head -1 || true)"
if [[ -n "$client_ver" ]]; then
  log "Connector ${SPARK_KAFKA_PACKAGE} uses kafka-clients ${client_ver}; broker is ${KAFKA_VERSION}"
  newest="$(printf '%s\n%s\n' "$client_ver" "$KAFKA_VERSION" | sort -V | tail -1)"
  if [[ "$newest" == "$KAFKA_VERSION" ]]; then ok "Client ${client_ver} <= broker ${KAFKA_VERSION}: compatible"
  else warn "Client ${client_ver} is NEWER than broker ${KAFKA_VERSION}; consider a newer Kafka."; fi
else
  warn "Could not read kafka-clients version from Maven; use --smoke-test instead."
fi

# --- Optional end-to-end smoke test: Kafka -> Spark -> MongoDB ---
if [[ "$SMOKE" == "--smoke-test" ]]; then
  log "Smoke test: Kafka -> Spark -> MongoDB (${MONGO_MODE})"
  bash "${SCRIPT_ROOT}/services.sh" start all
  TOPIC="smoke_$(date +%s)"
  "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --create --topic "$TOPIC" >/dev/null
  echo '{"ok":1}' | "${KAFKA_HOME}/bin/kafka-console-producer.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$TOPIC"

  tmp_py="$(mktemp --suffix=.py)"; tmp_out="$(mktemp)"
  cat > "$tmp_py" <<PY
import os
from pyspark.sql import SparkSession
uri = os.environ["MONGO_URI"]          # read from the environment: never written to a file
s = (SparkSession.builder.master("local[1]").appName("smoke")
     .config("spark.mongodb.read.connection.uri", uri)
     .config("spark.mongodb.write.connection.uri", uri)
     .getOrCreate())
df = (s.read.format("kafka")
        .option("kafka.bootstrap.servers", "${KAFKA_BOOTSTRAP}")
        .option("subscribe", "${TOPIC}")
        .option("startingOffsets", "earliest").load())
print("SMOKE_KAFKA_COUNT", df.count())
(df.selectExpr("CAST(value AS STRING) AS value")
   .write.format("mongodb").mode("append")
   .option("database", "smoke_test").option("collection", "${TOPIC}").save())
back = (s.read.format("mongodb")
          .option("database", "smoke_test").option("collection", "${TOPIC}").load())
print("SMOKE_MONGO_COUNT", back.count())
PY
  "${SPARK_HOME}/bin/spark-submit" --packages "${SPARK_PACKAGES}" \
    --conf "spark.redaction.regex=(?i)secret|password|token|access[.]key|uri" \
    "$tmp_py" > "$tmp_out" 2>&1 || true
  if grep -q "SMOKE_KAFKA_COUNT 1" "$tmp_out"; then ok "Spark read from Kafka"
  else warn "Kafka -> Spark FAILED"; fail=1; fi
  if grep -q "SMOKE_MONGO_COUNT 1" "$tmp_out"; then ok "Spark wrote to and read from MongoDB (connector OK)"
  else warn "Spark -> MongoDB FAILED"; fail=1; fi
  if (( fail )); then
    warn "Last log lines (connection strings masked):"
    tail -n 30 "$tmp_out" | sed -E 's#mongodb(\+srv)?://[^[:space:]"]+#mongodb://***#g' >&2
  fi

  "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --delete --topic "$TOPIC" >/dev/null || true
  "${PYSPARK_PYTHON}" -c 'import os; from pymongo import MongoClient; MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=15000).drop_database("smoke_test")' >/dev/null 2>&1 || true
  rm -f "$tmp_py" "$tmp_out"
fi

if (( fail == 0 )); then ok "All checks passed"; else die "Some checks failed; see warnings above."; fi