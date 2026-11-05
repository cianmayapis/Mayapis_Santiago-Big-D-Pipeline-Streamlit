#!/usr/bin/env bash
source "$(dirname "$0")/lib/common.sh"
[[ -n "${JAVA_HOME:-}" ]] || die "JAVA_HOME not set. Run 10_system_deps.sh first."

SPARK_TGZ="spark-${SPARK_VERSION}-bin-${SPARK_PACKAGE}.tgz"
SPARK_DIR="${INSTALL_ROOT}/spark-${SPARK_VERSION}-bin-${SPARK_PACKAGE}"

if [[ -x "${SPARK_DIR}/bin/spark-submit" ]]; then
  ok "Spark ${SPARK_VERSION} already installed at ${SPARK_DIR}"
else
  tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
  download_verified "spark/spark-${SPARK_VERSION}/${SPARK_TGZ}" "${tmp}/${SPARK_TGZ}"
  $SUDO tar -xzf "${tmp}/${SPARK_TGZ}" -C "${INSTALL_ROOT}"
  $SUDO chown -R "$USER":"$(id -gn)" "${SPARK_DIR}"
fi
$SUDO ln -sfn "${SPARK_DIR}" "${INSTALL_ROOT}/spark"
set_env_var SPARK_HOME "${INSTALL_ROOT}/spark"
set_env_var SPARK_KAFKA_PACKAGE "${SPARK_KAFKA_PACKAGE}"

out="$("${INSTALL_ROOT}/spark/bin/spark-submit" --version 2>&1 || true)"
grep -q "version ${SPARK_VERSION}" <<< "$out" \
  || die "spark-submit does not report version ${SPARK_VERSION}"
ok "Spark ${SPARK_VERSION} ready (Kafka package: ${SPARK_KAFKA_PACKAGE})"