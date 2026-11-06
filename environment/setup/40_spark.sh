#!/usr/bin/env bash
source "$(dirname "$0")/lib/common.sh"
[[ -n "${JAVA_HOME:-}" ]] || die "JAVA_HOME not set. Run 10_system_deps.sh first."

SPARK_TGZ="spark-${SPARK_VERSION}-bin-${SPARK_PACKAGE}.tgz"
SPARK_DIR="${SPARK_DIR_OVERRIDE:-${INSTALL_ROOT}/spark-${SPARK_VERSION}-bin-${SPARK_PACKAGE}}"

if [[ -x "${SPARK_DIR}/bin/spark-submit" ]]; then
  ok "Using existing Spark at ${SPARK_DIR}"
elif [[ -n "${SPARK_DIR_OVERRIDE:-}" ]]; then
  die "SPARK_DIR_OVERRIDE is set but ${SPARK_DIR}/bin/spark-submit was not found."
else
  tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
  download_verified "spark/spark-${SPARK_VERSION}/${SPARK_TGZ}" "${tmp}/${SPARK_TGZ}"
  $SUDO tar -xzf "${tmp}/${SPARK_TGZ}" -C "${INSTALL_ROOT}"
  $SUDO chown -R "$USER":"$(id -gn)" "${SPARK_DIR}"
fi

if [[ -n "${SPARK_DIR_OVERRIDE:-}" ]]; then
  set_env_var SPARK_HOME "${SPARK_DIR}"
else
  $SUDO ln -sfn "${SPARK_DIR}" "${INSTALL_ROOT}/spark"
  set_env_var SPARK_HOME "${INSTALL_ROOT}/spark"
fi
set_env_var SPARK_PACKAGES "${SPARK_PACKAGES}"
set_env_var SPARK_CHECKPOINT_DIR "${SPARK_CHECKPOINT_DIR}"
mkdir -p "${SPARK_CHECKPOINT_DIR}"

out="$("${SPARK_HOME}/bin/spark-submit" --version 2>&1 || true)"
grep -q "version ${SPARK_VERSION}" <<< "$out" \
  || die "spark-submit does not report version ${SPARK_VERSION}"
ok "Spark ${SPARK_VERSION} ready (packages: ${SPARK_PACKAGES})"