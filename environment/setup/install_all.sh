#!/usr/bin/env bash
# Master installer: MongoDB + Kafka + Spark + Conda env on Ubuntu.
# Usage: ./install_all.sh [--skip-mongo] [--smoke-test]
set -Eeo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SKIP_MONGO=0; SMOKE=""
for arg in "$@"; do
  case "$arg" in
    --skip-mongo)  SKIP_MONGO=1 ;;
    --smoke-test)  SMOKE="--smoke-test" ;;
    -h|--help)     sed -n '2,3p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "Unknown option: $arg"; exit 1 ;;
  esac
done

mkdir -p "${HERE}/logs"
LOG="${HERE}/logs/install_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG") 2>&1
echo "Logging to ${LOG}"

run() { echo; echo "=================== $1 ==================="; bash "${HERE}/$1"; }

run 00_preflight.sh
run 10_system_deps.sh
if (( SKIP_MONGO == 0 )); then run 20_mongodb.sh; fi
run 30_kafka.sh
run 40_spark.sh
run 50_conda_env.sh
echo; echo "=================== 60_verify.sh ==================="
bash "${HERE}/60_verify.sh" ${SMOKE}

echo
echo "Done. Open a new shell (or: source ~/.bashrc), then:"
echo "  ./services.sh start          # starts Kafka (+ topic) and MongoDB"
echo "  conda activate ${CONDA_ENV:-weather-pipeline}"