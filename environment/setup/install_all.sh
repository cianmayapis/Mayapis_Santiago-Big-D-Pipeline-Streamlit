#!/usr/bin/env bash
# Master installer: Kafka + Spark + Conda env (+ local MongoDB if MONGO_MODE=local).
# Usage: ./install_all.sh [--skip-mongo] [--smoke-test] [--test-requirements]
set -Eeo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${HERE}/lib/common.sh"          # loads config.env, helpers and the Mongo URI from .env

SKIP_MONGO=0; SMOKE=""; TEST_REQS=0
for arg in "$@"; do
  case "$arg" in
    --skip-mongo)        SKIP_MONGO=1 ;;
    --smoke-test)        SMOKE="--smoke-test" ;;
    --test-requirements) TEST_REQS=1 ;;
    -h|--help)           sed -n '2,3p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "Unknown option: $arg"; exit 1 ;;
  esac
done

mkdir -p "${HERE}/logs"
LOG="${HERE}/logs/install_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG") 2>&1
echo "Logging to ${LOG}"

if [[ "${MONGO_MODE}" == "atlas" ]]; then
  [[ -f "${PROJECT_ENV_FILE}" ]] || die "MONGO_MODE=atlas but ${PROJECT_ENV_FILE} was not found."
  [[ "${MONGO_URI}" =~ ^mongodb(\+srv)?:// ]] || die "MONGO_URI is missing or malformed in ${PROJECT_ENV_FILE}."
  ok "Atlas mode: MONGO_URI found in .env (host: $(mongo_host))"
fi

run() { echo; echo "=================== $1 ==================="; bash "${HERE}/$1"; }

run 00_preflight.sh
run 10_system_deps.sh
if (( SKIP_MONGO == 0 )); then run 20_mongodb.sh; fi
run 30_kafka.sh
run 40_spark.sh
run 50_conda_env.sh
echo; echo "=================== 60_verify.sh ==================="
bash "${HERE}/60_verify.sh" ${SMOKE}
if (( TEST_REQS )); then run 70_test_requirements.sh; fi

echo
echo "Done. Open a new shell (or: source ~/.bashrc), then:"
echo "  ./services.sh start          # Kafka (+ topic); MongoDB is Atlas, nothing local to start"
echo "  conda activate ${CONDA_ENV}"