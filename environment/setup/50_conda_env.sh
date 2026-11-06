#!/usr/bin/env bash
source "$(dirname "$0")/lib/common.sh"

find_conda_base() {
  if have conda; then conda info --base; return 0; fi
  local d
  for d in "$HOME/miniconda3" "$HOME/anaconda3" "$HOME/miniforge3"; do
    if [[ -x "$d/bin/conda" ]]; then echo "$d"; return 0; fi
  done
  return 1
}

if CONDA_BASE="$(find_conda_base)"; then
  ok "Using existing conda at ${CONDA_BASE}"
else
  case "$(uname -m)" in x86_64) arch=x86_64 ;; aarch64) arch=aarch64 ;; *) die "Unsupported arch" ;; esac
  installer="$(mktemp --suffix=.sh)"
  log "Installing Miniconda (${arch})"
  curl -fL --retry 3 -o "$installer" "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-${arch}.sh"
  bash "$installer" -b -p "$HOME/miniconda3"
  rm -f "$installer"
  CONDA_BASE="$HOME/miniconda3"
  "${CONDA_BASE}/bin/conda" init bash >/dev/null
  
fi
source "${CONDA_BASE}/etc/profile.d/conda.sh"

if conda env list | awk '{print $1}' | grep -qx "${CONDA_ENV}"; then
  ok "Conda env '${CONDA_ENV}' already exists"
else
  log "Creating conda env '${CONDA_ENV}' (python ${PY_VERSION}, conda-forge)"
  conda create -y -n "${CONDA_ENV}" --override-channels -c conda-forge "python=${PY_VERSION}" pip
fi

envpip() { conda run -n "${CONDA_ENV}" --no-capture-output python -m pip "$@"; }

log "Installing Python packages"
envpip install --upgrade pip
if [[ -f "${REQUIREMENTS_FILE}" ]]; then
  log "Installing from ${REQUIREMENTS_FILE}"
  envpip install -r "${REQUIREMENTS_FILE}"
  for pkg in pymongo requests; do
    grep -Eqi "^[[:space:]]*${pkg}\b" "${REQUIREMENTS_FILE}" \
      || warn "'${pkg}' is not listed (uncommented) in requirements.txt; a grader's 'pip install -r' would miss it."
  done
else
  warn "No requirements file at ${REQUIREMENTS_FILE}; installing the fallback list."
  envpip install "${BASE_PIP_PACKAGES[@]}"
fi
envpip install "${EXTRA_PIP_PACKAGES[@]}"

PY="${CONDA_BASE}/envs/${CONDA_ENV}/bin/python"
set_env_var CONDA_BASE "${CONDA_BASE}"
set_env_var PYSPARK_PYTHON "$PY"
set_env_var PYSPARK_DRIVER_PYTHON "$PY"
set_env_var CONDA_ENV_NAME "${CONDA_ENV}"
ok "Conda env ready. Activate with: conda activate ${CONDA_ENV}"