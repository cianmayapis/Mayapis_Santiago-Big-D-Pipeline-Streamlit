#!/usr/bin/env bash
# Usage: ./70_test_requirements.sh [path/to/requirements.txt]
# Fresh env + "pip install -r requirements.txt" ONLY, then import-check app.py/producer.py.
source "$(dirname "$0")/lib/common.sh"
REQ="${1:-$REQUIREMENTS_FILE}"
[[ -f "$REQ" ]] || die "requirements file not found: $REQ"
[[ -n "${CONDA_BASE:-}" ]] || die "CONDA_BASE not set. Run 50_conda_env.sh first."
source "${CONDA_BASE}/etc/profile.d/conda.sh"

TEST_ENV="${CONDA_ENV}-reqtest"
cleanup() { conda env remove -y -n "$TEST_ENV" >/dev/null 2>&1 || true; }
cleanup; trap cleanup EXIT

log "Creating clean env '${TEST_ENV}' (python ${PY_VERSION})"
conda create -y -n "$TEST_ENV" --override-channels -c conda-forge "python=${PY_VERSION}" pip >/dev/null
run() { conda run -n "$TEST_ENV" --no-capture-output "$@"; }

log "pip install -r ${REQ}"
run python -m pip install -r "$REQ"
run python -m pip check && ok "pip check: no broken dependencies"

dir="$(dirname "$REQ")"; files=()
for f in app.py producer.py; do [[ -f "$dir/$f" ]] && files+=("$dir/$f"); done
if ((${#files[@]} == 0)); then
  warn "No app.py/producer.py next to requirements.txt; skipping import check."
else
  run python - "${files[@]}" <<'PY'
import ast, sys, importlib.util, pathlib
missing = set()
for f in sys.argv[1:]:
    for n in ast.walk(ast.parse(pathlib.Path(f).read_text())):
        mods = []
        if isinstance(n, ast.Import):
            mods = [a.name.split(".")[0] for a in n.names]
        elif isinstance(n, ast.ImportFrom) and n.level == 0 and n.module:
            mods = [n.module.split(".")[0]]
        for m in mods:
            if m not in sys.stdlib_module_names and importlib.util.find_spec(m) is None:
                missing.add(m)
if missing:
    print("MISSING from requirements.txt:", ", ".join(sorted(missing))); sys.exit(1)
print("All third-party imports are covered by requirements.txt")
PY
  ok "Import check passed"
fi