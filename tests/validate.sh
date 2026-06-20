#!/usr/bin/env bash
# Static validation — no cluster, no Docker required. Parses every manifest, checks the
# Avro schemas, verifies the SOPS file is encrypted + the age key isn't tracked, and runs
# the Python unit tests when their deps are present. Tools that aren't installed are
# SKIPPED, not failed.
#
#   tests/validate.sh
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
pass=0; fail=0; skip=0
ok()   { printf '  [ OK ] %s\n' "$1"; pass=$((pass + 1)); }
no()   { printf '  [FAIL] %s\n' "$1"; fail=$((fail + 1)); }
skipt(){ printf '  [SKIP] %s\n' "$1"; skip=$((skip + 1)); }
section() { printf '\n== %s ==\n' "$1"; }

PYTHON="${PYTHON:-}"
if [ -z "$PYTHON" ]; then
  for p in .venv/Scripts/python.exe .venv/bin/python python3 python; do
    if command -v "$p" >/dev/null 2>&1; then PYTHON="$p"; break; fi
  done
fi

section "YAML / manifest parse"
if [ -n "$PYTHON" ] && "$PYTHON" -c "import yaml" >/dev/null 2>&1; then
  if "$PYTHON" - <<'PY'
import glob, sys, yaml
files = (glob.glob('ansible/**/*.y*ml', recursive=True)
         + glob.glob('streaming/*.yaml') + glob.glob('local/*.yml') + ['.sops.yaml'])
bad = 0
for f in sorted(set(files)):
    try:
        list(yaml.safe_load_all(open(f, encoding='utf-8')))
    except Exception as e:
        bad += 1; print(f'    {f}: {e}')
sys.exit(1 if bad else 0)
PY
  then ok "all Ansible + Spark + compose YAML parse"; else no "some YAML failed to parse"; fi
else
  skipt "no python with PyYAML available (YAML parse)"
fi

section "Avro schemas"
if [ -n "$PYTHON" ]; then
  if "$PYTHON" - <<'PY'
import json, glob, sys
bad = 0
for f in glob.glob('schemas/*.avsc'):
    try:
        s = json.load(open(f, encoding='utf-8'))
        assert s.get('type') == 'record' and s.get('fields')
    except Exception as e:
        bad += 1; print(f'    {f}: {e}')
sys.exit(1 if bad else 0)
PY
  then ok "Avro schemas are valid records"; else no "an .avsc failed to parse"; fi
else
  skipt "no python available (Avro schema check)"
fi

section "Secrets hygiene"
SOPS_FILE="ansible/secrets/secrets.sops.yaml"
if [ -f "$SOPS_FILE" ]; then
  if grep -q 'ENC\[' "$SOPS_FILE" && grep -q '^sops:' "$SOPS_FILE"; then
    ok "secrets.sops.yaml is SOPS-encrypted"
  else
    no "secrets.sops.yaml exists but is NOT encrypted (do not commit plaintext!)"
  fi
else
  skipt "secrets.sops.yaml not present (create from secrets.sops.example.yaml)"
fi
if command -v git >/dev/null 2>&1 && git ls-files --error-unmatch age.key >/dev/null 2>&1; then
  no "age.key is tracked by git (it must be gitignored!)"
else
  ok "age private key is not tracked by git"
fi

section "Python unit tests"
if [ -n "$PYTHON" ] && "$PYTHON" -c "import pytest, fastavro" >/dev/null 2>&1; then
  # Point Spark's worker at the same interpreter so the transform tests run (not skip/fail).
  if PYSPARK_PYTHON="$PYTHON" PYSPARK_DRIVER_PYTHON="$PYTHON" "$PYTHON" -m pytest -q >/dev/null 2>&1; then
    ok "pytest unit tests pass"
  else
    no "pytest unit tests failed (run: pytest -v)"
  fi
else
  skipt "pytest/fastavro not installed (pip install -r requirements-dev.txt)"
fi

section "Ansible syntax"
if command -v ansible-playbook >/dev/null 2>&1 && [ -f ansible/inventory/hosts.ini ]; then
  if ( cd ansible && ANSIBLE_CONFIG="$PWD/ansible.cfg" ansible-playbook -i inventory/hosts.ini site.yml --syntax-check >/dev/null 2>&1 ); then
    ok "ansible-playbook --syntax-check passed"
  else
    no "ansible-playbook --syntax-check failed"
  fi
elif ! command -v ansible-playbook >/dev/null 2>&1; then
  skipt "ansible-playbook not found (run from WSL)"
else
  skipt "ansible/inventory/hosts.ini not present (copy from the spark-k8s repo)"
fi

printf '\n== RESULT: %d passed, %d failed, %d skipped ==\n' "$pass" "$fail" "$skip"
[ "$fail" -eq 0 ]
