#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_BIN="${DATA_OPS_SCHEMA_PYTHON:-$REPO_ROOT/.venv/bin/python}"

: "${FINANCE_SCHEMA_DATABASE_DSN:?FINANCE_SCHEMA_DATABASE_DSN must contain the owner/admin PostgreSQL DSN}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing Data Ops Python executable at $PYTHON_BIN" >&2
  exit 1
fi

echo "==> Reconciling Data Ops entity-identity schema"
(
  cd "$REPO_ROOT"
  DATA_OPS_DATABASE_URL="$FINANCE_SCHEMA_DATABASE_DSN" \
    "$PYTHON_BIN" scripts/reconcile_entity_identity_schema.py --apply
)

echo "==> Reconciling Data Ops ETF schema"
(
  cd "$REPO_ROOT"
  "$PYTHON_BIN" scripts/reconcile_etf_canonical_schema.py \
    --database-dsn "$FINANCE_SCHEMA_DATABASE_DSN" \
    --apply
)
