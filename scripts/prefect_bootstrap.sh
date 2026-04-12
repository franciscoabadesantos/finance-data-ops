#!/usr/bin/env bash
set -euo pipefail

WORK_POOL_NAME="${1:-dataops-pool}"
AUTOMATION_FILE="${2:-orchestration/prefect/automations.yaml}"
BACKFILL_QUEUE_NAME="${3:-ticker-backfill}"
BACKFILL_QUEUE_LIMIT="${4:-4}"

echo "Ensuring work pool exists: ${WORK_POOL_NAME}"
if prefect work-pool inspect "${WORK_POOL_NAME}" >/dev/null 2>&1; then
  echo "Work pool already exists."
else
  prefect work-pool create "${WORK_POOL_NAME}" --type process
fi

echo "Deploying flows from prefect.yaml"
prefect deploy --all --prefect-file prefect.yaml

echo "Ensuring backfill queue exists: ${BACKFILL_QUEUE_NAME}"
if prefect work-queue inspect "${BACKFILL_QUEUE_NAME}" >/dev/null 2>&1; then
  echo "Backfill queue already exists."
else
  prefect work-queue create "${BACKFILL_QUEUE_NAME}" --pool "${WORK_POOL_NAME}" --limit "${BACKFILL_QUEUE_LIMIT}"
fi

if [ -f "${AUTOMATION_FILE}" ]; then
  echo "Creating automations from ${AUTOMATION_FILE}"
  prefect automation create --from-file "${AUTOMATION_FILE}"
else
  echo "Automation file not found: ${AUTOMATION_FILE}"
fi

cat <<'EOF'
Bootstrap complete.
Next steps:
1) Start a worker in your infrastructure:
   prefect worker start --pool dataops-pool
2) Verify deployment schedules, queue concurrency, and run history in Prefect Cloud UI.
EOF
