#!/usr/bin/env bash
set -euo pipefail

WORK_POOL_NAME="${1:-dataops-managed-pool}"
AUTOMATION_FILE="${2:-orchestration/prefect/automations.yaml}"

echo "Ensuring work pool exists: ${WORK_POOL_NAME}"
if prefect work-pool inspect "${WORK_POOL_NAME}" >/dev/null 2>&1; then
  echo "Work pool already exists."
else
  prefect work-pool create "${WORK_POOL_NAME}" --type prefect:managed
fi

echo "Deploying flows from prefect.yaml"
prefect deploy --all --prefect-file prefect.yaml

if [ -f "${AUTOMATION_FILE}" ]; then
  echo "Creating automations from ${AUTOMATION_FILE}"
  prefect automation create --from-file "${AUTOMATION_FILE}"
else
  echo "Automation file not found: ${AUTOMATION_FILE}"
fi

cat <<'EOF'
Bootstrap complete.
Next steps:
1) Verify deployment schedules and run history in Prefect Cloud UI.
2) Verify failed-flow and missed-run automations are enabled and targeting your notification blocks.
EOF
