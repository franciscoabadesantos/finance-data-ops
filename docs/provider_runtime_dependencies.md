# Provider Runtime Dependency Audit

Audit date: 2026-04-19

Scope:
- `src/finance_data_ops/providers/**`
- targeted search for `subprocess`, `os.system`, `shell=True`, `shutil.which`, `Popen`, `run`, `check_output`, `curl`, and `wget`

## Findings

### Explicit binary dependency

- [`src/finance_data_ops/providers/release_calendar.py`](/home/franciscosantos/finance-data-ops/src/finance_data_ops/providers/release_calendar.py)
  - Uses `subprocess.run([... \"curl\", ...])` in `_fetch_text_via_curl(...)`
  - Runtime dependency: `curl` must be present in the worker image
  - Current mitigation: [`services/finance-jobs-worker/Dockerfile`](/home/franciscosantos/finance-data-ops/services/finance-jobs-worker/Dockerfile) installs `curl`

### No other provider-level shell dependencies found

The audit found no additional provider files using:
- `subprocess`
- `os.system`
- `shell=True`
- `shutil.which`
- `Popen`
- `check_output`
- `wget`

## Operational note

Any new provider that shells out to a system binary must declare that binary in the runtime image explicitly. Do not rely on base-image defaults.
