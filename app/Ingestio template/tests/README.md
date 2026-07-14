# Estimator tests

Unit + validation tests for the cost/effort logic in
`notebooks/EDH_New_Source_Estimator_Job.py` and
`notebooks/EDH_Source_System_Estimator_Job.py`.

## Running

From the `app/Ingestio template` directory:

```bash
python3 -m unittest discover -s tests -v
```

No third-party dependencies are required — **no pytest, no pyspark, no
network**. The suite uses the stdlib `unittest`.

## How it works

The estimators are Databricks notebooks (they call `dbutils.widgets.*` at module
scope, `%run` a pricing utility, and write Delta tables via `spark`), so they
can't be imported directly. `estimator_harness.py` executes the **real notebook
source** in an isolated namespace with the Databricks-only pieces stubbed:

- `dbutils.widgets` is faked; a scenario dict supplies widget values.
- `save_results` is forced to `"false"`, which short-circuits every
  `spark ... saveAsTable(...)` block — so no Spark is needed.
- `pyspark.sql.types` is faked in `sys.modules` (the `StructType`s are built but
  never used when `save_results=false`).
- The live Azure price fetchers (`fetch_vm_price`, `fetch_adls_storage_price`,
  `fetch_egress_price`) are stubbed to return their hardcoded fallbacks, so
  every number is deterministic and offline.

Tests then assert on the notebook's resulting global variables.

## What's covered

- **Golden regression** — exact baseline outputs; an unintended model change fails.
- **Invariants** — total = sum of parts, annual = 12×monthly, ±10% variance bands, low ≤ estimate ≤ high, costs ≥ 0.
- **Monotonicity** — cost rises with volume / objects / frequency / egress; incremental < bulk.
- **Effort bucketing** — Simple/Medium/Complex thresholds.
- **Input validation** — bad enums and the bulk/CDC contradiction raise.
- **Lookup-table integrity** — the per-structure and per-method dicts stay key-aligned.
- **VM sizing** — a larger `vm_type` raises throughput and lowers runtime in both estimators.
