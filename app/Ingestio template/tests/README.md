# Estimator tests

Unit + validation tests for the in-app cost/effort estimators in the
`estimators/` package (`source_system.py` and `new_source.py`).

## Running

From the `app/Ingestio template` directory:

```bash
python3 -m unittest discover -s tests -v
```

No third-party dependencies are required — **no pytest, no pyspark, no
network**. The suite uses the stdlib `unittest`.

## How it works

The estimators are pure Python. Tests call `estimate(payload, prices=...)`
directly, pinning `prices` to the hardcoded fallbacks so every number is
deterministic and offline (no Azure Retail API calls). They assert on the
returned `request` / `estimation` / `combined` rows and on module constants.

## What's covered

- **Golden regression** — exact baseline outputs; an unintended model change fails.
- **Invariants** — total = sum of parts, annual = 12×monthly, ±10% variance bands, low ≤ estimate ≤ high, costs ≥ 0.
- **Monotonicity** — cost rises with volume / objects / frequency / egress; incremental < bulk.
- **SLA sizing** (Source System) — U-shaped cost in worker count, startup overhead, genuine `meets_sla` feasibility, worker cap.
- **"Not sure" defaults** (Source System) — VM→DS3, work-based cluster sizing (monotonic), moderate effort contingencies, null SLA makes no deadline claim.
- **Effort bucketing / volume tiers** — Simple/Medium/Complex thresholds.
- **Input validation** — bad enums and the bulk/CDC contradiction raise.
- **Lookup-table integrity** — the per-structure and per-method dicts stay key-aligned.

The reference notebooks under `notebooks/` are no longer executed; the
`estimators/` package is the live implementation and the unit under test.
