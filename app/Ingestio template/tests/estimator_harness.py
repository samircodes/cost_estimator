"""
Test harness for the Databricks estimator notebooks.

The estimator notebooks (`notebooks/EDH_*_Estimator_Job.py`) are Databricks
notebooks, not importable modules: they call `dbutils.widgets.*` at module
scope, `%run` a pricing utility, and write to Delta via `spark`. This harness
executes the REAL notebook source in an isolated namespace with those
Databricks-only dependencies stubbed out, so the actual cost/effort logic runs
offline and its result variables can be asserted on.

Key trick: every `spark.createDataFrame(...).write...saveAsTable(...)` in the
notebooks is guarded by `if save_results:`. We always pass `save_results="false"`
so no Spark work happens — only pure Python computation runs.

No third-party deps required (no pytest, no pyspark, no network): `pyspark` is
faked in `sys.modules`, and the live Azure price fetchers are stubbed to return
their hardcoded fallbacks so results are deterministic.
"""

import contextlib
import io
import os
import sys
import types

NOTEBOOK_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "notebooks")


class _FakeWidgets:
    """Mimics dbutils.widgets: text()/dropdown() register a default, get()
    returns the scenario override if provided, else the registered default."""

    def __init__(self, overrides):
        self._overrides = dict(overrides)
        self._defaults = {}

    def text(self, name, default="", *a, **k):
        self._defaults.setdefault(name, default)

    def dropdown(self, name, default, choices=None, *a, **k):
        self._defaults.setdefault(name, default)

    def get(self, name):
        if name in self._overrides:
            return self._overrides[name]
        if name in self._defaults:
            return self._defaults[name]
        raise KeyError(f"widget '{name}' was never defined and no override was given")


class _FakeDbutils:
    def __init__(self, overrides):
        self.widgets = _FakeWidgets(overrides)


def _install_fake_pyspark():
    """Inject a minimal fake `pyspark.sql.types` so the module-level
    `from pyspark.sql.types import (...)` succeeds. The Struct* classes are
    only constructed (never used, since save_results=false), so no-op stand-ins
    that accept any args are enough."""
    if "pyspark.sql.types" in sys.modules:
        return

    def _any(*a, **k):
        return None

    types_mod = types.ModuleType("pyspark.sql.types")
    for name in ("StructType", "StructField", "StringType", "DoubleType",
                 "IntegerType", "BooleanType", "TimestampType"):
        setattr(types_mod, name, _any)

    sql_mod = types.ModuleType("pyspark.sql")
    sql_mod.types = types_mod
    root = types.ModuleType("pyspark")
    root.sql = sql_mod

    sys.modules["pyspark"] = root
    sys.modules["pyspark.sql"] = sql_mod
    sys.modules["pyspark.sql.types"] = types_mod


def load_notebook(filename, widgets):
    """Execute a notebook's real source with Databricks deps stubbed.

    `widgets` overrides specific widget values; anything not overridden falls
    back to the notebook's own registered default. `save_results` is forced to
    "false" so no Spark writes are attempted.

    Returns the notebook's global namespace (dict) for assertions.
    """
    _install_fake_pyspark()

    scenario = dict(widgets)
    scenario["save_results"] = "false"

    def fetch_vm_price(sku_name, fallback, region=None):
        return fallback

    def fetch_adls_storage_price(fallback=0.023, region=None):
        return fallback

    def fetch_egress_price(fallback=0.087, region=None):
        return fallback

    ns = {
        "__name__": "__estimator_under_test__",
        "dbutils": _FakeDbutils(scenario),
        "spark": object(),          # never dereferenced when save_results=false
        "display": lambda *a, **k: None,
        "fetch_vm_price": fetch_vm_price,
        "fetch_adls_storage_price": fetch_adls_storage_price,
        "fetch_egress_price": fetch_egress_price,
    }

    path = os.path.join(NOTEBOOK_DIR, filename)
    with open(path, "r", encoding="utf-8") as fh:
        source = fh.read()

    code = compile(source, path, "exec")
    # Silence the notebooks' many print() calls during tests.
    with contextlib.redirect_stdout(io.StringIO()):
        exec(code, ns)
    return ns
