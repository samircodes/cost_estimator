"""Unit + validation tests for the Source System estimator.

Run:  python3 -m unittest discover -s tests -v
(from the "Ingestio template" directory; no pytest/pyspark/network needed.)

Tests call the live estimator module (estimators.source_system) directly with
prices pinned to the hardcoded fallbacks, so results are deterministic.
"""

import math
import unittest

from estimators import source_system as ss

# Pin prices to the hardcoded fallbacks so numbers are deterministic and offline.
PRICES = {"Standard_DS3_v2": 0.293, "Standard_DS5_v2": 1.17,
          "storage_per_gb": 0.023, "egress_per_gb": 0.087}

BASE = {
    "request_id":            "r-test",
    "ingestion_method":      "Operational Database",
    "source_system":         "IMS-RS",
    "data_structure":        "Sql Server",
    "source_objects":        "orders",
    "edh_table_names":       "edh_orders",
    "additional_gb":         "100",
    "ingestion_frequency":   "Daily",
    "load_type":             "Bulk",
    "primary_key_available": "Yes",
    "delete_handling":       "Ignore",
    "schema_stability":      "Stable",
    "cdc_method":            "Not Applicable",
    "vm_type":               "Standard_DS3_v2",
    "contains_phi":          "No",
}


def run(**overrides):
    """Returns a flat dict of the estimation row plus the combined variance
    bands."""
    payload = dict(BASE)
    payload.update(overrides)
    result = ss.estimate(payload, prices=PRICES)
    flat = dict(result["estimation"])
    for k, v in result["combined"].items():
        if k.endswith("_low") or k.endswith("_high"):
            flat[k] = v
    return flat


class GoldenValues(unittest.TestCase):
    """Regression lock on the baseline request (DS3 $0.293, ADLS $0.023)."""

    def setUp(self):
        self.ns = run()

    def test_compute_cost(self):
        self.assertAlmostEqual(self.ns["compute_cost"], 461.337, places=2)

    def test_storage_cost(self):
        self.assertAlmostEqual(self.ns["storage_cost"], 0.90, places=2)

    def test_networking_cost(self):
        self.assertAlmostEqual(self.ns["networking_cost"], 60.00, places=2)

    def test_total_monthly(self):
        self.assertAlmostEqual(self.ns["total_monthly_cost"], 522.237, places=2)

    def test_sizing_from_volume(self):
        # 100 GB Bulk / 50 GB-per-worker -> 2 workers (+1 driver). No SLA.
        self.assertEqual(self.ns["worker_nodes_estimated"], 2)
        self.assertEqual(self.ns["ingestion_nodes"], 3)
        self.assertAlmostEqual(self.ns["runtime_hrs"], 3.4333, places=3)
        self.assertIsNone(self.ns["meets_sla"])
        self.assertIsNone(self.ns["sla_time_hr"])

    def test_effort(self):
        self.assertAlmostEqual(self.ns["complexity_score"], 3.50, places=2)
        self.assertEqual(self.ns["complexity_level"], "Simple")
        self.assertAlmostEqual(self.ns["total_effort_days_estimate"], 4.00, places=2)


class CostInvariants(unittest.TestCase):

    def test_total_equals_sum_of_parts(self):
        ns = run()
        self.assertAlmostEqual(
            ns["total_monthly_cost"],
            ns["compute_cost"] + ns["storage_cost"] + ns["networking_cost"], places=6)

    def test_annual_is_twelve_months(self):
        ns = run()
        self.assertAlmostEqual(ns["total_annual_cost"], ns["total_monthly_cost"] * 12, places=6)

    def test_variance_band_is_plus_minus_ten_percent(self):
        ns = run()
        self.assertAlmostEqual(ns["total_cost_monthly_low"], ns["total_monthly_cost"] * 0.9, places=6)
        self.assertAlmostEqual(ns["total_cost_monthly_high"], ns["total_monthly_cost"] * 1.1, places=6)

    def test_all_costs_non_negative(self):
        ns = run()
        for key in ("compute_cost", "storage_cost", "networking_cost", "total_monthly_cost"):
            self.assertGreaterEqual(ns[key], 0.0, key)


class Monotonicity(unittest.TestCase):

    def test_more_volume_costs_more(self):
        self.assertLess(run(additional_gb="10")["total_monthly_cost"],
                        run(additional_gb="1000")["total_monthly_cost"])

    def test_incremental_cheaper_than_bulk(self):
        bulk = run(load_type="Bulk", cdc_method="Not Applicable")["total_monthly_cost"]
        incr = run(load_type="Incremental", cdc_method="Timestamp")["total_monthly_cost"]
        self.assertLess(incr, bulk)

    def test_more_objects_raises_cost_and_effort(self):
        one = run(source_objects="a", edh_table_names="x")
        five = run(source_objects="a,b,c,d,e", edh_table_names="v,w,x,y,z")
        self.assertLess(one["compute_cost"], five["compute_cost"])
        self.assertLess(one["total_effort_days_estimate"], five["total_effort_days_estimate"])

    def test_higher_frequency_costs_more(self):
        self.assertLess(run(ingestion_frequency="Monthly")["total_monthly_cost"],
                        run(ingestion_frequency="Daily")["total_monthly_cost"])

    def test_bigger_vm_needs_fewer_workers(self):
        ds3 = run(vm_type="Standard_DS3_v2")
        ds5 = run(vm_type="Standard_DS5_v2")
        self.assertGreater(ds5["throughput_gb_hr"], ds3["throughput_gb_hr"])
        self.assertLessEqual(ds5["worker_nodes_estimated"], ds3["worker_nodes_estimated"])
        self.assertLessEqual(ds5["compute_cost"], ds3["compute_cost"])


class VolumeSizing(unittest.TestCase):
    """Worker count is sized purely from volume (no SLA)."""

    def test_sizes_cluster_from_the_work(self):
        ns = run(additional_gb="100")
        self.assertEqual(ns["worker_nodes_estimated"], math.ceil(100 / ss.TARGET_GB_PER_WORKER))

    def test_sizing_is_monotonic(self):
        volumes = [50, 190, 199, 200, 210, 499, 500, 1000, 5000]
        workers = [run(additional_gb=str(v))["worker_nodes_estimated"] for v in volumes]
        for smaller, bigger in zip(workers, workers[1:]):
            self.assertLessEqual(smaller, bigger)

    def test_respects_worker_cap(self):
        ns = run(additional_gb="100000")
        self.assertEqual(ns["worker_nodes_estimated"], ss.MAX_WORKERS)

    def test_runtime_includes_startup_overhead(self):
        ns = run(additional_gb="1")
        self.assertGreaterEqual(ns["runtime_hrs"], ss.CLUSTER_STARTUP_HR)

    def test_no_sla_fields_claimed(self):
        ns = run()
        self.assertIsNone(ns["sla_time_hr"])
        self.assertIsNone(ns["meets_sla"])


class NotSureDefaults(unittest.TestCase):

    def test_vm_not_sure_resolves_to_ds3(self):
        ns = run(vm_type="Not sure")
        ds3 = run(vm_type="Standard_DS3_v2")
        self.assertEqual(ns["vm_type"], "Standard_DS3_v2")
        self.assertAlmostEqual(ns["compute_cost"], ds3["compute_cost"], places=6)

    def test_effort_fields_not_sure_use_moderate_contingency(self):
        ns = run(cdc_method="Not sure", delete_handling="Not sure",
                 primary_key_available="Not sure", load_type="Incremental")
        self.assertIn(ns["complexity_level"], ("Simple", "Medium", "Complex"))
        self.assertGreater(ns["total_effort_days_estimate"], 0.0)

    def test_all_not_sure_runs_cleanly(self):
        ns = run(vm_type="Not sure", cdc_method="Not sure",
                 delete_handling="Not sure", primary_key_available="Not sure",
                 load_type="Incremental")
        self.assertGreater(ns["total_monthly_cost"], 0.0)
        self.assertGreater(ns["total_effort_days_estimate"], 0.0)
        self.assertGreaterEqual(ns["worker_nodes_estimated"], 1)
        self.assertIsNone(ns["meets_sla"])


class MixLoadType(unittest.TestCase):

    def _mix(self, n_bulk, n_incr, **extra):
        objs = ",".join(f"t{i}" for i in range(n_bulk + n_incr))
        return run(load_type="Mix", bulk_table_count=str(n_bulk), incremental_table_count=str(n_incr),
                   source_objects=objs, edh_table_names=objs, cdc_method="Timestamp", **extra)

    def test_mix_blends_load_factor(self):
        # 2 bulk + 2 incremental -> (2*1.0 + 2*0.15)/4 = 0.575.
        ns = self._mix(2, 2)
        self.assertAlmostEqual(ns["load_factor"], 0.575, places=6)

    def test_mix_cost_between_bulk_and_incremental(self):
        bulk = run(load_type="Bulk", cdc_method="Not Applicable")["total_monthly_cost"]
        incr = run(load_type="Incremental", cdc_method="Timestamp")["total_monthly_cost"]
        mix = self._mix(2, 2)["total_monthly_cost"]
        self.assertLess(incr, mix)
        self.assertLess(mix, bulk)

    def test_all_bulk_mix_matches_bulk_factor(self):
        self.assertAlmostEqual(self._mix(4, 0)["load_factor"], 1.0, places=6)

    def test_mix_stores_counts(self):
        payload = dict(BASE)
        payload.update(load_type="Mix", bulk_table_count="3", incremental_table_count="1",
                       source_objects="a,b,c,d", edh_table_names="a,b,c,d", cdc_method="Timestamp")
        req = ss.estimate(payload, prices=PRICES)["request"]
        self.assertEqual(req["bulk_table_count"], 3)
        self.assertEqual(req["incremental_table_count"], 1)

    def test_mix_is_more_complex_than_pure_loads(self):
        bulk = run(load_type="Bulk", cdc_method="Not Applicable")["complexity_score"]
        mix = self._mix(2, 2)["complexity_score"]
        self.assertGreater(mix, bulk)

    def test_mix_zero_counts_raise(self):
        with self.assertRaises(ValueError):
            run(load_type="Mix", bulk_table_count="0", incremental_table_count="0",
                cdc_method="Timestamp")


class CustomLogicCdc(unittest.TestCase):

    def test_custom_logic_is_most_complex_cdc(self):
        log_based = run(load_type="Incremental", cdc_method="Log Based")["complexity_score"]
        custom = run(load_type="Incremental", cdc_method="Custom Logic")["complexity_score"]
        self.assertGreater(custom, log_based)


class EffortBucketing(unittest.TestCase):

    def test_simple_request_is_simple(self):
        ns = run(ingestion_method="File System", data_structure="csv",
                 cdc_method="Not Applicable", schema_stability="Stable",
                 delete_handling="Ignore", load_type="Bulk", primary_key_available="Yes")
        self.assertEqual(ns["complexity_level"], "Simple")

    def test_hard_request_is_complex(self):
        ns = run(ingestion_method="Operational Database", data_structure="Sybase",
                 cdc_method="Log Based", schema_stability="Highly Dynamic",
                 delete_handling="Hard", load_type="Incremental", primary_key_available="No")
        self.assertEqual(ns["complexity_level"], "Complex")

    def test_effort_min_max_bracket_estimate(self):
        ns = run()
        self.assertLessEqual(ns["total_effort_days_min"], ns["total_effort_days_estimate"])
        self.assertLessEqual(ns["total_effort_days_estimate"], ns["total_effort_days_max"])


class InputValidation(unittest.TestCase):

    def test_invalid_vm_type_raises(self):
        with self.assertRaises(ValueError):
            run(vm_type="Standard_NOPE")

    def test_unknown_data_structure_falls_back_gracefully(self):
        ns = run(data_structure="Other")
        self.assertGreater(ns["compute_cost"], 0.0)


class LookupTableIntegrity(unittest.TestCase):

    def test_structure_tables_share_keys(self):
        throughput = set(ss.THROUGHPUT_BY_STRUCTURE)
        self.assertEqual(throughput, set(ss.COMPRESSION_RATIO_BY_STRUCTURE))
        self.assertEqual(throughput, set(ss.DATA_STRUCTURE_COMPLEXITY))

    def test_ui_data_structures_are_known(self):
        ui = {"Sql Server", "Sybase", "Postgres", "csv", "parquet", "xlsb", "xls", "API"}
        self.assertTrue(ui.issubset(set(ss.THROUGHPUT_BY_STRUCTURE)))

    def test_method_tables_share_keys(self):
        methods = set(ss.INGESTION_DBU_BY_METHOD)
        self.assertEqual(methods, set(ss.INGESTION_METHOD_COMPLEXITY))


if __name__ == "__main__":
    unittest.main(verbosity=2)
