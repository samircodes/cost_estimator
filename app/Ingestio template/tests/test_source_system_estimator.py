"""
Unit + validation tests for EDH_Source_System_Estimator_Job.

Run:  python3 -m unittest tests.test_source_system_estimator -v
(from the "Ingestio template" directory; no pytest/pyspark/network needed.)

These execute the REAL notebook logic via tests/estimator_harness.py with
Databricks deps stubbed and live prices pinned to their hardcoded fallbacks,
so the numbers here are deterministic.
"""

import math
import unittest

from tests.estimator_harness import load_notebook

NB = "EDH_Source_System_Estimator_Job.py"

# A canonical, fully-specified request used as the baseline for most tests.
BASE = {
    "ingestion_method":      "Operational Database",
    "source_system":         "IMS-RS",
    "data_structure":        "Sql Server",
    "source_objects":        "orders",
    "edh_table_names":       "edh_orders",
    "additional_gb":         "100",
    "sla_time_hr":           "2",
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
    scenario = dict(BASE)
    scenario.update(overrides)
    return load_notebook(NB, scenario)


class GoldenValues(unittest.TestCase):
    """Regression lock on the exact outputs for the baseline request
    (DS3 fallback $0.293/hr, ADLS $0.023, egress $0.087). If the cost model
    changes deliberately, update these; an unintended change fails here."""

    def setUp(self):
        self.ns = run()

    def test_compute_cost(self):
        self.assertAlmostEqual(self.ns["compute_cost"], 395.645, places=2)

    def test_storage_cost(self):
        self.assertAlmostEqual(self.ns["storage_cost"], 0.90, places=2)

    def test_networking_cost(self):
        self.assertAlmostEqual(self.ns["networking_cost"], 60.00, places=2)

    def test_total_monthly(self):
        self.assertAlmostEqual(self.ns["total_monthly_cost"], 456.545, places=2)

    def test_sizing_meets_sla(self):
        # 100 GB, 2 hr SLA, 0.1 hr of that is startup -> 1.9 usable hr at
        # 15 GB/hr -> 4 workers (+1 driver); runtime 0.1 + 100/(4*15) = 1.767 <= 2.
        self.assertEqual(self.ns["worker_nodes_estimated"], 4)
        self.assertEqual(self.ns["ingestion_nodes"], 5)
        self.assertAlmostEqual(self.ns["runtime_hrs"], 1.7667, places=3)
        self.assertTrue(self.ns["meets_sla"])

    def test_effort(self):
        self.assertAlmostEqual(self.ns["complexity_score"], 3.50, places=2)
        self.assertEqual(self.ns["complexity_level"], "Simple")
        self.assertAlmostEqual(self.ns["total_effort_estimate"], 4.00, places=2)


class CostInvariants(unittest.TestCase):

    def test_total_equals_sum_of_parts(self):
        ns = run()
        self.assertAlmostEqual(
            ns["total_monthly_cost"],
            ns["compute_cost"] + ns["storage_cost"] + ns["networking_cost"],
            places=6,
        )

    def test_annual_is_twelve_months(self):
        ns = run()
        self.assertAlmostEqual(ns["total_annual_cost"], ns["total_monthly_cost"] * 12, places=6)

    def test_variance_band_is_plus_minus_ten_percent(self):
        ns = run()
        self.assertAlmostEqual(ns["total_low"], ns["total_monthly_cost"] * 0.9, places=6)
        self.assertAlmostEqual(ns["total_high"], ns["total_monthly_cost"] * 1.1, places=6)
        self.assertLessEqual(ns["total_low"], ns["total_monthly_cost"])
        self.assertLessEqual(ns["total_monthly_cost"], ns["total_high"])

    def test_all_costs_non_negative(self):
        ns = run()
        for key in ("compute_cost", "storage_cost", "networking_cost", "total_monthly_cost"):
            self.assertGreaterEqual(ns[key], 0.0, key)


class Monotonicity(unittest.TestCase):

    def test_more_volume_costs_more(self):
        lo = run(additional_gb="10")["total_monthly_cost"]
        hi = run(additional_gb="1000")["total_monthly_cost"]
        self.assertLess(lo, hi)

    def test_incremental_cheaper_than_bulk(self):
        # Incremental must pair with a CDC method (business rule enforced in the app).
        bulk = run(load_type="Bulk", cdc_method="Not Applicable")["total_monthly_cost"]
        incr = run(load_type="Incremental", cdc_method="Timestamp")["total_monthly_cost"]
        self.assertLess(incr, bulk)

    def test_more_objects_raises_cost_and_effort(self):
        one = run(source_objects="a", edh_table_names="x")
        five = run(source_objects="a,b,c,d,e", edh_table_names="v,w,x,y,z")
        self.assertLess(one["compute_cost"], five["compute_cost"])
        self.assertLess(one["total_effort_estimate"], five["total_effort_estimate"])

    def test_higher_frequency_costs_more(self):
        monthly = run(ingestion_frequency="Monthly")["total_monthly_cost"]
        daily = run(ingestion_frequency="Daily")["total_monthly_cost"]
        self.assertLess(monthly, daily)

    def test_bigger_vm_needs_fewer_workers(self):
        # vm_type now scales per-node throughput (DS5 = 4x DS3). With SLA-based
        # sizing, both VMs are sized to just meet the SLA, so a bigger VM shows
        # up as fewer worker nodes (and lower cost) rather than as a pure cost
        # penalty. (Was validation report finding #1.)
        ds3 = run(vm_type="Standard_DS3_v2")
        ds5 = run(vm_type="Standard_DS5_v2")
        self.assertGreater(ds5["throughput"], ds3["throughput"])
        self.assertLessEqual(ds5["worker_nodes_estimated"], ds3["worker_nodes_estimated"])
        self.assertLessEqual(ds5["compute_cost"], ds3["compute_cost"])
        self.assertTrue(ds5["meets_sla"])


class SlaSizing(unittest.TestCase):

    def test_tighter_sla_needs_more_workers(self):
        loose = run(sla_time_hr="8")
        tight = run(sla_time_hr="0.5")
        self.assertLess(loose["worker_nodes_estimated"], tight["worker_nodes_estimated"])

    def test_runtime_within_sla_when_feasible(self):
        for sla in ("0.5", "2", "8", "24"):
            with self.subTest(sla=sla):
                ns = run(sla_time_hr=sla)
                self.assertLessEqual(ns["runtime_hrs"], float(sla))
                self.assertTrue(ns["meets_sla"])

    def test_runtime_includes_startup_overhead(self):
        # Runtime is never below the fixed cluster startup cost, no matter how
        # wide the cluster gets - this is what stops scale-out being free.
        ns = run(sla_time_hr="0.5", additional_gb="1")
        self.assertGreaterEqual(ns["runtime_hrs"], ns["CLUSTER_STARTUP_HR"])

    def test_impossible_sla_is_infeasible(self):
        # An SLA shorter than cluster startup can't be met at any width, so
        # meets_sla must actually report False (a real feasibility check).
        ns = run(sla_time_hr="0.05")
        self.assertFalse(ns["meets_sla"])
        self.assertEqual(ns["worker_nodes_estimated"], ns["MAX_WORKERS"])

    def test_workers_capped(self):
        ns = run(sla_time_hr="0.2", additional_gb="100000")
        self.assertLessEqual(ns["worker_nodes_estimated"], ns["MAX_WORKERS"])

    def test_cost_is_u_shaped_in_sla(self):
        # Both a too-tight SLA (paying startup on many nodes) and a too-loose
        # one (a lone worker wasting the driver) cost more than the middle.
        tight = run(sla_time_hr="0.2")["total_monthly_cost"]
        middle = run(sla_time_hr="1")["total_monthly_cost"]
        loose = run(sla_time_hr="24")["total_monthly_cost"]
        self.assertLess(middle, tight)
        self.assertLess(middle, loose)

    def test_at_least_one_worker(self):
        ns = run(sla_time_hr="1000", additional_gb="1")
        self.assertGreaterEqual(ns["worker_nodes_estimated"], 1)

    def test_zero_sla_raises(self):
        with self.assertRaises(ValueError):
            run(sla_time_hr="0")


class NotSureDefaults(unittest.TestCase):
    """The business-facing form lets requesters answer 'Not sure' for the
    technical fields; the estimator must resolve each to a sensible default."""

    def test_vm_not_sure_resolves_to_ds3(self):
        ns = run(vm_type="Not sure")
        ds3 = run(vm_type="Standard_DS3_v2")
        self.assertEqual(ns["vm_type_effective"], "Standard_DS3_v2")
        self.assertAlmostEqual(ns["compute_cost"], ds3["compute_cost"], places=6)

    def test_sla_not_sure_makes_no_deadline_claim(self):
        # No deadline was given, so we must not invent one: both the SLA and the
        # feasibility verdict stay null rather than being fabricated.
        ns = run(sla_time_hr="Not sure")
        self.assertIsNone(ns["sla_time_hr"])
        self.assertIsNone(ns["meets_sla"])
        self.assertFalse(ns["sla_specified"])

    def test_sla_not_sure_sizes_cluster_from_the_work(self):
        # Workers come from TARGET_GB_PER_WORKER, not an invented deadline.
        ns = run(sla_time_hr="Not sure", additional_gb="100")
        expected = math.ceil(100 / ns["TARGET_GB_PER_WORKER"])
        self.assertEqual(ns["worker_nodes_estimated"], expected)

    def test_sla_not_sure_sizing_is_monotonic(self):
        # The old volume-bucketed SLA default produced cliffs where MORE data
        # got FEWER workers (199 GB -> 4 workers but 200 GB -> 2). Sizing from
        # the work directly must never decrease as volume grows.
        volumes = [50, 190, 199, 200, 210, 499, 500, 1000, 5000]
        workers = [run(sla_time_hr="Not sure", additional_gb=str(v))["worker_nodes_estimated"]
                   for v in volumes]
        for smaller, bigger in zip(workers, workers[1:]):
            self.assertLessEqual(smaller, bigger)

    def test_sla_not_sure_respects_worker_cap(self):
        ns = run(sla_time_hr="Not sure", additional_gb="100000")
        self.assertEqual(ns["worker_nodes_estimated"], ns["MAX_WORKERS"])

    def test_effort_fields_not_sure_use_moderate_contingency(self):
        ns = run(cdc_method="Not sure", delete_handling="Not sure",
                 primary_key_available="Not sure", load_type="Incremental")
        # CDC 1.0 + delete 0.5 + PK 0.25 all contribute; result stays a valid bucket.
        self.assertIn(ns["complexity_level"], ("Simple", "Medium", "Complex"))
        self.assertGreater(ns["total_effort_estimate"], 0.0)

    def test_all_not_sure_runs_cleanly(self):
        # A business user who answers "Not sure" to every technical field must
        # still get a usable estimate.
        ns = run(vm_type="Not sure", sla_time_hr="Not sure", cdc_method="Not sure",
                 delete_handling="Not sure", primary_key_available="Not sure",
                 load_type="Incremental")
        self.assertGreater(ns["total_monthly_cost"], 0.0)
        self.assertGreater(ns["total_effort_estimate"], 0.0)
        self.assertGreaterEqual(ns["worker_nodes_estimated"], 1)
        self.assertIsNone(ns["meets_sla"])


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
        self.assertLessEqual(ns["total_effort_min"], ns["total_effort_estimate"])
        self.assertLessEqual(ns["total_effort_estimate"], ns["total_effort_max"])


class InputValidation(unittest.TestCase):

    def test_invalid_vm_type_raises(self):
        with self.assertRaises(ValueError):
            run(vm_type="Standard_NOPE")

    def test_unknown_data_structure_falls_back_gracefully(self):
        # "Other" is a real UI option with no entry in the lookup dicts;
        # the notebook must not crash (it uses .get(..., default)).
        ns = run(data_structure="Other")
        self.assertGreater(ns["compute_cost"], 0.0)


class LookupTableIntegrity(unittest.TestCase):
    """Cross-checks on the constant dictionaries — catches a new data
    structure being added to one table but forgotten in another."""

    def test_structure_tables_share_keys(self):
        ns = run()
        throughput = set(ns["THROUGHPUT_BY_STRUCTURE"])
        compression = set(ns["COMPRESSION_RATIO_BY_STRUCTURE"])
        complexity = set(ns["DATA_STRUCTURE_COMPLEXITY"])
        self.assertEqual(throughput, compression)
        self.assertEqual(throughput, complexity)

    def test_ui_data_structures_are_known(self):
        # Every UI data-structure option (except the explicit "Other" escape
        # hatch) should exist in the lookup tables.
        ns = run()
        ui_options = {"Sql Server", "Sybase", "Postgres", "csv", "parquet", "xlsb", "xls", "API"}
        self.assertTrue(ui_options.issubset(set(ns["THROUGHPUT_BY_STRUCTURE"])))

    def test_method_tables_share_keys(self):
        ns = run()
        methods = set(ns["INGESTION_DBU_BY_METHOD"])
        self.assertEqual(methods, set(ns["NETWORK_COST_PER_GB"]))
        self.assertEqual(methods, set(ns["INGESTION_METHOD_COMPLEXITY"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
