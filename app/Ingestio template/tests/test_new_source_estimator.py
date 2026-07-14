"""
Unit + validation tests for EDH_New_Source_Estimator_Job.

Run:  python3 -m unittest tests.test_new_source_estimator -v
(from the "Ingestio template" directory; no pytest/pyspark/network needed.)

Executes the REAL notebook logic via tests/estimator_harness.py with
Databricks deps stubbed and live prices pinned to fallbacks (VM DS3 $0.293,
DS5 $1.17, ADLS $0.0208/$0.023, egress $0.087), so numbers are deterministic.
"""

import unittest

from tests.estimator_harness import load_notebook

NB = "EDH_New_Source_Estimator_Job.py"

BASE = {
    "source_gb":                  "100",
    "network_source_type":        "aws_s3",
    "copy_interval":              "bulk",
    "include_egress":             "false",
    "egress_gb":                  "0",
    "sla_time_hr":                "4",
    "vm_type":                    "Standard_DS5_v2",
    "data_distribution":          "Evenly distributed",
    "delivery_pattern":           "One large batch file/extract",
    "partition_key_availability": "Yes, a clear date/region/key field",
    "complexity_source_type":     "aws_s3",
    "transformation_logic":       "medium",
    "frequency":                  "daily",
    "contains_phi":               "No",
    "delete_handling":            "Ignore",
    "schema_stability":           "Stable",
    "cdc_method":                 "Not Applicable",
}


def run(**overrides):
    scenario = dict(BASE)
    scenario.update(overrides)
    return load_notebook(NB, scenario)


class GoldenValues(unittest.TestCase):

    def setUp(self):
        self.ns = run()

    def test_costs(self):
        self.assertAlmostEqual(self.ns["network"]["total_monthly_cost"], 323.43, places=2)
        self.assertAlmostEqual(self.ns["storage"]["grand_total_monthly"], 0.72, places=2)
        self.assertAlmostEqual(self.ns["compute"]["compute_cost_monthly"], 583.34, places=2)
        self.assertAlmostEqual(self.ns["total_monthly_cost"], 907.49, places=2)

    def test_sizing(self):
        self.assertEqual(self.ns["sizing"]["worker_nodes_estimated"], 3)
        self.assertTrue(self.ns["sizing"]["meets_sla"])

    def test_effort(self):
        eff = self.ns["effort"]
        self.assertEqual(eff["complexity_level"], "Medium")
        self.assertEqual(eff["total_effort_days"]["estimate"], 39.4)
        self.assertEqual(eff["total_effort_days"]["min"], 32)
        self.assertEqual(eff["total_effort_days"]["max"], 47)

    def test_volume_tier(self):
        self.assertEqual(self.ns["derived_volume_tier"], "medium")


class CostInvariants(unittest.TestCase):

    def test_total_equals_sum_of_parts(self):
        ns = run()
        self.assertAlmostEqual(
            ns["total_monthly_cost"],
            round(ns["network"]["total_monthly_cost"]
                  + ns["storage"]["grand_total_monthly"]
                  + ns["compute"]["compute_cost_monthly"], 2),
            places=2,
        )

    def test_annual_is_twelve_months(self):
        ns = run()
        self.assertAlmostEqual(ns["total_annual_cost"], round(ns["total_monthly_cost"] * 12, 2), places=2)

    def test_combined_variance_band(self):
        ns = run()
        self.assertAlmostEqual(ns["combined_total_low"], round(ns["total_monthly_cost"] * 0.9, 2), places=2)
        self.assertAlmostEqual(ns["combined_total_high"], round(ns["total_monthly_cost"] * 1.1, 2), places=2)


class Monotonicity(unittest.TestCase):

    def test_more_volume_costs_more(self):
        self.assertLess(run(source_gb="10")["total_monthly_cost"],
                        run(source_gb="1000")["total_monthly_cost"])

    def test_bigger_vm_ingests_faster(self):
        # Contrast with the Source System estimator: here a bigger VM has
        # higher per-node throughput, so runtime should not increase.
        ds3 = run(vm_type="Standard_DS3_v2")
        ds5 = run(vm_type="Standard_DS5_v2")
        self.assertLessEqual(ds5["sizing"]["estimated_runtime_hr"],
                             ds3["sizing"]["estimated_runtime_hr"])

    def test_egress_adds_cost(self):
        no_eg = run(include_egress="false", egress_gb="0")["network"]["total_monthly_cost"]
        eg = run(include_egress="true", egress_gb="50")["network"]["total_monthly_cost"]
        self.assertLess(no_eg, eg)


class VolumeTierDerivation(unittest.TestCase):

    def test_boundaries(self):
        cases = [("5", "tiny"), ("10", "small"), ("49", "small"), ("50", "medium"),
                 ("199", "medium"), ("200", "large"), ("1999", "very_large"), ("5000", "massive")]
        for gb, expected in cases:
            with self.subTest(gb=gb):
                self.assertEqual(run(source_gb=gb)["derived_volume_tier"], expected)


class ComplexityWeights(unittest.TestCase):

    def test_weights_sum_to_one(self):
        ns = run()
        self.assertAlmostEqual(sum(ns["COMPLEXITY_WEIGHTS"].values()), 1.0, places=3)


class InputValidation(unittest.TestCase):

    def test_bulk_with_cdc_raises(self):
        with self.assertRaises(ValueError):
            run(copy_interval="bulk", cdc_method="Timestamp")

    def test_incremental_without_cdc_raises(self):
        with self.assertRaises(ValueError):
            run(copy_interval="incremental", cdc_method="Not Applicable")

    def test_zero_volume_raises(self):
        with self.assertRaises(ValueError):
            run(source_gb="0")

    def test_bad_vm_type_raises(self):
        with self.assertRaises(ValueError):
            run(vm_type="Standard_NOPE")

    def test_bad_source_type_raises(self):
        with self.assertRaises(ValueError):
            run(complexity_source_type="not_a_real_source")

    def test_bad_network_source_type_raises(self):
        with self.assertRaises(ValueError):
            run(network_source_type="carrier_pigeon")


if __name__ == "__main__":
    unittest.main(verbosity=2)
