"""Unit + validation tests for the New Source estimator.

Run:  python3 -m unittest discover -s tests -v
(from the "Ingestio template" directory; no pytest/pyspark/network needed.)

Tests call the live estimator module (estimators.new_source) directly with
prices pinned to the hardcoded fallbacks, so results are deterministic.
"""

import unittest

from estimators import new_source as ns_mod

PRICES = {"Standard_DS3_v2": 0.293, "Standard_DS5_v2": 1.17,
          "adls_hot": 0.0208, "adls_managed": 0.023, "egress": 0.087}

BASE = {
    "request_id":                 "r-test",
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
    payload = dict(BASE)
    payload.update(overrides)
    return ns_mod.estimate(payload, prices=PRICES)["estimation"]


class GoldenValues(unittest.TestCase):

    def setUp(self):
        self.e = run()

    def test_costs(self):
        self.assertAlmostEqual(self.e["network_cost_monthly"], 323.43, places=2)
        self.assertAlmostEqual(self.e["storage_cost_monthly"], 0.72, places=2)
        self.assertAlmostEqual(self.e["compute_cost_monthly"], 583.34, places=2)
        self.assertAlmostEqual(self.e["total_cost_monthly"], 907.49, places=2)

    def test_sizing(self):
        self.assertEqual(self.e["worker_nodes_estimated"], 3)
        self.assertTrue(self.e["meets_sla"])

    def test_effort(self):
        self.assertEqual(self.e["complexity_level"], "Medium")
        self.assertEqual(self.e["total_effort_days_estimate"], 39.4)
        self.assertEqual(self.e["total_effort_days_min"], 32)
        self.assertEqual(self.e["total_effort_days_max"], 47)

    def test_volume_tier(self):
        self.assertEqual(self.e["derived_volume_tier"], "medium")


class CostInvariants(unittest.TestCase):

    def test_total_equals_sum_of_parts(self):
        e = run()
        self.assertAlmostEqual(
            e["total_cost_monthly"],
            round(e["network_cost_monthly"] + e["storage_cost_monthly"] + e["compute_cost_monthly"], 2),
            places=2)

    def test_annual_is_twelve_months(self):
        e = run()
        self.assertAlmostEqual(e["total_cost_annual"], round(e["total_cost_monthly"] * 12, 2), places=2)


class Monotonicity(unittest.TestCase):

    def test_more_volume_costs_more(self):
        self.assertLess(run(source_gb="10")["total_cost_monthly"],
                        run(source_gb="1000")["total_cost_monthly"])

    def test_bigger_vm_ingests_faster(self):
        ds3 = run(vm_type="Standard_DS3_v2")
        ds5 = run(vm_type="Standard_DS5_v2")
        self.assertLessEqual(ds5["estimated_runtime_hr"], ds3["estimated_runtime_hr"])

    def test_egress_adds_cost(self):
        no_eg = run(include_egress="false", egress_gb="0")["network_cost_monthly"]
        eg = run(include_egress="true", egress_gb="50")["network_cost_monthly"]
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
        self.assertAlmostEqual(sum(ns_mod.COMPLEXITY_WEIGHTS.values()), 1.0, places=3)


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
