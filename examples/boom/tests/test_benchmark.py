import unittest
import tempfile
from pathlib import Path

import prepare_capture

import benchmark
import binning


class DecisionEvidenceTests(unittest.TestCase):
    def report(self):
        evaluated = {"status": "evaluated", "applecider_eligible": True}
        phase = {"histories": [{}], "enrichment": [{"parity": {"source": evaluated, "equal": True}}],
                 "samples": [{"lag": {"oldest_age_seconds": 1.0}}],
                 "ingest_seconds": 2.0, "settled_packets_per_second": 50,
                 "recovery": {"references_per_second": 100}}
        return {"phases": {name: dict(phase) for name in ["baseline", "shadow", "recovery"]},
                "workload": {"kind": "fixture_derived", "objects": 1}, "filters": [{}],
                "filters_are_deployed_export": False,
                "targets": {"packets_per_second": None, "max_lag_seconds": None}}

    def test_fixture_success_does_not_claim_rollout_or_capacity(self):
        result = benchmark.summary(self.report())
        self.assertTrue(result["gates"]["history_parity"])
        self.assertTrue(result["gates"]["enrichment_parity"])
        self.assertFalse(result["gates"]["captured_workload"])
        self.assertFalse(result["gates"]["deployed_filter_inventory"])
        self.assertIsNone(result["gates"]["throughput_target"])
        self.assertEqual(result["verdict"], "more_evidence_required")

    def test_targets_and_empty_inference_are_not_silently_approved(self):
        report = self.report()
        report["targets"] = {"packets_per_second": 200, "max_lag_seconds": 0.5}
        report["phases"]["shadow"]["enrichment"] = []
        result = benchmark.summary(report)
        for key in ["throughput_target", "sampled_lag_target", "recovery_headroom", "enrichment_parity"]:
            self.assertFalse(result["gates"][key], key)

    def test_recovery_only_does_not_infer_healthy_throughput(self):
        report = self.report()
        report["phases"] = {"recovery": report["phases"]["recovery"]}
        report["targets"]["packets_per_second"] = 30
        report["phases"]["recovery"]["recovery"]["samples"] = [{"lag": {"oldest_age_seconds": 50}}]
        result = benchmark.summary(report)
        self.assertIsNone(result["gates"]["throughput_target"])
        self.assertIsNone(result["ingestion_elapsed_overhead_percent"])
        self.assertTrue(result["gates"]["recovery_headroom"])
        self.assertEqual(result["sampled_peak_age_seconds"], 50)

    def test_binning_uses_source_measurements_and_detects_stale_flux(self):
        history = {"version": 2, "prv_candidates": [
            {"jd": 2461000.51, "band": "g", "programid": 1, "psfFlux": 30, "psfFluxErr": 2},
            {"jd": 2461000.52, "band": "g", "programid": 1, "psfFlux": 20, "psfFluxErr": 1}]}
        rows = binning.reference(history, 86400)
        self.assertEqual(rows[0]["flux"], 22)
        self.assertEqual(rows[0]["n_measured"], 2)
        binning.compare(rows, rows)
        stale = [dict(rows[0], flux=18)]
        with self.assertRaises(AssertionError):
            binning.compare(rows, stale)

    def test_percentiles_use_observed_samples_without_inventing_missing_values(self):
        self.assertIsNone(benchmark.percentile([], 95))
        self.assertEqual(benchmark.percentile([3, 1, 2, 100], 95), 100)


class CapturePreparationTests(unittest.TestCase):
    def test_supplied_arrivals_determine_order_without_rewriting_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.avro").write_bytes(b"original a")
            (root / "b.avro").write_bytes(b"original b")
            result = prepare_capture.prepare(root, schedule={"a.avro": 20, "b.avro": 10})
            self.assertEqual([Path(row["path"]).name for row in result["packets"]], ["b.avro", "a.avro"])
            self.assertEqual(result["provenance"]["timing_basis"], "supplied_arrival_schedule")
            self.assertEqual((root / "a.avro").read_bytes(), b"original a")
            with self.assertRaises(ValueError):
                prepare_capture.prepare(root, schedule={"a.avro": -1, "b.avro": 0})


if __name__ == "__main__":
    unittest.main()
