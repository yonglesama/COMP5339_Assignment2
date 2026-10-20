"""Tests for Member C's OpenStreetMap augmentation stage."""

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("augmentation_run", ROOT / "src" / "augmentation" / "run.py")
run = importlib.util.module_from_spec(_spec)
sys.modules["augmentation_run"] = run
_spec.loader.exec_module(run)


class Helpers(unittest.TestCase):
    def test_operator_aliases(self):
        self.assertEqual(run.canonical_operator("Evie Networks"), "evie")
        self.assertEqual(run.canonical_operator("AmpCharge"), "ampol")
        self.assertEqual(run.canonical_operator("bp pulse"), "bp")
        self.assertTrue(run.operator_compatible("Ampol", ["AmpCharge", None]))
        self.assertFalse(run.operator_compatible("Evie", ["Tesla", "Chargefox"]))

    def test_socket_attributes(self):
        row = pd.Series(
            {
                "socket:type2_combo": "2",
                "socket:type2_combo:output": "150 kW",
                "socket:chademo": "1",
                "socket:type2": "no",
            }
        )
        labels, counts = run.socket_attributes(row)
        self.assertEqual(labels, "CHAdeMO; CCS Type 2")
        self.assertEqual(json.loads(counts), {"chademo": "1", "type2_combo": "2"})

    def test_conflicting_operator_is_not_accepted_by_distance_alone(self):
        locations = pd.DataFrame(
            [{"station_id": "A", "station_name": "A", "operator_name": "Evie", "address": "x",
              "postcode": "2000", "lga_name": "x", "latitude": -33.8, "longitude": 151.2}]
        )
        osm = pd.DataFrame(
            [{"osm_element_type": "node", "osm_element_id": 1, "latitude": -33.8,
              "longitude": 151.2, "operator": "Tesla"}]
        )
        match = run.choose_matches(locations, osm).iloc[0]
        self.assertEqual(match.match_status, "unmatched")
        self.assertIn("conflicting", match.review_reason)


class EndToEnd(unittest.TestCase):
    def test_cached_payload_creates_all_outputs_and_meets_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "data/processed").mkdir(parents=True)
            (root / "data/raw/augmentation").mkdir(parents=True)
            dc = pd.DataFrame(
                [
                    {"charger_id": "EV1", "station_id": "ST1", "station_name": "Evie A", "operator_name": "Evie",
                     "address": "1 A St", "postcode": "2000", "lga_name": "A", "longitude": 151.2,
                     "latitude": -33.8, "number_of_plugs": 2, "rating_kw": 50.0, "rating_text": "50 kW",
                     "source_program": "test"},
                    {"charger_id": "EV1B", "station_id": "ST1", "station_name": "Evie A", "operator_name": "Evie",
                     "address": "1 A St", "postcode": "2000", "lga_name": "A", "longitude": 151.2,
                     "latitude": -33.8, "number_of_plugs": 1, "rating_kw": 150.0, "rating_text": "150 kW",
                     "source_program": "test"},
                    {"charger_id": "EV2", "station_id": "ST2", "station_name": "Unknown B", "operator_name": "EVUp",
                     "address": "2 B St", "postcode": "2001", "lga_name": "B", "longitude": 151.3,
                     "latitude": -33.9, "number_of_plugs": 2, "rating_kw": 50.0, "rating_text": "50 kW",
                     "source_program": "test"},
                    {"charger_id": "EV3", "station_id": "ST3", "station_name": "Far C", "operator_name": "Tesla",
                     "address": "3 C St", "postcode": "2002", "lga_name": "C", "longitude": 151.4,
                     "latitude": -34.0, "number_of_plugs": 2, "rating_kw": 150.0, "rating_text": "150 kW",
                     "source_program": "test"},
                ]
            )
            dc = dc[[
                "charger_id", "station_id", "station_name", "address", "operator_name",
                "number_of_plugs", "rating_text", "rating_kw", "latitude", "longitude",
                "lga_name", "postcode", "source_program",
            ]]
            dc["station_name"] = None  # A missing source name must not affect coordinate/operator matching.
            dc["sa4_region"] = ["Sydney - City and Inner South"] * len(dc)
            dc.to_parquet(root / run.INPUT_DC, index=False)
            payload = {
                "osm3s": {"timestamp_osm_base": "2026-09-20T00:00:00Z"},
                "elements": [
                    {"type": "node", "id": 11, "lat": -33.8, "lon": 151.2,
                     "tags": {"amenity": "charging_station", "operator": "Evie Networks",
                              "capacity": "2", "socket:type2_combo": "2"}},
                    {"type": "node", "id": 12, "lat": -33.9, "lon": 151.3,
                     "tags": {"amenity": "charging_station", "fee": "no"}},
                ],
            }
            (root / run.RAW_JSON).write_text(json.dumps(payload), encoding="utf-8")

            summary = run.run(root)
            self.assertEqual(summary["unique_dc_locations"], 3)
            self.assertEqual(summary["augmented_locations"], 2)
            self.assertTrue((root / run.RAW_CSV).exists())
            self.assertTrue((root / run.SOURCE_METADATA).exists())
            self.assertTrue((root / run.OUT_ENRICHMENT).exists())
            self.assertTrue((root / run.OUT_COVERAGE).exists())
            self.assertTrue((root / run.OUT_REVIEW).exists())

            enrichment = pd.read_parquet(root / run.OUT_ENRICHMENT)
            self.assertEqual(len(enrichment), 4)
            self.assertEqual(list(enrichment.columns), list(dc.columns) + run.NEW_COLUMNS)
            pd.testing.assert_frame_equal(enrichment[list(dc.columns)], dc)
            self.assertNotIn("match_status", enrichment.columns)
            self.assertEqual(enrichment.loc[enrichment.station_id.eq("ST1"), "plug_types"].tolist(),
                             ["CCS Type 2", "CCS Type 2"])
            self.assertEqual(enrichment.set_index("station_id").loc["ST2", "pricing_information"], "free")
            self.assertTrue(enrichment.loc[enrichment.station_id.eq("ST3"), run.NEW_COLUMNS].isna().all(axis=None))


if __name__ == "__main__":
    unittest.main()
