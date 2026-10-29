from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "A2/src"
A2_ROOT = PROJECT_ROOT / "A2"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(A2_ROOT) not in sys.path:
    sys.path.insert(0, str(A2_ROOT))

from database.initialise import initialise_database
from database.repository import EventRepository
from validate_pipeline import validate_pipeline


class EventRepositoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = tempfile.TemporaryDirectory()
        cls.test_root = Path(cls.temporary.name)
        source_database = cls.test_root / "A1/data/final/ev_chargers.duckdb"
        schema = cls.test_root / "A2/sql/a2_schema.sql"
        mapping = cls.test_root / "A2/data/input/station_connector_mapping.csv"
        source_database.parent.mkdir(parents=True)
        schema.parent.mkdir(parents=True)
        mapping.parent.mkdir(parents=True)
        shutil.copy2(PROJECT_ROOT / "A1/data/final/ev_chargers.duckdb", source_database)
        shutil.copy2(PROJECT_ROOT / "A2/sql/a2_schema.sql", schema)
        shutil.copy2(
            PROJECT_ROOT / "A2/data/input/station_connector_mapping.csv", mapping
        )
        cls.base_database = initialise_database(cls.test_root)
        cls.pristine_database = cls.test_root / "pristine.duckdb"
        shutil.copy2(cls.base_database, cls.pristine_database)
        cls.demo_events = [
            json.loads(line)
            for line in (PROJECT_ROOT / "A2/data/demo/event_stream.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
            if line
        ]

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    def setUp(self) -> None:
        self.database = self.test_root / "case.duckdb"
        shutil.copy2(self.pristine_database, self.database)
        self.repository = EventRepository(self.database)

    def tearDown(self) -> None:
        self.repository.close()
        self.database.unlink(missing_ok=True)

    @staticmethod
    def topic(event: dict) -> str:
        return f"comp5339/ev/{event['station_id']}/{event['event_type']}"

    def test_demo_stream_is_normalised_and_duplicate_is_ignored(self) -> None:
        results = [
            self.repository.store_event(self.topic(event), event)
            for event in self.demo_events
        ]
        self.assertTrue(all(results))
        self.assertTrue(self.repository.has_event(self.demo_events[0]["event_id"]))
        self.assertFalse(
            self.repository.store_event(
                self.topic(self.demo_events[0]), self.demo_events[0]
            )
        )

        connection = self.repository._require_connection()
        counts = connection.execute(
            """SELECT
                   (SELECT COUNT(*) FROM mqtt_events),
                   (SELECT COUNT(*) FROM price_events),
                   (SELECT COUNT(*) FROM charging_sessions),
                   (SELECT COUNT(*) FROM meter_events)"""
        ).fetchone()
        self.assertEqual(counts, (32, 2, 1, 28))
        self.assertEqual(
            connection.execute("SELECT * FROM current_charging_metrics").fetchone(),
            (0, 0, 0.0),
        )
        historical = connection.execute(
            "SELECT * FROM historical_charging_metrics"
        ).fetchone()
        self.assertEqual(historical[:2], (1, 1))
        self.assertAlmostEqual(historical[2], 26.39)
        self.assertAlmostEqual(historical[3], 17.15)

    def test_dashboard_station_catalog_has_one_row_per_generated_station(self) -> None:
        stations = self.repository.get_dashboard_stations()
        self.assertEqual(len(stations), 414)
        self.assertEqual(len({row["station_id"] for row in stations}), 414)
        demo_station = next(
            row for row in stations if row["station_id"] == "STD7565099B8"
        )
        self.assertGreater(demo_station["connector_count"], 0)
        self.assertGreater(demo_station["max_rating_kw"], 0)
        self.assertIsNotNone(demo_station["latitude"])
        self.assertIsNotNone(demo_station["longitude"])
        self.assertTrue(demo_station["operator_names"])

    def test_live_and_historical_dashboard_queries(self) -> None:
        for event in self.demo_events[:5]:
            self.repository.store_event(self.topic(event), event)

        prices = self.repository.get_latest_prices()
        self.assertEqual(len(prices), 1)
        self.assertEqual(prices[0]["station_id"], "STD7565099B8")
        self.assertAlmostEqual(prices[0]["price_per_kwh"], 0.65)

        active = self.repository.get_active_sessions()
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0]["session_id"], "STD7565099B8-3-202610010100")
        self.assertAlmostEqual(active[0]["power_kw"], 41.3)
        self.assertAlmostEqual(active[0]["energy_kwh"], 1.085)

        curve = self.repository.get_session_meter_events(active[0]["session_id"])
        self.assertEqual(len(curve), 2)
        self.assertLess(curve[0]["event_time"], curve[1]["event_time"])
        self.assertAlmostEqual(curve[-1]["soc_pct"], 37.9)

        current = self.repository.get_current_metrics()
        self.assertEqual(current["active_session_count"], 1)
        self.assertEqual(current["active_station_count"], 1)
        self.assertAlmostEqual(current["current_power_kw"], 41.3)

        empty_history = self.repository.get_historical_metrics()
        self.assertEqual(empty_history["completed_session_count"], 0)

        for event in self.demo_events[5:]:
            self.repository.store_event(self.topic(event), event)

        self.assertEqual(self.repository.get_active_sessions(), [])
        completed = self.repository.get_historical_metrics()
        self.assertEqual(completed["completed_session_count"], 1)
        self.assertAlmostEqual(completed["total_energy_kwh"], 26.39)
        self.assertAlmostEqual(completed["total_revenue_aud"], 17.15)

    def test_final_pipeline_validation_reconciles_demo_stream(self) -> None:
        for event in self.demo_events:
            self.repository.store_event(self.topic(event), event)
        self.repository.close()
        shutil.copy2(self.database, self.base_database)
        stream = self.test_root / "A2/data/events/event_stream.jsonl"
        stream.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(PROJECT_ROOT / "A2/data/demo/event_stream.jsonl", stream)

        result = validate_pipeline(self.test_root)

        self.assertTrue(result["validation_passed"])
        self.assertEqual(result["prepared_events"], 32)
        self.assertEqual(result["raw_events"], 32)
        self.assertEqual(result["completed_sessions"], 1)
        report = self.test_root / "A2/data/quality/final_validation.csv"
        self.assertTrue(report.is_file())
        self.assertIn("validation_passed,True", report.read_text(encoding="utf-8"))

    def test_final_pipeline_validation_reports_incomplete_replay(self) -> None:
        for event in self.demo_events[:-1]:
            self.repository.store_event(self.topic(event), event)
        self.repository.close()
        shutil.copy2(self.database, self.base_database)
        stream = self.test_root / "A2/data/events/event_stream.jsonl"
        stream.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(PROJECT_ROOT / "A2/data/demo/event_stream.jsonl", stream)

        with self.assertRaisesRegex(RuntimeError, "Final pipeline validation failed"):
            validate_pipeline(self.test_root)

        report = self.test_root / "A2/data/quality/final_validation.csv"
        contents = report.read_text(encoding="utf-8")
        self.assertIn("validation_passed,False", contents)
        self.assertIn("active_sessions_at_end", contents)

    def test_unknown_session_curve_is_empty(self) -> None:
        self.assertEqual(self.repository.get_session_meter_events("unknown-session"), [])
        with self.assertRaises(ValueError):
            self.repository.get_session_meter_events("")

    def test_normalisation_failure_rolls_back_raw_event(self) -> None:
        event = deepcopy(
            next(item for item in self.demo_events if item["event_type"] == "meter")
        )
        event["event_id"] = "evt_repository_unknown_session"
        event["payload"]["session_id"] = "unknown-session"

        with self.assertRaisesRegex(ValueError, "unknown session_id"):
            self.repository.store_event(self.topic(event), event)

        self.assertFalse(self.repository.has_event(event["event_id"]))
        count = self.repository._require_connection().execute(
            "SELECT COUNT(*) FROM mqtt_events"
        ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_topic_mismatch_is_rejected_before_storage(self) -> None:
        event = self.demo_events[0]
        with self.assertRaisesRegex(ValueError, "Topic/event mismatch"):
            self.repository.store_event("comp5339/ev/wrong/price_update", event)
        self.assertFalse(self.repository.has_event(event["event_id"]))

    def test_read_only_repository_can_check_but_not_store(self) -> None:
        event = self.demo_events[0]
        self.repository.close()
        self.repository = EventRepository(self.database, read_only=True)
        self.assertFalse(self.repository.has_event(event["event_id"]))
        with self.assertRaises(PermissionError):
            self.repository.store_event(self.topic(event), event)


if __name__ == "__main__":
    unittest.main()
