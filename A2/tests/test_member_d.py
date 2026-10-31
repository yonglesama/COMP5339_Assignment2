"""D API, lifecycle, source protection and real MQTT dashboard integration."""
from pathlib import Path
import hashlib
import shutil
import tempfile
import json
import sys
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "A2/src"))
from dashboard.run import create_app
from database.repository import EventRepository
from dashboard.runtime import DashboardConflict, DashboardRuntime

SEED = ROOT / "A2/data/demo/a_generated_empty.duckdb"
ACTIVE_STATION = "ST4D056616A3"
ACTIVE_SESSION = "ST4D056616A3-4-202610010201"


def sha256(path):
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


class DashboardApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = tempfile.TemporaryDirectory(prefix="d-a-generated-api-")
        cls.active_fixture = Path(cls.fixture.name) / "active.duckdb"
        shutil.copy2(SEED, cls.active_fixture)
        repository = EventRepository(cls.active_fixture)
        try:
            events = [json.loads(line) for line in (ROOT / "A2/data/demo/event_stream.jsonl").read_text(encoding="utf-8").splitlines()]
            for event in events[:6]:
                repository.store_event(f"comp5339/ev/{event['station_id']}/{event['event_type']}", event)
        finally:
            repository.close()

    @classmethod
    def tearDownClass(cls):
        cls.fixture.cleanup()

    def setUp(self):
        self.runtime = DashboardRuntime(ROOT, mode="history", database_path=self.active_fixture)
        self.runtime.start()
        self.addCleanup(self.runtime.stop)
        self.client = create_app(self.runtime).test_client()

    def test_catalog_metrics_and_null_name_fallback(self):
        response = self.client.get("/api/snapshot")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertEqual(len(data["stations"]), 414)
        self.assertEqual(data["current_metrics"]["active_session_count"], 1)
        self.assertEqual(data["historical_metrics"]["completed_session_count"], 0)
        self.assertEqual(data["historical_metrics"]["total_energy_kwh"], 0)
        self.assertEqual(data["status"], "historical")
        self.assertTrue(all(s["display_name"] for s in data["stations"]))
        self.assertTrue(any(s["plug_types"] is None for s in data["stations"]))

    def test_curve_restored_and_timestamp_serialised(self):
        data = self.client.get(f"/api/stations/{ACTIVE_STATION}").get_json()
        self.assertEqual(data["curve_status"], "active")
        self.assertEqual(len(data["curve"]), 3)
        self.assertEqual(data["curve"][-1]["energy_kwh"], 1.691)
        self.assertEqual(data["curve"][-1]["event_time"], "2026-10-01T02:04:00")

    def test_session_cannot_be_read_from_another_station(self):
        self.assertEqual(self.client.get(f"/api/stations/STBBC75750B8?session_id={ACTIVE_SESSION}").status_code, 404)

    def test_missing_station_and_session_are_404(self):
        self.assertEqual(self.client.get("/api/stations/missing").status_code, 404)
        self.assertEqual(self.client.get(f"/api/stations/{ACTIVE_STATION}?session_id=missing").status_code, 404)

    def test_no_session_is_a_valid_empty_state(self):
        data = self.client.get("/api/stations/STBBC75750B8").get_json()
        self.assertEqual(data["curve"], [])
        self.assertEqual(data["curve_status"], "none")

    def test_history_has_no_write_or_reset_actions(self):
        before = sha256(self.active_fixture)
        self.assertEqual(self.client.post("/api/demo/replay", json={}).status_code, 409)
        self.assertEqual(self.client.post("/api/demo/reset", json={}).status_code, 409)
        self.assertTrue(self.runtime.repository.read_only)
        self.assertEqual(before, sha256(self.active_fixture))

    def test_assets_and_not_found(self):
        for path in ["/", "/assets/dashboard.js", "/assets/dashboard.css", "/assets/vendor/leaflet.js"]:
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 200)
                response.close()
        self.assertEqual(self.client.get("/api/not-found").status_code, 404)

    def test_actions_require_same_origin_json(self):
        self.assertEqual(self.client.post("/api/demo/reset").status_code, 415)
        self.assertEqual(self.client.post("/api/demo/reset", json={}, headers={"Origin":"https://other.example"}).status_code, 403)


class DashboardLifecycleTests(unittest.TestCase):
    def test_mqtt_requires_explicit_source_database(self):
        with self.assertRaises(ValueError):
            DashboardRuntime(ROOT, mode="mqtt")

    def test_missing_source_fails_without_starting_service(self):
        runtime = DashboardRuntime(ROOT, database_path=ROOT / "does-not-exist.duckdb")
        with self.assertRaises(FileNotFoundError):
            runtime.start()
        runtime.stop()

    def test_history_stop_is_idempotent(self):
        runtime = DashboardRuntime(ROOT)
        runtime.start()
        self.assertEqual(runtime.database_path, ROOT / "A2/data/final/ev_chargers_a2.duckdb")
        self.assertTrue(runtime.repository.read_only)
        self.assertEqual(runtime.snapshot()["historical_metrics"]["completed_session_count"],5084)
        with self.assertRaises(DashboardConflict):
            runtime.start()
        runtime.stop()
        runtime.stop()
        with self.assertRaises(DashboardConflict):
            runtime.snapshot()

    def test_real_mqtt_replay_history_curve_reset_and_source_protection(self):
        protected = [SEED, ROOT/"A1/data/final/ev_chargers.duckdb", ROOT/"A2/data/final/ev_chargers_a2.duckdb"]
        protected = [p for p in protected if p.is_file()]
        before = {p:sha256(p) for p in protected}
        runtime = DashboardRuntime(ROOT, mode="demo", replay_delay=.15)
        try:
            runtime.start()
            self.assertNotEqual(runtime.database_path, SEED)
            client = create_app(runtime).test_client()
            self.assertEqual(client.post("/api/demo/replay", json={}).status_code, 202)
            self.assertEqual(client.post("/api/demo/replay", json={}).status_code, 409)
            self.assertEqual(client.post("/api/demo/reset", json={}).status_code, 409)
            observed_active_session = observed_curve_growth = False
            deadline = time.monotonic()+25
            while runtime.snapshot()["replay"]["running"]:
                snapshot = runtime.snapshot()
                observed_active_session |= snapshot["current_metrics"]["active_session_count"] == 1
                if snapshot["current_metrics"]["active_session_count"]:
                    detail = runtime.station_detail(ACTIVE_STATION, ACTIVE_SESSION)
                    observed_curve_growth |= len(detail["curve"]) > 3
                if time.monotonic()>deadline:
                    self.fail("Real MQTT replay did not finish")
                time.sleep(.04)
            final = runtime.snapshot()
            self.assertIsNone(final["replay"]["error"])
            self.assertTrue(final["replay"]["finished"])
            self.assertEqual(final["replay"]["published_messages"], 36)
            self.assertGreaterEqual(final["replay"]["minimum_send_gap_seconds"], .1)
            self.assertTrue(observed_active_session)
            self.assertTrue(observed_curve_growth)
            self.assertEqual(final["current_metrics"]["active_session_count"], 0)
            self.assertEqual(final["historical_metrics"]["completed_session_count"], 1)
            self.assertAlmostEqual(final["historical_metrics"]["total_energy_kwh"],22.60)
            self.assertAlmostEqual(final["historical_metrics"]["total_revenue_aud"],16.72)
            detail = runtime.station_detail(ACTIVE_STATION, ACTIVE_SESSION)
            self.assertEqual(detail["curve_status"], "completed")
            self.assertEqual(len(detail["curve"]),32)
            self.assertEqual(client.post("/api/demo/replay",json={}).status_code,409)
            self.assertEqual(client.post("/api/demo/reset",json={}).status_code,200)
            reset = runtime.snapshot()
            self.assertEqual(reset["current_metrics"]["active_session_count"],0)
            self.assertEqual(reset["historical_metrics"]["completed_session_count"],0)
            self.assertEqual(len(runtime.station_detail(ACTIVE_STATION)["curve"]),0)
            self.assertFalse(reset["replay"]["finished"])
        finally:
            runtime.stop()
        self.assertEqual(before,{p:sha256(p) for p in protected})


if __name__ == "__main__":
    unittest.main()
