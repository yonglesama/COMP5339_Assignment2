"""Every C station can expose real history without changing its live state."""
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "A2/src"))
from dashboard.runtime import DashboardRuntime
from dashboard.run import create_app
from database.replay_snapshot import fingerprint


class StationHistoryDashboardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = ROOT / "A2/data/final/ev_chargers_a2.duckdb"
        cls.before = fingerprint(cls.source)
        cls.runtime = DashboardRuntime(ROOT, mode="history")
        cls.runtime.start()
        cls.client = create_app(cls.runtime).test_client()

    @classmethod
    def tearDownClass(cls):
        cls.runtime.stop()
        if fingerprint(cls.source) != cls.before:
            raise AssertionError("Viewing station history changed C's source database")

    def test_all_414_stations_default_to_their_own_recorded_curve(self):
        snapshot = self.runtime.snapshot()
        self.assertEqual(len(snapshot["stations"]), 414)
        for station in snapshot["stations"]:
            sid = station["station_id"]
            with self.subTest(station_id=sid):
                response = self.client.get(f"/api/stations/{sid}")
                self.assertEqual(response.status_code, 200)
                data = response.get_json()
                self.assertEqual(data["curve_status"], "completed")
                self.assertTrue(data["available_sessions"])
                self.assertEqual(data["session_id"], data["available_sessions"][0]["session_id"])
                self.assertTrue(data["curve"])
                self.assertTrue(all(p["station_id"] == sid and p["session_id"] == data["session_id"]
                                    for p in data["curve"]))
                self.assertEqual(data["state"]["active_session_count"], 0)
                self.assertFalse(any(s["is_active"] for s in data["available_sessions"]))
        final = self.runtime.snapshot()
        self.assertEqual(final["historical_metrics"]["completed_session_count"], 5084)
        self.assertEqual(final["current_metrics"]["active_session_count"], 0)

    def test_select_older_history_and_reject_other_station_session(self):
        sid = self.runtime.snapshot()["stations"][0]["station_id"]
        default = self.runtime.station_detail(sid)
        self.assertGreater(len(default["available_sessions"]), 1)
        older = default["available_sessions"][-1]["session_id"]
        response = self.client.get(f"/api/stations/{sid}?session_id={older}")
        self.assertEqual(response.status_code, 200)
        selected = response.get_json()
        self.assertEqual(selected["session_id"], older)
        self.assertTrue(selected["curve"])
        self.assertTrue(all(p["session_id"] == older for p in selected["curve"]))
        other_sid = next(s["station_id"] for s in self.runtime.snapshot()["stations"] if s["station_id"] != sid)
        self.assertEqual(self.client.get(f"/api/stations/{other_sid}?session_id={older}").status_code, 404)

    def test_browser_assets_cannot_keep_old_selection_code_cached(self):
        for url in ["/", "/assets/dashboard.js"]:
            response = self.client.get(url)
            self.assertEqual(response.headers["Cache-Control"], "no-store")
            response.close()

    def test_completed_curve_survives_concurrent_live_cache_eviction(self):
        sid = self.runtime.snapshot()["stations"][0]["station_id"]
        saved = self.runtime.station_detail(sid)
        state = self.runtime.state_store.get_station_state(sid)
        # Reproduce the boundary after C commits session_end but before B finishes
        # removing its active-session projection and already evicts cached meters.
        state["active_sessions"] = [{"session_id": saved["session_id"]}]
        with patch.object(self.runtime.state_store, "get_station_state", return_value=state), \
             patch.object(self.runtime.state_store, "get_session_curve", return_value=[]):
            data = self.runtime.station_detail(sid, saved["session_id"])
        self.assertEqual(data["curve_status"], "completed")
        self.assertEqual(data["curve"], saved["curve"])
        self.assertIsNone(data["session"])


if __name__ == "__main__":
    unittest.main()
