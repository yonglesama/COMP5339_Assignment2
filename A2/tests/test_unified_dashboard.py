"""C history + unmodified A MQTT replay: no fabricated data or double counting."""
from pathlib import Path
import json
import sys
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "A2/src"))
from dashboard.runtime import DashboardRuntime
from dashboard.run import create_app
from database.replay_snapshot import fingerprint


class UnifiedDashboardTests(unittest.TestCase):
    def test_three_stations_real_mqtt_curves_reconcile_duplicates_reset_and_source_protection(self):
        source = ROOT / "A2/data/final/ev_chargers_a2.duckdb"
        original_hash = fingerprint(source)
        runtime = DashboardRuntime(ROOT, mode="unified", replay_delay=.15)
        try:
            runtime.start()
            client = create_app(runtime).test_client()
            initial = runtime.snapshot()
            context = initial["replay_context"]
            self.assertNotEqual(runtime.database_path, source)
            self.assertEqual(runtime.source_database, source)
            self.assertEqual(len(initial["stations"]), 414)
            self.assertEqual(initial["historical_metrics"]["completed_session_count"], 5081)
            self.assertEqual(initial["current_metrics"]["active_session_count"], 0)
            self.assertEqual(context["selected_session_count"], 3)
            self.assertEqual(len(set(context["station_ids"])), 3)
            self.assertEqual(context["event_count"], 87)
            sample = runtime._publish_stream.read_text(encoding="utf-8").splitlines()
            wanted = {json.loads(line)["event_id"]: line for line in sample}
            with runtime.stream_path.open(encoding="utf-8-sig") as handle:
                for line in handle:
                    event = json.loads(line)
                    if event["event_id"] in wanted:
                        self.assertEqual(line.rstrip("\r\n"), wanted.pop(event["event_id"]))
            self.assertEqual(wanted, {})
            self.assertEqual(client.post("/api/demo/replay", json={}).status_code, 202)
            self.assertEqual(client.post("/api/demo/reset", json={}).status_code, 409)
            observed = {sid: set() for sid in context["session_ids"]}
            observed_three_at_once = False
            deadline = time.monotonic() + 40
            while runtime.snapshot()["replay"]["running"]:
                current = runtime.snapshot()
                observed_three_at_once |= current["current_metrics"]["active_station_count"] == 3
                for station, session in zip(context["station_ids"], context["session_ids"]):
                    detail = runtime.station_detail(station)
                    if detail["curve_status"] == "active" and len(detail["curve"]) > 0:
                        observed[session].add(len(detail["curve"]))
                        self.assertTrue(all(p["station_id"] == station and p["session_id"] == session
                                            for p in detail["curve"]))
                if time.monotonic() > deadline:
                    self.fail("Unified replay timed out")
                time.sleep(.04)
            final = runtime.snapshot()
            self.assertIsNone(final["replay"]["error"])
            self.assertTrue(final["replay"]["finished"])
            self.assertTrue(observed_three_at_once)
            self.assertTrue(all(len(counts) > 1 for counts in observed.values()))
            self.assertEqual(final["replay"]["published_messages"], 87)
            self.assertGreaterEqual(final["replay"]["minimum_send_gap_seconds"], .1)
            self.assertEqual(final["current_metrics"]["active_session_count"], 0)
            for key, expected in context["target_historical_metrics"].items():
                self.assertAlmostEqual(final["historical_metrics"][key], expected)
            self.assertEqual(runtime.subscriber.stored_messages, 87)
            self.assertEqual(runtime.subscriber.duplicate_messages, 0)
            # Resending an original event must not add another session or meter point.
            event = json.loads(sample[-1])
            result = runtime.subscriber.processor.process(
                f"comp5339/ev/{event['station_id']}/{event['event_type']}", event)
            self.assertFalse(result.stored)
            for station, session in zip(context["station_ids"], context["session_ids"]):
                detail = runtime.station_detail(station, session)
                self.assertEqual(detail["curve_status"], "completed")
                self.assertEqual(len(detail["curve"]), context["meter_counts"][session])
            self.assertEqual(client.post("/api/demo/replay", json={}).status_code, 409)
            self.assertEqual(client.post("/api/demo/reset", json={}).status_code, 200)
            reset = runtime.snapshot()
            self.assertEqual(reset["historical_metrics"]["completed_session_count"], 5081)
            self.assertEqual(reset["current_metrics"]["active_session_count"], 0)
            self.assertFalse(reset["replay"]["finished"])
        finally:
            runtime.stop()
        self.assertEqual(fingerprint(source), original_hash)


if __name__ == "__main__":
    unittest.main()
