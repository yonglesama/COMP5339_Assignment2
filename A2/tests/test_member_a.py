"""Member A regression tests. Run with standard-library unittest discovery."""
import csv
from datetime import datetime, timedelta
import json
import shutil
from pathlib import Path
import sys
import tempfile
import unittest

SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))
from events.contract import envelope, validate_event
from events.prepare import prepare
from generation.run import generate, validate_wholesale
from generation.export_stations import sha256
from publishing.run import inspect_stream, publish_events


def csv_file(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def fixture(data):
    directory = data / "generated"
    csv_file(directory / "stations.csv", [{"station_id": "ST_TEST", "site_type": "urban", "connectors": 1}])
    csv_file(directory / "prices_hourly.csv", [
        {"station_id": "ST_TEST", "ts": f"2026-09-01T{hour:02d}:00:00", "price_per_kwh": price, "wholesale_per_kwh": wholesale}
        for hour, price, wholesale in [(4, .55, .07), (1, .55, .06), (3, .60, .08), (2, .55, .09)]
    ])
    csv_file(directory / "sessions.csv", [{
        "session_id": "S1", "station_id": "ST_TEST", "connector_id": 1, "site_type": "urban",
        "vehicle_model": "Test EV", "start_ts": "2026-09-01T03:00:00", "charge_end_ts": "2026-09-01T03:02:00",
        "end_ts": "2026-09-01T03:02:00", "charge_minutes": 2, "energy_kwh": 1.5,
        "avg_kw": 45, "peak_kw": 50, "price_per_kwh": .6, "cost_aud": .9,
    }])
    csv_file(directory / "session_metering.csv", [
        {"ts": f"2026-09-01T03:0{minute}:00", "station_id": "ST_TEST", "connector_id": 1,
         "session_id": "S1", "voltage_v": 400, "current_a": 125, "power_kw": 50,
         "energy_kwh": energy, "soc_pct": soc}
        for minute, energy, soc in [(1, .7, 30), (2, 1.5, 31)]
    ])


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.data = Path(self.temp.name)
        fixture(self.data)

    def test_order_deduplication_tariff_return_and_repeatability(self):
        result = prepare(self.data, self.data)
        self.assertEqual(result["input_prices"], 4)
        self.assertEqual(result["price_update"], 3)
        path = self.data / "events/event_stream.jsonl"
        first = path.read_bytes()
        events = [json.loads(line) for line in first.splitlines()]
        self.assertEqual([e["event_type"] for e in events], [
            "price_update", "price_update", "session_start", "meter", "meter", "session_end", "price_update"])
        self.assertEqual([e["sequence_number"] for e in events], list(range(1, 8)))
        self.assertEqual(len({e["event_id"] for e in events}), 7)
        self.assertEqual(sum(inspect_stream(path).values()), 7)
        prepare(self.data, self.data)
        self.assertEqual(path.read_bytes(), first)

    def test_orphan_meter_rejected_without_replacing_prior_output(self):
        prepare(self.data, self.data)
        output = self.data / "events/event_stream.jsonl"
        previous = output.read_bytes()
        path = self.data / "generated/session_metering.csv"
        path.write_text(path.read_text().replace("S1", "UNKNOWN"), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Orphan"):
            prepare(self.data, self.data)
        self.assertEqual(output.read_bytes(), previous)

    def test_regressing_energy_rejected(self):
        path = self.data / "generated/session_metering.csv"
        path.write_text(path.read_text().replace("1.5,31", "0.1,31"), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "progression"):
            prepare(self.data, self.data)

    def test_unknown_station_rejected(self):
        path = self.data / "generated/prices_hourly.csv"
        path.write_text(path.read_text().replace("ST_TEST", "UNKNOWN"), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Unknown price station"):
            prepare(self.data, self.data)

    def test_session_tariff_mismatch_rejected(self):
        path = self.data / "generated/sessions.csv"
        path.write_text(path.read_text().replace("0.6,0.9", "0.5,0.9"), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "tariff"):
            prepare(self.data, self.data)

    def test_duplicate_session_rejected(self):
        path = self.data / "generated/sessions.csv"
        lines = path.read_text().splitlines()
        path.write_text("\n".join([*lines, lines[1]]) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Duplicate session"):
            prepare(self.data, self.data)


class GeneratorTests(unittest.TestCase):
    def test_missing_wholesale_interval_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "aemo.csv"
            start = datetime(2026, 9, 1)
            rows = [{"REGION": "NSW1", "SETTLEMENTDATE": (start + timedelta(minutes=5 * i)).strftime("%Y/%m/%d %H:%M:%S"), "RRP": -20}
                    for i in range(1, 289)]
            csv_file(path, rows)
            self.assertEqual(validate_wholesale([path], start, 1, "NSW1")["hours"], 24)
            csv_file(path, rows[:-1])
            with self.assertRaisesRegex(ValueError, "Incomplete"):
                validate_wholesale([path], start, 1, "NSW1")

    def test_original_october_profile_generates_and_prepares_events(self):
        source = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / "A2/data"
            config_dir = root / "A2/config"
            config_dir.mkdir(parents=True)
            shutil.copy2(source / "config/generator.assessment.json", config_dir)
            external = root / "A2/external/data_generator"
            external.mkdir(parents=True)
            shutil.copy2(source / "external/data_generator/generate_dataset.py", external)
            stations = data / "input/generator_stations.csv"
            csv_file(stations, [{"station_id": "ST_TEST", "name": "Test station", "lat": -33.87,
                                 "lon": 151.21, "site_type": "urban", "connectors": 1, "max_kw": 50}])
            csv_file(data / "quality/station_export_summary.csv", [
                {"metric": "profile", "value": "assessment", "note": "Test fixture"},
                {"metric": "station_csv_sha256", "value": sha256(stations), "note": "Test fixture"},
                {"metric": "exported_stations", "value": 1, "note": "Test fixture"},
            ])
            start = datetime(2026, 10, 1)
            cache = data / "generated/aemo_cache/PRICE_AND_DEMAND_202610_NSW1.csv"
            csv_file(cache, [{"REGION": "NSW1",
                "SETTLEMENTDATE": (start + timedelta(minutes=5 * i)).strftime("%Y/%m/%d %H:%M:%S"),
                "RRP": 50} for i in range(1, 24 * 12 + 1)])
            manifest = generate(root, "assessment")
            self.assertEqual(manifest["effective_config"]["start"], "2026-10-01")
            self.assertEqual(manifest["effective_config"]["days"], 1)
            self.assertEqual(manifest["row_counts"]["prices_hourly.csv"], 24)
            result = prepare(root)
            counts = inspect_stream(data / "events/event_stream.jsonl")
            self.assertEqual(sum(counts.values()), result["total_events"])
            self.assertEqual(counts["session_start"], counts["session_end"])
            self.assertGreater(counts["meter"], 0)


class PublisherTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "stream.jsonl"
        self.events = []
        for i, price in enumerate([.5, .6, .5], 1):
            event = envelope("price_update", f"2026-09-01T0{i}:00:00", "ST_TEST",
                             {"price_per_kwh": price, "wholesale_per_kwh": -.02, "currency": "AUD"})
            event["sequence_number"] = i
            self.events.append(event)

    def write(self):
        self.path.write_text("".join(json.dumps(e) + "\n" for e in self.events), encoding="utf-8")

    def test_duplicate_id_rejected(self):
        self.events[1]["event_id"] = self.events[0]["event_id"]
        self.write()
        with self.assertRaisesRegex(ValueError, "Duplicate event_id"):
            inspect_stream(self.path)

    def test_out_of_order_rejected(self):
        self.events[1]["event_time"] = "2026-08-31T00:00:00"
        self.write()
        with self.assertRaisesRegex(ValueError, "regresses"):
            inspect_stream(self.path)

    def test_repeated_sequence_rejected(self):
        self.events[1]["sequence_number"] = 1
        self.write()
        with self.assertRaisesRegex(ValueError, "strictly increase"):
            inspect_stream(self.path)

    def test_unchanged_price_rejected(self):
        self.events[1]["payload"]["price_per_kwh"] = .5
        self.write()
        with self.assertRaisesRegex(ValueError, "Unchanged price"):
            inspect_stream(self.path)

    def test_invalid_numeric_payload_rejected(self):
        self.events[0]["payload"]["price_per_kwh"] = float("nan")
        with self.assertRaises(ValueError):
            validate_event(self.events[0])

    def test_limit_and_empty_input(self):
        self.write()
        self.assertEqual(sum(inspect_stream(self.path, 1).values()), 1)
        with self.assertRaises(ValueError):
            inspect_stream(self.path, 0)
        self.path.write_text("")
        with self.assertRaisesRegex(ValueError, "empty"):
            inspect_stream(self.path)

    def test_pacing_qos_and_retain_with_broker_acknowledgements(self):
        class Clock:
            now = 0.0
            def clock(self): return self.now
            def sleep(self, value): self.now += value
        clock = Clock()
        calls = []
        class Message:
            rc = 0
            def wait_for_publish(self, timeout): pass
            def is_published(self): return True
        class Client:
            def publish(self, topic, payload, qos, retain):
                calls.append((clock.clock(), topic, json.loads(payload), qos, retain))
                return Message()
        result = publish_events(self.events, Client(), {}, delay=.1, clock=clock.clock, sleep=clock.sleep)
        self.assertEqual(result["published_messages"], 3)
        self.assertTrue(all(b[0] - a[0] >= .1 for a, b in zip(calls, calls[1:])))
        self.assertTrue(all(c[3] == 1 and c[4] is False for c in calls))
        self.assertEqual(calls[0][1], "comp5339/ev/ST_TEST/price_update")
        self.assertEqual([c[2] for c in calls], self.events)
        for delay in [.099, float("nan"), float("inf")]:
            with self.assertRaises(ValueError):
                publish_events(self.events, Client(), {}, delay=delay)

    def test_missing_acknowledgement_fails(self):
        class Message:
            rc = 0
            def wait_for_publish(self, timeout): pass
            def is_published(self): return False
        class Client:
            def publish(self, *args, **kwargs): return Message()
        with self.assertRaises(TimeoutError):
            publish_events(self.events, Client(), {}, delay=.1)

    def test_slow_ack_does_not_consume_required_inter_message_delay(self):
        class Clock:
            now = 0.0
            def clock(self): return self.now
            def sleep(self, value): self.now += value
        clock = Clock()
        sent = []
        class Message:
            rc = 0
            def wait_for_publish(self, timeout): clock.now += .5
            def is_published(self): return True
        class Client:
            def publish(self, *args, **kwargs):
                sent.append(clock.now)
                return Message()
        publish_events(self.events, Client(), {}, delay=.1, clock=clock.clock, sleep=clock.sleep)
        self.assertTrue(all(b - a >= .6 for a, b in zip(sent, sent[1:])))


if __name__ == "__main__":
    unittest.main()
