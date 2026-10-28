"""Publish a validated, ordered event stream with >=0.1 seconds between new events."""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
import itertools
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import time

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from common.paths import load_json, write_metric_csv
from events.contract import validate_event


def iter_events(path: Path, limit: int | None = None):
    with path.open(encoding="utf-8-sig") as handle:
        lines = handle if limit is None else itertools.islice(handle, limit)
        for line_number, line in enumerate(lines, 1):
            try:
                yield json.loads(line)
            except (ValueError, TypeError) as exc:
                raise ValueError(f"Invalid JSON on line {line_number}") from exc


def inspect_stream(path: Path, limit: int | None = None) -> Counter:
    if limit is not None and limit < 1:
        raise ValueError("limit must be positive")
    counts: Counter = Counter()
    previous_key, previous_sequence = None, 0
    latest_prices = {}
    with tempfile.TemporaryDirectory(prefix="a2-publish-check-") as directory:
        with closing(sqlite3.connect(Path(directory) / "ids.sqlite3")) as db:
            db.execute("PRAGMA journal_mode=OFF")
            db.execute("CREATE TABLE ids (id TEXT PRIMARY KEY) WITHOUT ROWID")
            for event in iter_events(path, limit):
                key = validate_event(event)
                if previous_key is not None and key < previous_key:
                    raise ValueError(f"Event time/type order regresses at {event['event_id']}")
                if event["sequence_number"] <= previous_sequence:
                    raise ValueError("sequence_number must strictly increase")
                try:
                    db.execute("INSERT INTO ids VALUES (?)", (event["event_id"],))
                except sqlite3.IntegrityError as exc:
                    raise ValueError(f"Duplicate event_id: {event['event_id']}") from exc
                if event["event_type"] == "price_update":
                    station, price = event["station_id"], event["payload"]["price_per_kwh"]
                    if station in latest_prices and latest_prices[station] == price:
                        raise ValueError(f"Unchanged price update: {station}")
                    latest_prices[station] = price
                previous_key, previous_sequence = key, event["sequence_number"]
                counts[event["event_type"]] += 1
                if sum(counts.values()) % 250000 == 0:
                    print(f"Validated {sum(counts.values())} selected events", flush=True)
    if not counts:
        raise ValueError("Cannot publish an empty event stream")
    return counts


def publish_events(events, client, config: dict, *, delay: float, progress: dict | None = None,
                   clock=time.monotonic, sleep=time.sleep) -> dict:
    if not math.isfinite(delay) or delay < 0.1:
        raise ValueError("Publishing delay must be finite and at least 0.1 seconds")
    prefix = config.get("topic_prefix", "comp5339/ev").strip("/")
    if not prefix or any(c in prefix for c in "+#"):
        raise ValueError("Invalid MQTT topic prefix")
    qos = config.get("qos", 1)
    if type(qos) is not int or qos not in (0, 1, 2):
        raise ValueError("MQTT qos must be 0, 1 or 2")
    last_ack = None
    last_call = None
    count = 0
    minimum_gap = None
    started = clock()
    for event in events:
        if last_ack is not None:
            deadline = last_ack + delay
            while (remaining := deadline - clock()) > 0:
                sleep(remaining)
        now = clock()
        if last_call is not None:
            gap = now - last_call
            minimum_gap = gap if minimum_gap is None else min(minimum_gap, gap)
        topic = f"{prefix}/{event['station_id']}/{event['event_type']}"
        message = client.publish(topic, json.dumps(event, ensure_ascii=False, separators=(",", ":"), allow_nan=False),
                                 qos=qos, retain=False)
        last_call = now
        if message.rc != 0:
            raise RuntimeError(f"MQTT publish failed (rc={message.rc}) at {event['event_id']}")
        message.wait_for_publish(timeout=config.get("publish_timeout_seconds", 15))
        if not message.is_published():
            raise TimeoutError(f"No MQTT acknowledgement for {event['event_id']}")
        # Paho can queue before its network thread writes. Waiting a full delay
        # after ACK also guarantees spacing when that write was delayed.
        last_ack = clock()
        count += 1
        if progress is not None:
            progress.update(published_messages=count, elapsed_seconds=round(clock() - started, 6),
                            minimum_send_gap_seconds=minimum_gap)
        if count % 1000 == 0:
            print(f"Published and acknowledged {count} messages", flush=True)
    return {"published_messages": count, "elapsed_seconds": round(clock() - started, 6),
            "minimum_send_gap_seconds": minimum_gap}


def run_publisher(root: Path, stream: Path, config: dict, delay: float,
                  limit: int | None, validate_only: bool, report: Path) -> dict:
    if not math.isfinite(delay) or delay < 0.1:
        raise ValueError("Publishing delay must be finite and at least 0.1 seconds")
    counts = inspect_stream(stream, limit)
    result = {"status": "validated_only", "selected_events": sum(counts.values()),
              "published_messages": 0, "delay_seconds": delay}
    if not validate_only:
        import paho.mqtt.client as mqtt
        connected = threading.Event()
        connection_error: list[str] = []
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                             client_id=config["client_ids"]["publisher"],
                             protocol=mqtt.MQTTv311, reconnect_on_failure=False)
        client.max_inflight_messages_set(1)
        client.connect_timeout = config.get("connect_timeout_seconds", 10)

        def on_connect(client, userdata, flags, reason_code, properties):
            if reason_code.is_failure:
                connection_error.append(str(reason_code))
            connected.set()

        client.on_connect = on_connect
        username = os.environ.get("MQTT_USERNAME")
        if username:
            client.username_pw_set(username, os.environ.get("MQTT_PASSWORD"))
        if config.get("tls", False):
            client.tls_set()
        result["status"] = "failed"
        try:
            client.connect(config["host"], int(config["port"]), config.get("keepalive_seconds", 60))
            client.loop_start()
            if not connected.wait(config.get("connect_timeout_seconds", 10)):
                raise TimeoutError("MQTT broker did not acknowledge connection")
            if connection_error:
                raise ConnectionError(connection_error[0])
            result.update(publish_events(iter_events(stream, limit), client, config, delay=delay, progress=result))
            result["status"] = "published"
        finally:
            client.disconnect()
            client.loop_stop()
            write_metric_csv(report, [(k, v, "MQTT publisher run") for k, v in result.items()])
    else:
        write_metric_csv(report, [(k, v, "Preflight only; no network messages sent") for k, v in result.items()])
    print(json.dumps(result, indent=2))
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--stream", type=Path, help="Default: A2/data/events/event_stream.jsonl")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--delay", type=float)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--validate-only", action="store_true", help="Validate without connecting to MQTT")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args(argv)
    root = args.project_root.resolve()
    config = load_json(args.config or root / "A2/config/mqtt.json")
    if args.host:
        config["host"] = args.host
    if args.port:
        config["port"] = args.port
    try:
        run_publisher(root, args.stream or root / "A2/data/events/event_stream.jsonl", config,
                      args.delay if args.delay is not None else config.get("publish_delay_seconds", 0.1),
                      args.limit, args.validate_only, args.report or root / "A2/data/quality/publishing_summary.csv")
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"Publisher failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
