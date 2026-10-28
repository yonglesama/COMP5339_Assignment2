"""Real loopback broker -> publisher -> independent subscriber reconciliation."""
import argparse
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time

A2 = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(A2 / "src"))
from common.paths import load_json, write_metric_csv
from publishing.run import iter_events, run_publisher


def smoke(stream: Path, report_prefix: str) -> None:
    import paho.mqtt.client as mqtt
    with socket.socket() as candidate:
        candidate.bind(("127.0.0.1", 0))
        port = candidate.getsockname()[1]
    expected = list(iter_events(stream))
    received, receive_times = [], []
    subscribed, complete = threading.Event(), threading.Event()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="member-a-smoke-observer")

    def on_connect(client, userdata, flags, reason, properties):
        if not reason.is_failure:
            client.subscribe("comp5339/ev/#", qos=1)

    def on_message(client, userdata, message):
        receive_times.append(time.monotonic())
        received.append((message.topic, json.loads(message.payload)))
        if len(received) >= len(expected):
            complete.set()

    client.on_connect = on_connect
    client.on_subscribe = lambda *args: subscribed.set()
    client.on_message = on_message
    quality = A2 / "data/quality"
    quality.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryFile(mode="w+b") as log:
        process = subprocess.Popen([sys.executable, str(A2 / "src/publishing/local_broker.py"), "--port", str(port)], stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 15
            while True:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=.2):
                        break
                except OSError:
                    if time.monotonic() > deadline or process.poll() is not None:
                        log.seek(0)
                        raise RuntimeError(log.read().decode(errors="replace"))
                    time.sleep(.05)
            client.connect("127.0.0.1", port)
            client.loop_start()
            if not subscribed.wait(10):
                raise TimeoutError("Independent subscriber did not receive SUBACK")
            config = load_json(A2 / "config/mqtt.json")
            config.update(host="127.0.0.1", port=port)
            result = run_publisher(A2.parent, stream, config, .1, None, False,
                                   quality / f"{report_prefix}_publishing.csv")
            if not complete.wait(10):
                raise AssertionError(f"Only received {len(received)}/{len(expected)} events")
            assert [e for _, e in received] == expected, "Lost, duplicated, reordered or changed MQTT events"
            assert all(topic == f"comp5339/ev/{event['station_id']}/{event['event_type']}" for topic, event in received)
            assert result["minimum_send_gap_seconds"] >= .1
            minimum_received = min(b - a for a, b in zip(receive_times, receive_times[1:]))
            write_metric_csv(quality / f"{report_prefix}_summary.csv", [
                ("transport", "MQTT 3.1.1 / QoS 1", "Actual TCP on isolated loopback port"),
                ("expected_messages", len(expected), stream.name),
                ("received_messages", len(received), "Independent paho-mqtt subscriber"),
                ("identical_payloads_and_order", True, "Compared all JSON values and MQTT topics"),
                ("minimum_publisher_gap_seconds", result["minimum_send_gap_seconds"], "Monotonic send-call boundary"),
                ("minimum_observed_receive_gap_seconds", minimum_received, "Receiver timing includes network/thread scheduling"),
                ("passed", True, "Temporary broker and subscriber shut down after test"),
            ])
            (quality / f"{report_prefix}_received.jsonl").write_text(
                "".join(json.dumps({"topic": topic, "event": event}, ensure_ascii=False) + "\n" for topic, event in received), encoding="utf-8")
            print(f"Real MQTT smoke test passed: {len(received)} identical ordered messages")
        finally:
            client.disconnect()
            client.loop_stop()
            process.terminate()
            process.wait(timeout=10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stream", type=Path, default=A2 / "data/examples/short_event_stream.jsonl")
    parser.add_argument("--report-prefix", default="mqtt_smoke")
    args = parser.parse_args()
    smoke(args.stream, args.report_prefix)
