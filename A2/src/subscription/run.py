"""Headless MQTT consumer, also reusable inside D's single-process dashboard."""
from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
import sys
from threading import Event

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from subscription.event_processor import EventProcessor, InvalidMessage
from subscription.state_store import StateStore

LOG = logging.getLogger("a2.subscriber")


class SubscriptionService:
    """Caller owns the repository and must close it after stop()."""

    def __init__(self, config: dict, repository, state_store: StateStore | None = None):
        import paho.mqtt.client as mqtt
        self.config = dict(config)
        # C's current repository deliberately validates this fixed prefix.
        if self.config.get("topic_prefix", "comp5339/ev").strip("/") != "comp5339/ev":
            raise ValueError("C's current repository requires topic_prefix=comp5339/ev")
        qos = self.config.get("qos", 1)
        if type(qos) is not int or qos not in (0, 1, 2):
            raise ValueError("Invalid subscription QoS")
        self.repository = repository
        self.state_store = state_store if state_store is not None else StateStore()
        self.processor = EventProcessor("comp5339/ev", self.state_store, repository)
        self.ready = Event()
        self.failed = Event()
        self.error: Exception | None = None
        self.received_messages = self.stored_messages = self.duplicate_messages = self.rejected_messages = 0
        self._started = False
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                   client_id=self.config["client_ids"]["subscriber"],
                                   clean_session=False, protocol=mqtt.MQTTv311, manual_ack=True)
        self._client.on_connect = self._on_connect
        self._client.on_subscribe = self._on_subscribe
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._client.reconnect_delay_set(min_delay=1, max_delay=30)
        self._client.connect_timeout = self.config.get("connect_timeout_seconds", 10)
        if os.environ.get("MQTT_USERNAME"):
            self._client.username_pw_set(os.environ["MQTT_USERNAME"], os.environ.get("MQTT_PASSWORD"))
        if self.config.get("tls", False):
            self._client.tls_set()

    def _fail(self, exc: Exception) -> None:
        self.error = exc
        self.failed.set()
        self.ready.clear()
        LOG.error("Subscription stopped: %s", exc)
        self._client.disconnect()

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            self._fail(ConnectionError(str(reason_code)))
            return
        rc, _ = client.subscribe("comp5339/ev/#", qos=self.config.get("qos", 1))
        if rc != 0:
            self._fail(ConnectionError(f"Subscribe failed: {rc}"))

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties):
        if any(code.is_failure for code in reason_codes):
            self._fail(ConnectionError("Broker rejected subscription"))
        else:
            self.ready.set()
            LOG.info("Subscribed to comp5339/ev/#; ready for publisher")

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.ready.clear()
        if reason_code.is_failure and not self.failed.is_set():
            LOG.warning("MQTT disconnected (%s); reconnecting with persistent session", reason_code)

    def _on_message(self, client, userdata, message):
        if self.failed.is_set():
            return
        self.received_messages += 1
        try:
            result = self.processor.process_message(message.topic, message.payload)
            self.stored_messages += int(result.stored)
            self.duplicate_messages += int(not result.stored)
        except InvalidMessage as exc:
            self.rejected_messages += 1
            LOG.warning("Rejected malformed message on %s: %s", message.topic, exc)
        except Exception as exc:
            # No ACK: stop rather than silently lose an event or advance past a broken session.
            self._fail(exc)
            return
        if message.qos:
            rc = client.ack(message.mid, message.qos)
            if rc != 0:
                self._fail(ConnectionError(f"MQTT acknowledgement failed: {rc}"))

    def start(self, timeout: float | None = None) -> None:
        if self._started:
            raise RuntimeError("Service already started")
        self.state_store.restore(self.repository)
        self._started = True
        try:
            self._client.connect(self.config.get("host", "localhost"), int(self.config.get("port", 1883)),
                                 self.config.get("keepalive_seconds", 60))
            self._client.loop_start()
            deadline = timeout if timeout is not None else self.config.get("connect_timeout_seconds", 10)
            import time
            end = time.monotonic() + deadline
            while not self.ready.wait(0.05):
                if self.failed.is_set():
                    raise RuntimeError("Subscription failed") from self.error
                if time.monotonic() >= end:
                    raise TimeoutError("Broker did not confirm subscription")
        except Exception:
            self.stop()
            raise

    def stop(self) -> None:
        if self._started:
            self._client.disconnect()
            self._client.loop_stop()
            self._started = False
        self.ready.clear()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--database", type=Path, help="Override A2 database; no automatic rebuild")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    root = args.project_root.resolve()
    repository = service = None
    try:
        from database.repository import EventRepository
        config = json.loads((args.config or root / "A2/config/mqtt.json").read_text(encoding="utf-8-sig"))
        if args.host:
            config["host"] = args.host
        if args.port is not None:
            config["port"] = args.port
        repository = EventRepository(args.database or root / "A2/data/final/ev_chargers_a2.duckdb")
        service = SubscriptionService(config, repository)
        service.start()
        while not service.failed.wait(0.5):
            pass
        return 1
    except KeyboardInterrupt:
        return 0
    except Exception:
        LOG.exception("Subscriber failed")
        return 1
    finally:
        if service is not None:
            service.stop()
            LOG.info("Received=%s stored=%s duplicates=%s rejected=%s", service.received_messages,
                     service.stored_messages, service.duplicate_messages, service.rejected_messages)
        if repository is not None:
            repository.close()


if __name__ == "__main__":
    raise SystemExit(main())
