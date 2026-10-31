"""D's single owner for B's consumer/state and C's repository.

Demo and MQTT modes write to private copies. History opens its source read-only.
This module contains no SQL or message parser.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
import json
import math
from pathlib import Path
import shutil
import socket
import subprocess
import sys
from threading import Event, RLock, Thread
import time
from typing import Any
from uuid import uuid4

from database.repository import EventRepository
from database.replay_snapshot import prepare_replay_snapshot
from publishing.run import run_publisher
from subscription.run import SubscriptionService
from subscription.state_store import StateStore


class DashboardConflict(RuntimeError):
    """A valid action cannot run in the runtime's current state."""


def json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


class DashboardRuntime:
    def __init__(
        self, project_root: Path, *, mode: str = "history",
        database_path: Path | None = None, stream_path: Path | None = None,
        mqtt_config: dict | None = None, replay_delay: float = 1.0,
    ) -> None:
        if mode not in {"demo", "mqtt", "history", "unified"}:
            raise ValueError("mode must be demo, mqtt, history or unified")
        if not math.isfinite(replay_delay) or replay_delay < .1:
            raise ValueError("Replay delay must be at least 0.1 seconds")
        self.project_root = Path(project_root).resolve()
        self.mode = mode
        if database_path is None:
            if mode == "mqtt":
                raise ValueError("MQTT mode requires an explicit --database source")
            database_path = self.project_root / (
                "A2/data/demo/a_generated_empty.duckdb" if mode == "demo"
                else "A2/data/final/ev_chargers_a2.duckdb"
            )
        self.source_database = Path(database_path).resolve()
        self.stream_path = Path(stream_path or self.project_root / (
            "A2/data/events/event_stream.jsonl" if mode == "unified"
            else "A2/data/demo/event_stream.jsonl")).resolve()
        self._publish_stream = self.stream_path
        self._replay_context: dict | None = None
        self.replay_delay = replay_delay
        self._config = deepcopy(mqtt_config) if mqtt_config is not None else json.loads(
            (self.project_root / "A2/config/mqtt.json").read_text(encoding="utf-8-sig")
        )
        self._config.setdefault("client_ids", {})
        suffix = uuid4().hex[:12]
        self._config["client_ids"].update(publisher=f"d-publisher-{suffix}", subscriber=f"d-subscriber-{suffix}")
        self._lock = RLock()
        self._stop = Event()
        self._worker: Thread | None = None
        self._broker: subprocess.Popen | None = None
        self._broker_log = None
        self._started = False
        self._catalog: dict[str, dict] = {}
        self._run_dir: Path | None = None
        self.database_path: Path | None = None
        self.repository: EventRepository | None = None
        self.state_store = StateStore()
        self.subscriber: SubscriptionService | None = None
        self._replay = self._empty_replay()

    @staticmethod
    def _empty_replay() -> dict:
        return {"running": False, "finished": False, "published_messages": 0,
                "selected_events": 0, "error": None}

    def _start_broker(self) -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        self._config.update(host="127.0.0.1", port=port, tls=False)
        self._broker_log = (self._run_dir / "broker.log").open("w", encoding="utf-8")
        self._broker = subprocess.Popen(
            [sys.executable, "-u", str(self.project_root / "A2/src/publishing/local_broker.py"), "--port", str(port)],
            stdout=self._broker_log, stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self._broker.poll() is not None:
                raise RuntimeError("Local broker failed; install the root requirements.txt and inspect the runtime broker.log")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=.2):
                    return
            except OSError:
                time.sleep(.05)
        raise TimeoutError("Local MQTT broker did not start within 15 seconds")

    def _open_data(self) -> None:
        if self.mode == "history":
            self.database_path = self.source_database
        else:
            self.database_path = self._run_dir / "dashboard.duckdb"
            if self.mode == "unified":
                self._publish_stream = self._run_dir / "a_selected_events.jsonl"
                self._replay_context = prepare_replay_snapshot(
                    self.source_database, self.database_path, self.stream_path, self._publish_stream)
            else:
                shutil.copy2(self.source_database, self.database_path)
        self.repository = EventRepository(self.database_path, read_only=self.mode == "history")
        self._catalog = {s["station_id"]: s for s in self.repository.get_dashboard_stations()}
        if self.mode == "history":
            self.state_store.restore(self.repository)
        else:
            self.subscriber = SubscriptionService(self._config, self.repository, self.state_store)
            self.subscriber.start()

    def start(self) -> None:
        with self._lock:
            if self._started:
                raise DashboardConflict("Dashboard is already running")
            if not self.source_database.is_file():
                raise FileNotFoundError(f"Database source not found: {self.source_database}")
            if self.mode in {"demo", "unified"} and not self.stream_path.is_file():
                raise FileNotFoundError(f"Demo continuation not found: {self.stream_path}")
            self._stop.clear()
            try:
                if self.mode != "history":
                    self._run_dir = self.project_root / "A2/data/runtime" / uuid4().hex
                    self._run_dir.mkdir(parents=True)
                if self.mode in {"demo", "unified"}:
                    self._start_broker()
                self._open_data()
                self._started = True
            except Exception:
                self._close_data()
                self._close_broker()
                raise

    def _close_data(self) -> None:
        if self.subscriber is not None:
            self.subscriber.stop()
            self.subscriber = None
        if self.repository is not None:
            self.repository.close()
            self.repository = None

    def _close_broker(self) -> None:
        if self._broker is not None:
            self._broker.terminate()
            try:
                self._broker.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._broker.kill()
                self._broker.wait()
            self._broker = None
        if self._broker_log is not None:
            self._broker_log.close()
            self._broker_log = None

    def stop(self) -> None:
        self._stop.set()
        worker = self._worker
        if worker is not None and worker.is_alive():
            worker.join(timeout=self._config.get("publish_timeout_seconds", 15) + self.replay_delay + 5)
            if worker.is_alive():
                raise RuntimeError("Publisher is still shutting down; database remains open")
        with self._lock:
            self._close_data()
            self._close_broker()
            self._started = False

    def _require_started(self) -> None:
        if not self._started or self.repository is None:
            raise DashboardConflict("Dashboard is not running")

    def snapshot(self) -> dict:
        with self._lock:
            self._require_started()
            states = {s["station_id"]: s for s in self.state_store.get_all_station_states()}
            stations = []
            for sid, static in self._catalog.items():
                live = states.get(sid, {})
                stations.append({**static,
                    "display_name": static.get("station_name") or static.get("address") or sid,
                    "price_per_kwh": live.get("price_per_kwh"),
                    "price_event_time": live.get("price_event_time"),
                    "currency": live.get("currency", "AUD"),
                    "active_session_count": live.get("active_session_count", 0),
                    "occupied_connector_count": live.get("occupied_connector_count", 0),
                })
            failed = self.subscriber is not None and self.subscriber.failed.is_set()
            connected = self.subscriber is not None and self.subscriber.ready.is_set()
            status = "historical" if self.mode == "history" else "error" if failed else "connected" if connected else "reconnecting"
            return json_value({
                "mode": self.mode, "status": status,
                "error": str(self.subscriber.error) if failed else None,
                "refresh_seconds": 1, "stations": stations,
                "active_sessions": self.state_store.get_active_sessions(),
                "current_metrics": self.repository.get_current_metrics(),
                "historical_metrics": self.repository.get_historical_metrics(),
                "replay": dict(self._replay),
                "replay_context": self._replay_context,
                "data_label": "C history + A MQTT replay · Private copy" if self.mode == "unified" else "A-generated event replay" if self.mode == "demo" else "C database · Historical snapshot" if self.mode == "history" else "Live event stream",
            })

    def station_detail(self, station_id: str, session_id: str | None = None) -> dict:
        with self._lock:
            self._require_started()
            if station_id not in self._catalog:
                raise KeyError(station_id)
            static = self._catalog[station_id]
            state = self.state_store.get_station_state(station_id)
            sessions = self.repository.get_station_sessions(station_id)
            if session_id is None and sessions:
                session_id = sessions[0]["session_id"]
            selected = next((s for s in sessions if s["session_id"] == session_id), None)
            if session_id and selected is None:
                raise KeyError(session_id)
            session = next((s for s in state["active_sessions"] if s["session_id"] == session_id), None) if selected and selected["is_active"] else None
            points = []
            if session_id:
                # The DB is committed before B updates its live cache. Always read
                # durable meters so a concurrent end/cache eviction cannot blank a curve.
                points = self.repository.get_session_meter_events(session_id)
                if not points and not selected["is_active"]:
                    raise KeyError(session_id)
                if any(p["station_id"] != station_id for p in points):
                    raise KeyError(session_id)
            return json_value({"station": static, "state": state,
                "available_sessions": sessions,
                "session": session, "session_id": session_id, "curve": points,
                "curve_status": "active" if selected and selected["is_active"] else "completed" if points else "none"})

    def start_replay(self) -> None:
        with self._lock:
            self._require_started()
            if self.mode not in {"demo", "unified"}:
                raise DashboardConflict("Replay is available in demo mode")
            if self._replay["running"]:
                raise DashboardConflict("A replay is already running")
            if self._replay["finished"]:
                raise DashboardConflict("Reset the demo before replaying these events again")
            if not self.subscriber.ready.is_set() or self.subscriber.failed.is_set():
                raise DashboardConflict("MQTT subscriber is not ready")
            self._replay = {**self._empty_replay(), "running": True}
            self._worker = Thread(target=self._replay_worker, name="dashboard-demo-publisher", daemon=True)
            self._worker.start()

    def _replay_worker(self) -> None:
        try:
            # Reuse A's complete publisher: preflight, QoS, ACK and pacing.
            result = run_publisher(self.project_root, self._publish_stream, deepcopy(self._config),
                self.replay_delay, None, False, self._run_dir / "publishing_summary.csv")
            expected = result["published_messages"]
            deadline = time.monotonic() + 10
            while self.subscriber.stored_messages < expected:
                if self.subscriber.failed.is_set():
                    raise RuntimeError(str(self.subscriber.error))
                if time.monotonic() >= deadline:
                    raise TimeoutError("Published events have not all been committed by the subscriber")
                if self._stop.wait(.02):
                    raise InterruptedError("Dashboard stopped during replay")
            with self._lock:
                self._replay.update(result, running=False, finished=True)
        except Exception as exc:
            with self._lock:
                self._replay.update(running=False, error=str(exc))

    def reset_demo(self) -> None:
        with self._lock:
            self._require_started()
            if self.mode not in {"demo", "unified"}:
                raise DashboardConflict("Only the private demo copy can be reset")
            if self._replay["running"]:
                raise DashboardConflict("Wait for the replay to finish before resetting")
            # Only this runtime's UUID-owned file is replaced, never its source.
            self._close_data()
            self.state_store = StateStore()
            self._replay = self._empty_replay()
            self._open_data()
