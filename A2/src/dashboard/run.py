"""Flask map dashboard. Default: read-only access to C's formal database."""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import sys

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from flask import Flask, jsonify, request, send_file
from werkzeug.serving import make_server
from dashboard.runtime import DashboardConflict, DashboardRuntime

ASSETS = Path(__file__).resolve().parent / "assets"


def create_app(runtime: DashboardRuntime) -> Flask:
    app = Flask(__name__, static_folder=str(ASSETS), static_url_path="/assets")
    app.json.sort_keys = False
    app.config["MAX_CONTENT_LENGTH"] = 4096

    @app.before_request
    def require_local_actions():
        if request.method == "POST":
            origin = request.headers.get("Origin")
            if origin and origin.rstrip("/") != request.host_url.rstrip("/"):
                return jsonify(error="Action must come from this dashboard"), 403
            if not request.is_json:
                return jsonify(error="Use application/json for dashboard actions"), 415

    @app.after_request
    def headers(response):
        if request.path.startswith("/api/") or request.path == "/" or request.path.startswith("/assets/"):
            response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @app.get("/")
    def index():
        return send_file(ASSETS / "index.html")

    @app.get("/api/snapshot")
    def snapshot():
        return jsonify(runtime.snapshot())

    @app.get("/api/stations/<station_id>")
    def station(station_id):
        try:
            return jsonify(runtime.station_detail(station_id, request.args.get("session_id") or None))
        except KeyError:
            return jsonify(error="Station or session not found at this station"), 404

    @app.post("/api/demo/replay")
    def replay():
        runtime.start_replay()
        return jsonify(accepted=True), 202

    @app.post("/api/demo/reset")
    def reset():
        runtime.reset_demo()
        return jsonify(reset=True)

    @app.errorhandler(DashboardConflict)
    def conflict(exc):
        return jsonify(error=str(exc)), 409

    @app.errorhandler(Exception)
    def error(exc):
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            return jsonify(error=exc.description), exc.code
        app.logger.exception("Dashboard request failed")
        return jsonify(error="Unable to load dashboard data; check the local server log"), 500

    return app


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=SRC_ROOT.parents[1])
    parser.add_argument("--mode", choices=["demo", "mqtt", "history", "unified"], default="history")
    parser.add_argument("--database", type=Path, help="Source database; live modes use a private copy")
    parser.add_argument("--stream", type=Path, help="Demo continuation JSONL")
    parser.add_argument("--mqtt-host")
    parser.add_argument("--mqtt-port", type=int)
    parser.add_argument("--port", type=int, default=8050)
    parser.add_argument("--replay-delay", type=float, default=1.0)
    parser.add_argument("--open-browser", action="store_true", help="Open the local dashboard in your browser")
    parser.add_argument("--initial-station", help="Station selected when the browser first opens")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    root = args.project_root.resolve()
    config = json.loads((root / "A2/config/mqtt.json").read_text(encoding="utf-8-sig"))
    if args.mqtt_host:
        config["host"] = args.mqtt_host
    if args.mqtt_port is not None:
        config["port"] = args.mqtt_port
    runtime = DashboardRuntime(root, mode=args.mode, database_path=args.database,
        stream_path=args.stream, mqtt_config=config, replay_delay=args.replay_delay)
    server = None
    try:
        runtime.start()
        server = make_server("127.0.0.1", args.port, create_app(runtime), threaded=True)
        from urllib.parse import urlencode
        url = f"http://127.0.0.1:{server.server_port}"
        if args.initial_station:
            url += "/?" + urlencode({"station": args.initial_station})
        print(f"Charge Atlas: {url}", flush=True)
        print(f"Mode: {args.mode}; database: {runtime.database_path}", flush=True)
        if args.open_browser:
            from threading import Timer
            import webbrowser
            Timer(1, webbrowser.open, args=[url]).start()
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    except Exception:
        logging.exception("Dashboard could not run")
        return 1
    finally:
        if server is not None:
            server.server_close()
        runtime.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
