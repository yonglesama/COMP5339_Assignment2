# Member D dashboard handoff

## Recommended Windows entry: one history/live page

`Start_Dashboard.cmd` now explicitly selects `--mode unified --replay-delay 0.5`.
It reads C's formal database and prepares a private working copy with three
completed sessions held out at three distinct stations. The unchanged matching
87 events are selected from A's complete stream and sent through A's real MQTT
publisher, B's subscriber and C's repository. History and live curves share one
page and one working database. Initial completed count is 5,081; completion returns
to 5,084, 129,422.20 kWh and AUD 88,908.69. Reset restores only that baseline.

The UI explicitly explains the holdout. This is a selected-session baseline, not
a global point-in-time snapshot. No A event ID, payload, sequence or simulated
timestamp is rewritten. `database/replay_snapshot.py` verifies A/C event identity,
source fingerprints and baseline counts before opening the subscriber. Each UUID
runtime directory contains `replay_provenance.json` and the selected A JSONL.

At the supplied dataset, the stations are ST6DC062AC32, ST1A14ED4000 and
STFFE05F9F25; their meter counts are 22, 17 and 39. Replay takes about 43 seconds.
Choose the active-station buttons to inspect each live curve. The generic station
and session routing is shared with external MQTT mode; no station-specific chart
code is used. This is representative multi-station validation, not a 172,803-event
paced MQTT execution.

## Read-only complete history remains available

`python A2/run_pipeline.py --stage dashboard` and `DashboardRuntime(root)`
open C's formal
`A2/data/final/ev_chargers_a2.duckdb` read-only by default.
The history-only view has no replay/reset actions and does not start a broker.
It shows 414 mapped DC stations, 5,084 completed sessions, 129,422.20 kWh and
AUD 88,908.69. Zero active sessions is the expected final state.
To open this mode in a browser on port 8052, run:

```powershell
python A2/src/dashboard/run.py --project-root . --mode history --port 8052 --open-browser
```
The earlier hand-written UI database, continuation stream and executable
references have been removed from the delivery project.

## Start

Use Python 3.11 to 3.13. Double-click `Start_Dashboard.cmd` on Windows, or:

```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe A2/src/dashboard/run.py --project-root . --mode unified --replay-delay 0.5 --open-browser
```

The default page is http://127.0.0.1:8050. Stop any older running dashboard
process before restarting; browser refresh does not reload the Python server.
The Flask server binds to loopback without the debugger/reloader.

## Optional live replay uses A-generated events

The optional command below opens port 8051 and explicitly selects demo mode. It
copies `A2/data/demo/a_generated_empty.duckdb`, which C's initialiser built
from A1 reference tables, the A2 schema and connector mapping with no events.
It then uses A's unchanged `A2/data/demo/event_stream.jsonl` for replay.
No hand-written charging values are inserted.

The A-generated 36-event subset contains two prices, one start, 32 meter
readings and one end for an MG4 at Evie's 73 Belmore Road, Randwick. IDs,
sequence numbers, timestamps and payloads match the complete generated stream.
Run demo uses A's publisher, QoS 1 and one-second spacing through B and C.
The final state has one completed session, 22.60 kWh, AUD 16.72 and zero active
sessions. Playback takes about 35 seconds but preserves original simulated
timestamps and the 32-minute charge duration.

Replay/reset act only on a UUID-owned copy under `A2/data/runtime/`.
Reset restores empty event history and cannot clear C's formal database.
Do not run a separate B subscriber against D's writable runtime copy.

```powershell
python -m pip install -r requirements.txt
python A2/src/dashboard/run.py --project-root . --mode demo --initial-station ST4D056616A3 --port 8051 --open-browser
```

## External MQTT mode

```powershell
python A2/src/dashboard/run.py --project-root . --mode mqtt --database PATH_TO_SOURCE.duckdb --mqtt-host localhost --mqtt-port 1883
```

An explicit source is required. D copies it before opening the writable repository
and prints the actual working file path. Broker/TLS/credentials follow A/B's
configuration. Use the populated working copy as the next source to resume.
A populated history correctly deduplicates previously received events.

## Module boundaries and UI

`dashboard/runtime.py` owns one C EventRepository and one B StateStore.
Live modes also own one B SubscriptionService; history only restores a snapshot.
D contains no SQL or MQTT payload parser. `dashboard/run.py` supplies Flask
routes; `assets/` contains Leaflet 1.9.4, HTML/CSS and SVG charts.

Markers show price/occupancy. Clicks show address/name, operator, plug types,
latest price and connector usage. Search, operator/occupied filters, all-station
bounds and active focus are available. A station URL query can preselect a site.
Selected/active markers are brought forward when nearby sites overlap.

Polling updates every second. Energy/power/SoC curves use received/generated
meter samples. After a selected session ends, C provides its historical curve.
Switching stations automatically selects an active session or, if none exists,
the newest completed session. The Session selector lists all recorded sessions
at the chosen station, labels Live/History, and preserves manual selection across
polls. Default selection follows a new live session automatically; switching
stations clears a previous manual selection. Completed curves are explicitly
described as historical. Empty stores remain empty without fabricated samples.
Missing names use addresses and missing
plug types remain Not provided. External base tiles need internet; station markers
remain usable on a grid if the tiles are unavailable.

## API

| Route | Purpose |
|---|---|
| GET /api/snapshot | Stations, live state, metrics and status |
| GET /api/stations/<id> | Details, available sessions and default live/latest-history curve |
| GET /api/stations/<id>?session_id=... | Explicit active/completed curve |
| POST /api/demo/replay | Publish A's generated events, demo/unified modes |
| POST /api/demo/reset | Restore only the private demo/unified baseline |

Unknown station/session pairs return 404. Inappropriate actions return 409.
Write actions require same-origin JSON. C datetimes are serialised as ISO.

## Verification and report

```powershell
python -m unittest discover -s A2/tests -p "test_*.py" -v
```

The 57-test suite includes 17 D tests. D test databases use C's empty schema and
unchanged A-generated messages, including active-state prefixes. Tests cover C as
the read-only default, APIs, curves, lifecycle, protected source hashes, real MQTT
replay and empty reset; they do not use the retired hand-written database.
The additional unified test checks three concurrent active stations, growth of
all three curves without cross-station mixing, exact A-line provenance, final C
metrics, deduplication, reset and formal database protection.
Four further tests verify every one of C's 414 station histories, older-session
selection, cross-station rejection, unchanged current metrics/source hashes and
no-store HTML/JS responses to avoid stale selection logic in the browser.
They also reproduce session-end cache eviction and verify that a committed
historical curve remains available. D reads committed meter rows from C for
both live polling and history, avoiding a mismatch with B's cache transitions.

Unified browser acceptance: run `node A2/tests/dashboard_unified_browser.cjs`
against the unified server. Set DASHBOARD_BASE_URL to change its default 8050 URL.
Current evidence is `data/quality/d_unified_validation.json`; figures are
`docs/images/unified_*.png`. The automated/browser checks pass with 87 real MQTT
messages and no JavaScript page errors.

Optional browser acceptance uses Node/Playwright. Start A replay on 8051 and
C history on 8052, then run `node A2/tests/dashboard_a_generated_browser.cjs`
with DASHBOARD_HISTORY_URL=http://127.0.0.1:8052.
Use DASHBOARD_BASE_URL / DASHBOARD_HISTORY_URL for other ports. The older
`dashboard_browser.cjs` delegates to the same A-data test.

Report figures are `docs/images/a_generated_*.png`. Report files are
`member_d_report.md` / `.pdf`, and evidence is in
`data/quality/d_a_generated_report_validation.json`.
Full stored history and representative MQTT replay are explicitly separated.
The existing PDF reports the earlier 36-event run; the new 87-event unified run
is documented separately in the current handoff and d_unified_validation.json.
Full-stream paced MQTT, durable outage recovery and production deployment remain
untested. Upstream plug types are missing for 248 stations.
See `member_d_ai_usage.md` for AI assistance.

References: Flask https://flask.palletsprojects.com/en/stable/quickstart/ ;
Leaflet https://leafletjs.com/reference.html ;
OpenStreetMap https://operations.osmfoundation.org/policies/tiles/ .
