# Installation and Operation

## Installation

Use Python 3.11 and run the following command from the project root containing
`A1/` and `A2/`:

```powershell
python -m pip install -r requirements.txt
```

The unified root file includes the local broker and MQTT test dependencies.

## Local replay

Terminal 1: start the broker on `127.0.0.1:1883`.

```powershell
python A2/src/publishing/local_broker.py
```

Terminal 2: run the subscriber for `comp5339/ev/#`. The subscriber interfaces
are implemented in `A2/src/subscription/`.

Terminal 3: publish the 36 demo messages with a one-second interval.

```powershell
python A2/src/publishing/run.py --project-root . --stream A2/data/demo/event_stream.jsonl --delay 1
```

For multi-computer testing, use a broker reachable by both machines. Add
`--host ADDRESS --port PORT` to the publishing command and configure the
subscriber with the same address. The local broker above accepts connections
only from the same computer.

Omit `--stream` to publish the complete one-day stream. The full stream has
172,803 messages and needs at least 4.8 hours at the minimum 0.1-second interval.
The final DuckDB imports and validates the complete stream offline through C's
same storage interface; the demo stream provides the real MQTT verification.

## Regenerate the data

Rebuild the formal dataset:

```powershell
python A2/run_member_a.py
```

The fixed parameters are 414 DC stations, one day beginning 2026-10-01, AEMO
NSW1 prices, `seed=5339`, and one-minute meter intervals. Price, session, and
meter CSV files are written to `A2/data/generated/`; the complete message stream
is written to `A2/data/events/event_stream.jsonl`.

Timestamps retain the supplied generator's fixed, offset-free simulation labels
and are not converted for daylight saving. Labels spanning a clock-change date
must not be interpreted directly as precise local instants.

For a small dataset, run `python A2/run_member_a.py --profile dev`; output is
written to `A2/data/dev/`.

To rebuild individual stages, run them in this order:

```powershell
python A2/run_pipeline.py --stage station-export
python A2/run_pipeline.py --stage synthetic-generation
python A2/run_pipeline.py --stage event-preparation
python A2/run_pipeline.py --stage demo-preparation
```

## Tests

```powershell
python -m unittest discover -s A2/tests -p test_member_a.py -v
python A2/validate_member_a.py
python A2/tests/mqtt_smoke.py --stream A2/data/demo/event_stream.jsonl --report-prefix mqtt_demo
```

The final command automatically starts a temporary broker and subscriber.
`A2/data/quality/mqtt_demo_summary.csv` should report
`expected_messages=36`, `received_messages=36`,
`identical_payloads_and_order=True`, and a minimum publish interval of at least
0.1 seconds.
