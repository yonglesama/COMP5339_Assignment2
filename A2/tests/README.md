# Member A tests

Run from the project root:

```powershell
python -m unittest discover -s A2/tests -p test_member_a.py -v
python A2/validate_member_a.py
python A2/tests/mqtt_smoke.py
python A2/tests/mqtt_smoke.py --stream A2/data/demo/event_stream.jsonl --report-prefix mqtt_demo
```

The unit tests cover event preparation, validation and publisher timing. The full-file validator checks A1 references, event order and session histories. MQTT tests require `A2/requirements-dev.txt`; they start a temporary broker and compare received messages with the source stream. Results are written to `A2/data/quality/`.

B/C tests and D's dashboard tests are included. Run all 57 tests with:

```powershell
python -m unittest discover -s A2/tests -p "test_*.py" -v
```

The D integration test uses a private copy of `a_generated_empty.duckdb`, a local
broker and A's real publisher. Optional browser acceptance is documented in
`A2/docs/member_d_handoff.md` and runs with `node A2/tests/dashboard_browser.cjs`.

`test_unified_dashboard.py` additionally checks three simultaneous stations using
87 unchanged A events, a C-derived historical baseline, final metric reconciliation,
duplicate suppression, private-copy reset and source hash protection. For unified
browser acceptance, start `Start_Dashboard.cmd` and run
`node A2/tests/dashboard_unified_browser.cjs` with `DASHBOARD_BASE_URL` set to
`http://127.0.0.1:8050`. This browser test starts and resets replay in that working copy.

`test_station_history_dashboard.py` checks all 414 formal-database stations,
default historical curves, explicit older-session selection, cross-station
isolation, session-end cache eviction, source hashes and uncached HTML/JS. Unified browser acceptance also
clicks three non-replay markers and selects older history before testing live
updates at the three replay stations.
