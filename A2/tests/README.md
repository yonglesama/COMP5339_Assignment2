# Member A tests

Run from the project root:

```powershell
python -m unittest discover -s A2/tests -p test_member_a.py -v
python A2/validate_member_a.py
python A2/tests/mqtt_smoke.py
python A2/tests/mqtt_smoke.py --stream A2/data/demo/event_stream.jsonl --report-prefix mqtt_demo
```

The unit tests cover event preparation, validation and publisher timing. The full-file validator checks A1 references, event order and session histories. MQTT tests require `A2/requirements-dev.txt`; they start a temporary broker and compare received messages with the source stream. Results are written to `A2/data/quality/`.

B/C/D tests and the integrated dashboard replay remain with their owners.
