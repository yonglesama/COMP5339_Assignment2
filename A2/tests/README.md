# Tests to be implemented by each owner

- A: generator input/output, event ordering and unchanged-price suppression.
- B: message validation and price/start/meter/end state transitions.
- C: idempotent storage, transaction rollback, queries and metrics.
- D: dashboard callbacks or smoke tests supported by the selected framework.
- Shared: one end-to-end MQTT replay and final data reconciliation.

Use `data/examples/short_event_stream.jsonl` as the initial cross-member fixture.
