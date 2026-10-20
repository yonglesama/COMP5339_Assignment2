# Shared example events

Member A owns the versioned JSON contract. These examples let B, C and D develop
without waiting for a full generator run.

`short_event_stream.jsonl` is one complete scenario:

```text
price change -> session start -> two meter readings -> session end
```

It uses a real A1 `station_id`. The files are fixtures, not assessed synthetic output.
When the contract changes, A updates the examples and all downstream contract tests in
the same change.
