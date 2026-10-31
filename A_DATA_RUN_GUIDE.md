# Reproducing the Report with A's Generated Data

The current report uses data produced by A through the supplied generator. It
does not use the earlier hand-written development dataset.

## View a live charging session

1. Run this command from the project root:

   ```powershell
   .venv\Scripts\python.exe A2/src/dashboard/run.py --project-root . --mode demo --initial-station ST4D056616A3 --port 8051 --open-browser
   ```

2. The browser opens port **8051** and selects **Evie, 73 Belmore Road, Randwick NSW 2031**.
3. All initial metrics are zero, with no price or active session. This is the expected state of an empty event database.
4. Click **Run demo** and wait about **35 seconds**. One session becomes active, the price changes from 0.75 to 0.74, and the charging curve grows.
5. The final state shows **0** active sessions, **1** completed session, **22.60 kWh**, **AUD 16.72**, and **32 meter points**.
6. Click **Reset demo** to restore the empty event database before running the demonstration again.

The replay uses 36 events selected by A from the full stream: 2 price events,
1 session start, 32 meter readings, and 1 session end. Event contents and
timestamps are unchanged. Messages are published one second apart, so this is
an accelerated replay rather than a real 35-second charging session. The source
data does not provide plug types, so the page correctly displays `Not provided`.

## View the complete historical metrics

Run the following command from the project root to open port **8052**:

```powershell
.venv\Scripts\python.exe A2/src/dashboard/run.py --project-root . --mode history --port 8052 --open-browser
```

The formal database opens read-only and displays:

- Completed sessions: **5,084**.
- Total energy: **129,422.20 kWh**.
- Total cost: **AUD 88,908.69**.
- Active sessions: **0**, because every session in this dataset has ended.

These figures come from the complete 172,803-event dataset and are separate
from the single-session demonstration above. The full dataset was reconciled
through the database validation process; all 172,803 events were not replayed
again over MQTT.

## Data protection and report files

Live replay writes only to an isolated runtime copy, and reset does not modify
the formal database. History mode is read-only. SHA-256 values for the original
A1 database, formal A2 database, and empty-event seed remain unchanged before
and after the validation run.

The report is `A2/docs/member_d_report.pdf`, with editable source at
`A2/docs/member_d_report.md`. Report screenshots begin with `a_generated_` and
are stored in `A2/docs/images/`.

The recommended `Start_Dashboard.cmd` launcher uses unified mode to show C's
historical metrics and replay A's three-station MQTT sample on the same page.
The `--mode demo` and `--mode history` commands above provide the standalone
single-session demonstration and the read-only historical view. The retired
hand-written test database is not included in the delivery.
