# COMP5339 Assignment 2

- `A1/` contains the completed Assignment 1 acquisition, cleaning, augmentation and database pipeline.
- `A2/` contains the Assignment 2 synthetic generation, MQTT, event-storage and dashboard pipeline.

After cloning, run `git lfs install` and `git lfs pull` once to retrieve the
formal databases and complete assessment event stream.

Use Python 3.11 to 3.13 and install the unified A1/A2 environment from the
repository root with `python -m pip install -r requirements.txt`.

On Windows, double-click `Start_Dashboard.cmd` for C's history and A's MQTT
replay in one page. See `D_QUICKSTART.md` for the three-station replay and
the explicitly labelled 5,081 -> 5,084 session baseline; the source DB is unchanged.

Start with [A2/README.md](A2/README.md) and inspect the team interfaces in
[A2/docs/team_contracts.md](A2/docs/team_contracts.md).
