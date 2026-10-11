import json
from pathlib import Path
from cdc import connect, replay, snapshot

events = [json.loads(s) for s in (Path(__file__).parent / "examples/changes.jsonl").read_text().splitlines()]
with connect() as db:
    first = replay(db, events)
    again = replay(db, events)
    assert first == {"applied": 3, "stale": 2, "duplicate": 1}
    assert again["duplicate"] == 6
    assert len(snapshot(db)) == 1
    print(json.dumps({"first_run": first, "replay": again, "state": snapshot(db, True)}, indent=2))
