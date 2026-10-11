"""Transactional replay of a versioned, single-source change log."""
import argparse
import json
import sqlite3
from pathlib import Path


def connect(path=":memory:"):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.executescript("""
        CREATE TABLE IF NOT EXISTS inbox (
            event_id TEXT PRIMARY KEY, payload TEXT NOT NULL, outcome TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS entity_state (
            entity_id TEXT PRIMARY KEY, version INTEGER NOT NULL,
            deleted INTEGER NOT NULL, document TEXT);
    """)
    return db


def normalize(event):
    if not isinstance(event, dict):
        raise ValueError("change must be an object")
    for field in ("event_id", "entity_id"):
        if not isinstance(event.get(field), str) or not event[field].strip():
            raise ValueError(f"{field} is required")
    if type(event.get("version")) is not int or not 0 <= event["version"] < 2**63:
        raise ValueError("version must be a nonnegative signed 64-bit integer")
    if event.get("op") not in ("upsert", "delete"):
        raise ValueError("op must be upsert or delete")
    if event["op"] == "upsert" and not isinstance(event.get("document"), dict):
        raise ValueError("upsert requires a document object")
    if event["op"] == "delete" and event.get("document") is not None:
        raise ValueError("delete must not carry a document")
    return json.dumps(event, sort_keys=True, separators=(",", ":"), allow_nan=False)


def replay(db, events):
    counts = dict(applied=0, stale=0, duplicate=0)
    with db:
        for event in events:
            payload = normalize(event)
            previous = db.execute("SELECT payload FROM inbox WHERE event_id=?",
                                  (event["event_id"],)).fetchone()
            if previous:
                if previous["payload"] != payload:
                    raise ValueError("event_id collision; batch rolled back")
                counts["duplicate"] += 1
                continue
            current = db.execute("SELECT * FROM entity_state WHERE entity_id=?",
                                 (event["entity_id"],)).fetchone()
            document = (json.dumps(event["document"], sort_keys=True, allow_nan=False)
                        if event["op"] == "upsert" else None)
            deleted = int(event["op"] == "delete")
            if current and event["version"] == current["version"]:
                if (document, deleted) != (current["document"], current["deleted"]):
                    raise ValueError("conflicting changes at same version; batch rolled back")
                outcome = "stale"
            elif current and event["version"] < current["version"]:
                outcome = "stale"
            else:
                db.execute("""INSERT INTO entity_state VALUES (?,?,?,?)
                    ON CONFLICT(entity_id) DO UPDATE SET version=excluded.version,
                    deleted=excluded.deleted, document=excluded.document""",
                           (event["entity_id"], event["version"], deleted, document))
                outcome = "applied"
            db.execute("INSERT INTO inbox VALUES (?,?,?)", (event["event_id"], payload, outcome))
            counts[outcome] += 1
    return counts


def snapshot(db, include_deleted=False):
    rows = db.execute("SELECT * FROM entity_state ORDER BY entity_id")
    return [dict(entity_id=r["entity_id"], version=r["version"], deleted=bool(r["deleted"]),
                 document=json.loads(r["document"]) if r["document"] else None)
            for r in rows if include_deleted or not r["deleted"]]


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("input", type=Path)
    p.add_argument("--db", default="replica.db")
    args = p.parse_args()
    db = connect(args.db)
    try:
        events = [json.loads(line) for line in args.input.read_text().splitlines() if line.strip()]
        print(json.dumps({"replay": replay(db, events), "state": snapshot(db, True)}, indent=2))
    finally:
        db.close()
