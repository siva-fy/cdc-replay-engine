# CDC Replay Engine

**Rebuild current entity state from duplicated and out-of-order changes.**

[![CI](https://github.com/siva-fy/cdc-replay-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/siva-fy/cdc-replay-engine/actions/workflows/ci.yml)

A small change-data-capture consumer that makes ordering, deletion and replay
semantics explicit. Python + SQLite, with a persistent inbox and versioned
entity state.

## Quick start

Python 3.11+ with SQLite support; no third-party dependencies.

~~~bash
git clone https://github.com/siva-fy/cdc-replay-engine.git
cd cdc-replay-engine
python3 demo.py
python3 -m unittest discover -s tests -v
~~~

The synthetic log includes an update arriving after a newer version, a delete
arriving before an old insert, and a repeated event. The first replay reports
**3 applied, 2 stale and 1 duplicate**. The second reports **6 duplicates**.
The deleted entity stays deleted.

## Architecture

~~~mermaid
flowchart LR
  A[Change log] --> B[Validate event envelope]
  B --> C{Event ID in inbox?}
  C -->|Same payload| D[Skip duplicate]
  C -->|New event| E[Compare entity version]
  E --> F[Upsert state or retain tombstone]
  F --> G[Commit inbox and state together]
~~~

## Durable replay

~~~bash
python3 cdc.py examples/changes.jsonl --db replica.db
python3 cdc.py examples/changes.jsonl --db replica.db
~~~

Each event has an `event_id`, `entity_id`, nonnegative integer `version` and
`op` (`upsert` or `delete`). Upserts carry a JSON `document`; deletes carry no
document or `null`.

| Input | Result |
| --- | --- |
| Unseen event with newer entity version | Apply state and record inbox entry |
| Unseen event with older version | Record as stale; preserve current state |
| Same event ID and same payload | Skip as duplicate |
| Same event ID, different payload | Reject and roll back the entire batch |
| Same entity/version, different state | Reject and roll back the entire batch |
| Delete followed by an older insert | Preserve tombstone; prevent resurrection |
| Upsert with a version newer than tombstone | Allow explicit recreation |

`snapshot(db)` hides tombstones by default; `snapshot(db, True)` includes them.
The inbox stores canonical input for collision detection and auditability.

## Assumptions and boundaries

Versions must come from one authoritative sequence per entity. This does not
resolve multi-source conflicts, consume database WAL, connect to Kafka, or provide
distributed exactly-once processing. Run one writer; concurrency coordination,
inbox pruning and broker offset checkpoints are future work. Inbox and entity
state are committed in the same SQLite transaction.

## Tests and next steps

Tests cover stale updates, tombstones, explicit recreation, duplicate delivery,
same-version conflicts, invalid envelopes, atomic rollback and entity isolation.
CI runs tests and the deterministic demo across Python 3.11–3.13.
[Inspect the output](examples/demo-output.txt).

Extend it with a Debezium-envelope adapter, checkpoint persistence, and
fault-injection tests around consumer restarts.

Original AI-assisted portfolio project using synthetic changes.
[Provenance](PROVENANCE.md) · [MIT license](LICENSE) ·
[SQLite UPSERT reference](https://www.sqlite.org/lang_upsert.html)
