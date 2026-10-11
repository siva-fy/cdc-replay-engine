import unittest
from cdc import connect, replay, snapshot


def change(version=1, **updates):
    return dict(event_id=str(version), entity_id="a", version=version, op="upsert",
                document={"plan": str(version)}) | updates


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.db = connect()
        self.addCleanup(self.db.close)

    def test_out_of_order_updates(self):
        result = replay(self.db, [change(3), change(1), change(2)])
        self.assertEqual(result["stale"], 2)
        self.assertEqual(snapshot(self.db)[0]["version"], 3)

    def test_tombstone_blocks_old_resurrection(self):
        replay(self.db, [change(3, op="delete", document=None), change(2)])
        self.assertEqual(snapshot(self.db), [])
        self.assertTrue(snapshot(self.db, True)[0]["deleted"])
        replay(self.db, [change(4)])
        self.assertEqual(snapshot(self.db)[0]["version"], 4)

    def test_replay_is_idempotent(self):
        replay(self.db, [change()])
        self.assertEqual(replay(self.db, [change()])["duplicate"], 1)

    def test_collision_rolls_back_prior_changes_in_batch(self):
        replay(self.db, [change()])
        with self.assertRaises(ValueError):
            replay(self.db, [change(2), change(3, event_id="1")])
        self.assertEqual(snapshot(self.db)[0]["version"], 1)
        self.assertEqual(self.db.execute("SELECT count(*) FROM inbox").fetchone()[0], 1)

    def test_equal_version_conflict(self):
        replay(self.db, [change()])
        with self.assertRaises(ValueError):
            replay(self.db, [change(event_id="other", document={"plan": "different"})])

    def test_invalid_change_rolls_back(self):
        with self.assertRaises(ValueError):
            replay(self.db, [change(), change(version=True, event_id="2")])
        self.assertEqual(snapshot(self.db), [])

    def test_entities_are_independent(self):
        replay(self.db, [change(), change(2, entity_id="b")])
        self.assertEqual(len(snapshot(self.db)), 2)


if __name__ == "__main__":
    unittest.main()
