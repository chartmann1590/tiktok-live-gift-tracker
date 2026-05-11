"""Tests for historical gift row repair (money.repair_*)."""
import os
import sqlite3
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from money import (  # noqa: E402
    diamonds_to_usd,
    repair_gifts_database,
    repair_row_diamonds_usd,
)


class TestRepairRowDiamondsUsd(unittest.TestCase):
    def test_rose_inferred_repeat(self):
        d, u, st = repair_row_diamonds_usd("Rose", 5)
        self.assertEqual(d, 5)
        self.assertEqual(st, "ok_inferred_repeat")
        self.assertEqual(u, diamonds_to_usd(5))

    def test_unknown_uses_stored_payload(self):
        d, u, st = repair_row_diamonds_usd("Unknown Gift Xyz", 42)
        self.assertEqual(d, 42)
        self.assertEqual(st, "payload_total")
        self.assertEqual(u, diamonds_to_usd(42))

    def test_ambiguous_indivisible_keeps_total(self):
        d, u, st = repair_row_diamonds_usd("Finger Heart", 13)
        self.assertEqual(d, 13)
        self.assertEqual(st, "ambiguous_indivisible")
        self.assertEqual(u, diamonds_to_usd(13))

    def test_explicit_repeat(self):
        d, u, st = repair_row_diamonds_usd(
            "Rose",
            999,
            repeat_count=3,
            payload_diamond_per_unit=0,
        )
        self.assertEqual(d, 3)
        self.assertEqual(st, "ok_explicit_repeat")
        self.assertEqual(u, diamonds_to_usd(3))

    def test_catalog_per_unit_with_inference(self):
        cat = {1001: 10}
        d, u, st = repair_row_diamonds_usd(
            "ignored name",
            50,
            gift_id=1001,
            catalog=cat,
        )
        self.assertEqual(d, 50)
        self.assertEqual(st, "ok_inferred_repeat")
        self.assertEqual(u, diamonds_to_usd(50))

    def test_idempotent_row(self):
        a = repair_row_diamonds_usd("GG", 10)
        b = repair_row_diamonds_usd("GG", a[0])
        self.assertEqual(a, b)


class TestRepairGiftsDatabase(unittest.TestCase):
    def setUp(self) -> None:
        fd, self._path = tempfile.mkstemp(suffix=".db")
        os.close(fd)

    def tearDown(self) -> None:
        if os.path.isfile(self._path):
            os.remove(self._path)
        for suf in ("-wal", "-shm"):
            p = self._path + suf
            if os.path.isfile(p):
                os.remove(p)

    def _init_db(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE gifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                sender TEXT NOT NULL,
                gift_name TEXT NOT NULL,
                diamond_value INTEGER NOT NULL,
                usd_value REAL NOT NULL,
                stream_id TEXT NOT NULL,
                timestamp TEXT DEFAULT '2020-01-01T00:00:00Z'
            );
            """
        )

    def test_updates_wrong_usd(self):
        conn = sqlite3.connect(self._path)
        try:
            self._init_db(conn)
            conn.execute(
                "INSERT INTO gifts (username, sender, gift_name, diamond_value, usd_value, stream_id) VALUES (?,?,?,?,?,?)",
                ("u", "s", "Rose", 5, 0.99, "sid"),
            )
            conn.commit()
        finally:
            conn.close()

        stats = repair_gifts_database(self._path, dry_run=False)
        self.assertEqual(stats["scanned"], 1)
        self.assertEqual(stats["updated"], 1)

        conn = sqlite3.connect(self._path)
        try:
            row = conn.execute(
                "SELECT diamond_value, usd_value FROM gifts WHERE id=1"
            ).fetchone()
            self.assertEqual(row[0], 5)
            self.assertAlmostEqual(row[1], diamonds_to_usd(5), places=2)
        finally:
            conn.close()

        stats2 = repair_gifts_database(self._path, dry_run=False)
        self.assertEqual(stats2["updated"], 0)
        self.assertEqual(stats2["unchanged"], 1)

    def test_dry_run_no_write(self):
        conn = sqlite3.connect(self._path)
        try:
            self._init_db(conn)
            conn.execute(
                "INSERT INTO gifts (username, sender, gift_name, diamond_value, usd_value, stream_id) VALUES (?,?,?,?,?,?)",
                ("u", "s", "Rose", 5, 0.0, "sid"),
            )
            conn.commit()
        finally:
            conn.close()

        repair_gifts_database(self._path, dry_run=True)
        conn = sqlite3.connect(self._path)
        try:
            row = conn.execute(
                "SELECT usd_value FROM gifts WHERE id=1"
            ).fetchone()
            self.assertEqual(row[0], 0.0)
        finally:
            conn.close()

    def test_optional_columns_passed(self):
        conn = sqlite3.connect(self._path)
        try:
            self._init_db(conn)
            conn.execute("ALTER TABLE gifts ADD COLUMN gift_id INTEGER")
            conn.execute("ALTER TABLE gifts ADD COLUMN repeat_count INTEGER")
            conn.execute(
                "ALTER TABLE gifts ADD COLUMN payload_diamond_count INTEGER"
            )
            conn.execute(
                "INSERT INTO gifts (username, sender, gift_name, diamond_value, usd_value, stream_id, gift_id, repeat_count, payload_diamond_count) VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    "u",
                    "s",
                    "n",
                    999,
                    0.0,
                    "sid",
                    1001,
                    4,
                    0,
                ),
            )
            conn.commit()
        finally:
            conn.close()

        repair_gifts_database(self._path, catalog={1001: 10})
        conn = sqlite3.connect(self._path)
        try:
            row = conn.execute(
                "SELECT diamond_value, usd_value FROM gifts WHERE id=1"
            ).fetchone()
            self.assertEqual(row[0], 40)
            self.assertAlmostEqual(row[1], diamonds_to_usd(40), places=2)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
