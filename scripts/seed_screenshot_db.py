"""
Create a disposable SQLite database with synthetic labels only (no real TikTok users).
Used with DISABLE_TIKTOK_LISTENERS=1 and DEMO_STREAM_ID for README / site screenshots.

1) Seed the demo database (from repo root, PowerShell example):

  $env:DATA_DIR = (Resolve-Path .\\docs\\images\\readme-demo-data)
  $env:DEMO_STREAM_ID = "demo_room_active"
  python scripts/seed_screenshot_db.py

2) Run exactly one app instance on an unused port (Windows may bind multiple
   listeners to the same port if several `python app.py` processes are started):

  $env:DISABLE_TIKTOK_LISTENERS = "1"
  $env:PORT = "28342"
  Start-Process python -ArgumentList "app.py" -WorkingDirectory (Get-Location)

3) Open http://127.0.0.1:28342/ in a browser, select the demo row, capture
   docs/images/readme-dashboard.png. For the empty state, use a fresh DATA_DIR
   with no gifts.db and no tracked_users rows, same flags, capture
   docs/images/readme-empty-state.png.
"""
from __future__ import annotations

import os
import sqlite3
import sys

# Synthetic label (documentation only; not a TikTok account).
DEMO_CREATOR = "demo_channel_slot"
DEMO_STREAM_CURRENT = os.environ.get("DEMO_STREAM_ID", "demo_room_active").strip() or "demo_room_active"
DEMO_STREAM_PAST = "demo_room_archive"

# Gift senders: generic labels, not TikTok handles.
SENDERS = ("Viewer A", "Viewer B", "Viewer C")


def _schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS gifts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            sender TEXT NOT NULL,
            gift_name TEXT NOT NULL,
            diamond_value INTEGER NOT NULL,
            usd_value REAL NOT NULL,
            stream_id TEXT NOT NULL,
            timestamp DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        );
        CREATE INDEX IF NOT EXISTS idx_gifts_username ON gifts(username);
        CREATE INDEX IF NOT EXISTS idx_gifts_username_stream ON gifts(username, stream_id);
        CREATE INDEX IF NOT EXISTS idx_gifts_timestamp ON gifts(timestamp);
        CREATE TABLE IF NOT EXISTS tracked_users (
            username TEXT PRIMARY KEY,
            added_at DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        );
        """
    )


def main() -> int:
    data_dir = os.environ.get("DATA_DIR", "").strip()
    if not data_dir:
        print("ERROR: Set DATA_DIR to the folder for gifts.db", file=sys.stderr)
        return 1
    os.makedirs(data_dir, exist_ok=True)
    db_path = os.path.join(data_dir, "gifts.db")
    for suffix in ("", "-wal", "-shm"):
        p = db_path + suffix if suffix else db_path
        if os.path.isfile(p):
            try:
                os.remove(p)
            except OSError as e:
                print(f"ERROR: Could not remove {p}: {e}", file=sys.stderr)
                return 1

    conn = sqlite3.connect(db_path)
    try:
        _schema(conn)
        conn.execute("DELETE FROM gifts")
        conn.execute("DELETE FROM tracked_users")
        conn.execute(
            "INSERT INTO tracked_users (username, added_at) VALUES (?, ?)",
            (DEMO_CREATOR, "2025-01-15T18:00:00Z"),
        )

        rows = [
            # Past stream
            (
                DEMO_CREATOR,
                SENDERS[0],
                "Rose",
                10,
                0.05,
                DEMO_STREAM_PAST,
                "2025-01-10T20:01:00Z",
            ),
            (
                DEMO_CREATOR,
                SENDERS[1],
                "Finger Heart",
                50,
                0.25,
                DEMO_STREAM_PAST,
                "2025-01-10T20:03:00Z",
            ),
            (
                DEMO_CREATOR,
                SENDERS[2],
                "Drama Queen",
                500,
                2.50,
                DEMO_STREAM_PAST,
                "2025-01-10T20:08:00Z",
            ),
            (
                DEMO_CREATOR,
                SENDERS[0],
                "Rose",
                10,
                0.05,
                DEMO_STREAM_PAST,
                "2025-01-10T20:12:00Z",
            ),
            # Current stream (matches DEMO_STREAM_ID default)
            (
                DEMO_CREATOR,
                SENDERS[1],
                "Ice Cream Cone",
                100,
                0.50,
                DEMO_STREAM_CURRENT,
                "2025-01-16T19:00:10Z",
            ),
            (
                DEMO_CREATOR,
                SENDERS[2],
                "Finger Heart",
                50,
                0.25,
                DEMO_STREAM_CURRENT,
                "2025-01-16T19:02:00Z",
            ),
            (
                DEMO_CREATOR,
                SENDERS[0],
                "Confetti",
                1000,
                5.00,
                DEMO_STREAM_CURRENT,
                "2025-01-16T19:05:00Z",
            ),
            (
                DEMO_CREATOR,
                SENDERS[1],
                "Rose",
                10,
                0.05,
                DEMO_STREAM_CURRENT,
                "2025-01-16T19:06:00Z",
            ),
            (
                DEMO_CREATOR,
                SENDERS[2],
                "Tiny Diny",
                100,
                0.50,
                DEMO_STREAM_CURRENT,
                "2025-01-16T19:09:00Z",
            ),
        ]
        conn.executemany(
            """
            INSERT INTO gifts (username, sender, gift_name, diamond_value, usd_value, stream_id, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    print(f"Seeded {db_path} with synthetic demo data ({len(rows)} gifts).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
