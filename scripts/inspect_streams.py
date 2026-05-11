"""
Read-only inspection of the gifts DB for a single TikTok user.

Usage:
    python scripts/inspect_streams.py <db_path> <username>

Prints:
    * table list
    * distinct usernames in `gifts`
    * tracked_users rows
    * per-stream summary (started_at, last_at, gift_count, total_usd) ordered by start time
"""
import sqlite3
import sys


def main(db_path: str, username: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    print(f"--- db: {db_path} ---")
    print("--- tables ---")
    for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        print(r["name"])

    print("--- distinct usernames in gifts ---")
    for r in conn.execute("SELECT DISTINCT username FROM gifts ORDER BY username"):
        print(repr(r["username"]))

    print("--- tracked_users ---")
    for r in conn.execute("SELECT username, added_at FROM tracked_users ORDER BY added_at"):
        print(dict(r))

    print(f"--- streams for {username!r} (oldest first) ---")
    rows = conn.execute(
        """
        SELECT stream_id,
               MIN(timestamp) AS started_at,
               MAX(timestamp) AS last_at,
               COUNT(*)       AS gift_count,
               ROUND(SUM(usd_value), 2) AS total_usd,
               COUNT(DISTINCT sender)   AS unique_senders
        FROM gifts
        WHERE username = ?
        GROUP BY stream_id
        ORDER BY MIN(timestamp) ASC
        """,
        (username,),
    ).fetchall()
    for i, r in enumerate(rows, start=1):
        d = dict(r)
        d["#"] = i
        print(d)
    print(f"total streams for {username}: {len(rows)}")
    conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: python scripts/inspect_streams.py <db_path> <username>", file=sys.stderr)
        sys.exit(2)
    main(sys.argv[1], sys.argv[2])
