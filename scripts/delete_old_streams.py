"""
Delete old/mock stream rows for a single TikTok user from `gifts.db`.

Schema (per app.py):
    gifts(id, username, sender, gift_name, diamond_value, usd_value, stream_id, timestamp)
    tracked_users(username, added_at)

There are no foreign keys; only `gifts` rows reference `stream_id`. We touch nothing
in `tracked_users` — the user stays tracked, only old gift history is removed.

Safety rules:
    * Always run inside a single transaction.
    * Always filter by BOTH `username` AND `stream_id` so other users' rows on the
      same stream_id (if any) are never touched.
    * Refuse to run if the keep_stream_id is missing or has 0 gifts for the user.
    * --apply must be passed explicitly; default is a dry-run that prints what
      would be deleted and rolls back.

Usage (host):
    python scripts/delete_old_streams.py H:\\tiktok-monitor\\data\\gifts.db vyly0175dsn \\
        --delete old_stream_1 --delete old_stream_2 --keep 7638302302566107917
    # add --apply to actually commit

Usage (docker):
    docker cp scripts/delete_old_streams.py <container>:/tmp/delete_old_streams.py
    docker exec <container> python /tmp/delete_old_streams.py /app/data/gifts.db vyly0175dsn \\
        --delete old_stream_1 --delete old_stream_2 --keep 7638302302566107917 --apply
"""
import argparse
import sqlite3
import sys


def summarize_streams(conn: sqlite3.Connection, username: str) -> list[dict]:
    rows = conn.execute(
        """
        SELECT stream_id,
               MIN(timestamp) AS started_at,
               MAX(timestamp) AS last_at,
               COUNT(*)       AS gift_count,
               ROUND(SUM(usd_value), 2) AS total_usd
        FROM gifts
        WHERE username = ?
        GROUP BY stream_id
        ORDER BY MIN(timestamp) ASC
        """,
        (username,),
    ).fetchall()
    return [dict(r) for r in rows]


def cross_user_check(
    conn: sqlite3.Connection, username: str, stream_ids: list[str]
) -> dict[str, list[str]]:
    """For each stream_id we plan to delete, list any OTHER usernames that share it."""
    out: dict[str, list[str]] = {}
    for sid in stream_ids:
        others = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT username FROM gifts WHERE stream_id = ? AND username != ?",
                (sid, username),
            ).fetchall()
        ]
        out[sid] = others
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("db_path")
    p.add_argument("username")
    p.add_argument("--delete", action="append", required=True, dest="delete_ids",
                   help="stream_id to delete (repeatable)")
    p.add_argument("--keep", action="append", default=[], dest="keep_ids",
                   help="stream_id that MUST remain intact (repeatable, used as a guard)")
    p.add_argument("--apply", action="store_true",
                   help="actually commit; without this flag everything is rolled back")
    args = p.parse_args()

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None  # manual transaction control

    print(f"=== db: {args.db_path}")
    print(f"=== user: {args.username}")

    before = summarize_streams(conn, args.username)
    print("--- streams BEFORE ---")
    for r in before:
        print(r)

    have_ids = {r["stream_id"] for r in before}
    missing_delete = [s for s in args.delete_ids if s not in have_ids]
    missing_keep = [s for s in args.keep_ids if s not in have_ids]
    if missing_delete or missing_keep:
        print(
            f"ABORT: stream_ids not present for user "
            f"(missing_delete={missing_delete}, missing_keep={missing_keep})",
            file=sys.stderr,
        )
        return 2

    overlap = cross_user_check(conn, args.username, args.delete_ids)
    for sid, others in overlap.items():
        if others:
            print(
                f"NOTE: stream_id {sid!r} is also used by other users {others}; "
                f"DELETE is scoped by username so they will NOT be touched."
            )

    conn.execute("BEGIN")
    try:
        deleted_per_stream: dict[str, int] = {}
        for sid in args.delete_ids:
            cur = conn.execute(
                "DELETE FROM gifts WHERE username = ? AND stream_id = ?",
                (args.username, sid),
            )
            deleted_per_stream[sid] = cur.rowcount

        for sid in args.keep_ids:
            n = conn.execute(
                "SELECT COUNT(*) FROM gifts WHERE username = ? AND stream_id = ?",
                (args.username, sid),
            ).fetchone()[0]
            if n == 0:
                raise RuntimeError(
                    f"safety check failed: keep stream_id {sid!r} has 0 rows after delete"
                )

        print("--- delete counts ---")
        for sid, n in deleted_per_stream.items():
            print(f"  {sid}: {n} rows from gifts")
        print(f"  TOTAL: {sum(deleted_per_stream.values())} rows from gifts")

        if args.apply:
            conn.execute("COMMIT")
            print("=== COMMITTED ===")
        else:
            conn.execute("ROLLBACK")
            print("=== DRY RUN — rolled back. Re-run with --apply to commit. ===")
    except Exception as e:
        conn.execute("ROLLBACK")
        print(f"ROLLBACK due to error: {e}", file=sys.stderr)
        return 1

    after = summarize_streams(conn, args.username)
    print("--- streams AFTER ---")
    for r in after:
        print(r)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
