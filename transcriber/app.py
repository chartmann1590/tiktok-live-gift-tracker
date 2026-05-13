import os
import sqlite3
import threading
import atexit

from flask import Flask, request, jsonify

from capture import AudioCaptureManager
from worker import TranscriptionWorker

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(
    os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
)
DB_PATH = os.path.join(DATA_DIR, "transcripts.db")

app = Flask(__name__)

active_captures: dict[str, AudioCaptureManager] = {}
active_lock = threading.Lock()
_worker: TranscriptionWorker | None = None


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = get_db()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS transcripts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            original_text TEXT NOT NULL,
            detected_language TEXT,
            chunk_index INTEGER NOT NULL DEFAULT 0,
            timestamp DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_transcripts_username ON transcripts(username)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_transcripts_username_stream ON transcripts(username, stream_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_transcripts_timestamp ON transcripts(timestamp)"
    )
    conn.commit()
    conn.close()


def _insert_transcript(username, stream_id, original_text, detected_language, chunk_index):
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO transcripts (username, stream_id, original_text, detected_language, chunk_index) "
            "VALUES (?, ?, ?, ?, ?)",
            (username, stream_id, original_text, detected_language, chunk_index),
        )
        conn.commit()
    finally:
        conn.close()


def _get_worker():
    global _worker
    if _worker is None:
        _worker = TranscriptionWorker()
    return _worker


@app.route("/api/health")
def api_health():
    return jsonify({"status": "ok"})


@app.route("/api/start", methods=["POST"])
def api_start():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip().lstrip("@").lower()
    stream_url = (data.get("stream_url") or "").strip()
    stream_id = (data.get("stream_id") or "unknown").strip()

    if not username or not stream_url:
        return jsonify({"error": "username and stream_url are required"}), 400

    with active_lock:
        for existing_user, existing_mgr in list(active_captures.items()):
            if existing_user != username:
                existing_mgr.stop()
                del active_captures[existing_user]

        if username in active_captures:
            old = active_captures.pop(username)
            old.stop()

        manager = AudioCaptureManager(
            username=username,
            stream_url=stream_url,
            stream_id=stream_id,
            worker=_get_worker(),
            on_transcript=_insert_transcript,
        )
        active_captures[username] = manager
        manager.start()

    return jsonify({"status": "started", "username": username})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip().lstrip("@").lower()

    if not username:
        return jsonify({"error": "username is required"}), 400

    with active_lock:
        manager = active_captures.pop(username, None)

    if manager:
        manager.stop()
        return jsonify({"status": "stopped", "username": username})

    return jsonify({"error": f"Not transcribing @{username}"}), 404


@app.route("/api/transcripts/<username>")
def api_transcripts(username):
    username = username.strip().lower()
    limit = request.args.get("limit", 50, type=int)
    limit = min(max(limit, 1), 500)
    stream_id = request.args.get("stream_id", None)
    offset = request.args.get("offset", 0, type=int)

    conn = get_db()
    try:
        if stream_id:
            rows = conn.execute(
                "SELECT id, username, stream_id, original_text, detected_language, "
                "chunk_index, timestamp FROM transcripts "
                "WHERE username = ? AND stream_id = ? ORDER BY id ASC LIMIT ? OFFSET ?",
                (username, stream_id, limit, offset),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, username, stream_id, original_text, detected_language, "
                "chunk_index, timestamp FROM transcripts "
                "WHERE username = ? ORDER BY id ASC LIMIT ? OFFSET ?",
                (username, limit, offset),
            ).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route("/api/transcripts/<username>/streams")
def api_transcript_streams(username):
    username = username.strip().lower()
    conn = get_db()
    try:
        rows = conn.execute(
            """
            SELECT stream_id,
                   MIN(timestamp) AS started_at,
                   MAX(timestamp) AS last_transcript_at,
                   COUNT(*) AS transcript_count
            FROM transcripts
            WHERE username = ?
            GROUP BY stream_id
            ORDER BY MIN(timestamp) DESC
            """,
            (username,),
        ).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route("/api/status/<username>")
def api_status(username):
    username = username.strip().lower()
    with active_lock:
        manager = active_captures.get(username)
    if manager:
        return jsonify({
            "username": username,
            "active": True,
            "stream_id": manager.stream_id,
        })
    return jsonify({"username": username, "active": False})


def _shutdown():
    with active_lock:
        for manager in active_captures.values():
            manager.stop()
        active_captures.clear()


atexit.register(_shutdown)

init_db()

if __name__ == "__main__":
    port = int(os.environ.get("TRANSCRIBER_PORT", "5001"))
    print(f"[*] TikTok Transcriber starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
