import os
import re
import sqlite3
import threading
import atexit

import requests
from flask import Flask, request, jsonify

from capture import AudioCaptureManager
from worker import TranscriptionWorker

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(
    os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
)
DB_PATH = os.path.join(DATA_DIR, "transcripts.db")

LIBRETRANSLATE_URL = os.environ.get("LIBRETRANSLATE_URL", "").rstrip("/")
LIBRETRANSLATE_API_KEY = os.environ.get("LIBRETRANSLATE_API_KEY", "")
DEFAULT_TARGET_LANG = (os.environ.get("DEFAULT_TARGET_LANG", "en") or "en").strip() or "en"

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
    for ddl in (
        "ALTER TABLE transcripts ADD COLUMN translated_text TEXT",
        "ALTER TABLE transcripts ADD COLUMN target_lang TEXT",
    ):
        try:
            conn.execute(ddl)
        except sqlite3.OperationalError:
            pass
    _init_transcript_search_index(conn)
    conn.commit()
    conn.close()


def _init_transcript_search_index(conn):
    try:
        conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS transcripts_fts
            USING fts5(original_text, translated_text, content='transcripts', content_rowid='id')
            """
        )
        conn.executescript(
            """
            CREATE TRIGGER IF NOT EXISTS transcripts_ai AFTER INSERT ON transcripts BEGIN
                INSERT INTO transcripts_fts(rowid, original_text, translated_text)
                VALUES (new.id, new.original_text, new.translated_text);
            END;
            CREATE TRIGGER IF NOT EXISTS transcripts_ad AFTER DELETE ON transcripts BEGIN
                INSERT INTO transcripts_fts(transcripts_fts, rowid, original_text, translated_text)
                VALUES('delete', old.id, old.original_text, old.translated_text);
            END;
            CREATE TRIGGER IF NOT EXISTS transcripts_au AFTER UPDATE OF original_text, translated_text ON transcripts BEGIN
                INSERT INTO transcripts_fts(transcripts_fts, rowid, original_text, translated_text)
                VALUES('delete', old.id, old.original_text, old.translated_text);
                INSERT INTO transcripts_fts(rowid, original_text, translated_text)
                VALUES (new.id, new.original_text, new.translated_text);
            END;
            """
        )
        row = conn.execute("SELECT COUNT(*) AS cnt FROM transcripts_fts").fetchone()
        source = conn.execute("SELECT COUNT(*) AS cnt FROM transcripts").fetchone()
        if row["cnt"] == 0 and source["cnt"] > 0:
            conn.execute(
                "INSERT INTO transcripts_fts(rowid, original_text, translated_text) "
                "SELECT id, original_text, translated_text FROM transcripts"
            )
    except sqlite3.OperationalError as e:
        print(f"[transcriber-search] SQLite FTS unavailable; using LIKE fallback: {e}", flush=True)


def _insert_transcript(username, stream_id, original_text, detected_language, chunk_index, target_lang=None):
    conn = get_db()
    try:
        cur = conn.execute(
            "INSERT INTO transcripts (username, stream_id, original_text, detected_language, chunk_index, target_lang) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (username, stream_id, original_text, detected_language, chunk_index, target_lang),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


_SENTENCE_BOUNDARY_RE = re.compile(r"(?<=[.!?。！？\n])\s+")
_COMMA_BOUNDARY_RE = re.compile(r"(?<=[,，;；:：])\s+")
_TRANSLATE_HARD_LIMIT = 150


def _word_chunk(text: str, limit: int) -> list[str]:
    words = text.split()
    if not words:
        return [text[:limit]] if text else []
    out: list[str] = []
    buf: list[str] = []
    cur = 0
    for w in words:
        if cur + len(w) + 1 > limit and buf:
            out.append(" ".join(buf))
            buf = [w]
            cur = len(w)
        else:
            buf.append(w)
            cur += len(w) + 1
    if buf:
        out.append(" ".join(buf))
    return out


def _split_for_translation(text: str) -> list[str]:
    """Break a transcript chunk into translation-friendly pieces.

    Argos Translate (LibreTranslate's default backend) silently truncates a
    single input at its model's max output tokens. Whisper output for live
    streams rarely has clean sentence punctuation — a 30-second chunk often
    arrives as one giant "sentence" and most of it disappears in translation.

    Strategy: passthrough short inputs (fast path). Cascade longer ones through
    sentence -> comma -> word splits, then greedy-merge resulting fragments
    back up to the limit so the translation server makes 1-2 model calls per
    chunk instead of N.
    """
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= _TRANSLATE_HARD_LIMIT:
        return [text]

    fragments: list[str] = []
    for sentence in _SENTENCE_BOUNDARY_RE.split(text):
        sentence = sentence.strip()
        if not sentence:
            continue
        if len(sentence) <= _TRANSLATE_HARD_LIMIT:
            fragments.append(sentence)
            continue
        for sub in _COMMA_BOUNDARY_RE.split(sentence):
            sub = sub.strip()
            if not sub:
                continue
            if len(sub) <= _TRANSLATE_HARD_LIMIT:
                fragments.append(sub)
            else:
                fragments.extend(_word_chunk(sub, _TRANSLATE_HARD_LIMIT))

    # Greedy merge: combine small fragments back up to the limit. Without this,
    # a comma-heavy chunk like "Không, không, không, không..." would produce N
    # separate model invocations even in batch mode.
    merged: list[str] = []
    buf: list[str] = []
    cur = 0
    for f in fragments:
        sep_len = 1 if buf else 0
        if buf and cur + sep_len + len(f) > _TRANSLATE_HARD_LIMIT:
            merged.append(" ".join(buf))
            buf = [f]
            cur = len(f)
        else:
            buf.append(f)
            cur += sep_len + len(f)
    if buf:
        merged.append(" ".join(buf))
    return merged


def _translate_batch(chunks: list[str], source_lang: str, target_lang: str) -> list[str]:
    """Translate a list of chunks in one LibreTranslate call.

    LibreTranslate accepts `q` as a string OR a list and returns
    `translatedText` in the same shape. Falls back to per-chunk calls if the
    server returns an unexpected shape.
    """
    if not chunks:
        return []
    payload = {
        "q": chunks,
        "source": source_lang or "auto",
        "target": target_lang,
        "format": "text",
    }
    if LIBRETRANSLATE_API_KEY:
        payload["api_key"] = LIBRETRANSLATE_API_KEY
    resp = requests.post(
        f"{LIBRETRANSLATE_URL}/translate",
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    body = resp.json() or {}
    field = body.get("translatedText", "")
    if isinstance(field, list) and len(field) == len(chunks):
        return [str(p or "").strip() for p in field]
    # Some LibreTranslate builds always return a string. Fall back to per-chunk.
    out: list[str] = []
    for c in chunks:
        single_payload = {**payload, "q": c}
        r = requests.post(
            f"{LIBRETRANSLATE_URL}/translate",
            json=single_payload,
            timeout=60,
        )
        r.raise_for_status()
        t = (r.json() or {}).get("translatedText", "")
        if isinstance(t, list):
            t = t[0] if t else ""
        out.append(str(t or "").strip())
    return out


def _translate_and_update(row_id, text, source_lang, target_lang):
    if not LIBRETRANSLATE_URL or not text:
        return
    chunks = _split_for_translation(text)
    if not chunks:
        return
    try:
        translated_parts = _translate_batch(chunks, source_lang or "auto", target_lang)
        translated = " ".join(p for p in translated_parts if p).strip()
        if not translated:
            return
        conn = get_db()
        try:
            conn.execute(
                "UPDATE transcripts SET translated_text = ? WHERE id = ?",
                (translated, row_id),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        print(f"[transcriber] translation failed for row {row_id}: {e}")


def _handle_transcript(username, stream_id, original_text, detected_language, chunk_index, target_lang):
    row_id = _insert_transcript(
        username, stream_id, original_text, detected_language, chunk_index, target_lang
    )
    if not LIBRETRANSLATE_URL or not target_lang:
        return
    if detected_language and detected_language.lower() == target_lang.lower():
        return
    threading.Thread(
        target=_translate_and_update,
        args=(row_id, original_text, detected_language or "auto", target_lang),
        daemon=True,
    ).start()


_worker_lock = threading.Lock()


def _get_worker():
    global _worker
    with _worker_lock:
        if _worker is None:
            _worker = TranscriptionWorker()
    return _worker


def _prewarm_worker():
    try:
        print("[*] Pre-warming Whisper worker at startup...", flush=True)
        _get_worker()
        print("[*] Whisper worker ready.", flush=True)
    except Exception as e:
        print(f"[!] Whisper worker pre-warm failed: {e}", flush=True)


@app.route("/api/health")
def api_health():
    return jsonify({
        "status": "ok",
        "worker_ready": _worker is not None,
    })


@app.route("/api/start", methods=["POST"])
def api_start():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip().lstrip("@").lower()
    stream_url = (data.get("stream_url") or "").strip()
    stream_id = (data.get("stream_id") or "unknown").strip()
    target_lang = (data.get("target_lang") or DEFAULT_TARGET_LANG).strip() or "en"

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

        def _on_transcript(u, s, txt, lang, idx, _tl=target_lang):
            _handle_transcript(u, s, txt, lang, idx, _tl)

        manager = AudioCaptureManager(
            username=username,
            stream_url=stream_url,
            stream_id=stream_id,
            worker=_get_worker(),
            on_transcript=_on_transcript,
        )
        active_captures[username] = manager
        manager.start()

    return jsonify({"status": "started", "username": username, "target_lang": target_lang})


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
        # Return the MOST RECENT `limit` rows (DESC + LIMIT) but order the
        # response ASC so callers can render top-to-bottom oldest-to-newest.
        # Otherwise long streams (>limit chunks) silently hide new chunks
        # behind a sliding window pinned to the start of the stream.
        if stream_id:
            rows = conn.execute(
                "SELECT * FROM ("
                "  SELECT id, username, stream_id, original_text, translated_text, "
                "  detected_language, target_lang, chunk_index, timestamp FROM transcripts "
                "  WHERE username = ? AND stream_id = ? ORDER BY id DESC LIMIT ? OFFSET ?"
                ") ORDER BY id ASC",
                (username, stream_id, limit, offset),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM ("
                "  SELECT id, username, stream_id, original_text, translated_text, "
                "  detected_language, target_lang, chunk_index, timestamp FROM transcripts "
                "  WHERE username = ? ORDER BY id DESC LIMIT ? OFFSET ?"
                ") ORDER BY id ASC",
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


_FTS_TOKEN_RE = re.compile(r'"[^"]+"|\S+')


def _clean_filter(value: str | None, *, username: bool = False) -> str:
    value = (value or "").strip()
    if username:
        value = value.lstrip("@").lower()
    return value


def _fts_query(text: str) -> str:
    tokens = []
    for raw in _FTS_TOKEN_RE.findall(text or ""):
        token = raw.strip().strip('"')
        if token:
            escaped = token.replace('"', '""')
            tokens.append(f'"{escaped}"')
    return " AND ".join(tokens)


def _search_filters(alias=""):
    prefix = f"{alias}." if alias else ""
    where = []
    params = []
    username = _clean_filter(request.args.get("username"), username=True)
    stream_id = _clean_filter(request.args.get("stream_id"))
    date_from = _clean_filter(request.args.get("date_from"))
    date_to = _clean_filter(request.args.get("date_to"))
    if username:
        where.append(f"{prefix}username = ?")
        params.append(username)
    if stream_id:
        where.append(f"{prefix}stream_id = ?")
        params.append(stream_id)
    if date_from:
        where.append(f"{prefix}timestamp >= ?")
        params.append(f"{date_from}T00:00:00Z")
    if date_to:
        where.append(f"{prefix}timestamp <= ?")
        params.append(f"{date_to}T23:59:59Z")
    return where, params


def _transcript_stats(conn, base_where, base_params, q):
    where = list(base_where)
    params = list(base_params)
    if q:
        where.append("(LOWER(original_text) LIKE ? OR LOWER(COALESCE(translated_text, '')) LIKE ?)")
        like = f"%{q.lower()}%"
        params.extend([like, like])
    if not where:
        where.append("1 = 1")
    stats = dict(conn.execute(
        f"""
        SELECT COUNT(*) AS transcript_count,
               COUNT(DISTINCT stream_id) AS transcript_stream_count
        FROM transcripts
        WHERE {" AND ".join(where)}
        """,
        params,
    ).fetchone())
    languages = conn.execute(
        f"""
        SELECT COALESCE(detected_language, 'unknown') AS language, COUNT(*) AS count
        FROM transcripts
        WHERE {" AND ".join(where)}
        GROUP BY COALESCE(detected_language, 'unknown')
        ORDER BY COUNT(*) DESC
        LIMIT 8
        """,
        params,
    ).fetchall()
    stats["languages"] = [dict(r) for r in languages]
    return stats


@app.route("/api/search/transcripts")
def api_search_transcripts():
    q = (request.args.get("q") or "").strip()
    limit = min(max(request.args.get("limit", 50, type=int), 1), 200)
    offset = max(request.args.get("offset", 0, type=int), 0)
    sort = (request.args.get("sort") or "newest").strip().lower()
    if sort not in {"newest", "oldest"}:
        sort = "newest"
    order = "DESC" if sort == "newest" else "ASC"

    conn = get_db()
    try:
        base_where, base_params = _search_filters()
        results = None
        if q:
            match = _fts_query(q)
            if match:
                try:
                    alias_where, alias_params = _search_filters(alias="t")
                    rows = conn.execute(
                        f"""
                        SELECT t.id, t.username, t.stream_id, t.original_text,
                               t.translated_text, t.detected_language, t.target_lang,
                               t.chunk_index, t.timestamp
                        FROM transcripts_fts
                        JOIN transcripts t ON t.id = transcripts_fts.rowid
                        WHERE {" AND ".join(["transcripts_fts MATCH ?"] + alias_where)}
                        ORDER BY t.timestamp {order}, t.id {order}
                        LIMIT ? OFFSET ?
                        """,
                        [match] + alias_params + [limit, offset],
                    ).fetchall()
                    results = [dict(r) for r in rows]
                except sqlite3.OperationalError:
                    results = None
            if results is None:
                like_where = base_where + [
                    "(LOWER(original_text) LIKE ? OR LOWER(COALESCE(translated_text, '')) LIKE ?)"
                ]
                like_params = base_params + [f"%{q.lower()}%", f"%{q.lower()}%"]
                rows = conn.execute(
                    f"""
                    SELECT id, username, stream_id, original_text, translated_text,
                           detected_language, target_lang, chunk_index, timestamp
                    FROM transcripts
                    WHERE {" AND ".join(like_where)}
                    ORDER BY timestamp {order}, id {order}
                    LIMIT ? OFFSET ?
                    """,
                    like_params + [limit, offset],
                ).fetchall()
                results = [dict(r) for r in rows]
        else:
            where = base_where or ["1 = 1"]
            rows = conn.execute(
                f"""
                SELECT id, username, stream_id, original_text, translated_text,
                       detected_language, target_lang, chunk_index, timestamp
                FROM transcripts
                WHERE {" AND ".join(where)}
                ORDER BY timestamp {order}, id {order}
                LIMIT ? OFFSET ?
                """,
                base_params + [limit, offset],
            ).fetchall()
            results = [dict(r) for r in rows]

        return jsonify({
            "results": results,
            "count": len(results),
            "stats": _transcript_stats(conn, base_where, base_params, q),
        })
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

threading.Thread(target=_prewarm_worker, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("TRANSCRIBER_PORT", "5001"))
    print(f"[*] TikTok Transcriber starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
