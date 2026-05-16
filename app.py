import json
import os
import sqlite3
import threading
import asyncio
import time
import re
from datetime import datetime, timezone

import requests as http_requests
from flask import Flask, render_template, request, jsonify

from money import diamonds_to_usd, index_gift_catalog, resolve_diamonds_per_unit

from TikTokLive import TikTokLiveClient
from TikTokLive.events import (
    GiftEvent,
    ConnectEvent,
    DisconnectEvent,
    LiveEndEvent,
    CommentEvent,
    EmoteChatEvent,
    ShareEvent,
    RoomUserSeqEvent,
    JoinEvent,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(
    os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
)
DB_PATH = os.path.join(DATA_DIR, "gifts.db")


def listeners_disabled() -> bool:
    """When true, no TikTok WebSocket threads are started (docs / screenshot runs)."""
    return os.environ.get("DISABLE_TIKTOK_LISTENERS", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )

os.makedirs(DATA_DIR, exist_ok=True)

LIBRETRANSLATE_URL = os.environ.get("LIBRETRANSLATE_URL", "").rstrip("/")
DEFAULT_TARGET_LANG = os.environ.get("DEFAULT_TARGET_LANG", "en")
LIBRETRANSLATE_API_KEY = os.environ.get("LIBRETRANSLATE_API_KEY", "")

OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "").rstrip("/")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.2")

TRANSCRIBER_URL = os.environ.get("TRANSCRIBER_URL", "").rstrip("/")

_translate_languages_cache = None
_translate_lang_lock = threading.Lock()

app = Flask(__name__)

active_lock = threading.Lock()
active_listeners: dict[str, dict] = {}
streamer_avatars: dict[str, str] = {}
sender_avatars: dict[str, str] = {}
current_transcription_user: str | None = None


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_db()
    conn.execute(
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
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_gifts_username ON gifts(username)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_gifts_username_stream ON gifts(username, stream_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_gifts_timestamp ON gifts(timestamp)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tracked_users (
            username TEXT PRIMARY KEY,
            added_at DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        )
        """
    )
    try:
        conn.execute("ALTER TABLE tracked_users ADD COLUMN auto_transcribe INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE tracked_users ADD COLUMN avatar_url TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE tracked_users ADD COLUMN avatar_fetched_at DATETIME")
    except sqlite3.OperationalError:
        pass
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            sender TEXT NOT NULL,
            sender_unique_id TEXT,
            message TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            msg_id TEXT,
            timestamp DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_chat_username ON chat_messages(username)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_chat_username_stream ON chat_messages(username, stream_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_chat_timestamp ON chat_messages(timestamp)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_chat_sender ON chat_messages(sender)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_chat_sender_uid ON chat_messages(sender_unique_id)"
    )
    cols = {r[1] for r in conn.execute("PRAGMA table_info(chat_messages)").fetchall()}
    if "msg_id" not in cols:
        conn.execute("ALTER TABLE chat_messages ADD COLUMN msg_id TEXT")
    existing_indexes = {r[1] for r in conn.execute("PRAGMA index_list(chat_messages)").fetchall()}
    if "idx_chat_dedup" in existing_indexes:
        conn.execute("DROP INDEX idx_chat_dedup")
    conn.execute(
        "DELETE FROM chat_messages WHERE id NOT IN (SELECT MIN(id) FROM chat_messages GROUP BY username, sender, message, stream_id)"
    )
    try:
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_dedup ON chat_messages(msg_id)"
        )
    except sqlite3.IntegrityError:
        conn.execute(
            "DELETE FROM chat_messages WHERE id NOT IN (SELECT MIN(id) FROM chat_messages WHERE msg_id IS NOT NULL GROUP BY msg_id)"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_dedup ON chat_messages(msg_id)"
        )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stream_summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            summary_type TEXT NOT NULL,
            summary_text TEXT NOT NULL,
            analyzed_count INTEGER NOT NULL DEFAULT 0,
            timestamp DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        )
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_summary_unique ON stream_summaries(username, stream_id, summary_type)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stream_participants (
            username TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            sender TEXT NOT NULL,
            sender_unique_id TEXT,
            first_seen DATETIME NOT NULL,
            last_seen DATETIME NOT NULL,
            comment_count INTEGER NOT NULL DEFAULT 0,
            gift_count INTEGER NOT NULL DEFAULT 0,
            share_count INTEGER NOT NULL DEFAULT 0,
            diamond_total INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (username, stream_id, sender)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_participants_stream ON stream_participants(username, stream_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_participants_lastseen ON stream_participants(username, stream_id, last_seen)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS viewer_samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            viewer_count INTEGER NOT NULL,
            timestamp DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_viewers_stream ON viewer_samples(username, stream_id, timestamp)"
    )
    _init_chat_search_index(conn)
    conn.commit()
    conn.close()


def _init_chat_search_index(conn):
    try:
        conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS chat_messages_fts
            USING fts5(message, content='chat_messages', content_rowid='id')
            """
        )
        conn.executescript(
            """
            CREATE TRIGGER IF NOT EXISTS chat_messages_ai AFTER INSERT ON chat_messages BEGIN
                INSERT INTO chat_messages_fts(rowid, message) VALUES (new.id, new.message);
            END;
            CREATE TRIGGER IF NOT EXISTS chat_messages_ad AFTER DELETE ON chat_messages BEGIN
                INSERT INTO chat_messages_fts(chat_messages_fts, rowid, message)
                VALUES('delete', old.id, old.message);
            END;
            CREATE TRIGGER IF NOT EXISTS chat_messages_au AFTER UPDATE OF message ON chat_messages BEGIN
                INSERT INTO chat_messages_fts(chat_messages_fts, rowid, message)
                VALUES('delete', old.id, old.message);
                INSERT INTO chat_messages_fts(rowid, message) VALUES (new.id, new.message);
            END;
            """
        )
        row = conn.execute("SELECT COUNT(*) AS cnt FROM chat_messages_fts").fetchone()
        source = conn.execute("SELECT COUNT(*) AS cnt FROM chat_messages").fetchone()
        if row["cnt"] == 0 and source["cnt"] > 0:
            conn.execute(
                "INSERT INTO chat_messages_fts(rowid, message) SELECT id, message FROM chat_messages"
            )
    except sqlite3.OperationalError as e:
        print(f"[search] SQLite FTS unavailable for chat search; using LIKE fallback: {e}")


def normalize_username(username: str) -> str:
    return username.strip().lstrip("@").lower()


def _insert_gift(username, sender, gift_name, diamond_value, usd_value, stream_id):
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO gifts (username, sender, gift_name, diamond_value, usd_value, stream_id) VALUES (?, ?, ?, ?, ?, ?)",
            (username, sender, gift_name, diamond_value, usd_value, stream_id),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_chat_message(username, sender, sender_uid, message, stream_id, msg_id=None):
    conn = get_db()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO chat_messages (username, sender, sender_unique_id, message, stream_id, msg_id) VALUES (?, ?, ?, ?, ?, ?)",
            (username, sender, sender_uid, message, stream_id, msg_id),
        )
        conn.commit()
    finally:
        conn.close()


def _upsert_participant(
    username: str,
    stream_id: str,
    sender: str,
    sender_uid: str | None,
    *,
    comments: int = 0,
    gifts: int = 0,
    shares: int = 0,
    diamonds: int = 0,
) -> None:
    """Record an engaged participant (commented / gifted / shared) for this stream.

    Idempotent counters: pass the per-event delta. first_seen is preserved on conflict;
    last_seen is bumped to now.
    """
    if not sender or not stream_id:
        return
    conn = get_db()
    try:
        conn.execute(
            """
            INSERT INTO stream_participants
                (username, stream_id, sender, sender_unique_id,
                 first_seen, last_seen,
                 comment_count, gift_count, share_count, diamond_total)
            VALUES (?, ?, ?, ?,
                    strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                    strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                    ?, ?, ?, ?)
            ON CONFLICT(username, stream_id, sender) DO UPDATE SET
                sender_unique_id = COALESCE(excluded.sender_unique_id, stream_participants.sender_unique_id),
                last_seen = excluded.last_seen,
                comment_count = stream_participants.comment_count + excluded.comment_count,
                gift_count    = stream_participants.gift_count    + excluded.gift_count,
                share_count   = stream_participants.share_count   + excluded.share_count,
                diamond_total = stream_participants.diamond_total + excluded.diamond_total
            """,
            (
                username, stream_id, sender, sender_uid,
                comments, gifts, shares, diamonds,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_viewer_sample(username: str, stream_id: str, viewer_count: int) -> None:
    if not stream_id:
        return
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO viewer_samples (username, stream_id, viewer_count) VALUES (?, ?, ?)",
            (username, stream_id, int(viewer_count)),
        )
        conn.commit()
    finally:
        conn.close()


def _add_tracked_user(username):
    conn = get_db()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO tracked_users (username) VALUES (?)",
            (username,),
        )
        conn.commit()
    finally:
        conn.close()


def _remove_tracked_user(username):
    conn = get_db()
    try:
        conn.execute(
            "DELETE FROM tracked_users WHERE username = ?", (username,)
        )
        conn.commit()
    finally:
        conn.close()


def _get_tracked_users():
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT username, added_at, auto_transcribe, avatar_url FROM tracked_users ORDER BY added_at ASC"
        ).fetchall()
        return [
            {
                "username": r["username"],
                "added_at": r["added_at"],
                "auto_transcribe": bool(r["auto_transcribe"]),
                "avatar_url": r["avatar_url"],
            }
            for r in rows
        ]
    finally:
        conn.close()


def _persist_avatar(username: str, url: str) -> None:
    conn = get_db()
    try:
        conn.execute(
            "UPDATE tracked_users SET avatar_url = ?, avatar_fetched_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE username = ?",
            (url, username),
        )
        conn.commit()
    finally:
        conn.close()


_PROFILE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.tiktok.com/",
}


def _fetch_profile_avatar(username: str) -> str | None:
    """Scrape the public TikTok profile page and return the user's avatar URL.

    Works for offline accounts and survives across server restarts. Returns None
    if the page can't be fetched or doesn't contain the expected JSON blob.
    """
    try:
        resp = http_requests.get(
            f"https://www.tiktok.com/@{username}",
            headers=_PROFILE_HEADERS,
            timeout=10,
        )
        if resp.status_code >= 400:
            return None
        html = resp.text
        marker = '<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">'
        start = html.find(marker)
        if start == -1:
            return None
        start += len(marker)
        end = html.find("</script>", start)
        if end == -1:
            return None
        blob = json.loads(html[start:end])
        scope = blob.get("__DEFAULT_SCOPE__", {})
        user_detail = scope.get("webapp.user-detail", {})
        user = user_detail.get("userInfo", {}).get("user", {})
        for key in ("avatarMedium", "avatarLarger", "avatarThumb"):
            url = user.get(key)
            if isinstance(url, str) and url:
                return url
        return None
    except Exception:
        return None


def _refresh_avatar(username: str) -> str | None:
    """Fetch a fresh avatar URL and persist it. Returns the URL or None."""
    url = _fetch_profile_avatar(username)
    if not url:
        return None
    streamer_avatars[username] = url
    try:
        _persist_avatar(username, url)
    except Exception:
        pass
    return url


def _avatar_refresh_loop():
    """Periodically refresh avatars for all tracked users.

    Catches both signed-URL expiration and avatar changes on TikTok. Runs at a
    slow cadence so we don't hammer TikTok or get rate-limited.
    """
    while True:
        try:
            tracked = _get_tracked_users()
            for t in tracked:
                _refresh_avatar(t["username"])
                time.sleep(2)
        except Exception:
            pass
        time.sleep(6 * 60 * 60)


def _set_auto_transcribe(username, enabled: bool):
    conn = get_db()
    try:
        conn.execute(
            "UPDATE tracked_users SET auto_transcribe = ? WHERE username = ?",
            (1 if enabled else 0, username),
        )
        conn.commit()
    finally:
        conn.close()


def _is_auto_transcribe(username) -> bool:
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT auto_transcribe FROM tracked_users WHERE username = ?",
            (username,),
        ).fetchone()
        return bool(row and row["auto_transcribe"])
    finally:
        conn.close()


def _maybe_auto_transcribe(username):
    """Start transcription for `username` if they're flagged auto_transcribe and live.

    Priority rule: a flagged streamer preempts any currently-running transcription
    UNLESS the user being transcribed is also flagged (equal priority — first wins).
    Idempotent — safe to call multiple times.
    """
    global current_transcription_user
    if not TRANSCRIBER_URL or not _is_auto_transcribe(username):
        return
    with active_lock:
        info = active_listeners.get(username)
        if not info or not info.get("live"):
            return
        if current_transcription_user == username:
            return  # already transcribing this user
        prev_user = current_transcription_user
        stream_id = info.get("stream_id") or "unknown"
        room_id = info.get("stream_id")
        cached_stream_url = info.get("stream_url")

    if prev_user and _is_auto_transcribe(prev_user):
        # Currently-transcribed user is also flagged → equal priority, don't preempt.
        return

    stream_url = cached_stream_url
    if not stream_url and room_id:
        stream_url = _fetch_fresh_stream_url(room_id)
    if not stream_url:
        print(f"[!] auto-transcribe: no stream URL for @{username} (room_id={room_id})")
        return

    if prev_user:
        print(f"[*] auto-transcribe priority: preempting @{prev_user} for @{username}")
        _stop_transcriber(prev_user)

    with active_lock:
        ok = _start_transcriber(username, stream_url, stream_id, target_lang=DEFAULT_TARGET_LANG)
        if ok:
            current_transcription_user = username
            print(f"[*] auto-transcribe started for @{username}")
        else:
            if current_transcription_user == prev_user:
                current_transcription_user = None
            print(f"[!] auto-transcribe: transcriber refused start for @{username}")


def _extract_stream_url(room_info):
    if not isinstance(room_info, dict):
        return None
    stream_url = room_info.get("stream_url")
    if not isinstance(stream_url, dict):
        return None
    try:
        pull_data = stream_url.get("live_core_sdk_data", {}).get("pull_data", {})
        stream_data_str = pull_data.get("stream_data")
        if stream_data_str:
            stream_data = json.loads(stream_data_str)
            data = stream_data.get("data", {})
            for quality in ("origin", "uhd", "hd", "sd", "ld"):
                main = data.get(quality, {}).get("main", {})
                for fmt in ("flv", "hls"):
                    url = main.get(fmt)
                    if isinstance(url, str) and url:
                        return url
    except Exception:
        pass
    # Older shape: flv_pull_url dict / hls_pull_url string / rtmp_pull_url string
    flv = stream_url.get("flv_pull_url")
    if isinstance(flv, dict):
        for key in ("FULL_HD1", "HD1", "SD1", "SD2", "ORIGIN"):
            v = flv.get(key)
            if isinstance(v, str) and v:
                return v
        for v in flv.values():
            if isinstance(v, str) and v:
                return v
    for key in ("hls_pull_url", "rtmp_pull_url"):
        v = stream_url.get(key)
        if isinstance(v, str) and v:
            return v
    return None


def _fetch_fresh_stream_url(room_id):
    try:
        resp = http_requests.get(
            "https://webcast.tiktok.com/webcast/room/info/",
            params={"room_id": room_id},
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Accept": "application/json",
                "Referer": "https://www.tiktok.com/",
            },
            timeout=10,
        )
        if resp.status_code >= 400:
            print(f"[!] webcast room/info {resp.status_code} for room {room_id}: {resp.text[:200]}")
            return None
        body = resp.json() or {}
        data = body.get("data") or {}
        url = _extract_stream_url(data)
        if not url:
            print(f"[!] webcast room/info returned no stream_url keys for room {room_id} (status_code={body.get('status_code')}, message={body.get('message')!r})")
        return url
    except Exception as e:
        print(f"[!] Failed to fetch fresh stream URL: {e}")
        return None


def _start_transcriber(username, stream_url, stream_id, target_lang=None):
    if not TRANSCRIBER_URL:
        return False
    try:
        resp = http_requests.post(
            f"{TRANSCRIBER_URL}/api/start",
            json={
                "username": username,
                "stream_url": stream_url,
                "stream_id": stream_id,
                "target_lang": (target_lang or DEFAULT_TARGET_LANG),
            },
            timeout=180,
        )
        if resp.status_code >= 400:
            print(f"[!] Transcriber start failed ({resp.status_code}): {resp.text}")
            return False
        return True
    except Exception as e:
        print(f"[!] Transcriber unreachable: {e}")
        return False


def _stop_transcriber(username):
    if not TRANSCRIBER_URL:
        return
    try:
        http_requests.post(
            f"{TRANSCRIBER_URL}/api/stop",
            json={"username": username},
            timeout=10,
        )
    except Exception:
        pass


def _query_ollama(system_prompt: str, chat_text: str) -> str | None:
    if not OLLAMA_BASE_URL or not chat_text.strip():
        return None
    try:
        resp = http_requests.post(
            f"{OLLAMA_BASE_URL}/api/chat",
            json={
                "model": OLLAMA_MODEL,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": chat_text},
                ],
                "stream": False,
            },
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("message", {}).get("content", "").strip() or None
    except Exception:
        return None


def _upsert_summary(username, stream_id, summary_type, summary_text, analyzed_count):
    conn = get_db()
    try:
        conn.execute(
            """
            INSERT INTO stream_summaries (username, stream_id, summary_type, summary_text, analyzed_count, timestamp)
            VALUES (?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            ON CONFLICT(username, stream_id, summary_type) DO UPDATE SET
                summary_text = excluded.summary_text,
                analyzed_count = excluded.analyzed_count,
                timestamp = excluded.timestamp
            """,
            (username, stream_id, summary_type, summary_text, analyzed_count),
        )
        conn.commit()
    finally:
        conn.close()


def _fetch_transcripts_for_summary(username, stream_id, limit):
    """Pull transcript rows from the transcriber service for use in AI summaries.
    Returns rows with timestamp + a best-language text (translated when available,
    falling back to original). Returns [] on any failure — summary still runs chat-only.
    """
    if not TRANSCRIBER_URL:
        return []
    try:
        resp = http_requests.get(
            f"{TRANSCRIBER_URL}/api/transcripts/{username}",
            params={"stream_id": stream_id, "limit": limit},
            timeout=10,
        )
        if not resp.ok:
            return []
        return resp.json() or []
    except Exception:
        return []


def _format_transcript_for_prompt(transcripts):
    """One line per chunk: '[HH:MM:SS] text'. Prefers translated_text, falls back to original."""
    lines = []
    for t in transcripts:
        text = (t.get("translated_text") or t.get("original_text") or "").strip()
        if not text:
            continue
        ts = (t.get("timestamp") or "")[11:19] or t.get("timestamp", "")
        lines.append(f"[{ts}] {text}")
    return "\n".join(lines)


def _build_summary_user_message(username, stream_id, chat_text, transcript_text):
    sections = [f"Here is data from @{username}'s TikTok livestream (stream ID: {stream_id})."]
    if transcript_text:
        sections.append(
            "STREAMER'S SPEECH (audio transcript, English translation when not already English):\n"
            + transcript_text
        )
    else:
        sections.append("STREAMER'S SPEECH: (no audio transcript available for this stream)")
    sections.append("VIEWER CHAT MESSAGES:\n" + chat_text)
    return "\n\n".join(sections)


def _ollama_live_overview_loop():
    while True:
        time.sleep(60)
        if not OLLAMA_BASE_URL:
            continue
        targets = []
        with active_lock:
            for uname, info in active_listeners.items():
                if info.get("live") and info.get("stream_id"):
                    targets.append((uname, info["stream_id"]))
        for uname, sid in targets:
            try:
                conn = get_db()
                try:
                    rows = conn.execute(
                        "SELECT sender, message FROM chat_messages WHERE username = ? AND stream_id = ? ORDER BY id DESC LIMIT 200",
                        (uname, sid),
                    ).fetchall()
                finally:
                    conn.close()
                transcripts = _fetch_transcripts_for_summary(uname, sid, limit=200)
                if not rows and not transcripts:
                    continue
                combined_count = len(rows) + len(transcripts)
                existing = None
                conn2 = get_db()
                try:
                    row = conn2.execute(
                        "SELECT analyzed_count FROM stream_summaries WHERE username = ? AND stream_id = ? AND summary_type = 'live_overview'",
                        (uname, sid),
                    ).fetchone()
                    existing = row["analyzed_count"] if row else None
                finally:
                    conn2.close()
                if existing is not None and combined_count <= existing:
                    continue
                chat_text = "\n".join(
                    f"{r['sender']}: {r['message']}" for r in reversed(rows)
                ) or "(no chat messages yet)"
                transcript_text = _format_transcript_for_prompt(transcripts)
                summary = _query_ollama(
                    "You are an assistant that analyzes TikTok livestreams. "
                    "You receive two data streams: the streamer's own speech (audio transcript, "
                    "translated to English when needed) and the viewers' chat messages. "
                    "Provide a concise overview (2-3 paragraphs) covering: what the streamer is "
                    "doing/saying, how viewers are reacting, main topics, overall sentiment and "
                    "energy, and notable interactions. Treat the transcript as the streamer's side "
                    "and chat as the audience's side — together they tell the full story. "
                    "Be specific and reference actual content where relevant.",
                    _build_summary_user_message(uname, sid, chat_text, transcript_text),
                )
                if summary:
                    _upsert_summary(uname, sid, "live_overview", summary, combined_count)
            except Exception:
                pass


def _ollama_full_summary(username, stream_id):
    if not OLLAMA_BASE_URL or not stream_id:
        return
    try:
        conn = get_db()
        try:
            rows = conn.execute(
                "SELECT sender, message FROM chat_messages WHERE username = ? AND stream_id = ? ORDER BY id ASC LIMIT 500",
                (username, stream_id),
            ).fetchall()
        finally:
            conn.close()
        transcripts = _fetch_transcripts_for_summary(username, stream_id, limit=500)
        if not rows and not transcripts:
            return
        chat_text = "\n".join(
            f"{r['sender']}: {r['message']}" for r in rows
        ) or "(no chat messages recorded)"
        transcript_text = _format_transcript_for_prompt(transcripts)
        summary = _query_ollama(
            "You are an assistant that analyzes TikTok livestreams. "
            "You receive two data streams: the streamer's own speech (audio transcript, "
            "translated to English when needed) and the viewers' chat messages. "
            "Provide a comprehensive end-of-stream summary covering: what the streamer "
            "talked about and did, main topics discussed, overall viewer sentiment, most "
            "active participants, key moments or highlights, recurring themes, and how "
            "the audience engaged with what the streamer was saying. Use the transcript "
            "to ground claims about the streamer's actual words. Be thorough but "
            "well-organized — use paragraphs and bullet points where helpful.",
            _build_summary_user_message(username, stream_id, chat_text, transcript_text),
        )
        if summary:
            _upsert_summary(username, stream_id, "full_summary", summary, len(rows) + len(transcripts))
    except Exception:
        pass


async def _run_listener(username: str):
    """
    Runs in a dedicated daemon thread via asyncio.run().
    Contains an outer retry loop that reconnects with exponential
    backoff whenever the stream ends or the connection drops.
    Only exits when the 'stopping' flag is set (user explicitly removed).
    """
    backoff = 10
    max_backoff = 300

    while True:
        with active_lock:
            info = active_listeners.get(username)
            if info is None or info.get("stopping"):
                return

        client = TikTokLiveClient(unique_id=f"@{username}")
        connected = asyncio.Event()
        stream_id_holder = [None]
        gift_catalog: dict[int, int] = {}
        _recent_chat: set[str] = set()

        @client.on(ConnectEvent)
        async def on_connect(event: ConnectEvent):
            stream_id_holder[0] = str(client.room_id)
            _recent_chat.clear()
            connected.set()
            with active_lock:
                if username in active_listeners:
                    active_listeners[username]["live"] = True
                    active_listeners[username]["stream_id"] = stream_id_holder[0]
                    active_listeners[username][
                        "connected_at"
                    ] = datetime.now(timezone.utc).isoformat()
                    active_listeners[username]["viewer_count"] = 0
                    active_listeners[username]["top_viewers"] = []
                    active_listeners[username]["recent_joiners"] = []
                    active_listeners[username]["_last_viewer_sample_at"] = 0
                    active_listeners[username]["viewer_count_updated_at"] = None
            try:
                ri = client.room_info
                if isinstance(ri, dict):
                    for key in ("owner", "user"):
                        owner = ri.get(key, {})
                        if isinstance(owner, dict):
                            for thumb_key in ("avatar_thumb", "avatar_medium", "avatar_large"):
                                thumb = owner.get(thumb_key)
                                if isinstance(thumb, dict):
                                    urls = thumb.get("url_list", [])
                                    if urls and isinstance(urls, list):
                                        streamer_avatars[username] = urls[0]
                                        break
                            if username in streamer_avatars:
                                break
                    stream_url = _extract_stream_url(ri)
                    if stream_url:
                        with active_lock:
                            if username in active_listeners:
                                active_listeners[username]["stream_url"] = stream_url
            except Exception:
                pass
            # Auto-transcribe trigger: fire in a background thread so we don't
            # block the asyncio event loop on the transcriber HTTP call.
            threading.Thread(
                target=_maybe_auto_transcribe, args=(username,), daemon=True
            ).start()

        @client.on(DisconnectEvent)
        async def on_disconnect(event: DisconnectEvent):
            _stop_transcriber(username)
            with active_lock:
                global current_transcription_user
                if current_transcription_user == username:
                    current_transcription_user = None
                if username in active_listeners:
                    active_listeners[username]["live"] = False
                    active_listeners[username]["stream_url"] = None
                    active_listeners[username]["viewer_count"] = 0
                    active_listeners[username]["top_viewers"] = []
                    active_listeners[username]["recent_joiners"] = []

        @client.on(LiveEndEvent)
        async def on_live_end(event: LiveEndEvent):
            _stop_transcriber(username)
            with active_lock:
                global current_transcription_user
                if current_transcription_user == username:
                    current_transcription_user = None
                if username in active_listeners:
                    active_listeners[username]["live"] = False
                    active_listeners[username]["stream_url"] = None
                    active_listeners[username]["viewer_count"] = 0
                    active_listeners[username]["top_viewers"] = []
                    active_listeners[username]["recent_joiners"] = []

        @client.on(GiftEvent)
        async def on_gift(event: GiftEvent):
            gift = event.gift
            if gift is None:
                return

            if gift.streakable and event.streaking:
                return

            sender_name = event.user.nickname or event.user.unique_id
            try:
                at = getattr(event.user, "avatar_thumb", None)
                if at and getattr(at, "m_urls", None):
                    sender_avatars[sender_name] = at.m_urls[0]
            except Exception:
                pass
            try:
                to = getattr(event, "to_user", None)
                if to and username not in streamer_avatars:
                    at2 = getattr(to, "avatar_thumb", None)
                    if at2 and getattr(at2, "m_urls", None):
                        streamer_avatars[username] = at2.m_urls[0]
            except Exception:
                pass

            per_unit = resolve_diamonds_per_unit(
                int(gift.id),
                gift.name or "",
                int(gift.diamond_count),
                gift_catalog,
            )
            diamonds = per_unit * int(event.repeat_count)
            usd = diamonds_to_usd(diamonds)
            sid = stream_id_holder[0] or "unknown"

            _insert_gift(
                username,
                sender_name,
                gift.name,
                diamonds,
                usd,
                sid,
            )
            try:
                _upsert_participant(
                    username, sid, sender_name,
                    getattr(event.user, "unique_id", None),
                    gifts=int(event.repeat_count) or 1,
                    diamonds=diamonds,
                )
            except Exception:
                pass

        @client.on(CommentEvent)
        async def on_comment(event: CommentEvent):
            try:
                msg_id = str(getattr(getattr(event, "base_message", None), "message_id", "")) or None
                sender_name = event.user.nickname or event.user.unique_id or ""
                sender_uid = event.user.unique_id or ""
                msg = event.comment or ""
                sid = stream_id_holder[0] or "unknown"
                if msg_id and msg_id in _recent_chat:
                    return
                if msg_id:
                    _recent_chat.add(msg_id)
                    if len(_recent_chat) > 5000:
                        _recent_chat.clear()
                _insert_chat_message(username, sender_name, sender_uid, msg, sid, msg_id)
                _upsert_participant(username, sid, sender_name, sender_uid, comments=1)
            except Exception:
                pass

        @client.on(ShareEvent)
        async def on_share(event: ShareEvent):
            try:
                user = getattr(event, "user", None)
                if not user:
                    return
                sender_name = getattr(user, "nickname", "") or getattr(user, "unique_id", "") or ""
                sender_uid = getattr(user, "unique_id", "") or ""
                if not sender_name:
                    return
                sid = stream_id_holder[0] or "unknown"
                _upsert_participant(username, sid, sender_name, sender_uid, shares=1)
                try:
                    at = getattr(user, "avatar_thumb", None)
                    if at and getattr(at, "m_urls", None):
                        sender_avatars[sender_name] = at.m_urls[0]
                except Exception:
                    pass
            except Exception:
                pass

        @client.on(RoomUserSeqEvent)
        async def on_room_user_seq(event: RoomUserSeqEvent):
            try:
                viewer_count = int(getattr(event, "m_total", 0) or 0)
                # TikTok populates either m_contributors (ranked donors) or seats
                # (live top-viewers). Small streams often leave m_contributors empty.
                contributors = (
                    list(getattr(event, "m_contributors", None) or [])
                    or list(getattr(event, "seats", None) or [])
                )
                top = []
                for c in contributors[:20]:
                    u = getattr(c, "m_user", None)
                    if not u:
                        continue
                    nick = getattr(u, "nickname", "") or getattr(u, "unique_id", "") or ""
                    if not nick:
                        continue
                    avatar_url = None
                    try:
                        at = getattr(u, "avatar_thumb", None)
                        if at and getattr(at, "m_urls", None):
                            avatar_url = at.m_urls[0]
                            sender_avatars[nick] = avatar_url
                    except Exception:
                        pass
                    top.append({
                        "rank": int(getattr(c, "m_rank", 0) or 0),
                        "score": int(getattr(c, "m_score", 0) or 0),
                        "sender": nick,
                        "sender_unique_id": getattr(u, "unique_id", "") or "",
                        "avatar_url": avatar_url,
                    })

                now_ts = time.time()
                sid = stream_id_holder[0] or "unknown"
                should_sample = False
                with active_lock:
                    info = active_listeners.get(username)
                    if info is not None:
                        info["viewer_count"] = viewer_count
                        info["top_viewers"] = top
                        info["viewer_count_updated_at"] = datetime.now(timezone.utc).isoformat()
                        last = info.get("_last_viewer_sample_at", 0)
                        if now_ts - last >= 30:
                            info["_last_viewer_sample_at"] = now_ts
                            should_sample = True
                if should_sample and sid != "unknown":
                    _insert_viewer_sample(username, sid, viewer_count)
            except Exception:
                pass

        @client.on(JoinEvent)
        async def on_join(event: JoinEvent):
            try:
                user = getattr(event, "user", None)
                if not user:
                    return
                nick = getattr(user, "nickname", "") or getattr(user, "unique_id", "") or ""
                uid = getattr(user, "unique_id", "") or ""
                if not nick:
                    return
                avatar_url = None
                try:
                    at = getattr(user, "avatar_thumb", None)
                    if at and getattr(at, "m_urls", None):
                        avatar_url = at.m_urls[0]
                        sender_avatars[nick] = avatar_url
                except Exception:
                    pass
                entry = {
                    "sender": nick,
                    "sender_unique_id": uid,
                    "avatar_url": avatar_url,
                    "joined_at": datetime.now(timezone.utc).isoformat(),
                }
                with active_lock:
                    info = active_listeners.get(username)
                    if info is None:
                        return
                    joiners = info.get("recent_joiners")
                    if joiners is None:
                        joiners = []
                        info["recent_joiners"] = joiners
                    # De-dup by uid/nick: bump existing entry to the front
                    key = uid or nick
                    for i, j in enumerate(joiners):
                        if (j.get("sender_unique_id") or j.get("sender")) == key:
                            joiners.pop(i)
                            break
                    joiners.insert(0, entry)
                    if len(joiners) > 30:
                        del joiners[30:]
            except Exception:
                pass

        @client.on(EmoteChatEvent)
        async def on_emote_chat(event: EmoteChatEvent):
            try:
                msg_id = str(getattr(getattr(event, "base_message", None), "message_id", "")) or None
                sender_name = ""
                sender_uid = ""
                user = getattr(event, "user", None)
                if user:
                    sender_name = getattr(user, "nickname", "") or getattr(user, "unique_id", "")
                    sender_uid = getattr(user, "unique_id", "") or ""
                msg = getattr(event, "content", "") or ""
                if not msg:
                    emote = getattr(event, "emote", None)
                    if emote:
                        msg = getattr(emote, "name", "[emote]") or "[emote]"
                if not msg:
                    return
                sid = stream_id_holder[0] or "unknown"
                if msg_id and msg_id in _recent_chat:
                    return
                if msg_id:
                    _recent_chat.add(msg_id)
                    if len(_recent_chat) > 5000:
                        _recent_chat.clear()
                _insert_chat_message(username, sender_name, sender_uid, msg, sid, msg_id)
            except Exception:
                pass

        try:
            task = await client.start(fetch_gift_info=True, fetch_room_info=True)
            gift_catalog.clear()
            gift_catalog.update(index_gift_catalog(client.gift_info))

            try:
                await asyncio.wait_for(connected.wait(), timeout=20)
            except asyncio.TimeoutError:
                pass

            if connected.is_set():
                backoff = 10
                with active_lock:
                    if username in active_listeners:
                        active_listeners[username]["live"] = True
                await task
        except Exception:
            pass
        finally:
            with active_lock:
                if username in active_listeners:
                    active_listeners[username]["live"] = False
            ended_stream_id = stream_id_holder[0]
            try:
                await client.disconnect()
            except Exception:
                pass
            if ended_stream_id:
                t = threading.Thread(
                    target=_ollama_full_summary,
                    args=(username, ended_stream_id),
                    daemon=True,
                )
                t.start()

        with active_lock:
            info = active_listeners.get(username)
            if info is None or info.get("stopping"):
                return

        await asyncio.sleep(backoff)
        backoff = min(int(backoff * 1.5), max_backoff)


def start_listener(username: str) -> bool:
    with active_lock:
        if username in active_listeners:
            return False

        if listeners_disabled():
            demo_sid = os.environ.get("DEMO_STREAM_ID", "demo_room_active").strip()
            if not demo_sid:
                demo_sid = "demo_room_active"
            active_listeners[username] = {
                "thread": None,
                "live": True,
                "stream_id": demo_sid,
                "connected_at": datetime.now(timezone.utc).isoformat(),
                "stopping": False,
            }
            return True

        t = threading.Thread(
            target=_thread_entry, args=(username,), daemon=True
        )
        active_listeners[username] = {
            "thread": t,
            "live": False,
            "stream_id": None,
            "connected_at": None,
            "stopping": False,
        }
        t.start()
        return True


def _thread_entry(username: str):
    asyncio.run(_run_listener(username))


def stop_listener(username: str) -> bool:
    global current_transcription_user
    with active_lock:
        info = active_listeners.get(username)
        if info is None:
            return False
        info["stopping"] = True
        _stop_transcriber(username)
        if current_transcription_user == username:
            current_transcription_user = None
        if info.get("thread") is None:
            active_listeners.pop(username, None)
            return True
    return True


def _start_all_tracked():
    if listeners_disabled():
        tracked = _get_tracked_users()
        for t in tracked:
            start_listener(t["username"])
        if tracked:
            print(
                f"[*] Demo mode: registered {len(tracked)} tracked user(s) without TikTok listeners"
            )
        return
    tracked = _get_tracked_users()
    for t in tracked:
        start_listener(t["username"])
    if tracked:
        print(f"[*] Auto-resumed tracking {len(tracked)} user(s): {', '.join(t['username'] for t in tracked)}")


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/search")
def search_page():
    return render_template("search.html")


@app.route("/api/listen", methods=["POST"])
def api_listen():
    data = request.get_json(silent=True) or {}
    raw = data.get("username", "").strip()
    if not raw:
        return jsonify({"error": "Username is required"}), 400

    username = normalize_username(raw)
    _add_tracked_user(username)
    threading.Thread(
        target=_refresh_avatar, args=(username,), daemon=True
    ).start()

    with active_lock:
        already = username in active_listeners and not active_listeners[
            username
        ].get("stopping")

    if already:
        return jsonify({"status": "already_tracking", "username": username})

    started = start_listener(username)
    if not started:
        return jsonify({"status": "already_tracking", "username": username})

    return jsonify({"status": "started", "username": username})


@app.route("/api/listen/<username>", methods=["DELETE"])
def api_stop_listener(username):
    username = normalize_username(username)
    _remove_tracked_user(username)
    found = stop_listener(username)
    if not found:
        return jsonify({"error": f"Not tracking @{username}"}), 404
    return jsonify({"status": "stopped", "username": username})


@app.route("/api/tracked", methods=["GET"])
def api_tracked():
    tracked = _get_tracked_users()
    conn = get_db()
    try:
        with active_lock:
            for t in tracked:
                info = active_listeners.get(t["username"])
                t["is_active"] = info is not None
                t["live"] = info.get("live", False) if info else False
                t["avatar_url"] = streamer_avatars.get(t["username"]) or t.get("avatar_url")
                if t["live"] and not listeners_disabled():
                    t["live_url"] = (
                        f"https://www.tiktok.com/@{t['username']}/live"
                    )
                else:
                    t["live_url"] = None

                row = conn.execute(
                    "SELECT COUNT(*) AS cnt, COALESCE(SUM(usd_value), 0) AS total FROM gifts WHERE username = ?",
                    (t["username"],),
                ).fetchone()
                t["total_gifts"] = row["cnt"]
                t["total_earnings"] = round(row["total"], 2)
    finally:
        conn.close()
    return jsonify(tracked)


@app.route("/api/tracked/<username>/auto_transcribe", methods=["PUT"])
def api_set_auto_transcribe(username):
    username = normalize_username(username)
    body = request.get_json(silent=True) or {}
    enabled = bool(body.get("enabled"))

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT 1 FROM tracked_users WHERE username = ?", (username,)
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return jsonify({"error": f"Not tracking @{username}"}), 404

    _set_auto_transcribe(username, enabled)
    if enabled:
        threading.Thread(
            target=_maybe_auto_transcribe, args=(username,), daemon=True
        ).start()
    return jsonify({"username": username, "auto_transcribe": enabled})


@app.route("/api/status/<username>")
def api_status(username):
    username = normalize_username(username)
    with active_lock:
        info = active_listeners.get(username)
    if info is None:
        return jsonify({"username": username, "status": "not_tracked"})
    return jsonify(
        {
            "username": username,
            "status": "live" if info.get("live") else "offline",
            "stream_id": info.get("stream_id"),
            "connected_at": info.get("connected_at"),
            "avatar_url": streamer_avatars.get(username),
            "live_url": (
                f"https://www.tiktok.com/@{username}/live"
                if info.get("live") and not listeners_disabled()
                else None
            ),
        }
    )


@app.route("/api/audience/<username>")
def api_audience_live(username):
    """Live audience snapshot: viewer count + top viewers (in-memory only).

    Returns 0/[] when the listener is offline. Top viewers come from TikTok's
    RoomUserSeqEvent — usually ~20 top-ranked users by score.
    """
    username = normalize_username(username)
    with active_lock:
        info = active_listeners.get(username)
        if info is None:
            return jsonify({
                "username": username, "live": False, "viewer_count": 0,
                "top_viewers": [], "stream_id": None,
            })
        return jsonify({
            "username": username,
            "live": bool(info.get("live")),
            "stream_id": info.get("stream_id"),
            "viewer_count": int(info.get("viewer_count", 0)),
            "top_viewers": list(info.get("top_viewers") or []),
            "recent_joiners": list(info.get("recent_joiners") or []),
            "updated_at": info.get("viewer_count_updated_at"),
        })


@app.route("/api/audience/<username>/participants")
def api_audience_participants(username):
    """Engaged participants (commented / gifted / shared) for a stream.

    Defaults to the current/most-recent stream. Use ?stream_id=... to query a
    specific past stream. Returns rows ordered by most recent activity first.
    """
    username = normalize_username(username)
    stream_id = request.args.get("stream_id", None)
    limit = min(max(request.args.get("limit", 200, type=int), 1), 1000)

    if not stream_id:
        with active_lock:
            info = active_listeners.get(username)
            stream_id = info.get("stream_id") if info else None
        if not stream_id:
            conn = get_db()
            try:
                row = conn.execute(
                    "SELECT stream_id FROM stream_participants WHERE username = ? ORDER BY last_seen DESC LIMIT 1",
                    (username,),
                ).fetchone()
            finally:
                conn.close()
            stream_id = row["stream_id"] if row else None

    if not stream_id:
        return jsonify({"username": username, "stream_id": None, "participants": []})

    conn = get_db()
    try:
        rows = conn.execute(
            """
            SELECT sender, sender_unique_id, first_seen, last_seen,
                   comment_count, gift_count, share_count, diamond_total
            FROM stream_participants
            WHERE username = ? AND stream_id = ?
            ORDER BY last_seen DESC
            LIMIT ?
            """,
            (username, stream_id, limit),
        ).fetchall()
    finally:
        conn.close()

    participants = []
    for r in rows:
        d = dict(r)
        d["avatar_url"] = sender_avatars.get(r["sender"])
        participants.append(d)
    return jsonify({
        "username": username,
        "stream_id": stream_id,
        "participants": participants,
    })


@app.route("/api/audience/<username>/viewers")
def api_audience_viewers_history(username):
    """Viewer-count time series for a stream (sampled every 30s)."""
    username = normalize_username(username)
    stream_id = request.args.get("stream_id", None)
    limit = min(max(request.args.get("limit", 500, type=int), 1), 5000)

    if not stream_id:
        with active_lock:
            info = active_listeners.get(username)
            stream_id = info.get("stream_id") if info else None
    if not stream_id:
        return jsonify({"username": username, "stream_id": None, "samples": []})

    conn = get_db()
    try:
        rows = conn.execute(
            """
            SELECT viewer_count, timestamp FROM viewer_samples
            WHERE username = ? AND stream_id = ?
            ORDER BY timestamp ASC
            LIMIT ?
            """,
            (username, stream_id, limit),
        ).fetchall()
    finally:
        conn.close()
    return jsonify({
        "username": username,
        "stream_id": stream_id,
        "samples": [dict(r) for r in rows],
    })


@app.route("/api/earnings/<username>")
def api_earnings(username):
    username = normalize_username(username)
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT COALESCE(SUM(usd_value), 0) AS total FROM gifts WHERE username = ?",
            (username,),
        ).fetchone()
        total_earnings = round(row["total"], 2)

        gift_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM gifts WHERE username = ?",
            (username,),
        ).fetchone()["cnt"]

        with active_lock:
            info = active_listeners.get(username)
        stream_id = info.get("stream_id") if info else None

        if stream_id:
            row2 = conn.execute(
                "SELECT COALESCE(SUM(usd_value), 0) AS stream_total FROM gifts WHERE username = ? AND stream_id = ?",
                (username, stream_id),
            ).fetchone()
            stream_earnings = round(row2["stream_total"], 2)

            stream_count = conn.execute(
                "SELECT COUNT(*) AS cnt FROM gifts WHERE username = ? AND stream_id = ?",
                (username, stream_id),
            ).fetchone()["cnt"]

            row_hist = conn.execute(
                "SELECT COALESCE(SUM(usd_value), 0) AS past_total FROM gifts WHERE username = ? AND stream_id != ?",
                (username, stream_id),
            ).fetchone()
            historical_earnings = round(row_hist["past_total"], 2)
        else:
            stream_earnings = 0.0
            stream_count = 0
            historical_earnings = total_earnings

        stream_row = conn.execute(
            "SELECT COUNT(DISTINCT stream_id) AS cnt FROM gifts WHERE username = ?",
            (username,),
        ).fetchone()
        total_streams = stream_row["cnt"]
        historical_gift_count = gift_count - stream_count

        return jsonify(
            {
                "username": username,
                "stream_earnings": stream_earnings,
                "stream_gift_count": stream_count,
                "historical_earnings": historical_earnings,
                "historical_gift_count": historical_gift_count,
                "total_earnings": total_earnings,
                "total_gift_count": gift_count,
                "total_streams": total_streams,
            }
        )
    finally:
        conn.close()


@app.route("/api/gifts/<username>")
def api_gifts(username):
    username = normalize_username(username)
    limit = request.args.get("limit", 50, type=int)
    limit = min(max(limit, 1), 200)
    stream_id = request.args.get("stream_id", None)
    offset = request.args.get("offset", 0, type=int)

    conn = get_db()
    try:
        if stream_id:
            rows = conn.execute(
                "SELECT id, sender, gift_name, diamond_value, usd_value, timestamp FROM gifts WHERE username = ? AND stream_id = ? ORDER BY id DESC LIMIT ? OFFSET ?",
                (username, stream_id, limit, offset),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, sender, gift_name, diamond_value, usd_value, timestamp, stream_id FROM gifts WHERE username = ? ORDER BY id DESC LIMIT ? OFFSET ?",
                (username, limit, offset),
            ).fetchall()
        gifts = []
        for r in rows:
            g = {
                "id": r["id"],
                "sender": r["sender"],
                "gift_name": r["gift_name"],
                "diamond_value": r["diamond_value"],
                "usd_value": round(float(r["usd_value"]), 2),
                "timestamp": r["timestamp"],
                "sender_avatar": sender_avatars.get(r["sender"]),
            }
            if not stream_id:
                g["stream_id"] = r["stream_id"]
            gifts.append(g)
        return jsonify(gifts)
    finally:
        conn.close()


@app.route("/api/streams/<username>")
def api_streams(username):
    username = normalize_username(username)
    streams: dict[str, dict] = {}
    conn = get_db()
    try:
        for r in conn.execute(
            """
            SELECT stream_id,
                   MIN(timestamp) AS started_at,
                   MAX(timestamp) AS last_gift_at,
                   COUNT(*) AS gift_count,
                   SUM(diamond_value) AS total_diamonds,
                   ROUND(SUM(usd_value), 2) AS total_usd,
                   COUNT(DISTINCT sender) AS unique_senders
            FROM gifts
            WHERE username = ?
            GROUP BY stream_id
            """,
            (username,),
        ).fetchall():
            streams[r["stream_id"]] = dict(r)
        for r in conn.execute(
            """
            SELECT stream_id,
                   MIN(timestamp) AS chat_started_at,
                   MAX(timestamp) AS last_chat_at,
                   COUNT(*) AS chat_count
            FROM chat_messages
            WHERE username = ?
            GROUP BY stream_id
            """,
            (username,),
        ).fetchall():
            s = streams.setdefault(r["stream_id"], {"stream_id": r["stream_id"]})
            s["chat_started_at"] = r["chat_started_at"]
            s["last_chat_at"] = r["last_chat_at"]
            s["chat_count"] = r["chat_count"]
    finally:
        conn.close()

    if TRANSCRIBER_URL:
        try:
            resp = http_requests.get(
                f"{TRANSCRIBER_URL}/api/transcripts/{username}/streams",
                timeout=10,
            )
            if resp.ok:
                for r in resp.json() or []:
                    sid = r.get("stream_id")
                    if not sid:
                        continue
                    s = streams.setdefault(sid, {"stream_id": sid})
                    s["transcript_started_at"] = r.get("started_at")
                    s["last_transcript_at"] = r.get("last_transcript_at")
                    s["transcript_count"] = r.get("transcript_count")
        except Exception:
            pass

    out = []
    for s in streams.values():
        s.setdefault(
            "started_at",
            s.get("chat_started_at") or s.get("transcript_started_at"),
        )
        s.setdefault("last_gift_at", None)
        s.setdefault("gift_count", 0)
        s.setdefault("total_diamonds", 0)
        s.setdefault("total_usd", 0)
        s.setdefault("unique_senders", 0)
        out.append(s)
    out.sort(key=lambda x: (x.get("started_at") or ""), reverse=True)
    return jsonify(out)


@app.route("/api/top-gifters/<username>")
def api_top_gifters(username):
    username = normalize_username(username)
    stream_id = request.args.get("stream_id", None)
    conn = get_db()
    try:
        if stream_id:
            rows = conn.execute(
                """
                SELECT sender,
                       COUNT(*) AS gift_count,
                       SUM(diamond_value) AS total_diamonds,
                       ROUND(SUM(usd_value), 2) AS total_usd
                FROM gifts
                WHERE username = ? AND stream_id = ?
                GROUP BY sender
                ORDER BY SUM(usd_value) DESC
                LIMIT 25
                """,
                (username, stream_id),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT sender,
                       COUNT(*) AS gift_count,
                       SUM(diamond_value) AS total_diamonds,
                       ROUND(SUM(usd_value), 2) AS total_usd
                FROM gifts
                WHERE username = ?
                GROUP BY sender
                ORDER BY SUM(usd_value) DESC
                LIMIT 25
                """,
                (username,),
            ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["avatar_url"] = sender_avatars.get(r["sender"])
            result.append(d)
        return jsonify(result)
    finally:
        conn.close()


@app.route("/api/chat/<username>")
def api_chat(username):
    username = normalize_username(username)
    limit = request.args.get("limit", 100, type=int)
    limit = min(max(limit, 1), 500)
    stream_id = request.args.get("stream_id", None)
    offset = request.args.get("offset", 0, type=int)

    conn = get_db()
    try:
        if stream_id:
            rows = conn.execute(
                "SELECT id, sender, sender_unique_id, message, stream_id, timestamp FROM chat_messages WHERE username = ? AND stream_id = ? ORDER BY id ASC LIMIT ? OFFSET ?",
                (username, stream_id, limit, offset),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, sender, sender_unique_id, message, stream_id, timestamp FROM chat_messages WHERE username = ? ORDER BY id ASC LIMIT ? OFFSET ?",
                (username, limit, offset),
            ).fetchall()
        messages = []
        for r in rows:
            messages.append({
                "id": r["id"],
                "sender": r["sender"],
                "sender_unique_id": r["sender_unique_id"],
                "message": r["message"],
                "stream_id": r["stream_id"],
                "timestamp": r["timestamp"],
            })
        return jsonify(messages)
    finally:
        conn.close()


@app.route("/api/chat/<username>/streams")
def api_chat_streams(username):
    username = normalize_username(username)
    conn = get_db()
    try:
        rows = conn.execute(
            """
            SELECT stream_id,
                   MIN(timestamp) AS started_at,
                   MAX(timestamp) AS last_message_at,
                   COUNT(*) AS message_count,
                   COUNT(DISTINCT sender) AS unique_chatters
            FROM chat_messages
            WHERE username = ?
            GROUP BY stream_id
            ORDER BY MIN(timestamp) DESC
            """,
            (username,),
        ).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route("/api/chat/<username>", methods=["DELETE"])
def api_chat_clear(username):
    username = normalize_username(username)
    stream_id = request.args.get("stream_id", None)
    conn = get_db()
    try:
        if stream_id:
            cur = conn.execute(
                "DELETE FROM chat_messages WHERE username = ? AND stream_id = ?",
                (username, stream_id),
            )
        else:
            cur = conn.execute(
                "DELETE FROM chat_messages WHERE username = ?",
                (username,),
            )
        deleted = cur.rowcount
        conn.commit()
        return jsonify({"status": "cleared", "deleted": deleted, "username": username})
    finally:
        conn.close()


_FTS_TOKEN_RE = re.compile(r'"[^"]+"|\S+')


def _clean_filter(value: str | None, *, username: bool = False) -> str:
    value = (value or "").strip()
    if username:
        value = value.lstrip("@").lower()
    return value


def _parse_search_args():
    limit = min(max(request.args.get("limit", 50, type=int), 1), 200)
    offset = max(request.args.get("offset", 0, type=int), 0)
    result_type = (request.args.get("type") or "all").strip().lower()
    if result_type not in {"all", "chat", "transcript"}:
        result_type = "all"
    sort = (request.args.get("sort") or "newest").strip().lower()
    if sort not in {"newest", "oldest"}:
        sort = "newest"
    return {
        "q": (request.args.get("q") or "").strip(),
        "type": result_type,
        "username": _clean_filter(request.args.get("username"), username=True),
        "sender": _clean_filter(request.args.get("sender"), username=True),
        "stream_id": _clean_filter(request.args.get("stream_id")),
        "date_from": _clean_filter(request.args.get("date_from")),
        "date_to": _clean_filter(request.args.get("date_to")),
        "sort": sort,
        "limit": limit,
        "offset": offset,
    }


def _fts_query(text: str) -> str:
    tokens = []
    for raw in _FTS_TOKEN_RE.findall(text):
        token = raw.strip().strip('"')
        if not token:
            continue
        escaped = token.replace('"', '""')
        tokens.append(f'"{escaped}"')
    return " AND ".join(tokens)


def _chat_where(args, include_sender=True, alias=""):
    prefix = f"{alias}." if alias else ""
    where = []
    params = []
    if args["username"]:
        where.append(f"{prefix}username = ?")
        params.append(args["username"])
    if include_sender and args["sender"]:
        where.append(f"(LOWER({prefix}sender) = ? OR LOWER(COALESCE({prefix}sender_unique_id, '')) = ?)")
        params.extend([args["sender"], args["sender"]])
    if args["stream_id"]:
        where.append(f"{prefix}stream_id = ?")
        params.append(args["stream_id"])
    if args["date_from"]:
        where.append(f"{prefix}timestamp >= ?")
        params.append(f"{args['date_from']}T00:00:00Z")
    if args["date_to"]:
        where.append(f"{prefix}timestamp <= ?")
        params.append(f"{args['date_to']}T23:59:59Z")
    return where, params


def _query_chat_search(conn, args, *, limit=None, offset=None):
    limit = args["limit"] if limit is None else limit
    offset = args["offset"] if offset is None else offset
    order = "DESC" if args["sort"] == "newest" else "ASC"
    where, params = _chat_where(args)
    q = args["q"]

    if q:
        match = _fts_query(q)
        if match:
            try:
                c_where, c_params = _chat_where(args, alias="c")
                sql_where = ["chat_messages_fts MATCH ?"] + c_where
                rows = conn.execute(
                    f"""
                    SELECT c.id, c.username, c.sender, c.sender_unique_id, c.message,
                           c.stream_id, c.timestamp
                    FROM chat_messages_fts
                    JOIN chat_messages c ON c.id = chat_messages_fts.rowid
                    WHERE {" AND ".join(sql_where)}
                    ORDER BY c.timestamp {order}, c.id {order}
                    LIMIT ? OFFSET ?
                    """,
                    [match] + c_params + [limit, offset],
                ).fetchall()
                return [_chat_result(r) for r in rows]
            except sqlite3.OperationalError:
                pass
        where.append("LOWER(message) LIKE ?")
        params.append(f"%{q.lower()}%")

    if not where:
        where.append("1 = 1")
    rows = conn.execute(
        f"""
        SELECT id, username, sender, sender_unique_id, message, stream_id, timestamp
        FROM chat_messages
        WHERE {" AND ".join(where)}
        ORDER BY timestamp {order}, id {order}
        LIMIT ? OFFSET ?
        """,
        params + [limit, offset],
    ).fetchall()
    return [_chat_result(r) for r in rows]


def _chat_result(row):
    return {
        "type": "chat",
        "id": row["id"],
        "username": row["username"],
        "sender": row["sender"],
        "sender_unique_id": row["sender_unique_id"],
        "text": row["message"],
        "stream_id": row["stream_id"],
        "timestamp": row["timestamp"],
    }


def _chat_search_stats(conn, args):
    where, params = _chat_where(args)
    if args["q"]:
        where.append("LOWER(message) LIKE ?")
        params.append(f"%{args['q'].lower()}%")
    if not where:
        where.append("1 = 1")
    return dict(conn.execute(
        f"""
        SELECT COUNT(*) AS chat_count,
               COUNT(DISTINCT stream_id) AS chat_stream_count,
               COUNT(DISTINCT sender) AS unique_chatters
        FROM chat_messages
        WHERE {" AND ".join(where)}
        """,
        params,
    ).fetchone())


def _gift_stats(conn, args):
    where = []
    params = []
    if args["username"]:
        where.append("username = ?")
        params.append(args["username"])
    if args["sender"]:
        where.append("LOWER(sender) = ?")
        params.append(args["sender"])
    if args["stream_id"]:
        where.append("stream_id = ?")
        params.append(args["stream_id"])
    if args["date_from"]:
        where.append("timestamp >= ?")
        params.append(f"{args['date_from']}T00:00:00Z")
    if args["date_to"]:
        where.append("timestamp <= ?")
        params.append(f"{args['date_to']}T23:59:59Z")
    if not where:
        where.append("1 = 1")
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS gift_count,
               COALESCE(SUM(diamond_value), 0) AS diamond_total,
               ROUND(COALESCE(SUM(usd_value), 0), 2) AS usd_total,
               COUNT(DISTINCT stream_id) AS gift_stream_count
        FROM gifts
        WHERE {" AND ".join(where)}
        """,
        params,
    ).fetchone()
    return dict(row)


def _participant_stats(conn, args):
    where = []
    params = []
    if args["username"]:
        where.append("username = ?")
        params.append(args["username"])
    if args["sender"]:
        where.append("(LOWER(sender) = ? OR LOWER(COALESCE(sender_unique_id, '')) = ?)")
        params.extend([args["sender"], args["sender"]])
    if args["stream_id"]:
        where.append("stream_id = ?")
        params.append(args["stream_id"])
    if not where:
        where.append("1 = 1")
    rows = conn.execute(
        f"""
        SELECT sender, sender_unique_id,
               MIN(first_seen) AS first_seen,
               MAX(last_seen) AS last_seen,
               SUM(comment_count) AS comment_count,
               SUM(gift_count) AS gift_count,
               SUM(share_count) AS share_count,
               SUM(diamond_total) AS diamond_total,
               COUNT(DISTINCT stream_id) AS streams
        FROM stream_participants
        WHERE {" AND ".join(where)}
        GROUP BY sender, sender_unique_id
        ORDER BY (SUM(comment_count) + SUM(gift_count) + SUM(share_count)) DESC,
                 MAX(last_seen) DESC
        LIMIT 25
        """,
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def _search_transcripts(args, *, for_stats=False):
    if not TRANSCRIBER_URL:
        return {"available": False, "error": "Transcription service is not configured"}
    if args["sender"]:
        return {
            "available": True,
            "results": [],
            "stats": {
                "transcript_count": 0,
                "transcript_stream_count": 0,
                "languages": [],
            },
        }
    params = {
        "q": args["q"],
        "username": args["username"],
        "stream_id": args["stream_id"],
        "date_from": args["date_from"],
        "date_to": args["date_to"],
        "sort": args["sort"],
        "limit": 1 if for_stats else args["limit"],
        "offset": 0 if for_stats else args["offset"],
    }
    try:
        resp = http_requests.get(
            f"{TRANSCRIBER_URL}/api/search/transcripts",
            params={k: v for k, v in params.items() if v not in ("", None)},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json() or {}
        data["available"] = True
        return data
    except http_requests.RequestException as e:
        return {"available": False, "error": f"Transcription service unavailable: {str(e)}"}


@app.route("/api/search")
def api_search():
    args = _parse_search_args()
    combined_limit = args["limit"] if args["type"] != "all" else min(args["limit"] * 2, 200)
    results = []
    transcript_status = {"available": bool(TRANSCRIBER_URL)}

    if args["type"] in {"all", "chat"}:
        conn = get_db()
        try:
            results.extend(_query_chat_search(conn, args, limit=combined_limit, offset=args["offset"]))
        finally:
            conn.close()

    if args["type"] in {"all", "transcript"}:
        transcript_data = _search_transcripts(args)
        transcript_status = {
            "available": transcript_data.get("available", False),
            "error": transcript_data.get("error"),
        }
        for r in transcript_data.get("results", []):
            results.append({
                "type": "transcript",
                "id": r.get("id"),
                "username": r.get("username"),
                "sender": None,
                "sender_unique_id": None,
                "text": r.get("original_text") or "",
                "translated_text": r.get("translated_text") or "",
                "detected_language": r.get("detected_language"),
                "target_lang": r.get("target_lang"),
                "stream_id": r.get("stream_id"),
                "timestamp": r.get("timestamp"),
            })

    reverse = args["sort"] == "newest"
    results.sort(key=lambda r: (r.get("timestamp") or "", r.get("id") or 0), reverse=reverse)
    if args["type"] == "all":
        results = results[:args["limit"]]

    return jsonify({
        "results": results,
        "count": len(results),
        "filters": {k: v for k, v in args.items() if k not in {"limit", "offset"}},
        "transcripts": transcript_status,
    })


@app.route("/api/search/stats")
def api_search_stats():
    args = _parse_search_args()
    conn = get_db()
    try:
        chat_stats = _chat_search_stats(conn, args)
        gift_stats = _gift_stats(conn, args)
        participants = _participant_stats(conn, args)
        stream_row = conn.execute(
            """
            SELECT COUNT(*) AS tracked_users FROM tracked_users
            """
        ).fetchone()
    finally:
        conn.close()

    transcript_data = _search_transcripts(args, for_stats=True)
    transcript_stats = transcript_data.get("stats") or {
        "transcript_count": 0,
        "transcript_stream_count": 0,
        "languages": [],
    }
    stream_count = len({
        *([] if not chat_stats.get("chat_stream_count") else ["chat"]),
        *([] if not gift_stats.get("gift_stream_count") else ["gift"]),
    })

    return jsonify({
        "chat": chat_stats,
        "gifts": gift_stats,
        "participants": participants,
        "transcripts": {
            **transcript_stats,
            "available": transcript_data.get("available", False),
            "error": transcript_data.get("error"),
        },
        "totals": {
            "tracked_users": stream_row["tracked_users"],
            "matching_messages": chat_stats.get("chat_count", 0) + transcript_stats.get("transcript_count", 0),
            "matching_stream_groups": stream_count + transcript_stats.get("transcript_stream_count", 0),
        },
    })


def _detected_language_from_lt(result: dict) -> str | None:
    """LibreTranslate returns detectedLanguage as an object or, on some builds, a string."""
    dl = result.get("detectedLanguage")
    if isinstance(dl, dict):
        return dl.get("language")
    if isinstance(dl, str):
        return dl
    return None


@app.route("/api/translate", methods=["POST"])
def api_translate():
    if not LIBRETRANSLATE_URL:
        return jsonify({"error": "LibreTranslate is not configured"}), 503

    data = request.get_json(silent=True) or {}
    text = data.get("text", "").strip()
    if not text:
        return jsonify({"translated_text": "", "detected_language": None})

    target_lang = data.get("target_lang") or DEFAULT_TARGET_LANG
    source_lang = data.get("source_lang") or "auto"

    payload = {
        "q": text,
        "source": source_lang,
        "target": target_lang,
        "format": "text",
    }
    if LIBRETRANSLATE_API_KEY:
        payload["api_key"] = LIBRETRANSLATE_API_KEY

    try:
        resp = http_requests.post(
            f"{LIBRETRANSLATE_URL}/translate",
            json=payload,
            timeout=15,
        )
        resp.raise_for_status()
        result = resp.json()
        return jsonify({
            "translated_text": result.get("translatedText", ""),
            "detected_language": _detected_language_from_lt(result),
        })
    except http_requests.RequestException as e:
        return jsonify({"error": f"Translation failed: {str(e)}"}), 502


@app.route("/api/translate/languages")
def api_translate_languages():
    global _translate_languages_cache

    if not LIBRETRANSLATE_URL:
        return jsonify({"error": "LibreTranslate is not configured"}), 503

    with _translate_lang_lock:
        if _translate_languages_cache is not None:
            return jsonify(_translate_languages_cache)

        try:
            resp = http_requests.get(
                f"{LIBRETRANSLATE_URL}/languages",
                timeout=10,
            )
            resp.raise_for_status()
            _translate_languages_cache = resp.json()
            return jsonify(_translate_languages_cache)
        except http_requests.RequestException as e:
            return jsonify({"error": f"Failed to fetch languages: {str(e)}"}), 502


@app.route("/api/transcribe/<username>", methods=["POST"])
def api_transcribe_start(username):
    global current_transcription_user
    username = normalize_username(username)
    if not TRANSCRIBER_URL:
        return jsonify({"error": "Transcription service is not configured"}), 503

    body = request.get_json(silent=True) or {}
    target_lang = (body.get("target_lang") or DEFAULT_TARGET_LANG or "en").strip() or "en"

    with active_lock:
        info = active_listeners.get(username)
        if not info:
            return jsonify({"error": f"Not tracking @{username}"}), 404
        if not info.get("live"):
            return jsonify({"error": f"@{username} is not currently live"}), 400
        stream_id = info.get("stream_id") or "unknown"
        room_id = info.get("stream_id")
        cached_stream_url = info.get("stream_url")

    stream_url = cached_stream_url
    if not stream_url and room_id:
        stream_url = _fetch_fresh_stream_url(room_id)
    if not stream_url:
        print(f"[!] No stream URL for @{username} (room_id={room_id}, cached={bool(cached_stream_url)})")
        return jsonify({
            "error": "Could not get a stream URL. The TikTok room info endpoint may be rate-limiting us. Wait a few seconds and try again, or restart the listener for this user."
        }), 400

    with active_lock:
        if current_transcription_user and current_transcription_user != username:
            _stop_transcriber(current_transcription_user)

        ok = _start_transcriber(username, stream_url, stream_id, target_lang=target_lang)
        if not ok:
            return jsonify({"error": "Transcription service failed to start (check transcriber logs)"}), 502

        current_transcription_user = username

    return jsonify({"status": "started", "username": username, "target_lang": target_lang})


@app.route("/api/transcribe/<username>", methods=["DELETE"])
def api_transcribe_stop(username):
    global current_transcription_user
    username = normalize_username(username)
    with active_lock:
        if current_transcription_user == username:
            current_transcription_user = None
    _stop_transcriber(username)
    return jsonify({"status": "stopped", "username": username})


@app.route("/api/transcribe/status")
def api_transcribe_status():
    with active_lock:
        user = current_transcription_user
    if not user:
        return jsonify({"active": False, "username": None})
    info = active_listeners.get(user, {})
    return jsonify({
        "active": True,
        "username": user,
        "stream_id": info.get("stream_id"),
        "configured": bool(TRANSCRIBER_URL),
    })


@app.route("/api/transcripts/<username>")
def api_transcripts(username):
    username = normalize_username(username)
    if not TRANSCRIBER_URL:
        return jsonify({"error": "Transcription service is not configured"}), 503
    try:
        params = {
            "limit": request.args.get("limit", 50, type=int),
            "offset": request.args.get("offset", 0, type=int),
        }
        stream_id = request.args.get("stream_id")
        if stream_id:
            params["stream_id"] = stream_id
        resp = http_requests.get(
            f"{TRANSCRIBER_URL}/api/transcripts/{username}",
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        return jsonify(resp.json())
    except http_requests.RequestException as e:
        return jsonify({"error": f"Transcription service unavailable: {str(e)}"}), 502


@app.route("/api/transcripts/<username>/streams")
def api_transcript_streams(username):
    username = normalize_username(username)
    if not TRANSCRIBER_URL:
        return jsonify({"error": "Transcription service is not configured"}), 503
    try:
        resp = http_requests.get(
            f"{TRANSCRIBER_URL}/api/transcripts/{username}/streams",
            timeout=15,
        )
        resp.raise_for_status()
        return jsonify(resp.json())
    except http_requests.RequestException as e:
        return jsonify({"error": f"Transcription service unavailable: {str(e)}"}), 502


@app.route("/api/summary/<username>")
def api_summary_current(username):
    username = normalize_username(username)
    with active_lock:
        info = active_listeners.get(username)
    stream_id = info.get("stream_id") if info else None
    if not stream_id:
        return jsonify({"username": username, "summary": None})
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT summary_text, analyzed_count, timestamp FROM stream_summaries WHERE username = ? AND stream_id = ? AND summary_type = 'live_overview'",
            (username, stream_id),
        ).fetchone()
        if row:
            return jsonify({
                "username": username,
                "stream_id": stream_id,
                "summary": row["summary_text"],
                "analyzed_count": row["analyzed_count"],
                "timestamp": row["timestamp"],
            })
        return jsonify({"username": username, "stream_id": stream_id, "summary": None})
    finally:
        conn.close()


@app.route("/api/summary/<username>/<stream_id>")
def api_summary_stream(username, stream_id):
    username = normalize_username(username)
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT summary_type, summary_text, analyzed_count, timestamp FROM stream_summaries WHERE username = ? AND stream_id = ? ORDER BY CASE summary_type WHEN 'full_summary' THEN 0 ELSE 1 END LIMIT 1",
            (username, stream_id),
        ).fetchone()
        if row:
            return jsonify({
                "username": username,
                "stream_id": stream_id,
                "summary_type": row["summary_type"],
                "summary": row["summary_text"],
                "analyzed_count": row["analyzed_count"],
                "timestamp": row["timestamp"],
            })
        return jsonify({"username": username, "stream_id": stream_id, "summary": None})
    finally:
        conn.close()


init_db()


def _bootstrap_avatars():
    """Hydrate streamer_avatars from DB and refresh any stale/missing entries."""
    try:
        for t in _get_tracked_users():
            if t.get("avatar_url"):
                streamer_avatars[t["username"]] = t["avatar_url"]
    except Exception:
        pass

    def _initial_refresh():
        try:
            for t in _get_tracked_users():
                _refresh_avatar(t["username"])
                time.sleep(2)
        except Exception:
            pass

    threading.Thread(target=_initial_refresh, daemon=True).start()
    threading.Thread(target=_avatar_refresh_loop, daemon=True).start()


_bootstrap_avatars()
_start_all_tracked()

if OLLAMA_BASE_URL:
    _overview_thread = threading.Thread(target=_ollama_live_overview_loop, daemon=True)
    _overview_thread.start()
    print(f"[*] Ollama AI chat analysis enabled (model: {OLLAMA_MODEL})")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
