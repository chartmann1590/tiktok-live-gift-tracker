import os
import sqlite3
import threading
import asyncio
import time
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

_translate_languages_cache = None
_translate_lang_lock = threading.Lock()

app = Flask(__name__)

active_lock = threading.Lock()
active_listeners: dict[str, dict] = {}
streamer_avatars: dict[str, str] = {}
sender_avatars: dict[str, str] = {}


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
    conn.commit()
    conn.close()


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
            "SELECT username, added_at FROM tracked_users ORDER BY added_at ASC"
        ).fetchall()
        return [{"username": r["username"], "added_at": r["added_at"]} for r in rows]
    finally:
        conn.close()


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
            except Exception:
                pass

        @client.on(DisconnectEvent)
        async def on_disconnect(event: DisconnectEvent):
            with active_lock:
                if username in active_listeners:
                    active_listeners[username]["live"] = False

        @client.on(LiveEndEvent)
        async def on_live_end(event: LiveEndEvent):
            with active_lock:
                if username in active_listeners:
                    active_listeners[username]["live"] = False

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
            try:
                await client.disconnect()
            except Exception:
                pass

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
    with active_lock:
        info = active_listeners.get(username)
        if info is None:
            return False
        info["stopping"] = True
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


@app.route("/api/listen", methods=["POST"])
def api_listen():
    data = request.get_json(silent=True) or {}
    raw = data.get("username", "").strip()
    if not raw:
        return jsonify({"error": "Username is required"}), 400

    username = normalize_username(raw)
    _add_tracked_user(username)

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
                t["avatar_url"] = streamer_avatars.get(t["username"])
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
    conn = get_db()
    try:
        rows = conn.execute(
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
            ORDER BY MIN(timestamp) DESC
            """,
            (username,),
        ).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


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


init_db()
_start_all_tracked()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
