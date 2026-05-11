# TikTok Live Gift Tracker

Real-time gift tracking dashboard for TikTok live streams. Connects to public TikTok WebSockets (no API keys required) and tracks every gift sent during a livestream with persistent historical data.

[![Live site — GitHub Pages](https://img.shields.io/badge/site-GitHub%20Pages-222?style=for-the-badge&logo=github&logoColor=white)](https://chartmann1590.github.io/tiktok-live-gift-tracker/)
[![View on GitHub](https://img.shields.io/badge/code-GitHub-181717?style=for-the-badge&logo=github)](https://github.com/chartmann1590/tiktok-live-gift-tracker)

![Docker](https://img.shields.io/badge/Docker-ready-blue)
![Python](https://img.shields.io/badge/Python-3.12-yellow)
![License](https://img.shields.io/badge/License-MIT-green)

## Support & sponsor

This project is **MIT-licensed and free to self-host** — no paywalls, no API keys, no tracking. If it helps your stream, your agency, or your workflow, **consider sponsoring** so I can keep improving reliability, docs, and features.

[![Buy Me a Coffee](https://img.shields.io/badge/Buy%20Me%20a%20Coffee-ffdd00?style=for-the-badge&logo=buymeacoffee&logoColor=black)](https://buymeacoffee.com/charleshartmann)

**Why donate?** Your support pays for time on bug fixes, compatibility when TikTok changes behavior, better dashboards, and keeping the docs site (above) up to date. A coffee’s worth goes a long way — thank you.

- **Buy Me a Coffee:** [buymeacoffee.com/charleshartmann](https://buymeacoffee.com/charleshartmann)
- **Project site (docs & overview):** [chartmann1590.github.io/tiktok-live-gift-tracker](https://chartmann1590.github.io/tiktok-live-gift-tracker/)

## Features

- **Real-time gift tracking** — captures every gift the moment it's sent during a live stream
- **Permanent storage** — all data saved to SQLite, survives container restarts
- **Auto-reconnect** — automatically reconnects when a streamer goes live again
- **Multi-streamer support** — track multiple users simultaneously
- **Stream history** — browse every past stream with full gift breakdowns
- **Top gifters leaderboard** — see who's donated the most across all time
- **Dark mode dashboard** — clean Tailwind CSS UI with live status indicators
- **One-command deploy** — Docker Compose with persistent volume

## Quick Start

```bash
git clone https://github.com/chartmann1590/tiktok-live-gift-tracker.git
cd tiktok-live-gift-tracker
docker compose up -d
```

Open [http://localhost:5000](http://localhost:5000) and enter a TikTok username.

## How It Works

1. Enter a TikTok `@username` in the search bar
2. The app connects to that user's live stream via public WebSockets (using [TikTokLive](https://github.com/isaackogan/TikTokLive))
3. Every gift event is captured, converted to USD (diamonds × $0.005), and saved to a local SQLite database
4. The dashboard auto-refreshes every 3 seconds showing live gifts, earnings, and stream history
5. If the stream ends, the app automatically reconnects when the user goes live again
6. Tracked users and all gift data persist across container restarts via a Docker volume

## Architecture

```
Flask (web server, main thread)
├── Serves the single-page dashboard
├── REST API endpoints for data queries
│
├── Background Thread #1 (asyncio event loop)
│   └── TikTokLiveClient("@user1") → GiftEvent → SQLite INSERT
│
├── Background Thread #2 (asyncio event loop)
│   └── TikTokLiveClient("@user2") → GiftEvent → SQLite INSERT
│
└── SQLite (WAL mode) — persistent gift history
```

**Gift streak handling:** TikTok gifts can be "streaked" (sent repeatedly). The app only records a gift when the streak ends to prevent double-counting.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Dashboard UI |
| `POST` | `/api/listen` | Start tracking a user |
| `DELETE` | `/api/listen/<user>` | Stop tracking a user |
| `GET` | `/api/tracked` | List all tracked users with stats |
| `GET` | `/api/status/<user>` | Live/offline status |
| `GET` | `/api/earnings/<user>` | Current stream, historical, and all-time earnings |
| `GET` | `/api/gifts/<user>` | Gift feed (all time or per-stream) |
| `GET` | `/api/streams/<user>` | All recorded streams with aggregated stats |
| `GET` | `/api/top-gifters/<user>` | Top gifters leaderboard |

## Configuration

### Persistent Storage

Compose uses a **named volume** (`tiktok_monitor_data` → `/app/data`) for SQLite (`gifts.db` and WAL files). That data lives in Docker’s volume store, not in the image layer, so `docker compose build --no-cache` and image rebuilds do not erase it.

To bind-mount a host folder instead (e.g. easy backups), replace the service `volumes` entry with `- ./data:/app/data` and keep `DATA_DIR=/app/data`.

**Do not** use `docker compose down -v` unless you intend to delete the named volume and its database.

If you already have a local `./data` folder from an older compose file, either switch the compose volume back to `- ./data:/app/data` or copy your `gifts.db*` files into the named volume before relying on it.

**Repairing gift totals:** Run `python scripts/repair_gift_rows.py` (optional `--db` / `--dry-run`) to recompute `diamond_value` and `usd_value` from SQLite + `gift_diamond_rates.json` without TikTok; see `money.repair_row_diamonds_usd` for legacy-row limits.

### Auto-Reconnect

Tracked users are saved in the database. When the container starts, it automatically resumes tracking all previously added users. Reconnection uses exponential backoff (10s → 300s max).

## Requirements

- Docker & Docker Compose
- No TikTok account, API keys, or login credentials needed

## Tech Stack

- **Backend:** Python 3.12, Flask
- **TikTok Listener:** [TikTokLive](https://github.com/isaackogan/TikTokLive) by Isaac Kogan
- **Database:** SQLite (WAL mode)
- **Frontend:** Vanilla JS, Tailwind CSS (CDN)
- **Deployment:** Docker

## License

MIT
