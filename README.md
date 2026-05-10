# TikTok Live Gift Tracker

Real-time gift tracking dashboard for TikTok live streams. Connects to public TikTok WebSockets (no API keys required) and tracks every gift sent during a livestream with persistent historical data.

![Docker](https://img.shields.io/badge/Docker-ready-blue)
![Python](https://img.shields.io/badge/Python-3.12-yellow)
![License](https://img.shields.io/badge/License-MIT-green)

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

The `docker-compose.yml` maps `./data:/app/data` so your SQLite database survives container updates:

```yaml
volumes:
  - ./data:/app/data
```

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
