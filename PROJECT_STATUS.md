# Project Status

Last updated: 2026-04-11

## What Is This

**Degen Collectibles** — a Discord deal parser + TikTok Shop livestream platform for a collectibles/trading card business.

Two main verticals:
1. **Discord deal parsing** — ingests Discord deal-log messages, parses them into structured transactions, normalizes for financial reporting
2. **TikTok Shop livestream tools** — order sync, real-time streamer dashboard, analytics, product management

Stack: Python 3.14, FastAPI, Uvicorn, PostgreSQL (prod) / SQLite (dev), discord.py, OpenAI API (parser fallback), TikTok Shop Open Platform API, TikSync SDK (live chat WebSocket), Jinja2 + vanilla JS + Chart.js, role-based auth.

---

## Deployment

### Machine A — Local Dev (Windows PC, travels with owner)

- Run: `powershell -ExecutionPolicy Bypass -File .\scripts\run_local_web.ps1`
- Run with production Postgres: `powershell -ExecutionPolicy Bypass -File .\scripts\run_local_web_pg.ps1`
- Web-only mode: Discord ingest, parser worker, and backfill are **disabled**
- **SQLite** database at `data/degen_live.db` (local copy, not synced with production)
- Access at `http://127.0.0.1:8000`
- Tailscale IP: `100.122.56.32`

### Machine B — Production (Windows PC, runs 24/7)

- Run: `powershell -ExecutionPolicy Bypass -File .\scripts\run_hosted.ps1`
- Runs web server + worker process with auto-restart and liveness watchdog
- Discord bot, parser worker, TikTok order sync, webhook listener, live chat all run here
- **PostgreSQL** database (`postgresql+psycopg://degen:degen42069@localhost:5432/degen_live`)
- Exposed via **Cloudflare tunnel** at `ops.degencollectibles.com`
- Automated Postgres backup via `scripts/backup_pg.ps1` (pg_dump → OneDrive via rclone)
- Tailscale IP: `100.110.34.106`

Both machines share the same codebase via git. Machine B auto-deploys on push to `main` via GitHub Actions.

---

## Architecture (Post-Refactor)

As of April 2026, `app/main.py` was refactored from ~12,000 lines into a modular router architecture:

| Layer | Files | Description |
|---|---|---|
| **App init** | `app/main.py` (~720 lines) | FastAPI app creation, middleware, static files, router includes |
| **Shared helpers** | `app/shared.py` (~5,000 lines) | Background tasks, state management, polling loops, shared utilities |
| **Route handlers** | `app/routers/` (16 modules) | One file per feature area |
| **Core logic** | `app/*.py` (38 files total) | Models, parser, worker, ingestion, reporting, etc. |
| **Templates** | `app/templates/` | Jinja2 HTML with inline JS/CSS |
| **Scripts** | `scripts/` (11 files) | Backfill, migration, scraping, backup utilities |
| **Tests** | `tests/` (25 files) | 233 passing tests |

### Router Modules

| Router | Path prefix | Purpose |
|---|---|---|
| `admin.py` | `/admin` | Admin pages, debug, health |
| `admin_actions.py` | `/admin` | Channel config, parser reruns, learned rules |
| `bookkeeping.py` | `/bookkeeping` | Sheet import, reconciliation |
| `channels_api.py` | `/channels`, `/messages` | Channel/message JSON APIs |
| `dashboard.py` | `/dashboard`, `/status`, `/ops-log` | Dashboard, status, ops log |
| `deals.py` | `/deals` | Deal detail, attachment routes |
| `hits.py` | `/hits` | Live hit tracker |
| `messages.py` | `/table`, `/review-table`, `/review` | Message tables, review workflow |
| `reports.py` | `/reports`, `/finance`, `/pnl` | Financial reports |
| `shopify.py` | `/shopify` | Shopify order sync, OAuth |
| `stream_manager.py` | `/stream-manager` | Stream team management |
| `tiktok_analytics.py` | `/tiktok/analytics` | Analytics charts, daily/stream data |
| `tiktok_orders.py` | `/tiktok/orders` | Order listing, webhook, sync |
| `tiktok_products.py` | `/tiktok/products` | Product management |
| `tiktok_streamer.py` | `/tiktok/streamer` | Live streamer dashboard |

### Key Backend Modules

| File | Purpose |
|---|---|
| `models.py` | SQLModel ORM classes (DiscordMessage, Transaction, TikTokOrder, User, etc.) |
| `db.py` | Database engine, session management, additive migrations (Postgres + SQLite) |
| `parser.py` | Rule-based deal parsing with OpenAI fallback |
| `worker.py` | Message queue processor, stitching logic |
| `tiktok_ingest.py` | OAuth token exchange, webhook parsing, order normalization |
| `tiktok_auth_refresh.py` | Background token refresh (every 30 min) |
| `tiktok_live_chat.py` | TikSync WebSocket for live chat + room ID capture |
| `reporting.py` | Financial summaries, TikTok order reporting |
| `transactions.py` | Normalized Transaction and TransactionItem sync |
| `discord_ingest.py` | Discord bot listener, raw message storage |
| `bookkeeping.py` | Bookkeeping sheet import + reconciliation |
| `config.py` | All env vars and app settings |
| `auth.py` | Role-based auth (admin, reviewer, viewer) |

### Key Scripts

| Script | Purpose |
|---|---|
| `scripts/tiktok_backfill.py` (~1,700 lines) | TikTok API signing, order/product fetching, analytics API calls |
| `scripts/backup_pg.ps1` | Automated Postgres backup → OneDrive via rclone |
| `scripts/run_hosted.ps1` | Production launch with auto-restart + liveness watchdog |
| `scripts/run_local_web.ps1` | Local dev launch (SQLite, web-only) |
| `scripts/run_local_web_pg.ps1` | Local dev launch connected to production Postgres via Tailscale |
| `scripts/shopify_backfill.py` | Shopify historical order import |
| `scripts/migrate_sqlite_to_postgres.py` | One-time migration tool |
| `scripts/scrape_tiktok_docs.py` | TikTok API docs scraper (685 pages → markdown/PDF) |
| `scripts/tiktok_giveaway_login.py` | browser-use agent for TikTok giveaway automation (WIP) |

---

## What's Working

### Discord Side
- Discord message ingestion from watched channels
- Rule-based parsing with OpenAI fallback for ambiguous cases
- Message stitching (grouping nearby related messages into one deal)
- Transaction normalization and financial reporting
- Review/approval workflow (`/table`, `/review-table`)
- Bookkeeping import (CSV, XLSX, Google Sheets auto-import) and reconciliation
- Ops log with filtering and pagination

### TikTok Shop Side
- OAuth flow for Shop tokens (auto-refresh every 30 min)
- Separate Creator OAuth flow (for real-time live analytics)
- Order sync: startup backfill + periodic polling + webhook enrichment
- **Streamer dashboard** (`/tiktok/streamer`):
  - Real-time order feed with toast notifications and sound alerts
  - Today's GMV + Stream GMV (manual or auto-detected range)
  - TikTok official GMV from API
  - Top Sellers and Top Buyers with Today/Stream toggle
  - Live chat panel (via TikSync WebSocket), collapsible
  - Refund alerts highlighted in red
  - Stream dividers between orders from different livestreams
  - LIVE/OFFLINE badge + "Now Streaming: [name]" display
  - "Log a Hit" feature for streamers
  - Copy-to-clipboard for customer labels
  - **GMV Goal Bar** — progress bar with click-to-edit target
  - **High-Value Order Alerts** — gold toast + distinct chime (default $100+)
  - **VIP Buyer Alerts** — purple toast with lifetime spend badge (default $5,000+)
  - **Order Velocity Sparkline** — SVG per-minute order rate
  - **Post-Stream Summary Card** — auto-triggered overlay with full stream stats + copy-to-clipboard
- **Analytics page** (`/tiktok/analytics`):
  - Daily GMV trend chart (7d/30d/60d/90d)
  - Stream session list with GMV, duration, revenue/hour, % change
  - Per-minute GMV chart for individual streams
  - Top sellers/buyers per stream (scrollable, default 20 shown)
  - **Repeat Buyer Tracking** — sortable buyer table with repeat badges, lifetime stats
  - **Product Performance Ranking** — revenue, qty, live/non-live split
  - **Stream-over-Stream Comparison** — side-by-side delta cards, "Pick Streams" or "Week vs Week" modes
- **Orders page** (`/tiktok/orders`):
  - Full order listing with date filters and livestream filter
- Product sync and management
- TikTok webhook with HMAC-SHA256 signature verification

### Infrastructure
- Role-based auth (admin, reviewer, viewer)
- User management UI (`/admin/users`)
- PostgreSQL with connection pooling (QueuePool, pool_size=5, max_overflow=10), TCP keepalives, pool_pre_ping, 30s statement timeout
- SQLite with WAL mode, busy_timeout, retry logic
- Liveness watchdog in production (auto-restarts unresponsive processes)
- Automated Postgres backups via rclone → OneDrive
- 233 passing tests across 25 test files
- CI workflow (GitHub Actions) with auto-deploy to Machine B

---

## Known Issues / Technical Debt

### Important
- **`app/templates/tiktok_streamer.html` is ~2,700 lines** with all CSS and JS inline. Should be split into separate assets.
- **`scripts/tiktok_backfill.py` is ~1,700 lines** and could be split into modules.
- **TikTok analytics data is delayed ~2 days** for the `overview_performance` endpoint.
- **Webhook payloads arrive incomplete** ($0.00, "Guest", no items). The app fetches full details async, but there's a brief window where incomplete data shows.
- **Creator token** for `live_core_stats` requires manual OAuth and a `live_room_id` from TikSync WebSocket. Fragile.
- **Buyer @username not available** — TikTok Shop API only returns `buyer_nickname` (display name) and `recipient_address.name` (shipping name), not the TikTok @handle. See "Planned Features" below.

### Nice to Have
- No automated end-to-end tests for the streamer dashboard
- Chat panel connection depends on TikSync third-party service availability
- README.md has some stale deployment info

---

## Planned Features / Under Discussion

### Buyer @Username Resolution
TikTok Shop API does not expose buyer TikTok usernames. Only `buyer_nickname` (display name like "Ross") and `recipient_address.name` (shipping name like "Ross Edward") are available. The @handle (e.g., "cashycash77") that TikTok shows in Seller Center is internal data not exposed via the API.

Options explored:
1. **Official TikTok Display API** (`/v2/user/info/`) — returns `username` but only for users who OAuth into your app. Can't look up arbitrary buyers.
2. **Third-party scraper APIs** — [TikLiveAPI](https://www.tikliveapi.com/documentation/users/info-by-id) accepts `user_id` → returns `uniqueId` (@handle). $9.90/100K requests. We have `user_id` from every order.
3. **browser-use agent** — use AI browser automation to scrape buyer username from TikTok Seller Center order detail page. Free, already have browser-use installed. Slower but no API cost.

### Streamer Dashboard Ideas (Backlog)
- **Product queue / run sheet** — pre-plan which products to showcase, advance through list
- **Countdown timer for flash deals** — streamer sets a timer, creates urgency
- **Comment-to-sale conversion tracker** — correlate chat spikes with order spikes
- **Live audience sentiment** — positive/negative gauge from chat keywords
- **"Who's watching" panel** — known buyers in chat with lifetime spend
- **First-time buyer alerts** — distinct notification for first-ever purchase
- **Inventory alerts** — warning when showcased product drops below threshold
- **Stream health monitor** — viewer count trend, connection quality
- **Quick sound effects board** — configurable clips for sales/milestones
- **Pinned notes / talking points** — streamer-only notepad
- **Milestone celebrations** — visual effects at 50th order, $10K GMV, etc.
- **Leaderboard race** — real-time "top spender this stream"
- **Streak tracker** — "12 orders in the last 5 minutes!" momentum counter
- **Clip bookmarking** — timestamp markers during stream for highlights

### TikTok Giveaway Automation (WIP)
Using `browser-use` AI agent to automate TikTok's native live giveaway feature through Seller Center. Login script exists at `scripts/tiktok_giveaway_login.py`.

### Multi-Tenancy Prep
Future goal: make the platform multi-tenant SaaS. No work started yet.

---

## Recent Commits (Last 20)

```
283ee22 Add automated PostgreSQL backup script with OneDrive upload via rclone
4d5cef4 Refactor app/main.py into modular router architecture
83470cd Fix em-dash character in run_hosted.ps1 causing PowerShell parse error
0845b03 Fix deploy health check: retry for up to 120s instead of single 30s attempt
7ac5dae Prevent server hangs: liveness watchdog, pool timeouts, statement timeout, backfill rate limit
5f5d456 Add VIP buyer badges, chat viewer presence tracking, and join messages
d706e42 Add firecrawl-py dependency for web scraping
a503d87 Show current streamer name on streamer dashboard header
0ed237a Add scrollable containers and show-more toggle to Top Buyers and Product Performance tables
eceb587 Document TikTok webhook signature algorithm as do-not-modify in AGENTS.md
3f51727 Update docs for streamer dashboard + analytics features
ba37142 Add streamer dashboard + analytics features (8 total)
87452d1 Fix TikTok webhook signature verification and re-enable strict mode
483539d Capture all webhook headers and expand signature debug candidates
f899d6d Fix streamer dashboard livestream dividers
a196dc1 Fix missing hmac import in webhook debug code
910a7d8 Auto-reload streamer dashboard on deploy via build version check
2829ad0 Fix streamer dashboard not detecting active livestreams
6ddd23e Revert TikTok webhook strict_signature to fix 3-day outage
3ec27cf Add TikTok analytics page with Chart.js charts, stream selector, KPIs
```

---

## Key Documentation

| File | What It Covers |
|---|---|
| `AGENTS.md` | Project rules, architecture, coding conventions for AI tools |
| `TIKTOK_API.md` | Every TikTok endpoint, auth flow, response shape, known gotchas |
| `TIKTOK_WEBHOOK_SIGNATURE.md` | Webhook HMAC-SHA256 algorithm (DO NOT MODIFY) |
| `PROJECT_STATUS.md` | This file — current state, deployment, known issues |
| `README.md` | Setup instructions, env vars |

## Env Vars Quick Reference

Essential for TikTok features:
```
TIKTOK_APP_KEY, TIKTOK_APP_SECRET, TIKTOK_REDIRECT_URI
TIKTOK_SHOP_CIPHER (needed for analytics APIs)
TIKTOK_LIVE_API_KEY, TIKTOK_LIVE_USERNAME (for live chat)
```

Tokens are stored in the DB after initial OAuth — not needed in .env after first auth.

See `app/config.py` for the complete list of all settings.
