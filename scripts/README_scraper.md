# TikTok username resolver (OpenClaw service)

A tiny FastAPI service that scrapes TikTok Seller Center to resolve an
`order_id` into the buyer's `@username`. Designed to run on a Linux host
(OpenClaw / any Ubuntu box) separately from the main app, so that Playwright
and its browser quirks never land on Machine B.

## Why

TikTok Shop's Order API does not return the buyer's `@username`, only
`buyer_nickname` and `recipient_address.name`. Streamers write `@username`
on packing labels, so we scrape it from the Seller Center UI.

## Architecture

```
   Machine B  ──── POST /resolve ────▶  OpenClaw scraper_server
  (Windows)         X-API-Key: ***       (Linux + Playwright)
                                              │
                                              ▼
                                     TikTok Seller Center UI
```

- Persistent Chromium context (logged in once, reused for weeks).
- SQLite cache at `data/scraper/username_cache.db` — same `order_id` is only
  scraped once.
- API-key protected. No DB access, no writes to the main app's data.

## One-time setup on OpenClaw

```bash
# 1. Clone + install deps
git clone https://github.com/jmanballa/degen-deal-parser.git
cd degen-deal-parser
python3 -m venv .venv
source .venv/bin/activate
pip install -r scripts/scraper_requirements.txt
python -m playwright install --with-deps chromium

# 2. Log in once. If OpenClaw is headless, do this on your Windows machine
#    and scp data/scraper/storage_state.json over to OpenClaw.
#    On a machine with a screen:
python scripts/scraper_login.py
#    The browser opens -> enter email/password + 2FA -> script exits when
#    it detects the authenticated dashboard.

# 3. Configure env (e.g. append to a .env on OpenClaw)
export SCRAPER_API_KEY="$(openssl rand -hex 32)"   # save this somewhere
# Optional:
#   SCRAPER_SELLER_CENTER_BASE=https://seller-us.tiktok.com
#   SCRAPER_ORDER_DETAIL_PATH='/order/detail?order_no={order_id}'
#   SCRAPER_USERNAME_SELECTORS='a[href*="/@"],a[href*="tiktok.com/@"]'
#   SCRAPER_CACHE_TTL_SECONDS=2592000   # 30 days
#   SCRAPER_HEADLESS=true

# 4. Start the server
uvicorn scripts.scraper_server:app --host 0.0.0.0 --port 8787
```

For production, run under `systemd` (see `systemd` snippet below) or `tmux`.

## Use it

```bash
curl -X POST http://OPENCLAW_HOST:8787/resolve \
     -H "X-API-Key: $SCRAPER_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"order_id": "577341910736147304"}'
# -> {"order_id":"577341910736147304","username":"@athena_collectibles","cached":false}
```

### From Machine B (main app)

The main app ships a thin client at `app/username_scraper_client.py` that's
feature-flagged off by default. To enable, add to Machine B's `.env`:

```
USERNAME_SCRAPER_BASE_URL=http://openclaw.tailnet-name.ts.net:8787
USERNAME_SCRAPER_API_KEY=<same value as SCRAPER_API_KEY on OpenClaw>
USERNAME_SCRAPER_TIMEOUT_SECONDS=8
```

Then from anywhere in the app:

```python
from app import username_scraper_client as scraper

if scraper.is_enabled():
    try:
        username = scraper.resolve_username(order.tiktok_order_id)
    except scraper.UsernameScraperSessionExpired:
        # re-run scripts/scraper_login.py on OpenClaw
        ...
    except scraper.UsernameScraperNotFound:
        username = None
    except scraper.UsernameScraperError:
        username = None
```

Until that call is wired into e.g. the webhook enrichment path, the service
is dormant from the main app's perspective.

## Failure modes

| HTTP | Meaning | What to do |
|------|---------|------------|
| 401  | bad or missing `X-API-Key` | set `SCRAPER_API_KEY` on the client side |
| 404  | order not found in Seller Center | probably a reverse/canceled order; skip |
| 502  | `scrape_error` — element missing / DOM changed | update `SCRAPER_USERNAME_SELECTORS` |
| 503  | `session_expired` — Seller Center kicked us out | re-run `scripts/scraper_login.py` |
| 500  | unexpected error | see server logs |

`GET /health` returns `{ok: true, session_valid, storage_state_exists, ...}`
and is unauthenticated so you can use it for uptime checks.

## Run as a systemd service (recommended)

`/etc/systemd/system/tiktok-scraper.service`:

```ini
[Unit]
Description=TikTok username resolver
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/degen-deal-parser
EnvironmentFile=/home/ubuntu/degen-deal-parser/.env
ExecStart=/home/ubuntu/degen-deal-parser/.venv/bin/uvicorn scripts.scraper_server:app --host 0.0.0.0 --port 8787
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now tiktok-scraper
journalctl -u tiktok-scraper -f
```

## Tuning the selector

Seller Center occasionally ships DOM changes. If `/resolve` starts returning
502 `scrape_error`, open the order-detail page in your browser, inspect the
element that shows `@username`, copy a stable CSS selector, and add it to
`SCRAPER_USERNAME_SELECTORS` (comma-separated, fallback order). The service
already tries `a[href*="/@"]`, which should survive most rebrands.

## Security notes

- The API key is the only gate. Don't expose port 8787 publicly; put it on a
  Tailscale tailnet or behind a WireGuard link so only Machine B can reach it.
- `storage_state.json` contains Seller Center session cookies — treat it like
  a password. It lives under `data/scraper/` which is in `.gitignore` via
  `data/*.db` but double-check before committing.
- Cache DB is local SQLite; nothing sensitive, just order_id -> username.
