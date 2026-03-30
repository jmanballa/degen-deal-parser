# Degen Deal Parser

Internal Discord deal and ledger parser for Degen Collectibles.

This app:
- ingests watched Discord deal-log channels
- stores raw messages and attachments
- parses buys, sales, trades, and expenses
- provides review, reporting, and bookkeeping reconciliation pages
- supports a partner-facing `/deals` view and employee-friendly `/review` flow

## Local Run

From the project root:

```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

For fast local UI and design work, use:

```powershell
.\scripts\run_ui_dev.ps1
```

That mode:
- runs against local SQLite at `data/degen_ui_dev.db`
- disables Discord ingest and parser worker
- keeps hot reload on for templates and styling
- avoids shared Postgres latency during design work

Then open:
- `http://127.0.0.1:8000/login`

## Important Env Vars

Core:
- `DISCORD_BOT_TOKEN`
- `OPENAI_API_KEY`
- `DATABASE_URL`
- `SESSION_SECRET`

Auth:
- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`
- `REVIEWER_USERNAME`
- `REVIEWER_PASSWORD`

Session / deploy:
- `PUBLIC_BASE_URL`
- `SESSION_COOKIE_NAME`
- `SESSION_HTTPS_ONLY`
- `SESSION_SAME_SITE`
- `SESSION_DOMAIN`

Worker controls:
- `DISCORD_INGEST_ENABLED`
- `PARSER_WORKER_ENABLED`

## Render Deployment

Recommended public URL:
- `ops.degencollectibles.com`

Recommended first deployment:
- host the FastAPI app separately from Shopify
- keep `degencollectibles.com` on Shopify
- point a subdomain like `ops.degencollectibles.com` to Render
- this repo now includes [render.yaml](/C:/Users/jeffr/discord-deal-parser/live-deal-parser/render.yaml) as a starting blueprint

### Render service settings

Build command:

```bash
pip install -r requirements.txt
```

Start command:

```bash
python -m uvicorn app.main:app --host 0.0.0.0 --port 10000
```

If you use the included Render blueprint:
- it mounts a persistent disk at `/var/data`
- it sets `DATABASE_URL=sqlite:////var/data/degen_live.db`
- you still need to fill in the secret env vars in Render

### Recommended production env vars

```text
PUBLIC_BASE_URL=https://ops.degencollectibles.com
SESSION_SECRET=<strong random secret>
SESSION_COOKIE_NAME=degen_session
SESSION_HTTPS_ONLY=true
SESSION_SAME_SITE=lax
SESSION_DOMAIN=ops.degencollectibles.com
```

If you use SQLite initially:
- set `DATABASE_URL` to a writable persistent disk path on the host
- do not rely on ephemeral container storage if you care about preserving data

Better long-term option:
- move to Postgres before broader partner/employee use

### Shared Postgres setup

If you want:
- Discord ingestion running on your local machine
- the employee/partner web UI running on Render
- both sharing the same data

then use one shared Postgres database for both.

This app now supports either of these `DATABASE_URL` forms:

```text
postgresql+psycopg://USER:PASSWORD@HOST:5432/DBNAME
postgres://USER:PASSWORD@HOST:5432/DBNAME
```

The app normalizes `postgres://...` to the SQLAlchemy `psycopg` driver automatically.

Recommended durable architecture:
- web UI host:
  - `DISCORD_INGEST_ENABLED=false`
  - `PARSER_WORKER_ENABLED=false`
- dedicated worker host:
  - `DISCORD_INGEST_ENABLED=true`
  - `PARSER_WORKER_ENABLED=true`

That keeps the employee/partner UI isolated from Discord connectivity and parser load, while the background ingest worker runs on an always-on host with a stable network path.

### Dedicated worker host

This repo now includes a standalone worker entrypoint:

```bash
python -m app.worker_service
```

That process:
- initializes the shared database
- seeds watched channels from `DISCORD_CHANNEL_IDS` if provided
- runs Discord ingestion
- runs the parser worker
- writes the shared runtime heartbeat used by `/dashboard`, `/table`, and `/status`

Recommended worker env vars:

```text
DATABASE_URL=<shared postgres url>
DISCORD_BOT_TOKEN=<discord token>
OPENAI_API_KEY=<openai key>
DISCORD_INGEST_ENABLED=true
PARSER_WORKER_ENABLED=true
STARTUP_BACKFILL_ENABLED=false
AUTH_RESEED_PASSWORDS=false
```

`STARTUP_BACKFILL_ENABLED=false` is recommended on an always-on worker host to reduce Discord startup pressure and avoid repeated heavy reconnect backfills on deploys.

### Render split deployment

The included [render.yaml](/C:/Users/jeffr/discord-deal-parser/live-deal-parser/render.yaml) now models the durable split:
- `degen-deal-parser` web service for the UI
- `degen-deal-parser-worker` worker service for Discord ingest + parser processing

Both services should use the same Postgres `DATABASE_URL`.

### Discord bot and parser worker

For production, prefer the dedicated worker host instead of relying on the FastAPI app lifespan to keep the background jobs alive.

## Shopify Domain Setup

Recommended approach:
- keep Shopify storefront on `degencollectibles.com`
- deploy this app on `ops.degencollectibles.com`

Do not use Shopify app proxy for this full internal tool unless you specifically want a storefront-coupled experience and are okay with proxy/auth constraints.

## Production Notes

- Rotate any existing Discord/OpenAI secrets before public deployment.
- Change default admin credentials before sharing the app.
- SQLite is okay for light internal use, but Postgres is the better next step.
- The app stores uploaded/imported bookkeeping state in the database, so persistent storage matters.
- If you split local ingestion from hosted UI, do not use two separate SQLite databases.
