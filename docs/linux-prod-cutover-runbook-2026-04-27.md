# Degen Linux Green Cutover Runbook + Preflight — 2026-04-27

_Last updated: 2026-04-27 16:40 UTC / 2026-04-27 09:40 PT_

This is the runbook for cutting over `ops.degencollectibles.com` from the current Windows Machine B production host to the Ubuntu/OpenClaw Green host.

**Current status:** preflight/runbook only. No cutover has been performed.

## Hard boundaries until Jeffrey explicitly says “cut over”

Do **not** do any of these during preflight/rehearsal:

- Do not switch Cloudflare tunnel/DNS/public traffic.
- Do not start the Green worker against production integrations.
- Do not mutate Machine B files/services/config.
- Do not write to Machine B Postgres.
- Do not copy production secrets into git.
- Do not delete source or destination media/data.
- Do not replace `/opt/degen/app/data` if it exists and is not the intended symlink.

Approved safe actions before explicit cutover:

- Read-only Machine B inventory.
- Read-only Machine B `pg_dump`.
- Copy media/cache from Machine B to Green without deleting either side.
- Restore dumps into isolated Green databases.
- Start Green web-only staging on localhost, with worker and external warmups disabled.
- Stop/disable Green staging after smoke tests.

## Known current state from preflight

### Code / deploy baseline

- `origin/main`: `f59a9ec29a0d08715212e58ddfee0911d66d1f04`
- Green dev repo `/home/ubuntu/degen-deal-parser`: `f59a9ec29a0d08715212e58ddfee0911d66d1f04`
- Green app checkout `/opt/degen/app`: `f59a9ec29a0d08715212e58ddfee0911d66d1f04`
- Machine B repo `C:\Users\Degen\degen-deal-parser`: `f59a9ec29a0d08715212e58ddfee0911d66d1f04`

Unrelated untracked files exist and should not be swept into this runbook:

- Green dev repo: untracked audit docs under `docs/`.
- Green app checkout: untracked `data` path due `/opt/degen/app/data -> /opt/degen/data` compatibility symlink.
- Machine B: untracked scratch scripts (`_check_ghost.py`, `_scout_health*.ps1`, `_scout_query.py`, `_tt_status.py`, `scripts/verify_match.py`) and `app/data/hit_images/`.

### Green host

- PostgreSQL: `17.9`, cluster `17/main`, listening only on `127.0.0.1:5432`.
- Green DBs:
  - `degen_green`: blank/local staging DB, ~12 MB.
  - `degen_green_snapshot`: restored prod snapshot rehearsal DB, ~3477 MB.
- Green venv: Python `3.11.15`; `pip check` clean.
- `/opt/degen/.env` exists, owner `degen:degen`, mode `600`.
- `/opt/degen/data` exists, owner `degen:degen`, mode `750`.
- `/var/log/degen` exists, owner `degen:degen`, mode `750`.
- Current Green env roots:
  - `DATA_ROOT=/opt/degen/data`
  - `MEDIA_ROOT=/opt/degen/data`
  - `LOG_DIR=/var/log/degen`
- Current Green staging flags:
  - `PUBLIC_HOST_MODE=0`
  - `EMPLOYEE_PORTAL_ENABLED=0`
  - `DISCORD_INGEST_ENABLED=0`
  - `PARSER_WORKER_ENABLED=0`
  - `SESSION_HTTPS_ONLY=0`
  - `PUBLIC_BASE_URL=http://127.0.0.1:8001`
- Current Green local services:
  - `degen-web-staging.service`: inactive/disabled.
  - no installed/running `degen-web.service` or `degen-worker.service` yet.
  - no listeners on `8000`, `8001`, or `15432`; Postgres only on `127.0.0.1:5432`.

### Green data/media cache

From preflight:

- `/opt/degen/data`: ~`5.7G`
- `/opt/degen/data/attachments`: `1912` top-level files
- `/opt/degen/data/attachments/thumbs`: `181` files
- `/opt/degen/data/hit_images`: `11` files

Important caveat from staging smoke:

- The restored Green snapshot currently has `attachmentasset_count=0` in the DB used for the `degen`-user systemd smoke, so `/attachments/{id}` could not be meaningfully tested there.
- On-disk attachment files and thumbs exist, but HTTP attachment routes require integer `attachmentasset.id` DB rows.
- Hit-image media route has been tested successfully: `/hit-images/338b4cbb3fe940ccaaf3e2f4fb977294.jpg` returned HTTP 200 under the `degen` systemd user.

### Machine B current prod

Read-only encoded PowerShell probe at 2026-04-27 16:40 UTC / 09:40 PT:

- Host: `DESKTOP-PPF7VK9`
- Repo HEAD: `f59a9ec29a0d08715212e58ddfee0911d66d1f04`
- Health: HTTP `200`, body `ok=true`, `db_ok=true`, local runtime `running`.
- Env hints:
  - `DATABASE_URL` present (redacted).
  - `DISCORD_INGEST_ENABLED=true`
  - `PARSER_WORKER_ENABLED=true`
  - `PUBLIC_BASE_URL=https://ops.degencollectibles.com`
- Machine B data counts:
  - `data`: `2089` files, `8875732125` bytes (~8.3 GiB)
  - `data\attachments`: `2085` files, `6036900191` bytes (~5.6 GiB)
  - `data\attachments\thumbs`: `173` files, `2017947` bytes
  - `data\hit_images`: missing
  - `data\v2_pending_scans`: `0` files
  - `data\v2_training_scans`: missing
  - `app\data`: `12` files, `7894976` bytes

Machine B listener/task checks via CIM failed with access denied from this SSH context. That is not a prod failure; it means use existing known service/runbook or a local admin shell for those checks if needed.

## Already-rehearsed milestones

1. Green Linux deploy foundation committed: `54e056d49601021eda347f1dcc842e537a8167eb`.
2. External startup warmups can be disabled: `8f6a8825881d095fde4c04882bc7c00139327d24`.
3. Durable runtime data root shipped: `f59a9ec29a0d08715212e58ddfee0911d66d1f04`.
4. Prod snapshot rehearsal completed via read-only Machine B dump + Green restore.
5. Media/cache pre-copy rehearsal completed without deleting or mutating Machine B.
6. Green app booted successfully as real Linux `degen` system user under systemd on `127.0.0.1:8001`.
7. Logs/data permissions verified for `degen`.

## Cutover readiness checklist

Do not start the real cutover until every item below is either checked or explicitly waived.

### A. Human/ops readiness

- [ ] Jeffrey explicitly says: **cut over**.
- [ ] Pick a quiet window where writes/orders/media changes can pause.
- [ ] Tell stream/store team not to use old app during the final sync window.
- [ ] Decide who validates public app after switch.
- [ ] Decide rollback timebox: e.g. if smoke fails for >10 minutes, roll back.

### B. Source/target versions

- [ ] `origin/main` HEAD recorded.
- [ ] Machine B HEAD recorded.
- [ ] `/opt/degen/app` HEAD recorded and equals intended release.
- [ ] Any deploy-time untracked runtime dirs are understood and not accidentally committed.

Commands:

```bash
cd /home/ubuntu/degen-deal-parser && git fetch origin main && git rev-parse origin/main
cd /opt/degen/app && git rev-parse HEAD && git status --short
ssh Degen@100.110.34.106 "powershell -NoProfile -Command 'cd C:\Users\Degen\degen-deal-parser; git rev-parse HEAD; git status --short'"
```

### C. Backups

- [ ] Create final Machine B Postgres dump using read-only connection/tunnel.
- [ ] Verify `pg_restore --list` succeeds on the dump.
- [ ] Store dump under `/opt/degen/backups/` or another backup staging path.
- [ ] Copy/ship backup off-machine before trusting Green as sole prod.
- [ ] Record Machine B rollback state before freeze.

Recommended dump pattern from Green host:

```bash
# Example only. Use current credentials from approved env/secret source.
ssh -N -L 15432:127.0.0.1:5432 Degen@100.110.34.106
PGPASSFILE=/path/to/secure.pgpass pg_dump \
  --host 127.0.0.1 --port 15432 --username <prod_user> \
  --format custom --verbose --file /opt/degen/backups/degen_live-final-$(date -u +%Y%m%dT%H%M%SZ).dump \
  degen_live
pg_restore --list /opt/degen/backups/degen_live-final-*.dump >/dev/null
```

### D. Freeze writes on Blue/Machine B

Pick one operational freeze method before cutover. Options:

1. **Soft freeze**: tell team to stop using app and stop stream/order mutations. Lowest technical risk, relies on humans.
2. **Stop app/worker on Machine B**: stronger freeze but causes downtime immediately.
3. **Set Blue read-only mode if implemented**: best long-term, not currently confirmed.

During final cutover, avoid having both Blue and Green workers active at the same time.

### E. Final DB restore on Green

- [ ] Drop/recreate the chosen Green production DB or restore into a fresh DB name, then atomically point env to it.
- [ ] Restore final custom dump with `pg_restore`.
- [ ] Check restore log for errors/warnings.
- [ ] Run row-count sanity checks.
- [ ] Confirm app can connect with Green env.

Example:

```bash
sudo -u postgres dropdb --if-exists degen_green_prod
sudo -u postgres createdb -O degen_green degen_green_prod
PGPASSWORD='<green_db_password>' pg_restore \
  --host 127.0.0.1 --port 5432 --username degen_green \
  --dbname degen_green_prod --verbose \
  /opt/degen/backups/degen_live-final-YYYYMMDDTHHMMSSZ.dump \
  2>&1 | tee /opt/degen/backups/restore-final-YYYYMMDDTHHMMSSZ.log
```

Sanity SQL:

```sql
select count(*) from discordmessage;
select count(*) from attachmentasset;
select count(*) from tiktok_orders;
select count(*) from shopify_orders;
select count(*) from "user";
```

### F. Final media/cache delta

The final media copy should be a **delta** after write freeze. It must not delete source or destination data.

Include:

- `data/attachments`
- `data/v2_pending_scans`
- `data/v2_scan_history.jsonl`
- `app/data`

Exclude stale SQLite DB files:

- `data/degen_live.db`
- `data/degen_live.db-shm`
- `data/degen_live.db-wal`

Recommended approach: read-only `bsdtar` stream over SSH from Machine B into `/opt/degen/data`, preserving structure, with explicit excludes. Do **not** install anything on Machine B.

### G. Green production service install/enable

Before public switch:

- [ ] Install real `degen-web.service` using systemd template from `deploy/systemd/degen-web.service.example`.
- [ ] Use `User=degen`, `Group=degen`.
- [ ] Use `EnvironmentFile=/opt/degen/.env`.
- [ ] Ensure `/opt/degen/.env` points at final Green production DB.
- [ ] Set `DATA_ROOT=/opt/degen/data` and `MEDIA_ROOT=/opt/degen/data`.
- [ ] Keep worker disabled until web is public-smoked or until explicitly ready.
- [ ] Start web on the intended local port for Cloudflare, probably `127.0.0.1:8000`.
- [ ] Verify `/health` locally.

Suggested pre-public local smoke:

```bash
sudo systemctl daemon-reload
sudo systemctl start degen-web.service
systemctl show degen-web.service -p ActiveState -p SubState -p MainPID -p NRestarts -p ExecMainStatus
curl -fsS http://127.0.0.1:8000/health
curl -fsS -o /tmp/degen-static.css http://127.0.0.1:8000/static/portal.css
```

### H. Worker enablement

Worker should not be enabled until there is a deliberate decision that Green is now the single production writer/integration host.

- [ ] Confirm Blue worker is stopped/frozen or Blue app is no longer receiving writes.
- [ ] Confirm Green env has real production integration secrets only after cutover approval.
- [ ] Start `degen-worker.service` only once.
- [ ] Verify logs show expected worker startup and no duplicate-ingest warnings.

### I. Cloudflare/public switch

Only after explicit approval:

- [ ] Confirm Green web is locally healthy on the exact target port.
- [ ] Switch Cloudflare tunnel config to Green target or enable Green tunnel route.
- [ ] Smoke public URL: `https://ops.degencollectibles.com/health` if exposed, login/core pages, static assets, media.
- [ ] Watch Green logs for 15–30 minutes.

### J. Rollback plan

Rollback should preserve evidence and restore public traffic to Machine B quickly.

Rollback trigger examples:

- Green web fails to start.
- Green DB restore has data integrity issues.
- Public smoke fails after Cloudflare switch.
- Critical app errors appear in Green logs.

Rollback steps:

1. Stop Green worker if started:

   ```bash
   sudo systemctl stop degen-worker.service
   ```

2. Stop Green web or leave it running only if useful for debug:

   ```bash
   sudo systemctl stop degen-web.service
   ```

3. Switch Cloudflare/tunnel public traffic back to Machine B.
4. Restart/unfreeze Machine B scheduled task/services if they were stopped.
5. Verify Machine B health and public URL.
6. Preserve Green logs and restore logs:

   ```bash
   sudo journalctl -u degen-web.service -u degen-worker.service --since '1 hour ago' > /opt/degen/backups/rollback-green-journal-$(date -u +%Y%m%dT%H%M%SZ).log
   sudo cp -a /var/log/degen /opt/degen/backups/degen-logs-rollback-$(date -u +%Y%m%dT%H%M%SZ)
   ```

## Non-destructive preflight result — 2026-04-27

### Passes

- Green and Machine B are on the same code commit: `f59a9ec29a0d08715212e58ddfee0911d66d1f04`.
- Machine B health endpoint is healthy: HTTP 200, `ok=true`, `db_ok=true`.
- Green Postgres 17 is running locally only.
- Green snapshot DB exists and is about same order of size as prod rehearsal: `degen_green_snapshot` ~3477 MB.
- Green data root is first-class and permissioned for `degen`.
- Green systemd smoke as `degen` already passed and was cleaned up.
- Green web/worker services are not active and public cutover has not happened.

### Warnings / things to resolve before final cutover

1. **Machine B final DB counts were not collected in the encoded preflight** because the `beacon/all.ps1` helper invocation shape produced stream-end behavior in earlier attempts. Previous snapshot rehearsal did collect full restored counts successfully. For final cutover, use the already-proven `pg_dump`/`pg_restore --list` and post-restore SQL counts on Green as the source of truth.
2. **Machine B CIM listener/scheduled-task checks returned access denied** from the SSH context. Use app health and known scheduled task inventory, or run a local admin check if exact task state must be confirmed.
3. **Machine B has more attachment files than current Green pre-copy** (`2085` vs Green `1912` top-level attachment files). This is expected drift after rehearsal; final media delta copy is mandatory.
4. **Machine B has `DISCORD_INGEST_ENABLED=true` and `PARSER_WORKER_ENABLED=true`**. During cutover, do not enable Green worker until Blue worker is frozen/stopped, or duplicate ingestion is possible.
5. **Untracked scratch files exist on Machine B**. Do not copy or depend on them for Green. They should be cleaned later, but not during cutover unless explicitly planned.
6. **Attachment route testing depends on DB rows matching files**. The next full final restore should be checked for `attachmentasset` count and at least one `/attachments/{id}` HTTP 200 test if rows exist.
7. **Off-machine backups are not proven by this preflight**. Do not treat Green as sole production until backup/off-machine copy and restore test are decided.

## Recommended next action

Before real cutover, run one more **dry final rehearsal** during a quiet moment:

1. Take a fresh read-only Machine B dump.
2. Restore it into a new Green rehearsal DB, e.g. `degen_green_rehearsal_YYYYMMDD`.
3. Run post-restore row counts.
4. Run final media delta copy into `/opt/degen/data` without deletes.
5. Boot Green web-only as `degen` on `127.0.0.1:8001` against the fresh rehearsal DB.
6. Test `/health`, static asset, hit image, and a real `/attachments/{id}` if `attachmentasset` rows exist.
7. Stop/disable staging and keep Cloudflare untouched.
8. Record measured time for dump, restore, media delta, and smoke.

If that rehearsal is boring and quick, the actual cutover can be scheduled with a known downtime estimate.

## Final dry rehearsal result — 2026-04-27 16:48–17:44 UTC / 09:48–10:44 PT

Jeffrey approved the final dry rehearsal with “sure do that”. The rehearsal stayed inside the approved boundaries:

- no Cloudflare switch
- no worker enablement
- no Machine B writes
- no production DB writes
- no source/destination deletes
- Green staging was stopped/disabled afterward

### Fresh Machine B dump

- Method: local Green `pg_dump` over SSH tunnel to Machine B Postgres.
- Source DB: `degen_live`.
- Dump artifact: `/tmp/degen_dry_rehearsal_20260427T164758Z/degen_live_20260427T164826Z.dump`.
- Dump size: `3.2G`.
- Dump duration: `1621s` (~27m 01s).
- `pg_restore --list`: passed, `688` archive entries.

### Green restore

Initial restore into `degen_green_rehearsal_20260427T164826Z` loaded data but returned code 1 because the archive referenced DB role `degen`, which does not exist as a Postgres role on Green. The errors were ownership/role noise, not row-load failures.

Clean restore was rerun with `--no-owner --no-acl`:

- Clean rehearsal DB: `degen_green_rehearsal_clean_20260427T171805Z`.
- Restore duration: `120s`.
- Restore log: `/tmp/degen_dry_rehearsal_20260427T164758Z/restore_clean_20260427T171805Z.log`.
- Row-count sanity:

```text
db_size|3480 MB
discordmessage|2171
attachmentasset|931
tiktok_orders|16601
shopify_orders|13902
```

### Media delta

Attempted `rsync` first, but `sudo rsync` failed because root did not have the Machine B SSH host key. Fallback used a Windows `tar` stream over SSH into Green `/opt/degen/data`.

- Method: no-delete tar stream from `C:\Users\Degen\degen-deal-parser\data` into `/opt/degen/data`.
- Excluded stale SQLite DB files: `degen_live.db`, `degen_live.db-shm`, `degen_live.db-wal`.
- Duration: `1334s` (~22m 14s).

Before:

```text
before_green
data_total=6045002692
attachment_files=1912
thumb_files=181
hit_image_files=11
```

After:

```text
after_green
duration_sec=1334
data_total=6608886334
attachment_files=2036
thumb_files=181
hit_image_files=11
tar_log_tail=
```

Note: the earlier preflight compared Machine B recursive attachment count with Green top-level attachment count. The dry-run delta result is the authoritative Green-side count after copy.

### Web-only staging smoke

Temporary staging env/drop-in pointed `degen-web-staging.service` at `degen_green_rehearsal_clean_20260427T171805Z` and ran as Linux user/group `degen` on `127.0.0.1:8001`. Worker and external warmups stayed disabled.

Smoke results:

```text
smoke_utc=2026-04-27T17:43:30Z
db=degen_green_rehearsal_clean_20260427T171805Z
attach_id=1711
hit_file=338b4cbb3fe940ccaaf3e2f4fb977294.jpg
service=
MainPID=3156921
NRestarts=0
ExecMainStatus=0
User=degen
Group=degen
ActiveState=active
SubState=running
listeners=
LISTEN 0      2048                     127.0.0.1:8001       0.0.0.0:*                                                 
health=
{"ok":true,"db_ok":true,"local_runtime_status":"running","local_runtime_label":"Running","local_runtime_needs_attention":false,"local_runtime_updated_at":"2026-04-27T17:43:17.214819+00:00","error":null}
HTTP=200 time=0.021858
static=
HTTP=200 bytes=34075 time=0.002992
root=
HTTP=303 bytes=0 time=0.002834
hit_image=
HTTP=200 bytes=1084565 time=0.011788
attachment=
HTTP=303 bytes=0 time=0.003281
attachment_thumb=
HTTP=303 bytes=0 time=0.003250
```

Attachment route detail:

```text
attach_id=1711
HTTP/1.1 303 See Other
date: Mon, 27 Apr 2026 17:43:40 GMT
server: uvicorn
content-length: 0
location: /team/login?next=%2Fattachments%2F1711

--- thumb ---
HTTP/1.1 303 See Other
date: Mon, 27 Apr 2026 17:43:40 GMT
server: uvicorn
content-length: 0
location: /team/login?next=%2Fattachments%2F1711%2Fthumb
```

Interpretation:

- `/health`: pass, HTTP 200, `ok=true`, `db_ok=true`.
- `/static/portal.css`: pass, HTTP 200, `34075` bytes.
- `/`: expected auth redirect, HTTP 303.
- `/hit-images/338b4cbb3fe940ccaaf3e2f4fb977294.jpg`: pass, HTTP 200, `1084565` bytes.
- `/attachments/1711` and `/attachments/1711/thumb`: HTTP 303 redirect to `/team/login?...`, meaning route/auth wiring works and the request reaches the app; this was not a public unauthenticated media 200. A logged-in browser smoke can test actual attachment body if needed.
- Logs showed: employee portal disabled, parser worker disabled, external price-cache warmups disabled.

### Cleanup verification

After smoke:

- stopped `degen-web-staging.service`
- removed `/etc/systemd/system/degen-web-staging.service.d/20-dry-rehearsal-degen.conf`
- removed `/opt/degen/dry-rehearsal.env`
- reloaded systemd
- verified `degen-web-staging.service` inactive/dead and disabled
- verified no listener on `8001`

### Timing estimate from rehearsal

Measured dry-run wall-clock components:

- Fresh DB dump: ~27m
- Clean DB restore: ~2m
- Media delta tar stream: ~22m
- Web-only smoke + cleanup: ~1–2m

Conservative real cutover freeze estimate: **~55–65 minutes** if using the same dump/media-transfer methods and similar data volume. This can probably be reduced by doing a media pre-copy shortly before freeze, then a smaller final delta.

### Remaining before actual cutover

- Pick freeze window and explicitly approve cutover.
- Decide whether to pre-copy media again shortly before freeze to reduce final delta time.
- For actual DB restore, use `pg_restore --no-owner --no-acl` from the start unless Green gets a matching Postgres role.
- Optionally do a logged-in browser smoke for attachment body downloads.
- Only then enable production Green web, switch Cloudflare, and start Green worker after Blue is frozen/stopped.

