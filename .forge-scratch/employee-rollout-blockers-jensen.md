# Employee Rollout Blockers - Jensen

## Sync Status

- Required shell sync command was attempted first:
  `cd /home/ubuntu/degen-deal-parser && git fetch origin main && git pull --rebase origin main`
- The checkout under `/home/ubuntu/degen-deal-parser` is read-only in this sandbox, so shell git failed at `.git/FETCH_HEAD`.
- Remote `origin/main` was verified through the GitHub connector as identical to local commit `237f6cf88ab20c877d473b610483318d7398afa3` before changes were made in the writable `/tmp` clone.

## Changes

- Password reset request flow:
  - Kept the neutral forgot-password response.
  - Changed reset-request identifier auditing from raw SHA-256 to HMAC-SHA256 keyed by `EMPLOYEE_TOKEN_HMAC_KEY`.
  - Removed match/delivery state from the public `password.reset_requested` audit payload. Matched accounts still create manager-visible `password.reset_manager_request` rows when SMS delivery is unavailable.
  - Added regression assertions that reset-request audit rows do not expose match state or dictionary-attackable identifier hashes.

- Hourly rate/payroll UI safety:
  - Kept manager-facing pay entry in dollars.
  - Added regression coverage that a legacy `hourly_rate_cents=25.00` submission is rejected instead of being stored as cents.

- CI/config deployability:
  - Added safe TikTok test defaults to the GitHub Tests workflow and pytest bootstrap without real secrets.
  - Set `TIKTOK_SYNC_ENABLED=false` for tests so inert TikTok defaults do not start live sync behavior.
  - Added `starlette<1.0.0` to `requirements.txt`; the local unpinned `starlette==1.0.0` stack deadlocks even a minimal FastAPI `TestClient`, which caused the CI-equivalent discover run to stall in TestClient-based tests.

## Verification

- Passed: `python3 -m unittest tests.test_employee_portal_wave3_hardening.AuditLogOnAuthEventsTests.test_forgot_confirmation_is_neutral_and_queues_matched_accounts tests.test_employee_portal_wave4.AdminProfileUpdateHardeningTests.test_profile_hourly_rate_parser_accepts_dollars -v`
- Passed: `python3 -m unittest tests.test_tiktok_reporting tests.test_tiktok_token_refresh -v`
- Passed: `python3 -m unittest tests.test_team_portal_timeoff tests.test_team_portal_mobile_nav tests.test_team_portal_announcements tests.test_team_portal_admin_nav tests.test_team_manager_tooling tests.test_employee_payroll_ops tests.test_employee_timecards -v`
- Passed: `python3 -m unittest tests.test_legacy_security_hardening tests.test_auth_key_separation tests.test_cycle_validation -v`
- Passed: `python3 -m compileall app tests`
- Attempted: `python3 -m unittest discover -s tests -v`
  - It progressed through admin, attachment, auth, cache, and Clockify integration tests, then timed out in the local TestClient stack at a Clockify webhook request.
  - A minimal FastAPI `TestClient` request also timed out locally with installed `starlette==1.0.0`, so the dependency constraint was added. This sandbox cannot reinstall from GitHub/PyPI because shell network DNS is blocked, so full discover could not be re-run against the constrained dependency set here.

## CI Expectation

- GitHub Actions should install `starlette<1.0.0` from `requirements.txt`, avoiding the local TestClient deadlock.
- TikTok auth tests should see safe, non-secret test values while production still requires real configured TikTok credentials when OAuth is used.
- CSRF remains enabled for production state-changing routes; tests use route/page tokens or direct handler calls.

## Remaining Rollout Risks

- A local commit was created in the writable clone.
- Remote publish is still blocked from this sandbox: shell `git push origin main` cannot resolve `github.com`, and the GitHub connector write calls were cancelled by the tool layer. The patch is committed locally but not pushed to `origin/main` from here.
- Full CI should be watched after push because this sandbox could not reinstall the pinned dependency or complete the remote publish.
- No browser smoke was run; changes are backend/test/config focused and covered by template/context tests.
