# Nightly Audit Fixes - Jensen - 2026-04-29

Base available locally: `5e2e9a83382ecbb60f0d9a48a02948072d0582fe`

## Fixes

1. Password reset session invalidation
   - `consume_password_reset_token()` now sets `User.session_invalidated_at` when the reset succeeds.
   - Self-service password change also stamps the field so other existing sessions are invalidated consistently.
   - Regression coverage verifies a stale active session cookie is rejected after reset.

2. Draft employee email uniqueness race
   - Removed the pre-insert email clash check from `create_draft_employee()`.
   - Employee creation now relies on the DB unique constraint for `EmployeeProfile.email_lookup_hash`.
   - `IntegrityError` on duplicate email is rolled back and returned as the existing friendly `draft_email_taken` error.

3. Purge reversibility
   - Added `EmployeePurgeTombstone` with a 24-hour restore window.
   - Purge now snapshots user metadata and encrypted profile PII before wiping fields.
   - Added `restore_employee_purge_tombstone()` and `/team/admin/employees/{user_id}/purge/undo`.
   - Restore marks the tombstone used, writes an audit row, and stamps `session_invalidated_at` so old cookies do not come back.

4. CSRF expiration
   - CSRF sessions keep the existing token key and now add `csrf_token_issued_at`.
   - Tokens older than 4 hours are rejected.
   - `issue_token()` refreshes missing/expired issued-at metadata so freshly rendered forms keep working.

5. Pay-rate query bound
   - `_pay_rate_rows()` now applies `LIMIT 500`.
   - Regression coverage verifies the helper caps returned rows.

## Tests Run

- `python3 -m py_compile app/csrf.py app/auth.py app/models.py app/routers/team_admin_employees.py tests/test_legacy_security_hardening.py tests/test_admin_employee_token_revocation.py tests/test_employee_portal_draft_flow.py tests/test_employee_portal_wave4.py`
- `timeout 90s python3 -m pytest tests/test_legacy_security_hardening.py tests/test_admin_employee_token_revocation.py -q` - passed, 17 tests
- `timeout 90s python3 -m pytest tests/test_admin_employee_token_revocation.py -q` - passed, 10 tests
- `timeout 90s python3 -m pytest tests/test_employee_portal_draft_flow.py::CreateDraftEmployeeTests -q` - passed, 4 tests
- `timeout 90s python3 -m pytest tests/test_employee_portal_wave4.py::AdminProfileUpdateHardeningTests::test_bulk_pay_rates_sorts_unpaid_people_to_bottom tests/test_employee_portal_wave4.py::AdminProfileUpdateHardeningTests::test_pay_rate_rows_are_limited -q` - passed, 2 tests
- `timeout 60s python3 -m pytest tests/test_employee_portal_draft_flow.py::CreateDraftEmployeeTests::test_duplicate_draft_email_is_reported_from_unique_constraint -q` - passed, 1 test
- `timeout 60s python3 -m pytest tests/test_employee_portal_wave4.py::AdminProfileUpdateHardeningTests::test_pay_rate_rows_are_limited -q` - passed, 1 test
- `python3 -m compileall app tests` - passed
- `git diff --check` - passed

## Verification Limits

- The required initial sync in `/home/ubuntu/degen-deal-parser` failed because `.git/FETCH_HEAD` is on a read-only filesystem in this sandbox.
- A fresh network fetch from GitHub also failed with `Could not resolve host: github.com`.
- The writable worktree was made by copying the read-only repo at local `main == origin/main == 5e2e9a83382ecbb60f0d9a48a02948072d0582fe` into `/tmp/degen-deal-parser-work-20260429`.
- GitHub connector verification showed remote `main` is still identical to `5e2e9a83382ecbb60f0d9a48a02948072d0582fe`.
- Shell push to `origin/main` failed with `Could not resolve host: github.com`.
- Full app-level `TestClient` portal tests hang in this sandbox during FastAPI lifespan startup before any route handler runs. Bounded runs were used and the direct/unit coverage above passed.

## Remaining Risks

- Purge now has a 24-hour encrypted tombstone and undo path, but there is not yet a visible admin UI button or employee notification.
- The pay-rate page is capped at 500 rows, not fully paginated.

## Commit

- Local commit: this commit. Exact SHA is recorded in the final handoff because
  a Git commit cannot contain its own final object ID without changing it.
