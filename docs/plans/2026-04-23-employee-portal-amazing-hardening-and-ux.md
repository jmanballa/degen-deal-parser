# Employee Portal Amazing Hardening and UX Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Finish the remaining employee portal and team admin audit backlog while also making the portal dramatically easier and safer for employees and managers to use.

**Architecture:** Keep the current FastAPI + SQLModel portal structure, extend the existing permission/auth patterns instead of inventing new ones, and land the work in vertical slices: security/runtime hardening first, then employee/admin workflow improvements, then schedule and dashboard polish. Every change must be pinned by regression tests before or alongside implementation.

**Tech Stack:** FastAPI, SQLModel, Jinja templates, pytest, existing employee portal/team admin routes under `app/routers/`.

---

### Task 1: Add `/team/admin` role-floor hardening

**Objective:** Prevent rank-and-file employees from accessing `/team/admin/*` routes even if a permission row is accidentally granted.

**Files:**
- Modify: `app/routers/team_admin.py`
- Test: `tests/test_team_portal_admin_nav.py`
- Test: `tests/test_employee_portal_wave4.py`

**Step 1: Write failing tests**
- Add a regression test proving an `employee` with an admin permission key is still denied on a `_permission_gate`-protected route.
- Keep manager/reviewer access tests in place so the new role floor does not break intended access.

**Step 2: Run the targeted tests to verify failure**
Run:
`PYTHONPATH=/home/ubuntu/degen-deal-parser python -m pytest tests/test_team_portal_admin_nav.py tests/test_employee_portal_wave4.py -q`

**Step 3: Implement minimal role floor**
- Update `_permission_gate()` in `app/routers/team_admin.py` so only `admin`, `manager`, and `reviewer` can proceed to permission-key evaluation.
- Continue to use `_admin_gate()` for admin-only pages.

**Step 4: Re-run the targeted tests**
Run the same pytest command and verify the new denial and intended manager/reviewer cases pass.

---

### Task 2: Make rate limiting proxy-aware

**Objective:** Ensure login/invite/reset rate limiting can identify real client IPs behind a trusted proxy.

**Files:**
- Modify: `app/rate_limit.py`
- Modify: `app/config.py`
- Test: `tests/test_employee_portal_wave4_hardening.py`

**Step 1: Write failing tests**
- Add tests for `_client_ip()` / rate limiting behavior covering:
  - default fallback to `request.client.host`
  - trusted `X-Forwarded-For`
  - malformed/empty forwarded header fallback

**Step 2: Run the targeted tests to verify failure**
Run:
`PYTHONPATH=/home/ubuntu/degen-deal-parser python -m pytest tests/test_employee_portal_wave4_hardening.py -q`

**Step 3: Implement minimal proxy-aware parsing**
- Add config knobs in `app/config.py` such as trusted proxy headers / trusted hop count.
- Update `app/rate_limit.py` to use trusted forwarded headers only when enabled.

**Step 4: Re-run the targeted tests**
Run the same pytest command and confirm the rate-limit identity logic passes.

---

### Task 3: Tighten runtime secret and secure-cookie defaults

**Objective:** Make production-style employee portal auth safer by default.

**Files:**
- Modify: `app/config.py`
- Test: `tests/test_employee_portal_wave4_hardening.py`

**Step 1: Write failing tests**
- Add config/runtime tests covering:
  - secure session cookies default to true unless explicitly disabled
  - portal-enabled runtime rejects default `SESSION_SECRET`
  - portal-enabled runtime rejects default admin password

**Step 2: Run the targeted tests to verify failure**
Run:
`PYTHONPATH=/home/ubuntu/degen-deal-parser python -m pytest tests/test_employee_portal_wave4_hardening.py -q`

**Step 3: Implement minimal runtime hardening**
- Change `session_https_only` default to secure-by-default.
- Update runtime validation so `employee_portal_enabled` also triggers secret validation, not just public-host mode.

**Step 4: Re-run the targeted tests**
Run the same pytest command and verify the config assertions pass.

---

### Task 4: Harden PII update authorization and audit detail

**Objective:** Require stronger authority for non-empty sensitive data writes and make audit entries more useful without logging raw PII.

**Files:**
- Modify: `app/routers/team_admin_employees.py`
- Test: `tests/test_wave47_admin_tools.py`

**Step 1: Write failing tests**
- Add tests proving:
  - edit-without-reveal permission cannot submit non-empty PII changes
  - full-authority user can still update PII
  - audit details contain change fingerprints/hashes, not raw PII

**Step 2: Run the targeted tests to verify failure**
Run:
`PYTHONPATH=/home/ubuntu/degen-deal-parser python -m pytest tests/test_wave47_admin_tools.py -q`

**Step 3: Implement minimal PII hardening**
- Require `admin.employees.reveal_pii` for non-empty PII updates.
- Add audit-safe change fingerprints for old/new values.
- Keep blank-field preservation behavior intact.

**Step 4: Re-run the targeted tests**
Run the same pytest command and confirm the new auth/audit expectations pass.

---

### Task 5: Clamp hourly rate input and preserve safe data on reject

**Objective:** Prevent absurd or malformed hourly-rate values from being stored.

**Files:**
- Modify: `app/routers/team_admin_employees.py`
- Possibly modify: `app/templates/team/admin/employee_detail.html`
- Test: `tests/test_wave47_admin_tools.py`
- Test: `tests/test_employee_portal_wave4_hardening.py`

**Step 1: Write failing tests**
- Add tests for valid, negative, oversized, and non-integer `hourly_rate_cents` submissions.
- Assert invalid submissions preserve the previous stored value and return a useful flash/error.

**Step 2: Run the targeted tests to verify failure**
Run:
`PYTHONPATH=/home/ubuntu/degen-deal-parser python -m pytest tests/test_wave47_admin_tools.py tests/test_employee_portal_wave4_hardening.py -q`

**Step 3: Implement minimal validation**
- Add sane bounds check in `admin_employee_profile_update()`.
- Reject invalid values without mutating stored rate data.

**Step 4: Re-run the targeted tests**
Run the same pytest command and confirm the validation cases pass.

---

### Task 6: Surface dropped-invite-email warnings to the employee

**Objective:** Tell new employees when onboarding succeeded but their requested email was not saved.

**Files:**
- Modify: `app/auth.py`
- Modify: `app/routers/team.py`
- Test: `tests/test_employee_portal_pii_capture.py`
- Test: `tests/test_employee_portal_wave4_hardening.py`

**Step 1: Write failing tests**
- Add a regression showing successful invite acceptance with an email collision returns a warning flash/banner and still logs `account.invite_email_dropped`.

**Step 2: Run the targeted tests to verify failure**
Run:
`PYTHONPATH=/home/ubuntu/degen-deal-parser python -m pytest tests/test_employee_portal_pii_capture.py tests/test_employee_portal_wave4_hardening.py -q`

**Step 3: Implement minimal plumbing**
- Make invite consumption return enough metadata for the route to know whether email was dropped.
- Include a user-facing flash in the redirect target.

**Step 4: Re-run the targeted tests**
Run the same pytest command and verify the success + warning behavior passes.

---

### Task 7: Improve employee search for names and email

**Objective:** Make the employee list easy to use by searching name, username, or email without exposing raw PII.

**Files:**
- Modify: `app/routers/team_admin_employees.py`
- Modify: `app/templates/team/admin/employees_list.html`
- Test: `tests/test_wave47_admin_tools.py`
- Test: `tests/test_employee_portal_draft_flow.py`

**Step 1: Write failing tests**
- Add tests proving employee search matches:
  - `username`
  - `display_name`
  - email via lookup hash
- Assert list rendering does not suddenly expose plaintext email.

**Step 2: Run the targeted tests to verify failure**
Run:
`PYTHONPATH=/home/ubuntu/degen-deal-parser python -m pytest tests/test_wave47_admin_tools.py tests/test_employee_portal_draft_flow.py -q`

**Step 3: Implement minimal search expansion**
- Join/filter against `EmployeeProfile.email_lookup_hash`.
- Expand query placeholder text to advertise supported search fields.

**Step 4: Re-run the targeted tests**
Run the same pytest command and verify the expanded search behavior passes.

---

### Task 8: Revoke pending invites on termination

**Objective:** Make termination fully shut the door on old onboarding links.

**Files:**
- Modify: `app/routers/team_admin_employees.py`
- Test: `tests/test_employee_portal_wave4.py`
- Test: `tests/test_employee_portal_wave4_hardening.py`

**Step 1: Write failing tests**
- Add a test where a user has a live invite, then gets terminated.
- Assert the invite is revoked/consumed and the old token can no longer be accepted.

**Step 2: Run the targeted tests to verify failure**
Run:
`PYTHONPATH=/home/ubuntu/degen-deal-parser python -m pytest tests/test_employee_portal_wave4.py tests/test_employee_portal_wave4_hardening.py -q`

**Step 3: Implement minimal invite revocation**
- In the termination transaction, revoke outstanding invite tokens tied to that user.
- Add audit context for the revocation count or explicit revoke events.

**Step 4: Re-run the targeted tests**
Run the same pytest command and verify termination also kills pending invites.

---

### Task 9: Add policy acknowledgements as real data, not just audit reconstruction

**Objective:** Make the policies page reliable and scalable with a dedicated acknowledgement table.

**Files:**
- Modify: `app/models.py`
- Modify: `app/routers/team.py`
- Test: `tests/test_employee_portal_wave3.py`

**Step 1: Write failing tests**
- Add tests proving policy acknowledgement inserts a dedicated row, remains idempotent per version, and still writes audit logs.

**Step 2: Run the targeted tests to verify failure**
Run:
`PYTHONPATH=/home/ubuntu/degen-deal-parser python -m pytest tests/test_employee_portal_wave3.py -q`

**Step 3: Implement minimal data model + route changes**
- Add `PolicyAcknowledgement` model/table.
- Read acknowledgement state from the new table.
- Continue writing audit rows for acknowledgements.

**Step 4: Re-run the targeted tests**
Run the same pytest command and verify policy state is persisted correctly.

---

### Task 10: Make schedule saves conflict-aware

**Objective:** Prevent managers from silently overwriting each other’s schedule edits.

**Files:**
- Modify: `app/routers/team_admin_schedule.py`
- Modify: relevant schedule template(s) under `app/templates/team/admin/`
- Test: `tests/test_wave47_admin_tools.py`

**Step 1: Write failing tests**
- Add stale-write regression coverage:
  - capture schedule form token from GET
  - mutate the schedule
  - submit the stale token
  - assert the overwrite is rejected with a conflict flash

**Step 2: Run the targeted tests to verify failure**
Run:
`PYTHONPATH=/home/ubuntu/degen-deal-parser python -m pytest tests/test_wave47_admin_tools.py -q`

**Step 3: Implement minimal optimistic concurrency**
- Add a week-scoped schedule fingerprint/token.
- Render it in the form and reject POSTs when the token is stale.

**Step 4: Re-run the targeted tests**
Run the same pytest command and verify both success and stale-conflict cases pass.

---

### Task 11: Make recurrence/copy scheduling closure-aware

**Objective:** Avoid scheduling people onto days the store is closed.

**Files:**
- Modify: `app/routers/team_admin_schedule.py`
- Test: `tests/test_wave47_admin_tools.py`

**Step 1: Write failing tests**
- Add a regression where a future/target date is closed and recurrence or copy logic would otherwise create a shift there.
- Assert the closed date is skipped while open dates still update.

**Step 2: Run the targeted tests to verify failure**
Run:
`PYTHONPATH=/home/ubuntu/degen-deal-parser python -m pytest tests/test_wave47_admin_tools.py -q`

**Step 3: Implement minimal closure awareness**
- Apply closure checks to recurring or copied destination dates before creating/updating `ShiftEntry` rows.

**Step 4: Re-run the targeted tests**
Run the same pytest command and verify closed-day skipping works.

---

### Task 12: Upgrade employee detail reveal UX

**Objective:** Make revealed PII feel safe, obvious, and short-lived instead of sticky and awkward.

**Files:**
- Modify: `app/templates/team/admin/employee_detail.html`
- Possibly modify: `app/routers/team_admin_employees.py`
- Test: `tests/test_wave47_admin_tools.py`

**Step 1: Write failing tests**
- Add response-content assertions showing revealed values now render with a visible hide affordance and short-lived visibility guidance.

**Step 2: Run the targeted tests to verify failure**
Run:
`PYTHONPATH=/home/ubuntu/degen-deal-parser python -m pytest tests/test_wave47_admin_tools.py -q`

**Step 3: Implement minimal UX improvements**
- Add a hide control and visible “shown briefly” copy.
- Add lightweight client-side auto-hide timer.
- Remove sticky/revealed affordances that make shoulder-surfing easier.

**Step 4: Re-run the targeted tests**
Run the same pytest command and verify the updated reveal UX is present.

---

### Task 13: Make the dashboard feel useful instead of placeholder-heavy

**Objective:** Give employees a cleaner, more intuitive dashboard with role-appropriate actions.

**Files:**
- Modify: `app/templates/team/dashboard.html`
- Modify: `app/routers/team.py`
- Test: `tests/test_employee_portal_wave3.py`
- Test: `tests/test_team_portal_admin_nav.py`

**Step 1: Write failing tests**
- Add tests covering:
  - employees do not see an irrelevant ops CTA
  - managers/admins/reviewers still see ops access
  - dead placeholder text is removed or reduced
  - dashboard keeps rendering with explicit `now_hour`

**Step 2: Run the targeted tests to verify failure**
Run:
`PYTHONPATH=/home/ubuntu/degen-deal-parser python -m pytest tests/test_employee_portal_wave3.py tests/test_team_portal_admin_nav.py -q`

**Step 3: Implement minimal dashboard cleanup**
- Remove or replace dead placeholder widgets.
- Make the primary action feel role-appropriate and actually useful.
- Remove the template fallback for `now_hour` and ensure the route always supplies it.

**Step 4: Re-run the targeted tests**
Run the same pytest command and verify the dashboard behavior passes.

---

### Task 14: Full regression verification, commit, and push

**Objective:** Confirm the employee portal remains stable after the full improvement pass.

**Files:**
- Verify all modified files

**Step 1: Run the highest-signal targeted suites**
Run:
`PYTHONPATH=/home/ubuntu/degen-deal-parser python -m pytest tests/test_employee_portal_wave3.py tests/test_employee_portal_wave4.py tests/test_employee_portal_wave4_hardening.py tests/test_employee_portal_pii_capture.py tests/test_employee_portal_draft_flow.py tests/test_team_portal_admin_nav.py tests/test_wave47_admin_tools.py -q`

**Step 2: Inspect git diff**
Run:
`git status --short && git diff --stat`

**Step 3: Commit**
Use a commit message matching the completed slice, for example:
`git commit -am "Complete employee portal hardening and UX polish"`

**Step 4: Push**
Run:
`git push`

---

## Notes
- Preserve the current session-version invalidation behavior from the recent Batch 3 fixes.
- Do not log raw PII in new audit details.
- Do not regress manager/reviewer access to intended admin-adjacent pages.
- Prefer additive, local changes over introducing new frameworks or major restructuring.
