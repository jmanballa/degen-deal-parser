import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from sqlmodel import Session, create_engine

from app.models import SQLModel, OperationsLog
from app.discord.ops_log import (
    count_recent_errors,
    list_operations_logs,
    parse_operations_log_details,
    redact_log_details,
    write_operations_log,
)


def _utcnow():
    return datetime.now(timezone.utc)


class OpsLogFilterTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        SQLModel.metadata.create_all(self.engine)
        self.session = Session(self.engine)

    def tearDown(self):
        self.session.close()

    def _add(self, event_type, level="info", source="worker", minutes_ago=0):
        row = OperationsLog(
            event_type=event_type,
            level=level,
            source=source,
            message=event_type,
            details_json="{}",
            created_at=_utcnow() - timedelta(minutes=minutes_ago),
        )
        self.session.add(row)
        self.session.commit()
        return row

    def test_no_filters_returns_all(self):
        self._add("queue.started")
        self._add("ingest.message")
        rows = list_operations_logs(self.session)
        self.assertEqual(len(rows), 2)

    def test_filter_by_level_error(self):
        self._add("queue.started", level="info")
        self._add("queue.failed", level="error")
        rows = list_operations_logs(self.session, level="error")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].level, "error")

    def test_filter_by_level_info(self):
        self._add("queue.started", level="info")
        self._add("queue.failed", level="error")
        rows = list_operations_logs(self.session, level="info")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].level, "info")

    def test_filter_by_event_type_prefix_queue(self):
        self._add("queue.started")
        self._add("queue.failed")
        self._add("ingest.message")
        rows = list_operations_logs(self.session, event_type_prefix="queue.")
        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertTrue(row.event_type.startswith("queue."))

    def test_filter_by_event_type_prefix_ingest(self):
        self._add("queue.started")
        self._add("ingest.message")
        self._add("ingest.deleted")
        rows = list_operations_logs(self.session, event_type_prefix="ingest.")
        self.assertEqual(len(rows), 2)

    def test_filter_by_date_since_excludes_old(self):
        self._add("queue.old", minutes_ago=120)
        self._add("queue.recent", minutes_ago=10)
        cutoff = _utcnow() - timedelta(minutes=30)
        rows = list_operations_logs(self.session, since=cutoff)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].event_type, "queue.recent")

    def test_filter_by_date_until_excludes_future(self):
        self._add("queue.old", minutes_ago=120)
        self._add("queue.recent", minutes_ago=10)
        cutoff = _utcnow() - timedelta(minutes=30)
        rows = list_operations_logs(self.session, until=cutoff)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].event_type, "queue.old")

    def test_combined_filters(self):
        self._add("queue.started", level="info", minutes_ago=10)
        self._add("queue.failed", level="error", minutes_ago=10)
        self._add("ingest.message", level="error", minutes_ago=10)
        rows = list_operations_logs(self.session, event_type_prefix="queue.", level="error")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].event_type, "queue.failed")

    def test_backward_compatible_no_args(self):
        for i in range(5):
            self._add(f"queue.event_{i}")
        rows = list_operations_logs(self.session)
        self.assertEqual(len(rows), 5)


class CountRecentErrorsTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        SQLModel.metadata.create_all(self.engine)
        self.session = Session(self.engine)

    def tearDown(self):
        self.session.close()

    def _add_error(self, minutes_ago=0):
        row = OperationsLog(
            event_type="queue.failed",
            level="error",
            source="worker",
            message="failed",
            details_json="{}",
            created_at=_utcnow() - timedelta(minutes=minutes_ago),
        )
        self.session.add(row)
        self.session.commit()

    def test_counts_errors_within_window(self):
        self._add_error(minutes_ago=10)
        self._add_error(minutes_ago=30)
        self._add_error(minutes_ago=90)  # outside 60-min window
        count = count_recent_errors(self.session, since_minutes=60)
        self.assertEqual(count, 2)

    def test_zero_when_no_errors(self):
        row = OperationsLog(
            event_type="queue.started",
            level="info",
            source="worker",
            message="ok",
            details_json="{}",
            created_at=_utcnow() - timedelta(minutes=5),
        )
        self.session.add(row)
        self.session.commit()
        count = count_recent_errors(self.session)
        self.assertEqual(count, 0)

    def test_counts_only_errors_not_warnings(self):
        row = OperationsLog(
            event_type="queue.warn",
            level="warning",
            source="worker",
            message="warn",
            details_json="{}",
            created_at=_utcnow() - timedelta(minutes=5),
        )
        self.session.add(row)
        self.session.commit()
        count = count_recent_errors(self.session)
        self.assertEqual(count, 0)

    def test_respects_custom_window(self):
        self._add_error(minutes_ago=5)
        self._add_error(minutes_ago=25)
        self.assertEqual(count_recent_errors(self.session, since_minutes=10), 1)
        self.assertEqual(count_recent_errors(self.session, since_minutes=30), 2)


class RedactLogDetailsTests(unittest.TestCase):
    """Area B: sensitive fields must be redacted recursively before persist and on read."""

    def test_redacts_top_level_token(self) -> None:
        redacted = redact_log_details({"access_token": "tok-abc-123", "user": "alice"})
        self.assertEqual(redacted["access_token"], "[REDACTED]")
        self.assertEqual(redacted["user"], "alice")

    def test_redacts_password_case_insensitive(self) -> None:
        redacted = redact_log_details({"PASSWORD": "hunter2", "Password_hint": "no"})
        self.assertEqual(redacted["PASSWORD"], "[REDACTED]")
        self.assertEqual(redacted["Password_hint"], "[REDACTED]")

    def test_redacts_nested_dict_secrets(self) -> None:
        details = {
            "request": {
                "headers": {
                    "Authorization": "Bearer abc",
                    "X-API-Key": "leak-me",
                    "Content-Type": "application/json",
                },
                "body": {"refresh_token": "rt-xyz", "shop_id": "shop-1"},
            }
        }
        redacted = redact_log_details(details)
        self.assertEqual(redacted["request"]["headers"]["Authorization"], "[REDACTED]")
        self.assertEqual(redacted["request"]["headers"]["X-API-Key"], "[REDACTED]")
        self.assertEqual(redacted["request"]["headers"]["Content-Type"], "application/json")
        self.assertEqual(redacted["request"]["body"]["refresh_token"], "[REDACTED]")
        self.assertEqual(redacted["request"]["body"]["shop_id"], "shop-1")

    def test_redacts_hyphenated_sensitive_header_names(self) -> None:
        redacted = redact_log_details(
            {
                "headers": {
                    "X-API-Key": "x-api-key-secret",
                    "api-key": "api-key-secret",
                    "private-key": "private-key-secret",
                    "session-id": "session-id-secret",
                    "Content-Type": "application/json",
                }
            }
        )
        headers = redacted["headers"]
        self.assertEqual(headers["X-API-Key"], "[REDACTED]")
        self.assertEqual(headers["api-key"], "[REDACTED]")
        self.assertEqual(headers["private-key"], "[REDACTED]")
        self.assertEqual(headers["session-id"], "[REDACTED]")
        self.assertEqual(headers["Content-Type"], "application/json")

    def test_redacts_entire_sensitive_subtree(self) -> None:
        redacted = redact_log_details({"credentials": {"value": "leak", "expires": "soon"}})
        self.assertEqual(redacted["credentials"], "[REDACTED]")

    def test_redacts_inside_lists(self) -> None:
        details = {"auth_attempts": [{"api_key": "k1"}, {"api_key": "k2"}]}
        redacted = redact_log_details(details)
        self.assertEqual(redacted["auth_attempts"][0]["api_key"], "[REDACTED]")
        self.assertEqual(redacted["auth_attempts"][1]["api_key"], "[REDACTED]")

    def test_redacts_bearer_value_in_free_text(self) -> None:
        details = {"note": "Authorization header was Bearer abc.def.ghi"}
        redacted = redact_log_details(details)
        self.assertNotIn("abc.def.ghi", json.dumps(redacted))

    def test_redacts_bare_token_shaped_value_in_free_text(self) -> None:
        details = {"note": "pull failed with tok-supersecret-xyz123"}
        redacted = redact_log_details(details)
        text = json.dumps(redacted)
        self.assertNotIn("tok-supersecret-xyz123", text)
        self.assertIn("[REDACTED]", text)

    def test_redacts_json_style_secret_assignment_in_free_text(self) -> None:
        details = {"error": '{"access_token": "tok_abcdefghijklmnopqrstuvwxyz", "ok": true}'}
        redacted = redact_log_details(details)
        text = json.dumps(redacted)
        self.assertNotIn("tok_abcdefghijklmnopqrstuvwxyz", text)
        self.assertIn("[REDACTED]", text)

    def test_redacts_tiktok_shop_cipher_in_structured_and_free_text(self) -> None:
        details = {
            "shop_cipher": "cipher-structured-secret",
            "error": '{"shop_cipher": "cipher-inline-secret", "message": "boom"}',
        }
        redacted = redact_log_details(details)
        text = json.dumps(redacted)
        self.assertNotIn("cipher-structured-secret", text)
        self.assertNotIn("cipher-inline-secret", text)
        self.assertIn("[REDACTED]", text)

    def test_redacts_combined_tiktok_signature_header_in_free_text(self) -> None:
        details = {"error": "webhook failed TikTok-Signature: t=1,s=combined-signature-secret"}
        redacted = redact_log_details(details)
        text = json.dumps(redacted)
        self.assertNotIn("combined-signature-secret", text)
        self.assertNotIn("t=1,s=", text)
        self.assertIn("[REDACTED]", text)

    def test_redacts_json_serialized_combined_tiktok_signature_header(self) -> None:
        details = {"error": '{"TikTok-Signature": "t=1,s=json-signature-secret"}'}
        redacted = redact_log_details(details)
        text = json.dumps(redacted)
        self.assertNotIn("json-signature-secret", text)
        self.assertNotIn("s=json", text)
        self.assertIn("[REDACTED]", text)

    def test_redacts_structured_signature_fields(self) -> None:
        details = {
            "received_sig": "received-signature-secret",
            "received_signature": "received-signature-full-secret",
            "parsed_header_signature": "parsed-signature-secret",
            "candidate_digests": {"hmac(secret,body)": "candidate-digest-secret"},
        }
        redacted = redact_log_details(details)
        self.assertEqual(redacted["received_sig"], "[REDACTED]")
        self.assertEqual(redacted["received_signature"], "[REDACTED]")
        self.assertEqual(redacted["parsed_header_signature"], "[REDACTED]")
        self.assertEqual(redacted["candidate_digests"], "[REDACTED]")
        self.assertNotIn("signature-secret", json.dumps(redacted))
        self.assertNotIn("candidate-digest-secret", json.dumps(redacted))

    def test_webhook_debug_capture_redacts_signature_values(self) -> None:
        from app.routers import tiktok_orders as tiktok_orders_module

        signature = "t=1,s=webhook-signature-secret"
        debug_payload = {
            "received_signature": signature,
            "parsed_header_signature": signature,
            "parsed_header_timestamp": "1710000000",
        }
        with patch("pathlib.Path.write_text") as write_text:
            write_text(json.dumps(tiktok_orders_module.redact_log_details(debug_payload), indent=2))

        captured = write_text.call_args.args[0]
        self.assertNotIn("webhook-signature-secret", captured)
        self.assertNotIn("t=1,s=", captured)
        self.assertIn("[REDACTED]", captured)

    def test_structured_log_line_redacts_tiktok_credentials_before_stdout(self) -> None:
        from app.runtime_logging import structured_log_line

        line = structured_log_line(
            runtime="tiktok_backfill",
            action="tiktok.products.product_failed",
            success=False,
            error='failed {"shop_cipher": "cipher-inline-secret", "access_token": "tok-structured-secret"}',
            shop_id="shop-1",
            shop_cipher="cipher-structured-secret",
            headers={
                "Authorization": "Bearer auth-header-secret",
                "X-TT-Signature": "tt-signature-secret",
                "X-TikTok-Signature": "tiktok-signature-secret",
                "TikTok-Signature": "t=1,s=combined-signature-secret",
            },
        )
        payload = json.loads(line)
        text = json.dumps(payload)
        self.assertEqual(payload["shop_cipher"], "[REDACTED]")
        self.assertEqual(payload["headers"]["Authorization"], "[REDACTED]")
        self.assertEqual(payload["headers"]["X-TT-Signature"], "[REDACTED]")
        self.assertEqual(payload["headers"]["X-TikTok-Signature"], "[REDACTED]")
        self.assertEqual(payload["headers"]["TikTok-Signature"], "[REDACTED]")
        self.assertNotIn("cipher-structured-secret", text)
        self.assertNotIn("cipher-inline-secret", text)
        self.assertNotIn("tok-structured-secret", text)
        self.assertNotIn("auth-header-secret", text)
        self.assertNotIn("tt-signature-secret", text)
        self.assertNotIn("tiktok-signature-secret", text)
        self.assertNotIn("combined-signature-secret", text)
        self.assertIn("[REDACTED]", text)

    def test_authorization_timestamps_are_not_redacted_as_secrets(self) -> None:
        stamp = "2026-05-19T04:00:00+00:00"
        redacted = redact_log_details({"last_authorization_at": stamp})
        self.assertEqual(redacted["last_authorization_at"], stamp)

    def test_redacts_colon_style_secret_assignment_in_free_text(self) -> None:
        details = {"error": "api_key: sk_test_abcdefghijklmnopqrstuvwxyz failed"}
        redacted = redact_log_details(details)
        text = json.dumps(redacted)
        self.assertNotIn("sk_test_abcdefghijklmnopqrstuvwxyz", text)
        self.assertIn("[REDACTED]", text)

    def test_non_sensitive_input_unchanged(self) -> None:
        details = {"queued_count": 5, "labels": ["a", "b"], "nested": {"x": 1}}
        redacted = redact_log_details(details)
        self.assertEqual(redacted, details)

    def test_handles_none_and_empty(self) -> None:
        self.assertEqual(redact_log_details(None), {})
        self.assertEqual(redact_log_details({}), {})

    def test_preserves_nested_null_values(self) -> None:
        details = {"before": {"value": None}, "after": None, "events": [None, {"token": "tok-secret-12345"}]}
        redacted = redact_log_details(details)
        self.assertIsNone(redacted["before"]["value"])
        self.assertIsNone(redacted["after"])
        self.assertIsNone(redacted["events"][0])
        self.assertEqual(redacted["events"][1]["token"], "[REDACTED]")


class WriteOperationsLogRedactionTests(unittest.TestCase):
    """write_operations_log must redact sensitive fields before persisting."""

    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:")
        SQLModel.metadata.create_all(self.engine)
        self.session = Session(self.engine)

    def tearDown(self) -> None:
        self.session.close()

    def test_persisted_details_json_does_not_contain_secret(self) -> None:
        row = write_operations_log(
            self.session,
            event_type="auth.session.refresh",
            message="refresh",
            details={"access_token": "tok-supersecret-xyz", "user": "alice"},
        )
        # Re-fetch raw row to check what was persisted.
        self.assertNotIn("tok-supersecret-xyz", row.details_json)
        parsed = json.loads(row.details_json)
        self.assertEqual(parsed["access_token"], "[REDACTED]")
        self.assertEqual(parsed["user"], "alice")

    def test_persisted_nested_secret_redacted(self) -> None:
        row = write_operations_log(
            self.session,
            event_type="tiktok.pull",
            message="pull",
            details={
                "shop_id": "shop-1",
                "credentials": {"refresh_token": "rt-leak-me", "scope": "shop"},
            },
        )
        self.assertNotIn("rt-leak-me", row.details_json)
        parsed = json.loads(row.details_json)
        self.assertEqual(parsed["credentials"], "[REDACTED]")

    def test_persisted_hyphenated_api_key_header_redacted(self) -> None:
        row = write_operations_log(
            self.session,
            event_type="http.request",
            message="request",
            details={"headers": {"X-API-Key": "persisted-api-key-secret", "Accept": "application/json"}},
        )
        self.assertNotIn("persisted-api-key-secret", row.details_json)
        parsed = json.loads(row.details_json)
        self.assertEqual(parsed["headers"]["X-API-Key"], "[REDACTED]")
        self.assertEqual(parsed["headers"]["Accept"], "application/json")


class ParseOperationsLogDetailsRedactionTests(unittest.TestCase):
    """parse_operations_log_details must redact on read for legacy rows already in DB."""

    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:")
        SQLModel.metadata.create_all(self.engine)
        self.session = Session(self.engine)

    def tearDown(self) -> None:
        self.session.close()

    def test_legacy_row_with_token_is_redacted_on_read(self) -> None:
        # Simulate a legacy row that was persisted before redaction landed.
        row = OperationsLog(
            event_type="legacy.event",
            level="info",
            source="worker",
            message="legacy",
            details_json=json.dumps({"access_token": "legacy-tok-abc"}),
        )
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)

        details = parse_operations_log_details(row)
        self.assertEqual(details["access_token"], "[REDACTED]")

    def test_malformed_json_returns_empty(self) -> None:
        row = OperationsLog(
            event_type="legacy.bad",
            level="info",
            source="worker",
            message="bad",
            details_json="not-json-at-all",
        )
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        self.assertEqual(parse_operations_log_details(row), {})


if __name__ == "__main__":
    unittest.main()
