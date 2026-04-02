import json
import shutil
import unittest
import uuid
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from sqlmodel import Session, SQLModel, create_engine

import app.main as main_module
from app.runtime_monitor import get_runtime_heartbeat_status
from app.models import RuntimeHeartbeat


class RuntimeSplitStatusTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_runtime_split_status" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "runtime_split_status.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_status_snapshot_uses_distinct_web_and_worker_heartbeats(self) -> None:
        with Session(self.engine) as session:
            session.add(
                RuntimeHeartbeat(
                    runtime_name="local_web_app",
                    host_name="web-host",
                    status="running",
                    details_json=json.dumps(
                        {
                            "service_mode": "web-app",
                            "discord_ingest_enabled": False,
                            "parser_worker_enabled": False,
                        }
                    ),
                )
            )
            session.add(
                RuntimeHeartbeat(
                    runtime_name="local_worker",
                    host_name="worker-host",
                    status="running",
                    details_json=json.dumps(
                        {
                            "service_mode": "worker-host",
                            "discord_status": "ready",
                            "discord_ingest_enabled": True,
                            "parser_worker_enabled": True,
                        }
                    ),
                )
            )
            session.commit()

            with patch.object(main_module, "APP_HEARTBEAT_RUNTIME_NAME", "local_web_app"), patch.object(
                main_module, "WORKER_RUNTIME_NAME", "local_worker"
            ):
                snapshot = main_module.build_status_snapshot(session)

        self.assertEqual(snapshot["app_runtime"]["host_name"], "web-host")
        self.assertEqual(snapshot["worker_runtime"]["host_name"], "worker-host")
        self.assertEqual(snapshot["app_runtime"]["label"], "Running")
        self.assertEqual(snapshot["worker_runtime"]["label"], "Running")
        self.assertEqual(snapshot["worker_runtime"]["details"].get("discord_status"), "ready")

    def test_status_snapshot_marks_recent_sqlite_contention_as_attention_needed(self) -> None:
        with Session(self.engine) as session:
            with patch.object(main_module, "recent_db_failure", return_value=True):
                snapshot = main_module.build_status_snapshot(session)

        self.assertFalse(snapshot["db_ok"])
        self.assertEqual(snapshot["db_health"]["label"], "Busy")
        self.assertTrue(
            any("SQLite recently reported write contention" in message for message in snapshot["alert_messages"])
        )

    def test_runtime_heartbeat_marks_degraded_status_as_attention_needed(self) -> None:
        with Session(self.engine) as session:
            session.add(
                RuntimeHeartbeat(
                    runtime_name="local_worker",
                    host_name="worker-host",
                    status="degraded",
                    details_json=json.dumps({"discord_status": "degraded"}),
                )
            )
            session.commit()

            heartbeat = get_runtime_heartbeat_status(
                session,
                "local_worker",
                runtime_label="Ingest Worker",
                updated_at_formatter=lambda value: value.isoformat() if value else "never",
            )

        self.assertEqual(heartbeat["label"], "Degraded")
        self.assertTrue(heartbeat["is_running"])
        self.assertTrue(heartbeat["needs_attention"])
        self.assertIn("degraded state", heartbeat["alert_message"])

    def test_health_endpoint_surfaces_db_and_runtime_status(self) -> None:
        @contextmanager
        def fake_managed_session():
            with Session(self.engine) as session:
                yield session

        with patch.object(main_module, "managed_session", fake_managed_session), patch.object(
            main_module,
            "get_database_health",
            return_value={
                "ok": False,
                "needs_attention": True,
                "label": "Busy",
                "checked_at_label": "just now",
            },
        ), patch.object(
            main_module,
            "get_runtime_heartbeat_status",
            return_value={
                "status": "degraded",
                "label": "Degraded",
                "needs_attention": True,
                "updated_at": None,
                "updated_at_label": "1 minute ago",
            },
        ):
            health = main_module.health()

        self.assertFalse(health.ok)
        self.assertFalse(health.db_ok)
        self.assertEqual(health.local_runtime_status, "degraded")
        self.assertEqual(health.local_runtime_label, "Degraded")
        self.assertTrue(health.local_runtime_needs_attention)


if __name__ == "__main__":
    unittest.main()
