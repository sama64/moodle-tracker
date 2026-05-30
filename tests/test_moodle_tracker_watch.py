import importlib.util
import json
from datetime import timedelta, timezone
from pathlib import Path


def load_watch_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "moodle_tracker_watch.py"
    spec = importlib.util.spec_from_file_location("moodle_tracker_watch", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_main_alerts_on_urgent_incomplete_deadline_without_new_change(tmp_path, capsys):
    module = load_watch_module()

    state_dir = tmp_path / "state"
    state_dir.mkdir()
    now_utc = module.datetime.now(timezone.utc)
    cursor_path = state_dir / "moodle_tracker_cursor.txt"
    cursor_path.write_text((now_utc - timedelta(days=1)).isoformat().replace("+00:00", "Z"))
    urgent_due_at = (now_utc + timedelta(hours=4)).isoformat().replace("+00:00", "Z")
    updated_at = (now_utc - timedelta(hours=1)).isoformat().replace("+00:00", "Z")

    def fake_fetch(path: str, method: str = "GET", data=None):
        if path == "/courses":
            return [{"id": 3, "display_name": "Termodinámica 0 021 B 1C 2026"}]
        if path == "/health/details":
            return {
                "details": {
                    "stale_collectors": [],
                    "source_auth_health": "healthy",
                }
            }
        if path.startswith("/changes/since?"):
            return [
                {
                    "id": 446,
                    "course_id": 3,
                    "item_type": "calendar_event",
                    "title": "CUESTIONARIO N°3 PARA RESPONDER cierra",
                    "updated_at": updated_at,
                    "meaningful_change": False,
                    "change_kind": "refresh_only",
                    "starts_at": urgent_due_at,
                    "due_at": None,
                    "primary_url": "https://moodle.example/calendar",
                }
            ]
        if path == "/deadlines/upcoming":
            return [
                {
                    "id": 98,
                    "course_id": 3,
                    "item_type": "quiz",
                    "title": "CUESTIONARIO N°3 PARA RESPONDER",
                    "due_at": urgent_due_at,
                    "completion_state": "incomplete",
                    "primary_url": "https://moodle.example/quiz/98",
                }
            ]
        raise AssertionError(f"Unexpected path: {path} method={method}")

    module.STATE_DIR = state_dir
    module.CURSOR_PATH = cursor_path
    module.DEADLINE_REMINDER_PATH = state_dir / "moodle_tracker_deadline_reminders.json"
    module.fetch = fake_fetch

    exit_code = module.main()
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["send_update"] is True
    assert any("CUESTIONARIO N°3 PARA RESPONDER" in line for line in payload["lines"])
    assert any("due soon" in line for line in payload["lines"])


def test_main_quietly_repairs_stale_collector_without_user_update(tmp_path, capsys):
    module = load_watch_module()

    state_dir = tmp_path / "state"
    state_dir.mkdir()
    cursor_path = state_dir / "moodle_tracker_cursor.txt"
    cursor_path.write_text("2026-04-20T00:00:00Z")

    health_calls = {"count": 0}

    def fake_fetch(path: str, method: str = "GET", data=None):
        if path == "/courses":
            return []
        if path == "/health/details":
            health_calls["count"] += 1
            if health_calls["count"] == 1:
                return {"details": {"stale_collectors": ["moodle_contents"], "source_auth_health": "healthy"}}
            return {"details": {"stale_collectors": [], "source_auth_health": "healthy"}}
        if path == "/deadlines/upcoming":
            return []
        if path.startswith("/changes/since?"):
            return []
        if path == "/sync/run/moodle_contents":
            assert method == "POST"
            return {"status": "completed", "stats": {}}
        raise AssertionError(f"Unexpected path: {path} method={method}")

    module.STATE_DIR = state_dir
    module.CURSOR_PATH = cursor_path
    module.DEADLINE_REMINDER_PATH = state_dir / "moodle_tracker_deadline_reminders.json"
    module.fetch = fake_fetch

    exit_code = module.main()
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["send_update"] is False
    assert payload["health_warning"] is False
    assert payload["repaired_collectors"] == ["moodle_contents"]
    assert payload["lines"] == []


def test_main_alerts_when_stale_collector_still_broken_after_retry(tmp_path, capsys):
    module = load_watch_module()

    state_dir = tmp_path / "state"
    state_dir.mkdir()
    cursor_path = state_dir / "moodle_tracker_cursor.txt"
    cursor_path.write_text("2026-04-20T00:00:00Z")

    health_calls = {"count": 0}

    def fake_fetch(path: str, method: str = "GET", data=None):
        if path == "/courses":
            return []
        if path == "/health/details":
            health_calls["count"] += 1
            return {"details": {"stale_collectors": ["moodle_contents"], "source_auth_health": "healthy"}}
        if path == "/deadlines/upcoming":
            return []
        if path.startswith("/changes/since?"):
            return []
        if path == "/sync/run/moodle_contents":
            assert method == "POST"
            return {"status": "failed", "stats": {}}
        raise AssertionError(f"Unexpected path: {path} method={method}")

    module.STATE_DIR = state_dir
    module.CURSOR_PATH = cursor_path
    module.DEADLINE_REMINDER_PATH = state_dir / "moodle_tracker_deadline_reminders.json"
    module.fetch = fake_fetch

    exit_code = module.main()
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["send_update"] is True
    assert payload["health_warning"] is True
    assert payload["repaired_collectors"] == []
    assert any("couldn't refresh automatically: moodle_contents" in line for line in payload["lines"])


def test_main_does_not_alert_when_only_non_user_blocking_file_collector_stays_stale(tmp_path, capsys):
    module = load_watch_module()

    state_dir = tmp_path / "state"
    state_dir.mkdir()
    cursor_path = state_dir / "moodle_tracker_cursor.txt"
    cursor_path.write_text("2026-04-20T00:00:00Z")

    def fake_fetch(path: str, method: str = "GET", data=None):
        if path == "/courses":
            return []
        if path == "/health/details":
            return {"details": {"stale_collectors": ["moodle_files"], "source_auth_health": "healthy"}}
        if path == "/deadlines/upcoming":
            return []
        if path.startswith("/changes/since?"):
            return []
        if path == "/sync/run/moodle_files":
            assert method == "POST"
            return {"status": "failed", "stats": {}}
        raise AssertionError(f"Unexpected path: {path} method={method}")

    module.STATE_DIR = state_dir
    module.CURSOR_PATH = cursor_path
    module.DEADLINE_REMINDER_PATH = state_dir / "moodle_tracker_deadline_reminders.json"
    module.fetch = fake_fetch

    exit_code = module.main()
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["send_update"] is False
    assert payload["health_warning"] is False
    assert payload["non_user_blocking_stale_collectors"] == ["moodle_files"]
    assert payload["lines"] == []


def test_main_suppresses_low_signal_material_and_forum_container_changes(tmp_path, capsys):
    module = load_watch_module()

    state_dir = tmp_path / "state"
    state_dir.mkdir()
    cursor_path = state_dir / "moodle_tracker_cursor.txt"
    cursor_path.write_text("2026-04-24T00:00:00Z")

    def fake_fetch(path: str, method: str = "GET", data=None):
        if path == "/courses":
            return [{"id": 3, "display_name": "Termodinámica 0 021 B 1C 2026"}]
        if path == "/health/details":
            return {"details": {"stale_collectors": [], "source_auth_health": "healthy"}}
        if path == "/deadlines/upcoming":
            return []
        if path.startswith("/changes/since?"):
            return [
                {
                    "id": 501,
                    "course_id": 3,
                    "item_type": "material_file",
                    "title": "ciclos 2° parte.pdf",
                    "updated_at": "2026-04-24T15:00:00Z",
                    "meaningful_change": True,
                    "change_kind": "content_changed",
                    "starts_at": None,
                    "due_at": None,
                    "primary_url": "https://moodle.example/resource/501",
                },
                {
                    "id": 502,
                    "course_id": 3,
                    "item_type": "forum",
                    "title": "Avisos",
                    "updated_at": "2026-04-24T15:01:00Z",
                    "meaningful_change": True,
                    "change_kind": "new",
                    "starts_at": None,
                    "due_at": None,
                    "primary_url": "https://moodle.example/forum/502",
                },
                {
                    "id": 503,
                    "course_id": 3,
                    "item_type": "material_file",
                    "title": "Clase 11/4 - Tablas de vapor",
                    "updated_at": "2026-04-24T15:02:00Z",
                    "meaningful_change": True,
                    "change_kind": "new",
                    "starts_at": None,
                    "due_at": None,
                    "primary_url": None,
                },
            ]
        raise AssertionError(f"Unexpected path: {path} method={method}")

    module.STATE_DIR = state_dir
    module.CURSOR_PATH = cursor_path
    module.DEADLINE_REMINDER_PATH = state_dir / "moodle_tracker_deadline_reminders.json"
    module.fetch = fake_fetch

    exit_code = module.main()
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["send_update"] is False
    assert payload["meaningful_count"] == 0
    assert payload["low_priority_count"] == 3
    assert payload["lines"] == []


def test_main_suppresses_non_course_beca_announcement(tmp_path, capsys):
    module = load_watch_module()

    state_dir = tmp_path / "state"
    state_dir.mkdir()
    cursor_path = state_dir / "moodle_tracker_cursor.txt"
    cursor_path.write_text("2026-04-30T21:49:53.177028Z")

    def fake_fetch(path: str, method: str = "GET", data=None):
        if path == "/courses":
            return [{"id": 1, "display_name": "Red de Estudiantes FI-UNLZ 2026"}]
        if path == "/health/details":
            return {"details": {"stale_collectors": [], "source_auth_health": "healthy"}}
        if path == "/deadlines/upcoming":
            return []
        if path.startswith("/changes/since?"):
            return [
                {
                    "id": 532,
                    "course_id": 1,
                    "item_type": "announcement",
                    "title": "Convocatoria a Becas \"Invest Your Talent in Italy\"",
                    "body_text": "Estimados/as estudiantes: Se encuentra abierta la convocatoria para becas.",
                    "updated_at": "2026-04-30T21:49:53.177028Z",
                    "meaningful_change": True,
                    "change_kind": "new",
                    "starts_at": None,
                    "due_at": None,
                    "primary_url": "https://moodle.example/forum/532",
                }
            ]
        raise AssertionError(f"Unexpected path: {path} method={method}")

    module.STATE_DIR = state_dir
    module.CURSOR_PATH = cursor_path
    module.DEADLINE_REMINDER_PATH = state_dir / "moodle_tracker_deadline_reminders.json"
    module.SCHEDULE_REMINDER_PATH = state_dir / "moodle_tracker_schedule_reminders.json"
    module.fetch = fake_fetch

    exit_code = module.main()
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["send_update"] is False
    assert payload["meaningful_count"] == 0
    assert payload["low_priority_count"] == 1
    assert payload["lines"] == []


def test_main_advances_cursor_past_equal_timestamp_results(tmp_path, capsys):
    module = load_watch_module()

    state_dir = tmp_path / "state"
    state_dir.mkdir()
    cursor_path = state_dir / "moodle_tracker_cursor.txt"
    cursor_path.write_text("2026-04-30T21:49:53.177028Z")

    def fake_fetch(path: str, method: str = "GET", data=None):
        if path == "/courses":
            return [{"id": 1, "display_name": "Red de Estudiantes FI-UNLZ 2026"}]
        if path == "/health/details":
            return {"details": {"stale_collectors": [], "source_auth_health": "healthy"}}
        if path == "/deadlines/upcoming":
            return []
        if path.startswith("/changes/since?"):
            return [
                {
                    "id": 532,
                    "course_id": 1,
                    "item_type": "announcement",
                    "title": "Convocatoria a Becas",
                    "body_text": "Beca informativa",
                    "updated_at": "2026-04-30T21:49:53.177028Z",
                    "meaningful_change": True,
                    "change_kind": "new",
                    "starts_at": None,
                    "due_at": None,
                    "primary_url": None,
                }
            ]
        raise AssertionError(f"Unexpected path: {path} method={method}")

    module.STATE_DIR = state_dir
    module.CURSOR_PATH = cursor_path
    module.DEADLINE_REMINDER_PATH = state_dir / "moodle_tracker_deadline_reminders.json"
    module.SCHEDULE_REMINDER_PATH = state_dir / "moodle_tracker_schedule_reminders.json"
    module.fetch = fake_fetch

    exit_code = module.main()
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["cursor_after"] == "2026-04-30T21:49:53.177029Z"
    assert cursor_path.read_text() == "2026-04-30T21:49:53.177029Z"


def test_main_still_alerts_on_actionable_new_assignment(tmp_path, capsys):
    module = load_watch_module()

    state_dir = tmp_path / "state"
    state_dir.mkdir()
    now_utc = module.datetime.now(timezone.utc)
    cursor_path = state_dir / "moodle_tracker_cursor.txt"
    cursor_path.write_text((now_utc - timedelta(hours=1)).isoformat().replace("+00:00", "Z"))
    due_at = (now_utc + timedelta(days=3)).isoformat().replace("+00:00", "Z")

    def fake_fetch(path: str, method: str = "GET", data=None):
        if path == "/courses":
            return [{"id": 5, "display_name": "Ciencia y Tecnología de los Materiales 0 029 TN A 1C 2026"}]
        if path == "/health/details":
            return {"details": {"stale_collectors": [], "source_auth_health": "healthy"}}
        if path == "/deadlines/upcoming":
            return []
        if path.startswith("/changes/since?"):
            return [
                {
                    "id": 476,
                    "course_id": 5,
                    "item_type": "assignment",
                    "title": "Entrega TP N°5 - tratamientos térmicos",
                    "updated_at": now_utc.isoformat().replace("+00:00", "Z"),
                    "meaningful_change": True,
                    "change_kind": "new",
                    "starts_at": None,
                    "due_at": due_at,
                    "primary_url": "https://moodle.example/assign/476",
                }
            ]
        raise AssertionError(f"Unexpected path: {path} method={method}")

    module.STATE_DIR = state_dir
    module.CURSOR_PATH = cursor_path
    module.DEADLINE_REMINDER_PATH = state_dir / "moodle_tracker_deadline_reminders.json"
    module.fetch = fake_fetch

    exit_code = module.main()
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["send_update"] is True
    assert payload["meaningful_count"] == 1
    assert any("Entrega TP N°5" in line for line in payload["lines"])


def test_main_keeps_existing_schedule_reminder_state_when_risks_temporarily_empty(tmp_path, capsys, monkeypatch):
    module = load_watch_module()

    state_dir = tmp_path / "state"
    state_dir.mkdir()
    cursor_path = state_dir / "moodle_tracker_cursor.txt"
    cursor_path.write_text("2026-04-30T21:49:53.177029Z")
    schedule_path = state_dir / "moodle_tracker_schedule_reminders.json"
    existing_key = "three_days_before|3|2026-05-02T03:00:00Z|Sist. Abiertos, Cerrados + Toberas + llenado 02/5/2026 PRIMER PARCIAL (08:30 a 10:00 hs) 7 09/5/2026 2do Ppio"
    schedule_path.write_text(json.dumps({existing_key: "2026-04-30T21:54:19Z"}))

    fixed_now = module.datetime(2026, 4, 30, 22, 0, tzinfo=timezone.utc)

    class FixedDateTime(module.datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now if tz is not None else fixed_now.replace(tzinfo=None)

    def fake_fetch(path: str, method: str = "GET", data=None):
        if path == "/courses":
            return [{"id": 3, "display_name": "Termodinámica 0 021 B 1C 2026"}]
        if path == "/health/details":
            return {"details": {"stale_collectors": [], "source_auth_health": "healthy"}}
        if path == "/deadlines/upcoming":
            return []
        if path == "/risks":
            return []
        if path.startswith("/changes/since?"):
            return []
        raise AssertionError(f"Unexpected path: {path} method={method}")

    module.STATE_DIR = state_dir
    module.CURSOR_PATH = cursor_path
    module.DEADLINE_REMINDER_PATH = state_dir / "moodle_tracker_deadline_reminders.json"
    module.SCHEDULE_REMINDER_PATH = schedule_path
    module.fetch = fake_fetch
    monkeypatch.setattr(module, "datetime", FixedDateTime)

    exit_code = module.main()
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["send_update"] is False
    assert json.loads(schedule_path.read_text()) == {existing_key: "2026-04-30T21:54:19Z"}


def test_main_alerts_on_upcoming_parcial_inside_schedule_doc(tmp_path, capsys, monkeypatch):
    module = load_watch_module()

    state_dir = tmp_path / "state"
    state_dir.mkdir()
    fixed_now = module.datetime(2026, 4, 26, 21, 0, tzinfo=timezone.utc)
    cursor_path = state_dir / "moodle_tracker_cursor.txt"
    cursor_path.write_text((fixed_now - timedelta(days=1)).isoformat().replace("+00:00", "Z"))

    class FixedDateTime(module.datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now if tz is not None else fixed_now.replace(tzinfo=None)

    def fake_fetch(path: str, method: str = "GET", data=None):
        if path == "/courses":
            return [{"id": 5, "display_name": "Ciencia y Tecnología de los Materiales"}]
        if path == "/health/details":
            return {"details": {"stale_collectors": [], "source_auth_health": "healthy"}}
        if path == "/deadlines/upcoming":
            return []
        if path.startswith("/changes/since?"):
            return []
        if path == "/risks":
            return [
                {
                    "id": 395,
                    "course_id": 5,
                    "title": "Cronograma de Ciencia de los Materiales. 1C-2026.docx",
                    "body_text": None,
                    "review_reason": "text_extraction_failed",
                    "primary_url": "https://moodle.example/resource/395",
                }
            ]
        if path == "/items/395/content":
            return {
                "artifacts": [
                    {
                        "extraction_status": "completed",
                        "extracted_text": "6 ( 28-04 ) PARCIAL 2 Templabilidad Curvas TTT Curvas CCT",
                    }
                ]
            }
        raise AssertionError(f"Unexpected path: {path} method={method}")

    module.STATE_DIR = state_dir
    module.CURSOR_PATH = cursor_path
    module.DEADLINE_REMINDER_PATH = state_dir / "moodle_tracker_deadline_reminders.json"
    module.SCHEDULE_REMINDER_PATH = state_dir / "moodle_tracker_schedule_reminders.json"
    module.fetch = fake_fetch
    monkeypatch.setattr(module, "datetime", FixedDateTime)

    exit_code = module.main()
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["send_update"] is True
    assert payload["urgent_schedule_count"] == 1
    assert any("PARCIAL 2" in line and "28/04" in line for line in payload["lines"])


def test_exam_reminder_policy_starts_at_three_days_not_seven():
    module = load_watch_module()

    now = module.datetime(2026, 5, 26, 12, 0, tzinfo=timezone.utc)
    seven_days_out = {
        "at": module.datetime(2026, 6, 2, 12, 0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    three_days_out = {
        "at": module.datetime(2026, 5, 29, 12, 0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
    }

    assert module.exam_reminder_bucket(seven_days_out, now) is None
    assert module.exam_reminder_bucket(three_days_out, now) == "three_days_before"


def test_date_only_exam_still_alerts_on_same_local_day():
    module = load_watch_module()

    now = module.datetime(2026, 6, 2, 18, 0, tzinfo=timezone.utc)
    date_only_event = {
        "at": module.datetime(2026, 6, 2, 3, 0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
        "all_day": True,
    }

    assert module.exam_reminder_bucket(date_only_event, now) == "same_day"
