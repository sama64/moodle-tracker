import importlib.util
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


def load_horizon_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "moodle_academic_horizon.py"
    spec = importlib.util.spec_from_file_location("moodle_academic_horizon", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_horizon_uses_item_content_extracted_text_for_schedule_doc_parciales(monkeypatch, capsys):
    module = load_horizon_module()
    fixed_now = datetime(2026, 4, 27, 0, 0, tzinfo=timezone.utc)

    def fake_fetch(path: str):
        if path == "/health/details":
            return {"details": {"stale_collectors": [], "source_auth_health": "healthy"}}
        if path == "/courses":
            return [{"id": 5, "display_name": "Ciencia y Tecnología de los Materiales"}]
        if path == "/deadlines/upcoming":
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
        raise AssertionError(f"Unexpected path: {path}")

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now if tz is not None else fixed_now.replace(tzinfo=None)

    monkeypatch.setattr(module, "fetch", fake_fetch)
    monkeypatch.setattr(module, "datetime", FixedDateTime)

    exit_code = module.main()
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["send_update"] is True
    assert payload["schedule_event_count"] == 1
    assert any("PARCIAL 2" in line and "28/04" in line for line in payload["lines"])
