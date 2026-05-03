from __future__ import annotations

from uni_tracker import worker


def test_run_collector_job_logs_failure_without_raising(monkeypatch, caplog) -> None:
    calls: list[str] = []

    def fake_run_collector(name: str) -> dict:
        calls.append(name)
        raise RuntimeError("network is unreachable")

    monkeypatch.setattr(worker, "run_collector", fake_run_collector)

    result = worker._run_collector_job("moodle_courses")

    assert result == {"status": "failed", "error": "network is unreachable"}
    assert calls == ["moodle_courses"]
    assert "Collector moodle_courses failed" in caplog.text


def test_run_collectors_job_continues_after_collector_failure(monkeypatch) -> None:
    calls: list[str] = []

    def fake_run_collector_job(name: str) -> dict:
        calls.append(name)
        if name == "moodle_contents":
            return {"status": "failed", "error": "boom"}
        return {"status": "completed"}

    monkeypatch.setattr(worker, "_run_collector_job", fake_run_collector_job)
    monkeypatch.setattr(worker, "_post_collection_job", lambda: None)
    monkeypatch.setattr(worker, "_enrichment_job", lambda: None)

    results = worker._run_collectors_job()

    assert calls == [
        "moodle_courses",
        "moodle_contents",
        "moodle_updates",
        "moodle_forums",
        "moodle_assignments",
        "moodle_grades",
        "moodle_calendar",
        "moodle_files",
    ]
    assert results["moodle_contents"] == {"status": "failed", "error": "boom"}
    assert results["moodle_files"] == {"status": "completed"}
