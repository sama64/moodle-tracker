from fastapi.testclient import TestClient

from uni_tracker.main import app


def test_collectors_endpoint() -> None:
    client = TestClient(app)
    response = client.get("/sync/collectors")
    assert response.status_code == 200
    payload = response.json()
    assert "moodle_courses" in payload
    assert "moodle_files" in payload
