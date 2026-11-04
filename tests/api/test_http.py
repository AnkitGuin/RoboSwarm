from uuid import uuid4
import pytest
from fastapi.testclient import TestClient
from robostream.api.app import app, get_settings


@pytest.fixture
def client(test_settings):
    app.dependency_overrides[get_settings] = lambda: test_settings
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


def payload():
    return {"event_id": str(uuid4()), "robot_id": "robot_demo", "session_id": "session_demo",
            "timestamp": "2026-01-01T00:00:00Z", "sequence_number": 0,
            "event_type": "observation", "joint_positions": [0] * 6}


def test_health_missing_episodes_and_schema_docs(client):
    assert client.get("/health").status_code == 200
    assert client.get("/api/v1/episodes/random").status_code == 404
    assert client.get("/openapi.json").status_code == 200
    assert client.get("/api/v1/episodes", params={"limit": -1}).status_code == 422
    assert client.get("/api/v1/episodes", params={"quality_status": "made-up"}).status_code == 422


def test_batch_ingest_and_validation_errors(client):
    r = client.post("/api/v1/ingest/batch", json={"source": "test", "records": [payload()]})
    assert r.status_code == 201, r.text
    result = r.json()
    assert result["accepted"] == 1
    assert client.get(f"/api/v1/ingest/batches/{result['batch_id']}").status_code == 200
    assert client.get("/api/v1/robots").json()["total"] == 1
    assert client.post("/api/v1/ingest/batch", json={"records": "not-a-list"}).status_code == 422
    assert client.post("/api/v1/ingest/batch", json={"records": []}).status_code == 201
    assert client.get("/api/v1/pipeline/runs/no-such-run").status_code == 404


def test_oversized_api_batch(client, test_settings):
    test_settings.max_batch_size = 1
    assert client.post("/api/v1/ingest/batch", json={"records": [payload(), payload()]}).status_code == 413


def test_api_processing_and_query(client):
    pytest.importorskip("pyarrow")
    pytest.importorskip("duckdb")
    first = payload()
    second = payload() | {"sequence_number": 1, "timestamp": "2026-01-01T00:00:00.05Z"}
    result = client.post("/api/v1/ingest/batch", json={"records": [first, second]}).json()
    response = client.post("/api/v1/pipeline/runs", json={"batch_id": result["batch_id"]})
    assert response.status_code == 201, response.text
    run_id = response.json()["run_id"]
    assert client.get(f"/api/v1/pipeline/runs/{run_id}/report").status_code == 200
    episodes = client.get("/api/v1/episodes").json()
    assert episodes["total"] == 1
    ep_id = episodes["items"][0]["episode_id"]
    assert client.get(f"/api/v1/episodes/{ep_id}/telemetry").json()["total"] == 2
