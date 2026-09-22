"""不同入口（直接调用 / HTTP）下结论必须一致，失败原因可区分。"""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.api import create_app


def test_http_and_direct_agree_on_commit_and_duplicate(service):
    client = TestClient(create_app(service))

    body = {
        "idempotency_key": "k1",
        "data_key": "doc-1",
        "payload": "hello",
        "timestamp_ns": 1000,
        "request_id": "req-1",
    }
    resp = client.post("/writes", json=body)
    assert resp.status_code == 200
    http_result = resp.json()

    direct = service.submit("k1", "doc-1", "hello", 1000, "req-1")
    assert direct.status.value == http_result["status"] == "COMMITTED"
    assert direct.request_id == http_result["request_id"] == "req-1"
    assert direct.payload == http_result["payload"] == "hello"
    assert direct.acked_replicas == http_result["acked_replicas"]

    # HTTP 查询同一结论
    q = client.get("/writes/k1").json()
    assert q["status"] == "COMMITTED" and q["request_id"] == "req-1"
    d = client.get("/data/doc-1").json()
    assert d["payload"] == "hello"


def test_http_rejection_matches_direct(service):
    client = TestClient(create_app(service))
    service.submit("k2", "doc-2", "first", 1000, "req-1")

    resp = client.post(
        "/writes",
        json={
            "idempotency_key": "k2",
            "data_key": "doc-2",
            "payload": "different",
            "timestamp_ns": 1001,
            "request_id": "req-2",
        },
    )
    assert resp.status_code == 409
    body = resp.json()
    assert body["status"] == "REJECTED_CONTENT_CONFLICT"
    assert "different payload" in body["reason"]


def test_http_failure_statuses_are_distinguishable(service):
    client = TestClient(create_app(service))

    # 不足：让 3 个副本宕机
    for rid in ["r2", "r3", "r4"]:
        client.post(f"/cluster/replicas/{rid}/alive", json={"alive": False})
    resp = client.post(
        "/writes",
        json={"idempotency_key": "k3", "data_key": "d3", "payload": "v",
              "timestamp_ns": 1, "request_id": "req-3"},
    )
    assert resp.status_code == 503
    assert resp.json()["status"] == "INSUFFICIENT_REPLICAS"

    for rid in ["r2", "r3", "r4"]:
        client.post(f"/cluster/replicas/{rid}/alive", json={"alive": True})
        client.post(f"/cluster/replicas/{rid}/faults", json={"ops": ["prepare"]})
    resp2 = client.post(
        "/writes",
        json={"idempotency_key": "k4", "data_key": "d4", "payload": "v",
              "timestamp_ns": 1, "request_id": "req-4"},
    )
    assert resp2.status_code == 503
    assert resp2.json()["status"] == "FAILED_QUORUM"
    # 两种 503 在响应体中可明确区分
    assert resp.json()["reason"] != resp2.json()["reason"]


def test_http_restart_and_conflict_query(service):
    client = TestClient(create_app(service))
    client.post(
        "/writes",
        json={"idempotency_key": "ka", "data_key": "doc", "payload": "A",
              "timestamp_ns": 2000, "request_id": "req-a"},
    )
    client.post(
        "/writes",
        json={"idempotency_key": "kb", "data_key": "doc", "payload": "B",
              "timestamp_ns": 1000, "request_id": "req-b"},
    )

    client.post("/cluster/restart-all")
    resp = client.get("/data/doc")
    assert resp.status_code == 200
    assert resp.json()["request_id"] == "req-a"

    conflicts = client.get("/conflicts", params={"data_key": "doc"}).json()
    assert conflicts["count"] == 1
    assert conflicts["conflicts"][0]["winner_request_id"] == "req-a"
    assert conflicts["conflicts"][0]["loser_request_id"] == "req-b"
