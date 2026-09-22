"""骨架冒烟：验证装配与健康检查。"""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.api import create_app


def test_service_assembles(service):
    assert service.cluster.quorum_for(5) == 3
    assert len(service.cluster.all_replicas()) == 5


def test_health_api(service):
    client = TestClient(create_app(service))
    resp = client.get("/health")
    assert resp.status_code == 200
