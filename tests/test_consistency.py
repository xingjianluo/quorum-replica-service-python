"""跨入口结论一致性与日志要素检查。"""
from __future__ import annotations

import logging

from fastapi.testclient import TestClient

from qrs.models import CommitCode


def test_http_and_coordinator_entrypoints_agree(client: TestClient, caplog) -> None:
    caplog.set_level(logging.INFO)
    # HTTP 入口
    http_res = client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-log-1", "timestamp": 1,
    })
    assert http_res.json()["code"] == CommitCode.COMMITTED.value

    # 查询入口（按幂等键 / 按数据键）结论一致
    by_idem = client.get("/writes/ik-1").json()
    by_key = client.get("/keys/k1").json()
    assert by_idem["value"] == "v1"
    assert by_key["visible"] is True and by_key["value"] == "v1"
    assert by_key["committed_replicas"] == by_idem["acked_replicas"]


def test_logs_contain_request_id_acks_and_basis(client: TestClient, capsys) -> None:
    client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-trace", "timestamp": 1,
    })
    captured = capsys.readouterr().out
    # 请求标识
    assert "req-trace" in captured
    # 确认副本
    assert "replica-0" in captured and "ACK" in captured
    # 判定结论与依据
    assert "verdict=COMMITTED" in captured
    assert "basis=" in captured and "多数派" in captured


def test_failure_logs_distinguish_reasons(client: TestClient, capsys) -> None:
    # QUORUM_FAILED：副本存活但写失败
    client.post("/cluster/fault/replica-1")
    client.post("/cluster/fault/replica-2")
    client.post("/writes", json={
        "idem_key": "ik-f", "key": "k1", "value": "v1",
        "request_id": "req-f", "timestamp": 1,
    })
    out1 = capsys.readouterr().out
    assert "verdict=QUORUM_FAILED" in out1

    # NOT_ENOUGH_REPLICAS：存活数不足
    client.post("/cluster/clear-fault/replica-1")
    client.post("/cluster/clear-fault/replica-2")
    client.post("/cluster/fail/replica-1")
    client.post("/cluster/fail/replica-2")
    client.post("/writes", json={
        "idem_key": "ik-n", "key": "k2", "value": "v2",
        "request_id": "req-n", "timestamp": 2,
    })
    out2 = capsys.readouterr().out
    assert "verdict=NOT_ENOUGH_REPLICAS" in out2
