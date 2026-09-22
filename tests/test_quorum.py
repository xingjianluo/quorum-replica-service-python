"""多数派确认：部分失败可区分、未达多数派不暴露成功、补复制收敛。"""
from __future__ import annotations

from fastapi.testclient import TestClient

from qrs.models import CommitCode


def test_quorum_requires_majority_acks(client: TestClient) -> None:
    body = client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-1", "timestamp": 1,
    }).json()
    assert body["quorum"] == 2
    assert len(body["acked_replicas"]) >= 2


def test_two_write_faults_returns_quorum_failed(client: TestClient) -> None:
    client.post("/cluster/fault/replica-1")
    client.post("/cluster/fault/replica-2")
    r = client.post("/writes", json={
        "idem_key": "ik-fail", "key": "k1", "value": "v1",
        "request_id": "req-fail", "timestamp": 1,
    })
    # 3 副本都存活，但仅 1 个能确认 -> 504 QUORUM_FAILED
    assert r.status_code == 504
    body = r.json()
    assert body["code"] == CommitCode.QUORUM_FAILED.value
    assert body["acked_replicas"] == ["replica-0"]
    assert set(body["pending_replicas"]) == {"replica-1", "replica-2"}
    assert "未达多数派" in body["reason"]
    # 未达多数派的写入不得读为可见
    read = client.get("/keys/k1").json()
    assert read["visible"] is False
    assert read["committed_replicas"] == []


def test_not_enough_alive_is_distinguishable(client: TestClient) -> None:
    client.post("/cluster/fail/replica-1")
    client.post("/cluster/fail/replica-2")
    r = client.post("/writes", json={
        "idem_key": "ik-short", "key": "k1", "value": "v1",
        "request_id": "req-short", "timestamp": 1,
    })
    # 存活副本数本身不足多数派 -> 503，与 QUORUM_FAILED 明确区分
    assert r.status_code == 503
    body = r.json()
    assert body["code"] == CommitCode.NOT_ENOUGH_REPLICAS.value
    assert "存活副本" in body["reason"]


def test_quorum_failure_survives_coordinator_restart(client: TestClient) -> None:
    client.post("/cluster/fault/replica-1")
    client.post("/cluster/fault/replica-2")
    client.post("/writes", json={
        "idem_key": "ik-fail", "key": "k1", "value": "v1",
        "request_id": "req-fail", "timestamp": 1,
    })
    client.post("/cluster/restart-coordinator")
    body = client.get("/writes/ik-fail").json()
    assert body["code"] == CommitCode.QUORUM_FAILED.value
    assert body["acked_replicas"] == ["replica-0"]


def test_resume_drives_convergence_after_recovery(client: TestClient) -> None:
    client.post("/cluster/fault/replica-1")
    client.post("/cluster/fault/replica-2")
    first = client.post("/writes", json={
        "idem_key": "ik-fail", "key": "k1", "value": "v1",
        "request_id": "req-fail", "timestamp": 1,
    }).json()
    assert first["code"] == CommitCode.QUORUM_FAILED.value

    # 恢复一个副本后补复制：2/3 达多数派
    client.post("/cluster/clear-fault/replica-1")
    resumed = client.post("/writes/ik-fail/resume")
    assert resumed.status_code == 200
    body = resumed.json()
    assert body["code"] == CommitCode.COMMITTED.value
    assert set(body["acked_replicas"]) >= {"replica-0", "replica-1"}
    # 读取随之可见，且查询入口结论一致
    assert client.get("/keys/k1").json()["visible"] is True
    assert client.get("/writes/ik-fail").json()["code"] == CommitCode.COMMITTED.value


def test_redundant_replay_converges(client: TestClient) -> None:
    """落后/重复投递的复制数据在重放后必须收敛到一致状态。"""
    cluster = client.app.state.cluster
    coord = cluster.coordinator
    # 先提交
    coord.commit("ik-1", "k1", "v1", "req-1", timestamp=1)
    # 模拟重复/落后投递：旧写入 ts=0 重放到每个副本
    for rep in cluster.replicas:
        conflict = rep.apply_write("k1", "stale", "req-stale", 0, False)
        assert conflict is not None
        coord.conflicts.append(conflict)
        # 同一请求重复投递为 no-op
        assert rep.apply_write("k1", "v1", "req-1", 1, False) is None
    # 状态收敛：全部副本仍是 req-1/v1 且已提交
    for rep in cluster.replicas:
        st = rep.get("k1")
        assert st.request_id == "req-1" and st.value == "v1" and st.committed
    read = client.get("/keys/k1").json()
    assert read["visible"] is True and read["value"] == "v1"
    losers = [c for c in coord.get_conflicts("k1") if c.loser_request_id == "req-stale"]
    assert len(losers) == 3  # 败方未被静默丢弃


def test_single_replica_failure_still_commits(client: TestClient) -> None:
    client.post("/cluster/fail/replica-2")
    body = client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-1", "timestamp": 1,
    }).json()
    assert body["code"] == CommitCode.COMMITTED.value
    assert set(body["acked_replicas"]) == {"replica-0", "replica-1"}


def test_duplicate_submit_of_failed_write_not_masked_as_success(client: TestClient) -> None:
    """未达多数派的写入，重复提交必须重放失败结论，不得暴露为成功。"""
    client.post("/cluster/fault/replica-1")
    client.post("/cluster/fault/replica-2")
    payload = {
        "idem_key": "ik-fail", "key": "k1", "value": "v1",
        "request_id": "req-first", "timestamp": 1,
    }
    first = client.post("/writes", json=payload)
    assert first.status_code == 504
    # 完全相同的重试（即使换 request_id）：仍 504 且 replay 标记
    retry = client.post("/writes", json={**payload, "request_id": "req-retry"})
    assert retry.status_code == 504
    body = retry.json()
    assert body["code"] == CommitCode.QUORUM_FAILED.value
    assert body["replay"] is True
    assert body["request_id"] == "req-first"  # 指向首次判定
    assert client.get("/keys/k1").json()["visible"] is False

    # 存活不足的失败结论重放时同样保留 503
    client.post("/cluster/clear-fault/replica-1")
    client.post("/cluster/clear-fault/replica-2")
    client.post("/cluster/fail/replica-1")
    client.post("/cluster/fail/replica-2")
    p2 = {
        "idem_key": "ik-short", "key": "k2", "value": "v2",
        "request_id": "req-s1", "timestamp": 2,
    }
    assert client.post("/writes", json=p2).status_code == 503
    assert client.post("/writes", json=p2).status_code == 503
