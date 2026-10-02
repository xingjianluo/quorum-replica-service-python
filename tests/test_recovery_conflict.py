"""副本/协调者重启后的重放恢复，以及确定性冲突解决。"""
from __future__ import annotations

from fastapi.testclient import TestClient

from qrs.models import CommitCode


def test_replica_restart_replays_without_duplicate_effect(client: TestClient) -> None:
    client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-1", "timestamp": 1,
    })
    # 重启每个副本：仅靠本地 WAL 恢复
    for rid in ("replica-0", "replica-1", "replica-2"):
        info = client.post(f"/cluster/restart/{rid}").json()
        assert info["keys"] == 1
    read = client.get("/keys/k1").json()
    assert read["visible"] is True and read["value"] == "v1"

    # 重放后再次收到重复写入，仍被去重，不产生第二条生效
    cluster = client.app.state.cluster
    for rep in cluster.replicas:
        assert rep.apply_write("k1", "v1", "req-1", 1, False) is None
    assert client.get("/keys/k1").json()["value"] == "v1"


def test_coordinator_restart_preserves_idem_and_conflicts(client: TestClient) -> None:
    client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-1", "timestamp": 1,
    })
    client.post("/writes", json={
        "idem_key": "ik-2", "key": "k1", "value": "v2",
        "request_id": "req-2", "timestamp": 2,
    })
    client.post("/cluster/restart-coordinator")

    # 幂等表恢复：首次结果与拒绝语义都保持
    got = client.get("/writes/ik-1").json()
    assert got["code"] == CommitCode.COMMITTED.value and got["value"] == "v1"
    replay = client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-x", "timestamp": 1,
    })
    assert replay.json()["code"] == CommitCode.REPLAYED.value
    mismatch = client.post("/writes", json={
        "idem_key": "ik-2", "key": "k1", "value": "OTHER",
        "request_id": "req-y", "timestamp": 2,
    })
    assert mismatch.status_code == 409

    # 冲突历史可从副本 WAL 重建
    conflicts = client.get("/conflicts", params={"key": "k1"}).json()["conflicts"]
    assert len(conflicts) == 3
    assert all(c["winner"]["request_id"] == "req-2" for c in conflicts)
    assert {c["loser"]["request_id"] for c in conflicts} == {"req-1"}
    assert "LWW" in conflicts[0]["rule"]


def test_conflict_resolved_by_timestamp_then_request_id(client: TestClient) -> None:
    cluster = client.app.state.cluster
    coord = cluster.coordinator
    # 后到的旧时间戳写入必须落败
    coord.commit("ik-new", "k1", "new", "req-new", timestamp=10)
    coord.commit("ik-old", "k1", "old", "req-old", timestamp=5)
    read = client.get("/keys/k1").json()
    assert read["value"] == "new"
    conflicts = client.get("/conflicts", params={"key": "k1"}).json()["conflicts"]
    assert all(c["winner"]["request_id"] == "req-new" for c in conflicts)
    assert all(c["loser"]["request_id"] == "req-old" for c in conflicts)

    # 时间戳相同：request_id 字典序较大者胜（确定性 tie-break）
    coord.commit("ik-a", "k2", "a-val", "req-a", timestamp=7)
    coord.commit("ik-b", "k2", "b-val", "req-b", timestamp=7)
    assert client.get("/keys/k2").json()["value"] == "b-val"
    k2conf = client.get("/conflicts", params={"key": "k2"}).json()["conflicts"]
    assert all(c["winner"]["request_id"] == "req-b" for c in k2conf)
    assert all(c["loser"]["request_id"] == "req-a" for c in k2conf)


def test_conflict_queryable_after_full_restart(client: TestClient) -> None:
    client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-1", "timestamp": 1,
    })
    client.post("/writes", json={
        "idem_key": "ik-2", "key": "k1", "value": "v2",
        "request_id": "req-2", "timestamp": 2,
    })
    # 全部副本 + 协调者重启
    for rid in ("replica-0", "replica-1", "replica-2"):
        client.post(f"/cluster/restart/{rid}")
    client.post("/cluster/restart-coordinator")
    conflicts = client.get("/conflicts", params={"key": "k1"}).json()
    assert conflicts["count"] == 3
    # 败方内容仍可查、规则可解释
    for c in conflicts["conflicts"]:
        assert c["loser"]["request_id"] == "req-1"
        assert c["winner"]["request_id"] == "req-2"
        assert c["detail"]
    assert client.get("/keys/k1").json()["value"] == "v2"


def test_minority_residue_not_visible_after_restart(client: TestClient) -> None:
    """未达多数派的写入在条件未恢复时仍是少数派残留，不可见；恢复后自动收敛。"""
    # replica-1 宕机、replica-2 存活但写失败：仅 replica-0 确认 -> 504
    client.post("/cluster/fail/replica-1")
    client.post("/cluster/fault/replica-2")
    client.post("/writes", json={
        "idem_key": "ik-fail", "key": "k1", "value": "minority",
        "request_id": "req-fail", "timestamp": 1,
    })
    # 仅重启协调者（故障条件仍在）：残留依然存在但不应被读为可见，
    # 状态查询明确为 WAITING，并指出卡在存活/确认副本不足。
    client.post("/cluster/restart-coordinator")
    read = client.get("/keys/k1").json()
    assert read["visible"] is False
    assert read["committed_replicas"] == []
    status = client.get("/writes/ik-fail/status").json()
    assert status["status"] == "WAITING"
    assert status["readable"] is False
    assert status["acked_replicas"] == []
    assert "replica-1" in status["pending_replicas"]

    # 副本恢复（宕机者重启、故障解除）：无需手工 resume，自动对账补齐并收敛
    client.post("/cluster/restart/replica-1")
    client.post("/cluster/clear-fault/replica-2")
    assert client.get("/writes/ik-fail/status").json()["status"] == "SUCCESS"
    assert client.get("/keys/k1").json()["visible"] is True
    # 全部副本再重启一遍，最终状态仍是同一已提交值，不重复生效
    for rid in ("replica-0", "replica-1", "replica-2"):
        client.post(f"/cluster/restart/{rid}")
    assert client.get("/keys/k1").json()["visible"] is True
    assert client.get("/keys/k1").json()["value"] == "minority"


def test_concurrent_conflicting_writes_resolve_deterministically(client: TestClient) -> None:
    """同一数据键的并发冲突写入按 LWW 全序确定解决，双方都不被静默丢弃。"""
    from concurrent.futures import ThreadPoolExecutor

    cluster = client.app.state.cluster

    # 两路并发使用不同 idem_key 写同一数据键，时间戳相同 -> request_id 定胜负
    def write(rid: str):
        return cluster.coordinator.commit(
            idem_key=f"ik-{rid}", key="hot", value=f"val-{rid}",
            request_id=rid, timestamp=5,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        outs = list(pool.map(write, ["req-aaa", "req-bbb"]))

    assert all(o.code is CommitCode.COMMITTED for o in outs)
    read = client.get("/keys/hot").json()
    # 字典序更大的 req-bbb 确定获胜
    assert read["request_id"] == "req-bbb"
    assert read["value"] == "val-req-bbb"
    assert read["visible"] is True
    # 败方可在冲突历史中查到（每副本各一条），未被静默丢弃
    conflicts = client.get("/conflicts", params={"key": "hot"}).json()["conflicts"]
    assert len(conflicts) == 3
    assert all(c["winner"]["request_id"] == "req-bbb" for c in conflicts)
    assert all(c["loser"]["request_id"] == "req-aaa" for c in conflicts)
    # 两个幂等键各自仍可查到首次结果（互不覆盖对方的幂等结论）
    assert client.get("/writes/ik-req-aaa").json()["code"] == "COMMITTED"
    assert client.get("/writes/ik-req-bbb").json()["value"] == "val-req-bbb"


def test_resume_after_replica_restart_converges(client: TestClient) -> None:
    """写入时一个副本宕机且另两个写失败；重启/恢复后补复制收敛。"""
    cluster = client.app.state.cluster
    cluster.fail_replica("replica-2")
    cluster.inject_write_fault("replica-1")
    r = client.post("/writes", json={
        "idem_key": "ik-gap", "key": "k9", "value": "v9",
        "request_id": "req-gap", "timestamp": 9,
    })
    assert r.json()["code"] == CommitCode.QUORUM_FAILED.value

    # 宕机副本重启（其 WAL 上没有该写入），故障副本恢复
    client.post("/cluster/restart/replica-2")
    client.post("/cluster/clear-fault/replica-1")
    resumed = client.post("/writes/ik-gap/resume")
    assert resumed.json()["code"] == CommitCode.COMMITTED.value
    # 全部副本（含重启者）最终收敛到相同的已提交状态
    for rep in cluster.replicas:
        st = rep.get("k9")
        assert st is not None and st.value == "v9" and st.committed is True
