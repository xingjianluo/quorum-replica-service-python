"""自动对账：落后副本补齐、重复投递幂等、未达多数派自动收敛、并发冲突收敛。"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from qrs.api import create_app
from qrs.cluster import Cluster
from qrs.models import CommitCode


@pytest.fixture()
def auto_cluster(tmp_path):
    """开启自动对账的集群：副本恢复时自动触发一轮对账。"""
    cl = Cluster(base_dir=str(tmp_path / "auto"), n=3, auto_reconcile=True)
    yield cl
    cl.cleanup()


@pytest.fixture()
def auto_client(auto_cluster):
    app = create_app(auto_cluster)
    with TestClient(app) as c:
        yield c


def test_reconcile_repairs_lagging_replica(client: TestClient) -> None:
    """副本宕机错过写入，回来后经对账补齐到相同的已提交状态。"""
    cluster = client.app.state.cluster
    cluster.fail_replica("replica-2")
    body = client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-1", "timestamp": 1,
    }).json()
    assert body["code"] == CommitCode.COMMITTED.value
    assert cluster.replicas[2].data.get("k1") is None  # 落后副本没有该写入

    cluster.restart_replica("replica-2")  # 副本恢复（WAL 上无此写入）
    report = cluster.reconcile()
    assert "replica-2" in report.repaired_replicas
    assert report.converged is True
    st = cluster.replicas[2].get("k1")
    assert st.value == "v1" and st.request_id == "req-1" and st.committed
    # 各副本已提交状态一致
    for rep in cluster.replicas:
        s = rep.get("k1")
        assert (s.value, s.request_id, s.committed) == ("v1", "req-1", True)


def test_reconcile_duplicate_delivery_is_idempotent(client: TestClient) -> None:
    """对账补投 + 重复投递同一请求：不重复生效、不盖掉已确认写入。"""
    cluster = client.app.state.cluster
    cluster.fail_replica("replica-2")
    client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-1", "timestamp": 1,
    })
    cluster.restart_replica("replica-2")
    lagging = cluster.replicas[2]

    r1 = cluster.reconcile()
    assert "replica-2" in r1.repaired_replicas
    wal_len_after_first = len(lagging.log.read_all())

    # 第二轮对账：已是最新已提交状态，不再补投（幂等）
    r2 = cluster.reconcile()
    assert "replica-2" not in r2.repaired_replicas
    assert r2.converged is True
    assert len(lagging.log.read_all()) == wal_len_after_first

    # 滞后副本上同一 request_id 的重复投递是 no-op
    assert lagging.apply_write("k1", "v1", "req-1", 1, False) is None
    st = lagging.get("k1")
    assert (st.value, st.request_id, st.committed) == ("v1", "req-1", True)
    # 已确认写入不被旧写入覆盖：更旧的写入落败且状态不变
    conflict = lagging.apply_write("k1", "stale", "req-0", 0, False)
    assert conflict is not None and conflict.winner_request_id == "req-1"
    assert lagging.get("k1").value == "v1"


def test_pending_write_converges_via_reconcile(client: TestClient) -> None:
    """未达多数派的写入：状态可查（pending/卡点/确认数），条件恢复后对账自动收敛。"""
    client.post("/cluster/fault/replica-1")
    client.post("/cluster/fault/replica-2")
    r = client.post("/writes", json={
        "idem_key": "ik-p", "key": "kp", "value": "vp",
        "request_id": "req-p", "timestamp": 1,
    })
    assert r.status_code == 504

    # 状态查询：仍在等待、卡在确认不足、只有 1 个副本持有
    st = client.get("/writes/ik-p/status").json()
    assert st["phase"] == "pending"
    assert st["stuck_at"] == "wait_acks"
    assert st["last_verdict"] == CommitCode.QUORUM_FAILED.value
    assert st["held_by"] == ["replica-0"]
    assert st["committed_by"] == []
    assert st["quorum"] == 2
    # 未确认内容读取入口不可见
    assert client.get("/keys/kp").json()["visible"] is False

    # 条件恢复后对账自动推进到最终一致
    client.post("/cluster/clear-fault/replica-1")
    client.post("/cluster/clear-fault/replica-2")
    report = client.post("/cluster/reconcile").json()
    assert "req-p" in report["retried_requests"]
    assert report["converged"] is True

    st2 = client.get("/writes/ik-p/status").json()
    assert st2["phase"] == "succeeded"
    assert st2["stuck_at"] is None
    assert len(st2["committed_by"]) >= 2
    assert client.get("/keys/kp").json()["visible"] is True
    # 查询入口结论同步升级为 COMMITTED
    assert client.get("/writes/ik-p").json()["code"] == CommitCode.COMMITTED.value


def test_status_distinguishes_wait_replicas(client: TestClient) -> None:
    """存活不足时卡在 wait_replicas，与确认不足可区分。"""
    client.post("/cluster/fail/replica-1")
    client.post("/cluster/fail/replica-2")
    r = client.post("/writes", json={
        "idem_key": "ik-s", "key": "ks", "value": "vs",
        "request_id": "req-s", "timestamp": 1,
    })
    assert r.status_code == 503
    st = client.get("/writes/ik-s/status").json()
    assert st["phase"] == "pending"
    assert st["stuck_at"] == "wait_replicas"
    assert st["held_by"] == []  # 未做任何复制
    assert set(st["unreachable_replicas"]) == {"replica-1", "replica-2"}


def test_auto_reconcile_on_replica_recovery(auto_client: TestClient) -> None:
    """开启 auto_reconcile 后，副本恢复即自动补齐，无需人工调用对账。"""
    cluster = auto_client.app.state.cluster
    cluster.fail_replica("replica-2")
    auto_client.post("/writes", json={
        "idem_key": "ik-a", "key": "ka", "value": "va",
        "request_id": "req-a", "timestamp": 1,
    })
    assert cluster.replicas[2].data.get("ka") is None
    # 副本重启回来：自动对账把它补齐
    cluster.restart_replica("replica-2")
    st = cluster.replicas[2].get("ka")
    assert st is not None and st.value == "va" and st.committed
    assert auto_client.get("/keys/ka").json()["visible"] is True


def test_auto_reconcile_converges_pending_write(auto_client: TestClient) -> None:
    """开启 auto_reconcile 后，故障解除即自动续推未决写入到最终一致。"""
    auto_client.post("/cluster/fault/replica-1")
    auto_client.post("/cluster/fault/replica-2")
    r = auto_client.post("/writes", json={
        "idem_key": "ik-ap", "key": "kap", "value": "vap",
        "request_id": "req-ap", "timestamp": 1,
    })
    assert r.status_code == 504
    assert auto_client.get("/writes/ik-ap/status").json()["phase"] == "pending"
    # 故障解除触发自动对账：写入收敛为已提交
    auto_client.post("/cluster/clear-fault/replica-1")
    st = auto_client.get("/writes/ik-ap/status").json()
    assert st["phase"] == "succeeded"
    assert len(st["committed_by"]) >= 2
    assert auto_client.get("/keys/kap").json()["visible"] is True


def test_reconcile_after_full_restart_keeps_committed_data(client: TestClient) -> None:
    """重启恢复 + 对账：已提交数据不丢、不重复生效，少数派残留不被当成已提交。"""
    cluster = client.app.state.cluster
    client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-1", "timestamp": 1,
    })
    # 另一笔写入只落到少数派（未达多数派）
    client.post("/cluster/fault/replica-1")
    client.post("/cluster/fault/replica-2")
    client.post("/writes", json={
        "idem_key": "ik-2", "key": "k2", "value": "minority",
        "request_id": "req-2", "timestamp": 2,
    })
    client.post("/cluster/clear-fault/replica-1")
    client.post("/cluster/clear-fault/replica-2")

    for rid in ("replica-0", "replica-1", "replica-2"):
        client.post(f"/cluster/restart/{rid}")
    client.post("/cluster/restart-coordinator")

    # 重启后已提交数据仍在，少数派残留不可见
    assert client.get("/keys/k1").json()["visible"] is True
    assert client.get("/keys/k2").json()["visible"] is False

    report = client.post("/cluster/reconcile").json()
    assert report["converged"] is True
    # ik-2 被自动续推为已提交；ik-1 不重复生效（各副本仍只有一条生效记录）
    assert client.get("/writes/ik-2").json()["code"] == CommitCode.COMMITTED.value
    for rep in cluster.replicas:
        assert rep.get("k1").request_id == "req-1"
        assert rep.get("k2").request_id == "req-2"
        assert rep.get("k1").committed and rep.get("k2").committed


def test_reconcile_with_concurrent_conflicting_writes(client: TestClient) -> None:
    """并发冲突写入 + 副本落后：对账后各副本收敛到同一个 LWW 胜者。"""
    cluster = client.app.state.cluster
    cluster.fail_replica("replica-2")

    def write(rid: str):
        return cluster.coordinator.commit(
            idem_key=f"ik-{rid}", key="hot", value=f"val-{rid}",
            request_id=rid, timestamp=5,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        outs = list(pool.map(write, ["req-aaa", "req-bbb"]))
    assert all(o.code is CommitCode.COMMITTED for o in outs)

    cluster.restart_replica("replica-2")
    report = cluster.reconcile()
    assert report.converged is True
    # 全部副本（含落后者）收敛到同一胜者 req-bbb，且已提交
    for rep in cluster.replicas:
        st = rep.get("hot")
        assert st.request_id == "req-bbb"
        assert st.value == "val-req-bbb"
        assert st.committed is True
    read = client.get("/keys/hot").json()
    assert read["request_id"] == "req-bbb" and read["visible"] is True
