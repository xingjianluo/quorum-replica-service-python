"""被取代写入的终态失败判定、状态三态查询、默认启动的自动自愈。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from qrs.api import create_app
from qrs.cluster import Cluster
from qrs.models import CommitCode


@pytest.fixture()
def default_cluster(tmp_path):
    """不显式传 auto_reconcile 的默认启动集群（应默认开启自愈）。"""
    cl = Cluster(base_dir=str(tmp_path / "default"), n=3)
    yield cl
    cl.cleanup()


@pytest.fixture()
def default_client(default_cluster):
    app = create_app(default_cluster)
    with TestClient(app) as c:
        yield c


def _commit_records(cluster, request_id: str) -> list[dict]:
    """收集所有副本 WAL 中指定 request_id 的 commit 标记记录。"""
    found = []
    for rep in cluster.replicas:
        for rec in rep.log.read_all():
            if rec.get("op") == "commit" and rec.get("request_id") == request_id:
                found.append((rep.replica_id, rec))
    return found


def cluster_replica_state(client: TestClient, replica_id: str, key: str):
    """读取集群中指定副本上某个键的当前状态。"""
    cluster = client.app.state.cluster
    rep = next(r for r in cluster.replicas if r.replica_id == replica_id)
    return rep.data.get(key)


def test_superseded_write_not_judged_success_by_reconcile(
    client: TestClient, capsys,
) -> None:
    """未达多数派的写入被同键更新的已提交写入顶掉后：

    对账续推不得改判成功；零副本持有不得报成功；副本上不留已提交痕迹。
    """
    cluster = client.app.state.cluster
    # 两个副本写故障：req-old 只落到 replica-0，未达多数派
    client.post("/cluster/fault/replica-1")
    client.post("/cluster/fault/replica-2")
    r_old = client.post("/writes", json={
        "idem_key": "ik-old", "key": "k", "value": "old",
        "request_id": "req-old", "timestamp": 1,
    })
    assert r_old.status_code == 504
    # 故障解除后，同键更新的写入达成多数派，彻底顶掉 req-old
    client.post("/cluster/clear-fault/replica-1")
    client.post("/cluster/clear-fault/replica-2")
    r_new = client.post("/writes", json={
        "idem_key": "ik-new", "key": "k", "value": "new",
        "request_id": "req-new", "timestamp": 2,
    })
    assert r_new.json()["code"] == CommitCode.COMMITTED.value
    # 没有任何副本还持有 req-old 的内容
    assert all(
        rep.data.get("k") is None or rep.data["k"].request_id != "req-old"
        for rep in cluster.replicas
    )

    # 对账续推：req-old 必须被判 SUPERSEDED，而不是 COMMITTED
    report = client.post("/cluster/reconcile").json()
    assert "ik-old" in report["superseded"]
    assert "req-old" not in report["retried_requests"]
    assert report["converged"] is True

    # 查询入口如实反映失败，不得报成功
    got = client.get("/writes/ik-old")
    assert got.status_code == 409
    assert got.json()["code"] == CommitCode.SUPERSEDED.value
    st = client.get("/writes/ik-old/status").json()
    assert st["phase"] == "failed"
    assert st["held_by"] == [] and st["committed_by"] == []
    assert st["superseded_by"] == "req-new"
    assert "取代" in st["basis"]

    # 副本上不得留下 req-old 的已提交痕迹（WAL 中无其 commit 标记）
    assert _commit_records(cluster, "req-old") == []
    # 数据键读取只反映已确认的 req-new
    read = client.get("/keys/k").json()
    assert read["visible"] is True and read["value"] == "new"

    # 日志可核对：请求标识、判定结论与依据
    out = capsys.readouterr().out
    assert "req-old" in out and "SUPERSEDED" in out and "basis=" in out

    # 再次对账/续推/重复提交：终态不变，绝不翻回成功
    assert client.post("/cluster/reconcile").json()["converged"] is True
    resumed = client.post("/writes/ik-old/resume")
    assert resumed.status_code == 409
    assert resumed.json()["code"] == CommitCode.SUPERSEDED.value
    replay = client.post("/writes", json={
        "idem_key": "ik-old", "key": "k", "value": "old",
        "request_id": "req-old-retry", "timestamp": 1,
    })
    assert replay.status_code == 409
    assert replay.json()["code"] == CommitCode.SUPERSEDED.value
    assert _commit_records(cluster, "req-old") == []


def test_superseded_with_minority_residue_leaves_no_commit_trace(
    client: TestClient,
) -> None:
    """少数派残留仍在某个副本上时，续推也不得给它补 commit 标记。"""
    cluster = client.app.state.cluster
    # req-old 只落到 replica-0
    client.post("/cluster/fault/replica-1")
    client.post("/cluster/fault/replica-2")
    client.post("/writes", json={
        "idem_key": "ik-old", "key": "k", "value": "old",
        "request_id": "req-old", "timestamp": 1,
    })
    # replica-0 宕机（残留保留在其 WAL 上），更新的写入在另两个副本上提交
    client.post("/cluster/fail/replica-0")
    client.post("/cluster/clear-fault/replica-1")
    client.post("/cluster/clear-fault/replica-2")
    client.post("/writes", json={
        "idem_key": "ik-new", "key": "k", "value": "new",
        "request_id": "req-new", "timestamp": 2,
    })
    # replica-0 回来：WAL 重放后仍持有 req-old 的未提交残留
    client.post("/cluster/restart/replica-0")
    assert cluster.replicas[0].data["k"].request_id == "req-old"
    assert cluster.replicas[0].data["k"].committed is False

    report = client.post("/cluster/reconcile").json()
    assert "ik-old" in report["superseded"]
    assert report["converged"] is True
    # 残留被已提交的 req-new 覆盖，且 req-old 在全副本无任何 commit 标记
    for rep in cluster.replicas:
        st = rep.get("k")
        assert (st.request_id, st.value, st.committed) == ("req-new", "new", True)
    assert _commit_records(cluster, "req-old") == []
    st = client.get("/writes/ik-old/status").json()
    assert st["phase"] == "failed" and st["held_by"] == []


def test_status_three_phases_and_read_never_shows_unconfirmed(
    client: TestClient,
) -> None:
    """状态查询明确区分 succeeded / failed / pending，等待时可见卡点与确认数。"""
    # pending：确认数不足，卡在 wait_acks
    client.post("/cluster/fault/replica-1")
    client.post("/cluster/fault/replica-2")
    client.post("/writes", json={
        "idem_key": "ik-p", "key": "k", "value": "v1",
        "request_id": "req-p", "timestamp": 1,
    })
    st = client.get("/writes/ik-p/status").json()
    assert st["phase"] == "pending"
    assert st["stuck_at"] == "wait_acks"
    assert st["acked_replicas"] == ["replica-0"]
    assert st["held_by"] == ["replica-0"] and st["committed_by"] == []
    # 读取入口任何时候都看不到未确认内容
    assert client.get("/keys/k").json()["visible"] is False

    # failed：同键更新的写入提交后，未决写入实时可查为失败（无需等对账）
    client.post("/cluster/clear-fault/replica-1")
    client.post("/cluster/clear-fault/replica-2")
    client.post("/writes", json={
        "idem_key": "ik-w", "key": "k", "value": "v2",
        "request_id": "req-w", "timestamp": 2,
    })
    st = client.get("/writes/ik-p/status").json()
    assert st["phase"] == "failed"
    assert st["stuck_at"] is None
    assert st["superseded_by"] == "req-w"
    assert "取代" in st["basis"] or "确认数" in st["basis"]
    # 对账把失败结论持久化
    client.post("/cluster/reconcile")
    st = client.get("/writes/ik-p/status").json()
    assert st["phase"] == "failed"
    assert st["last_verdict"] == CommitCode.SUPERSEDED.value

    # succeeded：正常提交
    st = client.get("/writes/ik-w/status").json()
    assert st["phase"] == "succeeded"
    assert st["stuck_at"] is None
    assert len(st["committed_by"]) >= 2
    assert client.get("/keys/k").json()["value"] == "v2"


def test_superseded_verdict_survives_coordinator_restart(client: TestClient) -> None:
    """SUPERSEDED 终态判定持久化，协调者重启后仍然如实反映失败。"""
    client.post("/cluster/fault/replica-1")
    client.post("/cluster/fault/replica-2")
    client.post("/writes", json={
        "idem_key": "ik-old", "key": "k", "value": "old",
        "request_id": "req-old", "timestamp": 1,
    })
    client.post("/cluster/clear-fault/replica-1")
    client.post("/cluster/clear-fault/replica-2")
    client.post("/writes", json={
        "idem_key": "ik-new", "key": "k", "value": "new",
        "request_id": "req-new", "timestamp": 2,
    })
    client.post("/cluster/reconcile")
    assert client.get("/writes/ik-old").json()["code"] == CommitCode.SUPERSEDED.value

    client.post("/cluster/restart-coordinator")
    got = client.get("/writes/ik-old")
    assert got.status_code == 409
    assert got.json()["code"] == CommitCode.SUPERSEDED.value
    assert client.get("/writes/ik-old/status").json()["phase"] == "failed"


def test_default_startup_self_heals_lagging_replica(default_client: TestClient) -> None:
    """默认启动（不传 auto_reconcile）：副本落后后恢复即自动补齐，无需人工对账。"""
    cluster = default_client.app.state.cluster
    assert cluster.auto_reconcile is True
    cluster.fail_replica("replica-2")
    default_client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-1", "timestamp": 1,
    })
    assert cluster.replicas[2].data.get("k1") is None
    # 副本恢复：自动对账补齐缺口
    cluster.restart_replica("replica-2")
    st = cluster.replicas[2].get("k1")
    assert (st.value, st.request_id, st.committed) == ("v1", "req-1", True)
    assert default_client.get("/keys/k1").json()["visible"] is True


def test_default_startup_self_heals_pending_write(default_client: TestClient) -> None:
    """默认启动：写故障解除后未达多数派的写入自动收敛到最终一致。"""
    default_client.post("/cluster/fault/replica-1")
    default_client.post("/cluster/fault/replica-2")
    r = default_client.post("/writes", json={
        "idem_key": "ik-p", "key": "kp", "value": "vp",
        "request_id": "req-p", "timestamp": 1,
    })
    assert r.status_code == 504
    assert default_client.get("/writes/ik-p/status").json()["phase"] == "pending"
    # 故障解除即自动续推，无需人工调用对账
    default_client.post("/cluster/clear-fault/replica-1")
    st = default_client.get("/writes/ik-p/status").json()
    assert st["phase"] == "succeeded"
    assert len(st["committed_by"]) >= 2
    assert default_client.get("/keys/kp").json()["visible"] is True
    # 解除最后一个故障：自动对账把落后副本也补齐
    default_client.post("/cluster/clear-fault/replica-2")
    assert cluster_replica_state(default_client, "replica-2", "kp") is not None
    # 自动收敛不重复生效：再手动对账一轮无任何补投/续推
    report = default_client.post("/cluster/reconcile").json()
    assert report["retried_requests"] == []
    assert report["repaired_replicas"] == []
    assert report["converged"] is True


def test_default_startup_superseded_write_converges_to_failed(
    default_client: TestClient,
) -> None:
    """默认启动：被取代的写入在自动对账中收敛为终态失败，而非成功。"""
    cluster = default_client.app.state.cluster
    default_client.post("/cluster/fault/replica-1")
    default_client.post("/cluster/fault/replica-2")
    r = default_client.post("/writes", json={
        "idem_key": "ik-old2", "key": "k2", "value": "old2",
        "request_id": "req-old2", "timestamp": 1,
    })
    assert r.status_code == 504
    # 故障带外恢复（如磁盘自行恢复，未走 clear-fault 入口），
    # 同键更新的写入先达成多数派，把 req-old2 顶掉
    for rep in cluster.replicas:
        rep.fault_write = False
    default_client.post("/writes", json={
        "idem_key": "ik-new2", "key": "k2", "value": "new2",
        "request_id": "req-new2", "timestamp": 2,
    })
    # 下一个恢复事件（副本重启）触发自动对账：ik-old2 被判 SUPERSEDED
    default_client.post("/cluster/restart/replica-2")
    got = default_client.get("/writes/ik-old2")
    assert got.status_code == 409
    assert got.json()["code"] == CommitCode.SUPERSEDED.value
    st = default_client.get("/writes/ik-old2/status").json()
    assert st["phase"] == "failed"
    assert default_client.get("/keys/k2").json()["value"] == "new2"
    assert _commit_records(cluster, "req-old2") == []
