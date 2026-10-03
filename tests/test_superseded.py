"""被取代写入的终态失败、三态状态查询、默认启动自愈。"""
from __future__ import annotations

from fastapi.testclient import TestClient

from qrs.api import create_app
from qrs.models import CommitCode


def _write(client: TestClient, idem_key: str, key: str, value: str,
           request_id: str, timestamp: int):
    return client.post("/writes", json={
        "idem_key": idem_key, "key": key, "value": value,
        "request_id": request_id, "timestamp": timestamp,
    })


def _holders(cluster, key: str, request_id: str) -> list[str]:
    return [
        r.replica_id for r in cluster.replicas
        if r.data.get(key) is not None and r.data[key].request_id == request_id
    ]


def _committed_holders(cluster, key: str, request_id: str) -> list[str]:
    return [
        r.replica_id for r in cluster.replicas
        if r.data.get(key) is not None
        and r.data[key].request_id == request_id
        and r.data[key].committed
    ]


def test_superseded_write_not_judged_success_by_reconcile(
    manual_client: TestClient,
) -> None:
    """同键更新的已提交写入生效后，未达多数派的旧写入续推不得被判成功。"""
    cluster = manual_client.app.state.cluster
    # 1) 两个副本写故障：旧写入未达多数派，仅 replica-0 短暂留存
    manual_client.post("/cluster/fault/replica-1")
    manual_client.post("/cluster/fault/replica-2")
    r1 = _write(manual_client, "ik-old", "k", "old", "req-1", 1)
    assert r1.status_code == 504
    assert _holders(cluster, "k", "req-1") == ["replica-0"]

    # 2) 故障恢复，同键更新的写入达成多数派，把旧写入彻底顶掉
    manual_client.post("/cluster/clear-fault/replica-1")
    manual_client.post("/cluster/clear-fault/replica-2")
    r2 = _write(manual_client, "ik-new", "k", "new", "req-2", 2)
    assert r2.json()["code"] == CommitCode.COMMITTED.value
    assert _holders(cluster, "k", "req-1") == []  # 已无任何副本持有旧写入

    # 3) 对账续推：旧写入不得改判成功，应终态判定为 SUPERSEDED
    report = manual_client.post("/cluster/reconcile").json()
    assert "req-1" in report["superseded"]
    assert "req-1" not in report["retried_requests"]
    assert report["converged"] is True

    got = manual_client.get("/writes/ik-old").json()
    assert got["code"] == CommitCode.SUPERSEDED.value
    assert "取代" in got["reason"]
    # 零副本持有却报成功的矛盾不得出现：无任何副本持有/提交旧写入
    assert _holders(cluster, "k", "req-1") == []
    assert _committed_holders(cluster, "k", "req-1") == []
    # 读取入口只看到新写入
    read = manual_client.get("/keys/k").json()
    assert read["request_id"] == "req-2" and read["visible"] is True

    # 4) 终态幂等：再对账、再 resume、重复提交都不会翻回成功
    assert manual_client.post("/cluster/reconcile").json()["converged"] is True
    assert manual_client.get("/writes/ik-old").json()["code"] == \
        CommitCode.SUPERSEDED.value
    resumed = manual_client.post("/writes/ik-old/resume")
    assert resumed.json()["code"] == CommitCode.SUPERSEDED.value
    dup = _write(manual_client, "ik-old", "k", "old", "req-1b", 1)
    assert dup.json()["code"] == CommitCode.SUPERSEDED.value  # 重放失败，不掩盖
    assert _committed_holders(cluster, "k", "req-1") == []


def test_superseded_write_leaves_no_commit_trace_on_lagging_replica(
    manual_client: TestClient,
) -> None:
    """少数派副本仍持有旧写入时，续推被判取代后也不留下已提交痕迹。"""
    cluster = manual_client.app.state.cluster
    manual_client.post("/cluster/fault/replica-1")
    manual_client.post("/cluster/fault/replica-2")
    _write(manual_client, "ik-old", "k", "old", "req-1", 1)
    # replica-0 持有旧写入后宕机；其余副本恢复故障后提交更新的写入
    manual_client.post("/cluster/fail/replica-0")
    manual_client.post("/cluster/clear-fault/replica-1")
    manual_client.post("/cluster/clear-fault/replica-2")
    _write(manual_client, "ik-new", "k", "new", "req-2", 2)
    # replica-0 回来：仍把 req-1 当作当前（未提交）状态
    manual_client.post("/cluster/restart/replica-0")
    assert cluster.replicas[0].data["k"].request_id == "req-1"
    assert cluster.replicas[0].data["k"].committed is False

    report = manual_client.post("/cluster/reconcile").json()
    assert "req-1" in report["superseded"]
    assert report["converged"] is True
    # 旧写入在任何副本上都没有已提交痕迹；落后副本被补齐为新写入
    assert _committed_holders(cluster, "k", "req-1") == []
    assert _holders(cluster, "k", "req-1") == []
    for rep in cluster.replicas:
        st = rep.get("k")
        assert (st.request_id, st.value, st.committed) == ("req-2", "new", True)
    assert manual_client.get("/keys/k").json()["visible"] is True


def test_status_distinguishes_succeeded_failed_pending(
    manual_client: TestClient,
) -> None:
    """状态查询明确区分成功/失败/等待三态，失败给出可读依据。"""
    # 成功态
    _write(manual_client, "ik-ok", "k-ok", "v", "req-ok", 1)
    st = manual_client.get("/writes/ik-ok/status").json()
    assert st["phase"] == "succeeded"
    assert st["stuck_at"] is None and st["failure_reason"] is None
    assert len(st["committed_by"]) >= 2

    # 等待态：确认数不足，卡在 wait_acks，能看到实时确认进度
    manual_client.post("/cluster/fault/replica-1")
    manual_client.post("/cluster/fault/replica-2")
    _write(manual_client, "ik-wait", "k-wait", "v", "req-w", 1)
    st = manual_client.get("/writes/ik-wait/status").json()
    assert st["phase"] == "pending"
    assert st["stuck_at"] == "wait_acks"
    assert st["held_by"] == ["replica-0"] and st["committed_by"] == []
    assert st["quorum"] == 2
    # 等待中的写入读取入口不可见
    assert manual_client.get("/keys/k-wait").json()["visible"] is False

    # 失败态：被更新的已提交写入取代（无需等对账，查询即如实反映）
    _write(manual_client, "ik-old", "k-f", "old", "req-old", 1)
    manual_client.post("/cluster/clear-fault/replica-1")
    manual_client.post("/cluster/clear-fault/replica-2")
    _write(manual_client, "ik-new", "k-f", "new", "req-new", 2)
    st = manual_client.get("/writes/ik-old/status").json()
    assert st["phase"] == "failed"
    assert st["failure_reason"] == "superseded"
    assert st["superseded_by"] == "req-new"
    assert st["held_by"] == []
    assert "取代" in st["basis"] and "确认数不足" in st["basis"]
    # 等待态写入不受同键新提交影响，仍可续推收敛
    assert manual_client.get("/writes/ik-wait/status").json()["phase"] == "pending"
    manual_client.post("/cluster/reconcile")
    assert manual_client.get("/writes/ik-wait/status").json()["phase"] == "succeeded"


def test_superseded_logs_contain_request_id_acks_and_basis(
    manual_client: TestClient, capsys,
) -> None:
    """续推判定日志包含请求标识、确认副本与判定依据。"""
    manual_client.post("/cluster/fault/replica-1")
    manual_client.post("/cluster/fault/replica-2")
    _write(manual_client, "ik-old", "k", "old", "req-trace-old", 1)
    manual_client.post("/cluster/clear-fault/replica-1")
    manual_client.post("/cluster/clear-fault/replica-2")
    _write(manual_client, "ik-new", "k", "new", "req-trace-new", 2)
    capsys.readouterr()  # 清空前置日志
    manual_client.post("/cluster/reconcile")
    out = capsys.readouterr().out
    # 请求标识 + 判定结论 + 取代依据
    assert "req-trace-old" in out
    assert "verdict=SUPERSEDED" in out
    assert "superseded_by=req-trace-new" in out
    assert "basis=" in out and "取代" in out
    # 确认副本进度在日志中可见
    assert "held_by=" in out


def test_superseded_verdict_survives_coordinator_restart(
    manual_client: TestClient,
) -> None:
    """SUPERSEDED 终态判定持久化，协调者重启后依然如实反映失败。"""
    manual_client.post("/cluster/fault/replica-1")
    manual_client.post("/cluster/fault/replica-2")
    _write(manual_client, "ik-old", "k", "old", "req-1", 1)
    manual_client.post("/cluster/clear-fault/replica-1")
    manual_client.post("/cluster/clear-fault/replica-2")
    _write(manual_client, "ik-new", "k", "new", "req-2", 2)
    manual_client.post("/cluster/reconcile")
    assert manual_client.get("/writes/ik-old").json()["code"] == \
        CommitCode.SUPERSEDED.value

    manual_client.post("/cluster/restart-coordinator")
    got = manual_client.get("/writes/ik-old").json()
    assert got["code"] == CommitCode.SUPERSEDED.value
    st = manual_client.get("/writes/ik-old/status").json()
    assert st["phase"] == "failed" and st["failure_reason"] == "superseded"
    # 重启后再对账也不会把被取代写入翻回成功
    manual_client.post("/cluster/reconcile")
    assert manual_client.get("/writes/ik-old").json()["code"] == \
        CommitCode.SUPERSEDED.value


def test_default_startup_auto_repairs_lagging_replica(
    client: TestClient,
) -> None:
    """默认启动（未显式配置）即自愈：副本落后后恢复，自动补齐无需人工对账。"""
    cluster = client.app.state.cluster
    assert cluster.auto_reconcile is True  # 默认开启
    client.post("/cluster/fail/replica-2")
    _write(client, "ik-1", "k1", "v1", "req-1", 1)
    assert cluster.replicas[2].data.get("k1") is None
    # 副本恢复即自动补齐（没有调用任何对账接口）
    client.post("/cluster/restart/replica-2")
    st = cluster.replicas[2].get("k1")
    assert (st.value, st.request_id, st.committed) == ("v1", "req-1", True)
    assert client.get("/keys/k1").json()["visible"] is True


def test_default_startup_auto_converges_pending_write(
    client: TestClient,
) -> None:
    """默认启动即自愈：写故障解除后，未达多数派的写入自动收敛到最终一致。"""
    client.post("/cluster/fault/replica-1")
    client.post("/cluster/fault/replica-2")
    r = _write(client, "ik-p", "kp", "vp", "req-p", 1)
    assert r.status_code == 504
    assert client.get("/writes/ik-p/status").json()["phase"] == "pending"
    # 故障解除即自动续推（没有调用任何对账/续推接口）
    client.post("/cluster/clear-fault/replica-1")
    st = client.get("/writes/ik-p/status").json()
    assert st["phase"] == "succeeded"
    assert len(st["committed_by"]) >= 2
    assert client.get("/keys/kp").json()["visible"] is True
    # 已提交数据不因后续自动对账重复生效或丢失
    client.post("/cluster/restart/replica-2")
    assert client.get("/keys/kp").json()["visible"] is True


def test_default_app_wiring_self_heals() -> None:
    """默认方式创建的服务（create_app()）集群自愈开关默认打开。"""
    app = create_app()
    try:
        assert app.state.cluster.auto_reconcile is True
    finally:
        app.state.cluster.cleanup()
