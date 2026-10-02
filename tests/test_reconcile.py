"""本轮新增能力：自动对账、未达多数派后续收敛、历史落盘数据兼容与恢复校验。

覆盖：
- 落后副本回归后自动补齐、已提交写入补发 commit 标记、最终各副本状态一致
- 滞后副本重复投递不重复生效、不覆盖已确认写入
- 副本/协调者重启后的自动收敛与恢复校验
- 未达多数派写入的状态查询（WAITING/SUCCESS/FAILED、卡点、确认副本数）
- 并发冲突写入下的对账：败方终态 FAILED、不静默丢弃、幂等语义保持
- 升级前历史格式（v1）落盘数据的兼容读取与收敛
"""
from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from qrs.models import CommitCode, WriteStatus


def test_behind_replica_backfilled_on_return(client: TestClient) -> None:
    """副本落后/短暂不可用后回来：自动补齐缺口且最终各副本已提交状态一致。"""
    cluster = client.app.state.cluster

    # replica-2 写入期间宕机，只在 0/1 上提交成功
    cluster.fail_replica("replica-2")
    r = client.post("/writes", json={
        "idem_key": "ik-1", "key": "acct-1", "value": "100",
        "request_id": "req-1", "timestamp": 1,
    })
    assert r.json()["code"] == CommitCode.COMMITTED.value
    assert "replica-2" not in r.json()["acked_replicas"]

    # 宕机副本重启（WAL 落后，缺 req-1），集群自动对账补齐
    client.post("/cluster/restart/replica-2")
    states = []
    for rep in cluster.replicas:
        st = rep.get("acct-1")
        assert st is not None
        assert st.request_id == "req-1" and st.value == "100" and st.committed
        states.append((st.request_id, st.value, st.committed))
    assert len(set(states)) == 1  # 三副本完全一致
    read = client.get("/keys/acct-1").json()
    assert read["visible"] is True and read["value"] == "100"
    assert set(read["committed_replicas"]) == {
        "replica-0", "replica-1", "replica-2",
    }


def test_redundant_delivery_to_lagging_replica_is_idempotent(client: TestClient) -> None:
    """向落后副本重复/乱序投递：去重 no-op，不丢已提交数据、不盖掉已确认写入。"""
    cluster = client.app.state.cluster
    client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-1", "timestamp": 10,
    })
    lagger = cluster.replicas[2]
    # 重复投递同一 request_id：no-op
    assert lagger.apply_write("k1", "v1", "req-1", 10, False) is None
    # 更旧的乱序写入落败，不覆盖已确认状态
    conflict = lagger.apply_write("k1", "stale", "req-old", 1, False)
    assert conflict is not None and conflict.loser_request_id == "req-old"
    st = lagger.get("k1")
    assert st.request_id == "req-1" and st.value == "v1" and st.committed

    # 重启后再次重复投递，仍去重；对账也不会重复生效
    client.post("/cluster/restart/replica-2")
    rep2 = cluster.replicas[2]
    assert rep2.apply_write("k1", "v1", "req-1", 10, False) is None
    result = client.post("/writes/reconcile").json()
    assert result["backfilled"] == [] and result["upgraded"] == []
    assert client.get("/keys/k1").json()["value"] == "v1"


def test_restart_drives_pending_write_to_converge(client: TestClient) -> None:
    """协调者与副本重启后，恢复校验正常、未达多数派写入自动推进收敛。"""
    client.post("/cluster/fault/replica-1")
    client.post("/cluster/fault/replica-2")
    client.post("/writes", json={
        "idem_key": "ik-p", "key": "k1", "value": "v1",
        "request_id": "req-p", "timestamp": 1,
    })
    # 故障仍在时重启：校验如实报告 pending，读不可见
    info = client.post("/cluster/restart-coordinator").json()
    assert info["idem_keys"] == 1
    verify = client.get("/cluster/verify").json()
    assert verify["pending_requests"] == ["req-p"]
    assert verify["ok"] is True  # 无已提交写入丢失
    assert client.get("/keys/k1").json()["visible"] is False

    # 副本全部重启 + 协调者重启，故障消除：自动对账收敛
    client.post("/cluster/clear-fault/replica-1")
    client.post("/cluster/clear-fault/replica-2")
    client.post("/cluster/restart/replica-1")
    client.post("/cluster/restart/replica-2")
    status = client.get("/writes/ik-p/status").json()
    assert status["status"] == WriteStatus.SUCCESS.value
    assert status["readable"] is True
    assert len(status["acked_replicas"]) >= 2
    assert client.get("/writes/ik-p").json()["code"] == CommitCode.COMMITTED.value
    assert client.get("/keys/k1").json()["visible"] is True


def test_pending_write_status_query_and_auto_convergence(client: TestClient) -> None:
    """未达多数派：明确失败响应 + 可查 WAITING/卡点/确认数；恢复后自动收敛，期间不可见。"""
    client.post("/cluster/fault/replica-1")
    client.post("/cluster/fault/replica-2")
    r = client.post("/writes", json={
        "idem_key": "ik-wait", "key": "k1", "value": "v1",
        "request_id": "req-wait", "timestamp": 1,
    })
    # 调用方得到明确失败
    assert r.status_code == 504
    assert r.json()["code"] == CommitCode.QUORUM_FAILED.value

    # 可查询当前状态：WAITING、卡在哪、多少副本确认；读取入口看不到
    status = client.get("/writes/ik-wait/status").json()
    assert status["status"] == WriteStatus.WAITING.value
    assert status["request_id"] == "req-wait"
    assert status["acked_replicas"] == []
    assert "replica-1" in status["pending_replicas"]
    assert "未达多数派" in status["step"] or "多数派" in status["step"]
    assert status["readable"] is False
    # 读取入口即使命中少数派残留也不可见（200 + visible=false），未确认内容不暴露
    read = client.get("/keys/k1").json()
    assert read["visible"] is False and read["committed_replicas"] == []
    pending = client.get("/writes").json()
    assert pending["count"] == 1
    assert pending["writes"][0]["request_id"] == "req-wait"

    # 只恢复一个副本：clear-fault 自动对账 -> SUCCESS
    client.post("/cluster/clear-fault/replica-1")
    status2 = client.get("/writes/ik-wait/status").json()
    assert status2["status"] == WriteStatus.SUCCESS.value
    assert len(status2["acked_replicas"]) == 2
    assert status2["readable"] is True
    read = client.get("/keys/k1").json()
    assert read["visible"] is True and read["value"] == "v1"
    # 首次判定记录随之升级为最终结果，GET /writes 与 status 结论一致
    assert client.get("/writes/ik-wait").json()["code"] == CommitCode.COMMITTED.value

    # 剩余副本恢复：缺口自动补齐，三副本一致
    client.post("/cluster/clear-fault/replica-2")
    cluster = client.app.state.cluster
    for rep in cluster.replicas:
        st = rep.get("k1")
        assert st.request_id == "req-wait" and st.value == "v1" and st.committed


def test_pending_write_losing_conflict_reaches_failed_terminal(client: TestClient) -> None:
    """未达多数派的旧写入被更新的已提交写入 LWW 取代：终态 FAILED，不再等待/不可见。"""
    cluster = client.app.state.cluster
    cluster.inject_write_fault("replica-1")
    cluster.inject_write_fault("replica-2")
    client.post("/writes", json={
        "idem_key": "ik-old", "key": "k1", "value": "old",
        "request_id": "req-old", "timestamp": 1,
    })
    # 故障保持期间提交更新的写入：仅 replica-0 可达，但 replica-0 上 req-old
    # 是当前获胜者，req-new 以更大的 timestamp 覆盖它并在 0 上提交……
    # 注意：此时 req-new 同样无法达多数派；需要先恢复 1/2 中的一个使 req-new
    # 达多数派（自动对账会同时评估两者：req-new 升级，req-old 落败 FAILED）。
    client.post("/writes", json={
        "idem_key": "ik-new", "key": "k1", "value": "new",
        "request_id": "req-new", "timestamp": 2,
    })
    client.post("/cluster/clear-fault/replica-1")  # 解除即自动对账
    result = client.post("/writes/reconcile").json()  # 再跑一次结果应稳定
    assert "req-new" not in result["waiting"]
    assert client.get("/writes/ik-new").json()["code"] == CommitCode.COMMITTED.value
    assert "req-old" in result["failed"]

    status = client.get("/writes/ik-old/status").json()
    assert status["status"] == WriteStatus.FAILED.value
    assert status["superseded_by"] == "req-new"
    assert status["readable"] is False
    # 之后全部恢复，req-old 也不会再被升级
    client.post("/cluster/clear-fault/replica-2")
    assert client.get("/writes/ik-old/status").json()["status"] == "FAILED"
    # resume 也不能把它救回成功
    resumed = client.post("/writes/ik-old/resume")
    assert resumed.status_code == 504
    # 读取入口只能看到获胜的新值
    read = client.get("/keys/k1").json()
    assert read["visible"] is True and read["request_id"] == "req-new"
    # 败方不静默丢弃：冲突历史可查
    conflicts = client.get("/conflicts", params={"key": "k1"}).json()["conflicts"]
    assert any(c["loser"]["request_id"] == "req-old" for c in conflicts)
    # 幂等键语义不变：同键异内容仍 409，首次结论查询仍在
    assert client.get("/writes/ik-old").json()["code"] == CommitCode.QUORUM_FAILED.value
    mismatch = client.post("/writes", json={
        "idem_key": "ik-old", "key": "k1", "value": "OTHER",
        "request_id": "req-x", "timestamp": 3,
    })
    assert mismatch.status_code == 409


def test_concurrent_conflicting_writes_converge_deterministically(client: TestClient) -> None:
    """并发冲突写入 + 落后副本回归：对账按 LWW 确定收敛，幂等结论互不覆盖。"""
    cluster = client.app.state.cluster
    cluster.fail_replica("replica-2")  # 写入期间该副本落后

    def write(rid: str):
        return cluster.coordinator.commit(
            idem_key=f"ik-{rid}", key="hot", value=f"val-{rid}",
            request_id=rid, timestamp=5,
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        outs = list(pool.map(write, ["req-aaa", "req-bbb", "req-ccc", "req-ddd"]))
    assert all(o.code is CommitCode.COMMITTED for o in outs)

    # 落后副本回归：账本按历史顺序补齐，最终由 LWW 全序确定唯一获胜者
    client.post("/cluster/restart/replica-2")
    read = client.get("/keys/hot").json()
    assert read["request_id"] == "req-ddd" and read["value"] == "val-req-ddd"
    for rep in cluster.replicas:
        st = rep.get("hot")
        assert st.request_id == "req-ddd" and st.committed is True
    # 每个幂等键的首次结论仍可查、互不覆盖
    for rid in ["req-aaa", "req-bbb", "req-ccc", "req-ddd"]:
        got = client.get(f"/writes/ik-{rid}").json()
        assert got["code"] == CommitCode.COMMITTED.value
        assert got["request_id"] == rid
    # 败方全部留存在冲突历史中（落后副本上补齐时重新产生冲突记录）
    losers = {
        c["loser"]["request_id"]
        for c in client.get("/conflicts", params={"key": "hot"}).json()["conflicts"]
    }
    assert {"req-aaa", "req-bbb", "req-ccc"} <= losers


def test_legacy_v1_wal_data_starts_reads_and_converges(client: TestClient, capsys) -> None:
    """升级前的 v1 历史落盘数据（无版本字段/半行/孤儿标记）：可启动、可读取、可收敛。"""
    base_dir = client.app.state.cluster.base_dir
    client.app.state.cluster.cleanup()

    from qrs.api import create_app
    from qrs.cluster import Cluster
    from qrs.storage import JsonlLog

    # 直接播种 v1 格式（无 "v" 字段）：r0/r1 有 req-1，r2 落后缺失；
    # r0 另有崩溃半行与找不到 write 的孤儿 commit 标记。
    w0 = JsonlLog(os.path.join(base_dir, "replica-0.wal"))
    w0.append_raw_line(json.dumps({
        "op": "write", "key": "k1", "value": "v1",
        "request_id": "req-1", "timestamp": 11, "committed": False,
    }, sort_keys=True))
    w0.append_raw_line('{"op": "write", "key": "k1", "value": "br')  # 崩溃半行
    w0.append_raw_line(json.dumps({"op": "commit", "key": "ghost",
                                   "request_id": "req-ghost"}))
    w0.append_raw_line(json.dumps({"op": "commit", "key": "k1",
                                   "request_id": "req-1"}))
    w1 = JsonlLog(os.path.join(base_dir, "replica-1.wal"))
    w1.append_raw_line(json.dumps({
        "op": "write", "key": "k1", "value": "v1",
        "request_id": "req-1", "timestamp": 11, "committed": False,
    }, sort_keys=True))
    JsonlLog(os.path.join(base_dir, "replica-2.wal"))  # 空文件：落后副本
    clog = JsonlLog(os.path.join(base_dir, "coordinator.log"))
    clog.append_raw_line(json.dumps({
        "idem_key": "ik-1", "code": "COMMITTED", "request_id": "req-1",
        "key": "k1", "value": "v1", "timestamp": 11,
        "acked": ["replica-0", "replica-1"], "pending": ["replica-2"],
        "quorum": 2, "reason": "历史数据", "replay": False,
    }, sort_keys=True))

    # 用旧数据目录直接启动：不报错，且自动对账
    legacy = Cluster(base_dir=base_dir, n=3)
    app2 = create_app(legacy)
    with TestClient(app2) as c2:
        read = c2.get("/keys/k1").json()
        assert read["visible"] is True and read["value"] == "v1"
        assert set(read["committed_replicas"]) == {
            "replica-0", "replica-1", "replica-2",
        }
        # 恢复校验：半行与孤儿标记被如实报告，但不阻断启动
        verify = c2.get("/cluster/verify").json()
        assert verify["ok"] is True
        joined = "\n".join(verify["anomalies"])
        assert "半行/损坏" in joined and "ghost" in joined
        # 三副本最终一致
        for rep in legacy.replicas:
            st = rep.get("k1")
            assert st is not None and st.value == "v1" and st.committed is True
        # 旧数据上幂等语义与冲突规则保持一致
        replay = c2.post("/writes", json={
            "idem_key": "ik-1", "key": "k1", "value": "v1",
            "request_id": "req-again", "timestamp": 11,
        })
        assert replay.json()["code"] == CommitCode.REPLAYED.value
        mismatch = c2.post("/writes", json={
            "idem_key": "ik-1", "key": "k1", "value": "OTHER",
            "request_id": "req-mis", "timestamp": 12,
        })
        assert mismatch.status_code == 409

    # 重启后仍收敛、不重复生效（seen_requests 从 v1 WAL 重建）
    legacy.restart_replica("replica-0")
    assert legacy.replicas[0].get("k1").committed is True
    legacy.cleanup()


def test_reconcile_logs_contain_trace_evidence(client: TestClient, capsys) -> None:
    """对账日志可看出请求标识、确认副本、判定依据。"""
    client.post("/cluster/fault/replica-1")
    client.post("/cluster/fault/replica-2")
    client.post("/writes", json={
        "idem_key": "ik-log", "key": "k1", "value": "v1",
        "request_id": "req-log", "timestamp": 1,
    })
    capsys.readouterr()  # 清空提交阶段日志
    client.post("/cluster/clear-fault/replica-1")  # 触发自动对账
    out = capsys.readouterr().out
    assert "req-log" in out            # 请求标识
    assert "replica-1" in out          # 补齐/确认副本
    assert "backfill" in out           # 补齐动作
    assert "verdict=SUCCESS" in out    # 判定结论


def test_failed_terminal_and_waiting_survive_coordinator_restart(client: TestClient) -> None:
    """FAILED 终态与 WAITING 状态在协调者/副本重启后保持一致，不被误升级或丢失。"""
    # 构造 req-old（旧、未达多数派）与 req-new（较新、将达多数派）
    client.post("/cluster/fault/replica-1")
    client.post("/cluster/fault/replica-2")
    client.post("/writes", json={
        "idem_key": "ik-old", "key": "k1", "value": "old",
        "request_id": "req-old", "timestamp": 1,
    })
    client.post("/writes", json={
        "idem_key": "ik-new", "key": "k1", "value": "new",
        "request_id": "req-new", "timestamp": 2,
    })
    client.post("/cluster/clear-fault/replica-1")  # req-new 升级，req-old FAILED
    assert client.get("/writes/ik-old/status").json()["status"] == "FAILED"

    # 协调者重启：从日志重建后仍判 FAILED，req-new 仍 COMMITTED
    client.post("/cluster/restart-coordinator")
    assert client.get("/writes/ik-old/status").json()["status"] == "FAILED"
    assert client.get("/writes/ik-new").json()["code"] == CommitCode.COMMITTED.value
    assert client.get("/keys/k1").json()["request_id"] == "req-new"

    # 所有副本重启后，败方仍不生效、不重复，胜方一致可见
    for rid in ("replica-0", "replica-1", "replica-2"):
        client.post(f"/cluster/restart/{rid}")
    assert client.get("/writes/ik-old/status").json()["status"] == "FAILED"
    read = client.get("/keys/k1").json()
    assert read["visible"] is True and read["request_id"] == "req-new"

    # 另一个仍在等待的请求：恢复后自动收敛
    client.post("/cluster/fault/replica-1")
    client.post("/cluster/fault/replica-2")
    client.post("/writes", json={
        "idem_key": "ik-w2", "key": "k2", "value": "v2",
        "request_id": "req-w2", "timestamp": 3,
    })
    client.post("/cluster/restart-coordinator")
    assert client.get("/writes/ik-w2/status").json()["status"] == "WAITING"
    client.post("/cluster/clear-fault/replica-1")
    client.post("/cluster/clear-fault/replica-2")
    assert client.get("/writes/ik-w2/status").json()["status"] == "SUCCESS"
