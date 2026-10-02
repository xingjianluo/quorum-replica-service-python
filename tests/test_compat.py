"""历史落盘数据（v0 格式）的兼容恢复：能启动、能读、不重复生效、语义一致。"""
from __future__ import annotations

import json
import os

import pytest
from fastapi.testclient import TestClient

from qrs.api import create_app
from qrs.cluster import Cluster
from qrs.models import CommitCode


def _write_lines(path: str, records: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.writelines(json.dumps(rec, ensure_ascii=False) + "\n" for rec in records)


@pytest.fixture()
def legacy_dir(tmp_path):
    """构造一套 v0 格式的落盘数据：

    - 副本 WAL：write 记录无 "op"/"committed" 字段；commit 标记格式不变；
    - 协调者日志：无 pending/quorum/reason/replay/timestamp 字段。
    """
    base = str(tmp_path / "legacy")
    # 三个副本都存有 req-1 的写入（v0 格式），其中两个带 commit 标记
    for i in range(3):
        records = [
            {"key": "k1", "value": "v1", "request_id": "req-1", "timestamp": 1},
        ]
        if i < 2:
            records.append(
                {"op": "commit", "key": "k1", "request_id": "req-1"},
            )
        _write_lines(os.path.join(base, f"replica-{i}.wal"), records)
    _write_lines(os.path.join(base, "coordinator.log"), [
        {
            "idem_key": "ik-1", "code": "COMMITTED", "request_id": "req-1",
            "key": "k1", "value": "v1",
            "acked": ["replica-0", "replica-1"],
        },
    ])
    return base


@pytest.fixture()
def legacy_client(legacy_dir):
    cluster = Cluster(base_dir=legacy_dir, n=3)
    app = create_app(cluster)
    with TestClient(app) as c:
        yield c
    cluster.cleanup()


def test_legacy_data_boots_and_reads(legacy_client: TestClient) -> None:
    """v0 落盘数据重启后能正常启动，已提交数据可读可见。"""
    read = legacy_client.get("/keys/k1").json()
    assert read["visible"] is True
    assert read["value"] == "v1"
    assert read["request_id"] == "req-1"
    assert set(read["committed_replicas"]) == {"replica-0", "replica-1"}


def test_legacy_idem_semantics_preserved(legacy_client: TestClient) -> None:
    """v0 幂等记录恢复后：重复提交重放首次结论，异内容拒绝，查询一致。"""
    got = legacy_client.get("/writes/ik-1").json()
    assert got["code"] == CommitCode.COMMITTED.value
    assert got["value"] == "v1"
    # quorum 迁移为当前多数派而非 0
    assert got["quorum"] == 2

    replay = legacy_client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-retry", "timestamp": 1,
    })
    assert replay.status_code == 200
    assert replay.json()["code"] == CommitCode.REPLAYED.value
    assert replay.json()["request_id"] == "req-1"  # 指向首次判定

    mismatch = legacy_client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "OTHER",
        "request_id": "req-evil", "timestamp": 1,
    })
    assert mismatch.status_code == 409


def test_legacy_recovery_no_duplicate_effect(legacy_client: TestClient) -> None:
    """恢复后同一 request_id 再投递仍是幂等 no-op，不重复生效。"""
    cluster = legacy_client.app.state.cluster
    for rep in cluster.replicas:
        assert "req-1" in rep.seen_requests  # v0 记录也计入去重集合
        assert rep.apply_write("k1", "v1", "req-1", 1, False) is None
    # 对账不会把已一致的副本当成缺口：
    # replica-2 缺 commit 标记，补标记属正常修复；之后第二轮完全无修复
    cluster.reconcile()
    report2 = cluster.reconcile()
    assert report2.repaired_replicas == []
    assert report2.converged is True
    for rep in cluster.replicas:
        st = rep.get("k1")
        assert (st.value, st.request_id, st.committed) == ("v1", "req-1", True)


def test_legacy_state_obeys_conflict_rules(legacy_client: TestClient) -> None:
    """v0 恢复的状态参与 LWW：更新写入生效，更旧写入落败，规则一致。"""
    # 更新的写入覆盖旧数据生效
    legacy_client.post("/writes", json={
        "idem_key": "ik-2", "key": "k1", "value": "v2",
        "request_id": "req-2", "timestamp": 5,
    })
    assert legacy_client.get("/keys/k1").json()["value"] == "v2"
    # 比 v0 状态还旧的写入落败并记入冲突历史
    cluster = legacy_client.app.state.cluster
    conflict = cluster.replicas[0].apply_write("k1", "ancient", "req-0", 0, False)
    assert conflict is not None
    assert conflict.winner_request_id == "req-2"
    assert "LWW" in conflict.rule
    assert cluster.replicas[0].get("k1").value == "v2"


def test_unrecognized_and_corrupt_records_skipped(tmp_path) -> None:
    """无法识别的记录与半行损坏被跳过，不影响已确认数据恢复。"""
    base = str(tmp_path / "mixed")
    _write_lines(os.path.join(base, "replica-0.wal"), [
        {"key": "k1", "value": "v1", "request_id": "req-1", "timestamp": 1},
        {"op": "commit", "key": "k1", "request_id": "req-1"},
        {"op": "unknown-op", "key": "k1"},          # 未知操作：跳过
        {"key": "broken"},                           # 缺要素：跳过
    ])
    # 文件末尾追加半行损坏（模拟崩溃）
    with open(os.path.join(base, "replica-0.wal"), "ab") as fh:
        fh.write(b'{"op": "write", "key": "k2", "val')
    _write_lines(os.path.join(base, "coordinator.log"), [
        {"idem_key": "ik-1", "code": "COMMITTED", "request_id": "req-1",
         "key": "k1", "value": "v1"},
        {"idem_key": "ik-x", "code": "TOTALLY_UNKNOWN"},  # 未知判定码：跳过
        {"no_idem_key": True},                            # 缺要素：跳过
    ])
    cluster = Cluster(base_dir=base, n=3)
    try:
        rep0 = cluster.replicas[0]
        st = rep0.get("k1")
        assert st is not None and st.value == "v1" and st.committed
        assert "k2" not in rep0.data  # 半行未生效
        assert set(cluster.coordinator.idem_table) == {"ik-1"}
    finally:
        cluster.cleanup()


def test_legacy_pending_write_resumable_after_upgrade(tmp_path) -> None:
    """v0 里未达多数派的写入，升级恢复后状态可查、可续推收敛。"""
    base = str(tmp_path / "legacy-pending")
    # 只有 replica-0 持有该写入（v0 格式），协调者记录 QUORUM_FAILED（v0 格式）
    _write_lines(os.path.join(base, "replica-0.wal"), [
        {"key": "kp", "value": "vp", "request_id": "req-p", "timestamp": 3},
    ])
    for i in (1, 2):
        _write_lines(os.path.join(base, f"replica-{i}.wal"), [])
    _write_lines(os.path.join(base, "coordinator.log"), [
        {"idem_key": "ik-p", "code": "QUORUM_FAILED", "request_id": "req-p",
         "key": "kp", "value": "vp", "acked": ["replica-0"]},
    ])
    cluster = Cluster(base_dir=base, n=3)
    app = create_app(cluster)
    with TestClient(app) as client:
        st = client.get("/writes/ik-p/status").json()
        assert st["phase"] == "pending"
        assert st["stuck_at"] == "wait_acks"
        assert st["quorum"] == 2  # v0 缺失的 quorum 已迁移
        assert st["held_by"] == ["replica-0"]
        assert client.get("/keys/kp").json()["visible"] is False
        # 升级后可直接续推收敛
        report = client.post("/cluster/reconcile").json()
        assert report["converged"] is True
        assert client.get("/writes/ik-p/status").json()["phase"] == "succeeded"
        assert client.get("/keys/kp").json()["visible"] is True
    cluster.cleanup()
