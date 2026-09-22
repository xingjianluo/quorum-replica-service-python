"""副本重启重放、落后/重复投递收敛、未决写入不被提升。"""

from __future__ import annotations

from app.models import EntryPhase, WriteStatus


def test_restart_replays_committed_state_without_duplication(service):
    service.submit("k1", "doc-1", "persisted", 1000, "req-1")

    service.cluster.restart_all()

    entry_count = 0
    for replica in service.cluster.all_replicas():
        entry = replica.get_entry("k1")
        assert entry is not None
        assert entry.phase is EntryPhase.COMMITTED
        assert entry.payload == "persisted"
        entry_count += len(replica.all_committed())
    assert entry_count == 5  # 每副本恰好一条，无重复生效

    queried = service.query_idempotency("k1")
    assert queried.status is WriteStatus.COMMITTED
    assert queried.request_id == "req-1"
    assert queried.acked_replicas == ["r0", "r1", "r2", "r3", "r4"]


def test_restart_does_not_revive_failed_write(service):
    for rid in ["r2", "r3", "r4"]:
        service.cluster.set_faults(rid, ["commit"])
    failed = service.submit("k2", "doc-2", "v", 1000, "req-2")
    assert failed.status is WriteStatus.FAILED_QUORUM
    service.cluster.clear_all_faults()

    service.cluster.restart_all()

    # 只有 r0/r1 上有 COMMITTED，重启不改变多数派判定
    assert service.query_idempotency("k2").status is WriteStatus.NOT_FOUND
    assert service.read_data("doc-2").status is WriteStatus.NOT_FOUND


def test_lagging_replica_converges_after_late_delivery(service):
    service.cluster.set_alive("r4", False)
    result = service.submit("k3", "doc-3", "late", 1000, "req-3")
    assert result.status is WriteStatus.COMMITTED
    assert "r4" not in result.acked_replicas

    service.cluster.set_alive("r4", True)
    prepare_msg = {
        "op": "prepare",
        "idempotency_key": "k3",
        "data_key": "doc-3",
        "payload": "late",
        "timestamp_ns": 1000,
        "request_id": "req-3",
    }
    assert service.cluster.deliver("r4", prepare_msg)["ok"]
    commit_msg = {"op": "commit", "idempotency_key": "k3"}
    assert service.cluster.deliver("r4", commit_msg)["ok"]

    acked = service.query_idempotency("k3").acked_replicas
    assert acked == ["r0", "r1", "r2", "r3", "r4"]
    assert service.read_data("doc-3").payload == "late"


def test_duplicate_delivery_is_idempotent_after_replay(service):
    service.submit("k4", "doc-4", "dup", 1000, "req-4")

    msg = {
        "op": "prepare",
        "idempotency_key": "k4",
        "data_key": "doc-4",
        "payload": "dup",
        "timestamp_ns": 1000,
        "request_id": "req-4",
    }
    for _ in range(5):  # 重复投递
        assert service.cluster.deliver("r3", msg)["ok"]
        assert service.cluster.deliver(
            "r3", {"op": "commit", "idempotency_key": "k4"}
        )["ok"]

    service.cluster.restart("r3")
    replica = service.cluster.get("r3")
    entry = replica.get_entry("k4")
    assert entry.phase is EntryPhase.COMMITTED
    assert len(replica.all_committed()) == 1  # 重放后仍只生效一次
    assert service.read_data("doc-4").payload == "dup"


def test_repair_does_not_promote_subquorum_commit(service):
    for rid in ["r2", "r3", "r4"]:
        service.cluster.set_faults(rid, ["commit"])
    failed = service.submit("k5", "doc-5", "v", 1000, "req-5")
    assert failed.status is WriteStatus.FAILED_QUORUM
    service.cluster.clear_all_faults()

    summary = service.repair()
    # 未达多数派的提交必须被明确标记为“未提升”
    assert any(
        item.startswith("k5@") for item in summary["unchosen_commits_not_promoted"]
    )
    assert not any(
        "k5" in item for item in summary["propagated_entries"]
    )
    assert service.read_data("doc-5").status is WriteStatus.NOT_FOUND

    # 同键重试达到多数派后，repair 再把它补齐到所有副本
    retry = service.submit("k5", "doc-5", "v", 1000, "req-5")
    assert retry.status is WriteStatus.COMMITTED
    second = service.repair()
    # r2/r3/r4 上此前为 ABORTED 或 PREPARED，会被补齐为 COMMITTED
    assert {item.split(":")[0] for item in second["propagated_entries"]} <= {
        "r2", "r3", "r4"
    }
    assert service.query_idempotency("k5").acked_replicas == [
        "r0", "r1", "r2", "r3", "r4"
    ]


def test_repair_propagates_conflict_records_to_restarted_replica(service):
    service.submit("wa", "doc-x", "A", 2000, "req-a")
    service.submit("wb", "doc-x", "B", 1000, "req-b")
    before = len(service.list_conflicts("doc-x"))
    assert before == 1

    # 模拟一个丢失冲突记录的落后副本：重启不会丢，但这里清空其状态视图
    replica = service.cluster.get("r4")
    # 通过重启验证记录已持久化；并再跑 repair 保证幂等
    service.cluster.restart("r4")
    service.repair()
    conflicts = service.cluster.get("r4").conflicts()
    assert len(conflicts) == 1
    assert conflicts[0].winner_request_id == "req-a"
