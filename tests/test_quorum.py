"""多数派确认：不足与部分失败必须可区分；未达多数派不得暴露为成功。"""

from __future__ import annotations

import logging

from app.models import EntryPhase, WriteStatus


def test_insufficient_replicas_at_precheck(service, caplog):
    """存活副本 < 多数派：不产生任何写入，原因与部分失败区分。"""
    caplog.set_level(logging.INFO, logger="quorum.coordinator")
    for rid in ["r2", "r3", "r4"]:  # 只剩 2 个存活 < 3
        service.cluster.set_alive(rid, False)

    result = service.submit("k-x", "doc-x", "v", 1000, "req-x")

    assert result.status is WriteStatus.INSUFFICIENT_REPLICAS
    assert result.alive_replicas == 2
    assert result.quorum == 3
    assert "no write attempted" in result.reason
    assert result.acked_replicas == []
    # 任意副本上都不应留下任何痕迹
    for replica in service.cluster.all_replicas():
        assert replica.get_entry("k-x") is None
    assert any("basis=alive_below_quorum" in r.message for r in caplog.records)


def test_prepare_phase_partial_failure_is_distinguishable(service, caplog):
    """5 个都存活（过预检），但 3 个 prepare 失败：FAILED_QUORUM 且已回滚。"""
    caplog.set_level(logging.INFO, logger="quorum.coordinator")
    for rid in ["r2", "r3", "r4"]:
        service.cluster.set_faults(rid, ["prepare"])

    result = service.submit("k-p", "doc-p", "v", 1000, "req-p")

    assert result.status is WriteStatus.FAILED_QUORUM
    assert result.alive_replicas == 5  # 与 INSUFFICIENT_REPLICAS 的关键区别
    assert result.acked_replicas == ["r0", "r1"]
    assert "prepare" in result.reason and "quorum" in result.reason
    assert any("basis=prepare_acks_below_quorum" in r.message for r in caplog.records)
    # prepare 成功的副本收到 abort，最终不留下 PREPARED 悬挂
    for rid in ["r0", "r1"]:
        entry = service.cluster.get(rid).get_entry("k-p")
        assert entry.phase is EntryPhase.ABORTED


def test_commit_phase_partial_failure_not_exposed(service, caplog):
    """prepare 全过、仅 2 个 commit 落盘：结论失败且读不到该值。"""
    caplog.set_level(logging.INFO, logger="quorum.coordinator")
    for rid in ["r2", "r3", "r4"]:
        service.cluster.set_faults(rid, ["commit"])

    result = service.submit("k-c", "doc-c", "v", 1000, "req-c")

    assert result.status is WriteStatus.FAILED_QUORUM
    assert result.acked_replicas == ["r0", "r1"]
    assert "commit" in result.reason
    assert any("basis=commit_acks_below_quorum" in r.message for r in caplog.records)

    # 少数派已提交的事实不得被任何入口暴露为成功
    assert service.query_idempotency("k-c").status is WriteStatus.NOT_FOUND
    assert service.read_data("doc-c").status is WriteStatus.NOT_FOUND


def test_failure_reasons_are_distinguishable(service):
    # 情形 A：预检不足
    for rid in ["r2", "r3", "r4"]:
        service.cluster.set_alive(rid, False)
    insufficient = service.submit("ka", "da", "v", 1000, "req-a")
    for rid in ["r2", "r3", "r4"]:
        service.cluster.set_alive(rid, True)

    # 情形 B：投递中途失败
    for rid in ["r2", "r3", "r4"]:
        service.cluster.set_faults(rid, ["prepare"])
    partial = service.submit("kb", "db", "v", 1000, "req-b")

    assert insufficient.status is WriteStatus.INSUFFICIENT_REPLICAS
    assert partial.status is WriteStatus.FAILED_QUORUM
    assert insufficient.reason != partial.reason
    assert "no write attempted" in insufficient.reason
    assert "failed replicas" in partial.reason


def test_retry_after_failed_quorum_completes_with_same_key(service):
    """未达多数派的写入可用同一幂等键重试并最终成功。"""
    for rid in ["r2", "r3", "r4"]:
        service.cluster.set_faults(rid, ["commit"])
    first = service.submit("k-r", "doc-r", "final", 1000, "req-r")
    assert first.status is WriteStatus.FAILED_QUORUM

    service.cluster.clear_all_faults()
    retry = service.submit("k-r", "doc-r", "final", 1000, "req-r")
    assert retry.status is WriteStatus.COMMITTED
    assert retry.request_id == "req-r"
    assert service.read_data("doc-r").payload == "final"
    assert len(retry.acked_replicas) >= 3


def test_retry_after_commit_failure_reprepares_aborted_replicas(service):
    """commit 阶段少数派落盘后，其余副本被 abort；同键重试必须能在它们上重新生效。"""
    for rid in ["r2", "r3", "r4"]:
        service.cluster.set_faults(rid, ["commit"])
    first = service.submit("k-ab", "doc-ab", "v", 1000, "req-ab")
    assert first.status is WriteStatus.FAILED_QUORUM
    service.cluster.clear_all_faults()

    # 此时 r2/r3/r4 应为 ABORTED
    for rid in ["r2", "r3", "r4"]:
        assert service.cluster.get(rid).get_entry("k-ab").phase is EntryPhase.ABORTED

    retry = service.submit("k-ab", "doc-ab", "v", 1000, "req-ab")
    assert retry.status is WriteStatus.COMMITTED, retry
    assert retry.acked_replicas == ["r0", "r1", "r2", "r3", "r4"], retry.acked_replicas
    assert service.read_data("doc-ab").payload == "v"


def test_retry_after_clean_abort_uses_legal_transitions(service):
    """abort 成功落盘后，同键重试走 ABORTED→PREPARED→COMMITTED；
    且不允许 ABORTED 直接 commit。"""
    from app.replica import ReplicaFailure

    for rid in ["r2", "r3", "r4"]:
        service.cluster.set_faults(rid, ["commit"])
    first = service.submit("k-ca", "doc-ca", "v", 1000, "req-ca")
    assert first.status is WriteStatus.FAILED_QUORUM
    service.cluster.clear_all_faults()

    for rid in ["r2", "r3", "r4"]:
        # 协调器的失败清理可能已把它们 abort；统一推进到干净的 ABORTED
        phase = service.cluster.get(rid).get_entry("k-ca").phase
        assert phase in (EntryPhase.PREPARED, EntryPhase.ABORTED)
        service.cluster.get(rid).abort("k-ca")
        assert service.cluster.get(rid).get_entry("k-ca").phase is EntryPhase.ABORTED
        # 非法迁移：ABORTED 不允许直接 commit
        try:
            service.cluster.get(rid).commit("k-ca")
            raise AssertionError("ABORTED -> COMMITTED must be rejected")
        except ReplicaFailure:
            pass

    retry = service.submit("k-ca", "doc-ca", "v", 1000, "req-ca")
    assert retry.status is WriteStatus.COMMITTED
    assert retry.acked_replicas == ["r0", "r1", "r2", "r3", "r4"]

    service.cluster.restart_all()
    assert service.read_data("doc-ca").payload == "v"
