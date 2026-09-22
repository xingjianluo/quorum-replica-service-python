"""并发冲突写入：确定且可解释的 LWW 裁决，双方都不被静默丢弃。"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

from app.models import WriteStatus


def _commit_pair(service, a_ts, b_ts, a_rid="req-a", b_rid="req-b"):
    a = service.submit("key-a", "doc", "payload-A", a_ts, a_rid)
    b = service.submit("key-b", "doc", "payload-B", b_ts, b_rid)
    assert a.status is WriteStatus.COMMITTED
    assert b.status is WriteStatus.COMMITTED
    return a, b


def test_larger_timestamp_wins(service, caplog):
    caplog.set_level(logging.INFO, logger="quorum.coordinator")
    _commit_pair(service, a_ts=2000, b_ts=1000)

    read = service.read_data("doc")
    assert read.status is WriteStatus.COMMITTED
    assert read.request_id == "req-a"
    assert read.payload == "payload-A"
    assert "req-b" in read.reason and "LWW_TIMESTAMP_NS_THEN_REQUEST_ID" in read.reason

    record = service.list_conflicts("doc")[0]
    assert record.winner_request_id == "req-a"
    assert record.loser_request_id == "req-b"
    assert record.winner_timestamp_ns == 2000
    assert record.loser_timestamp_ns == 1000


def test_resolution_is_order_independent(service):
    """后提交的一方时间戳更大时，后写者胜；提交顺序不影响裁决。"""
    _commit_pair(service, a_ts=1000, b_ts=2000)

    read = service.read_data("doc")
    assert read.request_id == "req-b"
    assert read.payload == "payload-B"
    record = service.list_conflicts("doc")[0]
    assert record.winner_request_id == "req-b"
    assert record.loser_request_id == "req-a"


def test_tie_timestamp_breaks_by_request_id(service):
    _commit_pair(service, a_ts=500, b_ts=500, a_rid="req-aaa", b_rid="req-zzz")

    read = service.read_data("doc")
    assert read.request_id == "req-zzz"
    record = service.list_conflicts("doc")[0]
    assert record.winner_request_id == "req-zzz"
    assert record.loser_request_id == "req-aaa"


def test_loser_is_not_silently_dropped(service):
    """败方写入仍可按其幂等键查询到自身已提交，并能在冲突记录中查到。"""
    _commit_pair(service, a_ts=2000, b_ts=1000)

    loser_view = service.query_idempotency("key-b")
    assert loser_view.status is WriteStatus.COMMITTED
    assert loser_view.request_id == "req-b"
    assert loser_view.payload == "payload-B"

    record = service.list_conflicts("doc")[0]
    assert record.loser_idempotency_key == "key-b"
    assert record.winner_idempotency_key == "key-a"


def test_conflict_persists_through_restart_and_converges(service):
    _commit_pair(service, a_ts=2000, b_ts=1000)
    service.cluster.restart_all()

    records = service.list_conflicts("doc")
    assert len(records) == 1
    assert records[0].winner_request_id == "req-a"
    assert service.read_data("doc").request_id == "req-a"
    # 每个副本重放后都重建出同一条冲突（不重复）
    for replica in service.cluster.all_replicas():
        assert len(replica.conflicts()) == 1


def test_three_way_conflict_all_parties_recorded(service):
    service.submit("ka", "doc", "A", 3000, "req-a")
    service.submit("kb", "doc", "B", 2000, "req-b")
    service.submit("kc", "doc", "C", 1000, "req-c")

    records = service.list_conflicts("doc")
    pairs = {
        tuple(sorted((r.winner_request_id, r.loser_request_id))) for r in records
    }
    assert pairs == {("req-a", "req-b"), ("req-a", "req-c"), ("req-b", "req-c")}
    read = service.read_data("doc")
    assert read.request_id == "req-a"
    assert "req-b" in read.reason and "req-c" in read.reason


def test_concurrent_conflicting_writes_resolve_deterministically(service):
    """并发冲突：所有线程都拿到成功提交，读取结论始终唯一确定。"""

    def submit(args):
        rid, ts = args
        return service.submit(f"key-{rid}", "hot-doc", f"payload-{rid}", ts, rid)

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(
            pool.map(
                submit,
                [("req-a", 1000), ("req-b", 2000), ("req-c", 3000)] * 2,
            )
        )

    assert all(r.status is WriteStatus.COMMITTED for r in results)
    read = service.read_data("hot-doc")
    assert read.request_id == "req-c"  # 最大时间戳
    # 重复提交不产生新冲突对
    pairs = {
        tuple(sorted((r.winner_request_id, r.loser_request_id)))
        for r in service.list_conflicts("hot-doc")
    }
    assert pairs == {("req-b", "req-c"), ("req-a", "req-c"), ("req-a", "req-b")}
