"""幂等键语义：重复提交一致、同键不同内容可区分拒绝、并发不重复生效。"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

from app.models import WriteStatus


def test_first_submit_commits_with_quorum(service, caplog):
    caplog.set_level(logging.INFO, logger="quorum.coordinator")
    result = service.submit("key-1", "doc-1", "hello", 1000, "req-1")

    assert result.status is WriteStatus.COMMITTED
    assert len(result.acked_replicas) >= result.quorum
    assert result.duplicate is False
    assert result.request_id == "req-1"
    # 判定日志必须带请求标识、确认副本与判定依据
    assert any("request_id=req-1" in r.message for r in caplog.records)
    assert any("basis=commit_quorum_met" in r.message for r in caplog.records)
    assert any("r0" in r.message for r in caplog.records)


def test_duplicate_submission_returns_identical_first_result(service):
    first = service.submit("key-1", "doc-1", "hello", 1000, "req-1")
    duplicate = service.submit("key-1", "doc-1", "hello", 1000, "req-2")

    assert duplicate.status is WriteStatus.COMMITTED
    assert duplicate.duplicate is True
    # 必须回放“首次”结论：request_id / 内容 / 确认副本完全一致
    assert duplicate.request_id == first.request_id == "req-1"
    assert duplicate.payload == first.payload == "hello"
    assert duplicate.acked_replicas == first.acked_replicas

    # 查询入口也得到同一结论
    queried = service.query_idempotency("key-1")
    assert queried.status is WriteStatus.COMMITTED
    assert queried.request_id == "req-1"
    assert queried.payload == "hello"
    assert queried.acked_replicas == first.acked_replicas


def test_same_key_different_content_rejected_with_distinct_reason(service):
    service.submit("key-1", "doc-1", "hello", 1000, "req-1")
    rejected = service.submit("key-1", "doc-1", "tampered", 1001, "req-9")

    assert rejected.status is WriteStatus.REJECTED_CONTENT_CONFLICT
    assert rejected.acked_replicas == []
    assert "already bound" in rejected.reason
    assert "different payload" in rejected.reason
    # 与“未达多数派”失败原因可明确区分
    assert "quorum" not in rejected.reason

    # 原结论不受影响
    queried = service.query_idempotency("key-1")
    assert queried.payload == "hello" and queried.request_id == "req-1"


def test_concurrent_identical_submissions_apply_once(service):
    request_ids = [f"req-{i}" for i in range(10)]

    def submit(rid: str):
        return service.submit("hot-key", "doc-hot", "same-body", 1000, rid)

    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(submit, request_ids))

    committed = [r for r in results if r.status is WriteStatus.COMMITTED]
    assert len(committed) == 10
    # 所有结论必须一致：同一个生效 request_id、同一份内容、同一批确认副本
    winner_ids = {r.request_id for r in committed}
    assert len(winner_ids) == 1
    non_duplicates = [r for r in committed if not r.duplicate]
    assert len(non_duplicates) == 1
    assert all(r.payload == "same-body" for r in committed)
    ack_sets = {tuple(r.acked_replicas) for r in committed}
    assert len(ack_sets) == 1
    # 每个副本只应有一条记录（没有重复生效）
    for replica in service.cluster.all_replicas():
        assert replica.get_entry("hot-key") is not None


def test_concurrent_same_key_different_content_cannot_override(service):
    """并发同一键但内容不同：至多一方生效，另一方必须被明确拒绝。"""

    def submit(args):
        rid, payload = args
        return service.submit(
            "race-key", "doc-race", payload, 1000, rid
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(
                submit,
                [("req-a", "payload-a")] * 4 + [("req-b", "payload-b")] * 4,
            )
        )

    statuses = [r.status for r in results]
    assert WriteStatus.COMMITTED in statuses
    assert WriteStatus.REJECTED_CONTENT_CONFLICT in statuses
    winners = {
        r.request_id for r in results if r.status is WriteStatus.COMMITTED
    }
    # 所有成功结果都指向同一个首次请求
    assert len(winners) == 1
    queried = service.query_idempotency("race-key")
    assert queried.request_id in winners
    assert all(
        r.payload == queried.payload
        for r in results
        if r.status is WriteStatus.COMMITTED
    )
