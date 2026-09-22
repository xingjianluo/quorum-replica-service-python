"""幂等键语义：重复提交一致、内容冲突可区分、并发不重复生效。"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from qrs.api import create_app
from qrs.models import CommitCode


def test_first_commit_then_replay_returns_consistent_result(client: TestClient) -> None:
    r1 = client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-1", "timestamp": 1,
    })
    assert r1.status_code == 200
    body = r1.json()
    assert body["code"] == CommitCode.COMMITTED.value
    assert set(body["acked_replicas"]) == {"replica-0", "replica-1", "replica-2"}

    # 同幂等键、同内容、不同 request_id 重复提交
    r2 = client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-dup", "timestamp": 999,
    })
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["code"] == CommitCode.REPLAYED.value
    assert body2["replay"] is True
    # 必须与首次提交得到一致结果
    assert body2["request_id"] == "req-1"
    assert body2["value"] == "v1"
    assert body2["acked_replicas"] == body["acked_replicas"]
    assert body2["quorum"] == body["quorum"]

    # 查询入口的结论与提交入口一致
    r3 = client.get("/writes/ik-1")
    assert r3.json()["code"] == CommitCode.COMMITTED.value
    assert r3.json()["value"] == "v1"


def test_same_idem_key_different_value_is_rejected(client: TestClient) -> None:
    client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-1", "timestamp": 1,
    })
    r = client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "DIFFERENT",
        "request_id": "req-2", "timestamp": 2,
    })
    assert r.status_code == 409
    assert r.json()["code"] == CommitCode.CONTENT_MISMATCH.value
    assert "内容不同" in r.json()["reason"]
    # 首次结果不受影响
    assert client.get("/writes/ik-1").json()["value"] == "v1"


def test_same_idem_key_different_target_key_is_rejected(client: TestClient) -> None:
    client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-1", "timestamp": 1,
    })
    r = client.post("/writes", json={
        "idem_key": "ik-1", "key": "other-key", "value": "v1",
        "request_id": "req-2", "timestamp": 2,
    })
    assert r.status_code == 409
    assert r.json()["code"] == CommitCode.CONTENT_MISMATCH.value


def test_concurrent_duplicate_submits_apply_once(client: TestClient) -> None:
    n = 16

    def submit(i: int) -> dict:
        r = client.post("/writes", json={
            "idem_key": "ik-hot", "key": "k1", "value": "v1",
            "request_id": f"req-{i}", "timestamp": 100,
        })
        return r.json()

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(submit, range(n)))

    codes = {b["code"] for b in results}
    assert codes <= {CommitCode.COMMITTED.value, CommitCode.REPLAYED.value}
    # 恰好一个首次生效，其余全部重放，且所有响应指向同一首次结果
    committed = [b for b in results if b["code"] == CommitCode.COMMITTED.value]
    replayed = [b for b in results if b["code"] == CommitCode.REPLAYED.value]
    assert len(committed) == 1
    assert len(replayed) == n - 1
    winner_ids = {b["request_id"] for b in results}
    assert winner_ids == {committed[0]["request_id"]}
    assert {b["value"] for b in results} == {"v1"}

    # 直接对协调者对象并发提交同样不得重复生效
    cluster = client.app.state.cluster
    with ThreadPoolExecutor(max_workers=8) as pool:
        direct = list(pool.map(
            lambda i: cluster.coordinator.commit(
                f"ik-direct", "k2", "v2", f"dreq-{i}", timestamp=200,
            ),
            range(n),
        ))
    assert sum(o.code is CommitCode.COMMITTED for o in direct) == 1
    assert all(
        o.request_id == direct[0].request_id
        or o.code is CommitCode.REPLAYED
        for o in direct
    )
    winner = next(o for o in direct if o.code is CommitCode.COMMITTED)
    assert all(o.request_id == winner.request_id for o in direct)


def test_concurrent_mismatch_and_replay_never_contradict(client: TestClient) -> None:
    client.post("/writes", json={
        "idem_key": "ik-1", "key": "k1", "value": "v1",
        "request_id": "req-1", "timestamp": 1,
    })

    def call(kind: int):
        if kind == 0:  # 合法重放
            return client.post("/writes", json={
                "idem_key": "ik-1", "key": "k1", "value": "v1",
                "request_id": "req-ok", "timestamp": 1,
            }).json()["code"]
        # 内容冲突
        return client.post("/writes", json={
            "idem_key": "ik-1", "key": "k1", "value": "BAD",
            "request_id": "req-bad", "timestamp": 2,
        }).json()["code"]

    with ThreadPoolExecutor(max_workers=8) as pool:
        kinds = [i % 2 for i in range(20)]
        codes = list(pool.map(call, kinds))
    assert set(codes) <= {
        CommitCode.REPLAYED.value, CommitCode.CONTENT_MISMATCH.value,
    }
    assert CommitCode.REPLAYED.value in codes
    assert CommitCode.CONTENT_MISMATCH.value in codes
