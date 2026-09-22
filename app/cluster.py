"""副本集群：在进程内持有多个 Replica，并模拟网络投递。"""

from __future__ import annotations

import threading
from pathlib import Path

from app.models import WriteRequest
from app.replica import Replica
from app.storage import ReplicaStorage


class ReplicaCluster:
    """进程内多副本集合 + 故障注入 + 重启/重放支持。"""

    def __init__(self, base_dir: str, replica_count: int = 5) -> None:
        if replica_count < 1 or replica_count % 2 == 0:
            raise ValueError("replica_count must be a positive odd number")
        self._base_dir = Path(base_dir)
        self._lock = threading.RLock()
        self._replicas: dict[str, Replica] = {}
        self.replica_count = replica_count
        self._bootstrap()

    @staticmethod
    def quorum_for(n: int) -> int:
        """多数派：floor(n/2)+1。"""
        return n // 2 + 1

    def quorum(self) -> int:
        return self.quorum_for(self.replica_count)

    def _replica_dir(self, replica_id: str) -> Path:
        return self._base_dir / replica_id

    def _build_replica(self, replica_id: str) -> Replica:
        storage = ReplicaStorage(self._replica_dir(replica_id))
        replica = Replica(replica_id, storage)
        # 重启沿用“宕机”这一进程内模拟状态由调用方决定；默认恢复为存活
        replica.set_alive(True)
        return replica

    def _bootstrap(self) -> None:
        with self._lock:
            self._base_dir.mkdir(parents=True, exist_ok=True)
            for i in range(self.replica_count):
                rid = f"r{i}"
                self._replicas[rid] = self._build_replica(rid)

    def replica_ids(self) -> list[str]:
        return [f"r{i}" for i in range(self.replica_count)]

    def get(self, replica_id: str) -> Replica:
        with self._lock:
            return self._replicas[replica_id]

    def all_replicas(self) -> list[Replica]:
        with self._lock:
            return [self._replicas[rid] for rid in self.replica_ids()]

    def alive_replicas(self) -> list[Replica]:
        with self._lock:
            return [r for r in self.all_replicas() if r.is_alive()]

    def set_alive(self, replica_id: str, alive: bool) -> None:
        self.get(replica_id).set_alive(alive)

    def set_faults(self, replica_id: str, ops) -> None:
        """让副本存活但在指定操作（prepare/commit/abort）上失败。"""
        self.get(replica_id).set_faults(ops)

    def clear_faults(self, replica_id: str) -> None:
        self.get(replica_id).clear_faults()

    def clear_all_faults(self) -> None:
        for replica in self.all_replicas():
            replica.clear_faults()

    def restart(self, replica_id: str) -> Replica:
        """模拟节点重启：重新构造 Replica 并重放持久化日志。"""
        with self._lock:
            replica = self._build_replica(replica_id)
            self._replicas[replica_id] = replica
            return replica

    def restart_all(self) -> None:
        for rid in self.replica_ids():
            self.restart(rid)

    def deliver(self, replica_id: str, payload: dict) -> dict:
        """模拟一次复制消息投递（供延迟/重复投递测试）。

        payload: {"op": "prepare"|"commit"|"abort", "request": WriteRequest 字段...}
        返回 {"ok": True} 或 {"ok": False, "error": "..."}；
        目标副本宕机时返回错误而不抛异常，模拟投递失败。
        """
        replica = self.get(replica_id)
        op = payload["op"]
        try:
            if op == "prepare":
                req = WriteRequest(
                    idempotency_key=payload["idempotency_key"],
                    data_key=payload["data_key"],
                    payload=payload["payload"],
                    timestamp_ns=int(payload["timestamp_ns"]),
                    request_id=payload["request_id"],
                )
                replica.prepare(req)
            elif op == "commit":
                replica.commit(payload["idempotency_key"])
            elif op == "abort":
                replica.abort(payload["idempotency_key"])
            else:
                return {"ok": False, "error": f"unknown op {op!r}"}
            return {"ok": True}
        except Exception as exc:  # noqa: BLE001 - 投递层把异常转为可判定结果
            return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
