"""进程内多实例集群：在本地用多个副本实例 + 独立持久化文件模拟跨节点复制。

每个副本是一个独立的 Replica 实例，拥有自己的 JSONL WAL 文件；
协调者也有独立的提交日志。副本/协调者“重启”= 重建实例并从各自文件重放。
"""
from __future__ import annotations

import os
import shutil
import tempfile

from qrs.coordinator import Coordinator
from qrs.replica import Replica
from qrs.storage import JsonlLog


class Cluster:
    """持有 N 个副本与 1 个协调者，支持故障注入与重启恢复。"""

    def __init__(self, base_dir: str | None = None, n: int = 3) -> None:
        self.base_dir = base_dir or tempfile.mkdtemp(prefix="qrs-")
        self.n = n
        os.makedirs(self.base_dir, exist_ok=True)
        self.replicas: list[Replica] = []
        self.coordinator: Coordinator | None = None
        self._build()

    def _build(self) -> None:
        self.replicas = [
            Replica(
                replica_id=f"replica-{i}",
                log=JsonlLog(os.path.join(self.base_dir, f"replica-{i}.wal")),
            )
            for i in range(self.n)
        ]
        self.coordinator = Coordinator(
            self.replicas,
            JsonlLog(os.path.join(self.base_dir, "coordinator.log")),
        )
        self.coordinator.recover()

    def fail_replica(self, replica_id: str) -> None:
        """令某个副本故障（宕机；后续复制/读取对其失败）。"""
        rep = self._find(replica_id)
        rep.alive = False
        rep.fault_write = False
        print(f"[cluster] {replica_id} FAILED (down)", flush=True)

    def inject_write_fault(self, replica_id: str) -> None:
        """注入写入故障：实例存活但写盘抛错（用于区分部分失败）。"""
        rep = self._find(replica_id)
        rep.fault_write = True
        print(f"[cluster] {replica_id} write-fault injected", flush=True)

    def clear_write_fault(self, replica_id: str) -> None:
        """解除写入故障注入。"""
        self._find(replica_id).fault_write = False
        print(f"[cluster] {replica_id} write-fault cleared", flush=True)

    def restart_replica(self, replica_id: str) -> Replica:
        """重启某个副本：丢弃内存状态，仅从本地 WAL 重放恢复。"""
        idx = next(
            i for i, r in enumerate(self.replicas) if r.replica_id == replica_id
        )
        rep = Replica(
            replica_id,
            JsonlLog(os.path.join(self.base_dir, f"replica-{idx}.wal")),
        )
        rep.recover()
        self.replicas[idx] = rep
        print(
            f"[cluster] {replica_id} RESTARTED, recovered "
            f"{len(rep.data)} keys / {len(rep.seen_requests)} requests",
            flush=True,
        )
        return rep

    def restart_coordinator(self) -> Coordinator:
        """重启协调者：从提交日志重放幂等表、从副本 WAL 重建冲突历史。"""
        self.coordinator = Coordinator(
            self.replicas,
            JsonlLog(os.path.join(self.base_dir, "coordinator.log")),
        )
        self.coordinator.recover()
        print(
            f"[cluster] coordinator RESTARTED, recovered "
            f"{len(self.coordinator.idem_table)} idem keys / "
            f"{len(self.coordinator.conflicts)} conflicts", flush=True,
        )
        return self.coordinator

    def cleanup(self) -> None:
        """删除全部本地持久化文件（测试用）。"""
        shutil.rmtree(self.base_dir, ignore_errors=True)

    def alive_replicas(self) -> list[Replica]:
        return [r for r in self.replicas if r.alive]

    @property
    def quorum(self) -> int:
        return self.n // 2 + 1

    def _find(self, replica_id: str) -> Replica:
        for rep in self.replicas:
            if rep.replica_id == replica_id:
                return rep
        raise KeyError(f"未知副本: {replica_id}")
