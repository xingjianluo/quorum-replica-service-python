"""单副本：持有本地数据状态，写入先持久化 WAL 再生效。

WAL 记录两类操作：
- write:  一次复制写入（key/value/request_id/timestamp/committed）
- commit: 某个 request_id 获得多数派确认后的提交标记

同一 request_id 的写入重复投递为幂等 no-op；不同 request_id 写入同一键
按确定性 LWW 规则解决，冲突记录返回给协调者。
"""
from __future__ import annotations

from dataclasses import dataclass

from qrs.models import CONFLICT_RULE, ConflictRecord
from qrs.storage import JsonlLog


@dataclass
class ReplicaState:
    """副本上某个数据键的当前状态。"""
    key: str
    value: str
    request_id: str
    timestamp: int
    committed: bool = False


def _precedes(ts_a: int, rid_a: str, ts_b: int, rid_b: str) -> bool:
    """返回 (ts_a, rid_a) 在 LWW 规则下是否严格先于（应输给）(ts_b, rid_b)。"""
    return (ts_a, rid_a) < (ts_b, rid_b)


class Replica:
    """进程内模拟的一个复制副本，拥有独立的本地持久化文件。"""

    def __init__(self, replica_id: str, log: JsonlLog) -> None:
        self.replica_id = replica_id
        self.log = log
        self.alive = True
        self.fault_write = False  # 故障注入：写入抛错，但实例仍存活
        self.data: dict[str, ReplicaState] = {}
        self.seen_requests: set[str] = set()

    # 恢复 -------------------------------------------------------------
    def recover(self) -> None:
        """从本地 WAL 重放恢复状态（重复/乱序记录同样收敛）。"""
        for rec in self.log.read_all():
            op = rec.get("op")
            if op == "write":
                self._ingest(
                    rec["key"], rec["value"], rec["request_id"],
                    rec["timestamp"], rec["committed"],
                )
            elif op == "commit":
                state = self.data.get(rec["key"])
                if state is not None and state.request_id == rec["request_id"]:
                    state.committed = True

    # 复制写入 ----------------------------------------------------------
    def apply_write(
        self,
        key: str,
        value: str,
        request_id: str,
        timestamp: int,
        committed: bool,
    ) -> ConflictRecord | None:
        """持久化并应用一条复制写入；返回冲突记录（无冲突/重复时为 None）。"""
        if not self.alive:
            raise RuntimeError(f"replica {self.replica_id} is down")
        if self.fault_write:
            raise IOError(f"replica {self.replica_id} injected write fault")
        record = {
            "op": "write", "key": key, "value": value,
            "request_id": request_id, "timestamp": timestamp,
            "committed": committed,
        }
        self.log.append(record)
        conflict = self._ingest(
            key, value, request_id, timestamp, committed,
        )
        return conflict

    def mark_committed(self, key: str, request_id: str) -> None:
        """把已存的获胜写入标记为已提交（幂等）。"""
        if not self.alive:
            raise RuntimeError(f"replica {self.replica_id} is down")
        state = self.data.get(key)
        if state is None or state.request_id != request_id:
            # 该副本还没有获胜写入或已有更新的获胜者：不做无意义标记。
            return
        if state.committed:
            return
        self.log.append({"op": "commit", "key": key, "request_id": request_id})
        state.committed = True

    # 查询 -------------------------------------------------------------
    def get(self, key: str) -> ReplicaState | None:
        if not self.alive:
            raise RuntimeError(f"replica {self.replica_id} is down")
        return self.data.get(key)

    # 内部 -------------------------------------------------------------
    def _ingest(
        self, key: str, value: str, request_id: str,
        timestamp: int, committed: bool,
    ) -> ConflictRecord | None:
        # 同一 request_id 重复投递：幂等去重，绝不重复生效。
        if request_id in self.seen_requests:
            return None
        self.seen_requests.add(request_id)

        current = self.data.get(key)
        if current is None:
            self.data[key] = ReplicaState(
                key=key, value=value, request_id=request_id,
                timestamp=timestamp, committed=committed,
            )
            return None

        if _precedes(timestamp, request_id, current.timestamp, current.request_id):
            # 迟到/旧序写入：按规则落败，不覆盖当前状态。
            return ConflictRecord(
                key=key,
                winner_request_id=current.request_id,
                winner_timestamp=current.timestamp,
                loser_request_id=request_id,
                loser_timestamp=timestamp,
                rule=CONFLICT_RULE,
                detail=(
                    f"replica {self.replica_id}: 迟到写入 {request_id}@ts={timestamp} "
                    f"落败于已生效写入 {current.request_id}@ts={current.timestamp}"
                ),
            )

        if (timestamp, request_id) == (current.timestamp, current.request_id):
            return None

        # 新写入在全序上更靠后：覆盖生效，原状态成为败方。
        # committed 标记不跨 request_id 继承，是否已提交以新写入自身为准。
        loser_rid, loser_ts = current.request_id, current.timestamp
        self.data[key] = ReplicaState(
            key=key, value=value, request_id=request_id,
            timestamp=timestamp, committed=committed,
        )
        return ConflictRecord(
            key=key,
            winner_request_id=request_id,
            winner_timestamp=timestamp,
            loser_request_id=loser_rid,
            loser_timestamp=loser_ts,
            rule=CONFLICT_RULE,
            detail=(
                f"replica {self.replica_id}: 写入 {request_id}@ts={timestamp} 覆盖"
                f"写入 {loser_rid}@ts={loser_ts} 生效"
            ),
        )
