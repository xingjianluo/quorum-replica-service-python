"""单副本：持有本地数据状态，写入先持久化 WAL 再生效。

WAL 记录两类操作：
- write:  一次复制写入（key/value/request_id/timestamp/committed）
- commit: 某个 request_id 获得多数派确认后的提交标记

同一 request_id 的写入重复投递为幂等 no-op；不同 request_id 写入同一键
按确定性 LWW 规则解决，冲突记录返回给协调者。
"""
from __future__ import annotations

from dataclasses import dataclass

from qrs.models import (
    CONFLICT_RULE,
    WAL_VERSION,
    WAL_VERSION_V1,
    ConflictRecord,
)
from qrs.storage import JsonlLog

_WRITE_FIELDS = ("key", "value", "request_id", "timestamp")


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
        # 重放旧/异常落盘数据时发现的可解释问题（不阻断启动）。
        self.recovery_anomalies: list[str] = []

    # 恢复 -------------------------------------------------------------
    def recover(self) -> None:
        """从本地 WAL 重放恢复状态（v1/v2 记录归一化，重复/乱序同样收敛）。"""
        records, skipped = self.log.read_all_with_errors()
        for item in skipped:
            self.recovery_anomalies.append(f"replica {self.replica_id}: {item}")
        for rec in records:
            self._replay_record(rec)

    def _replay_record(self, rec: dict) -> None:
        op = rec.get("op")
        version = rec.get("v", WAL_VERSION_V1)  # 缺省视为历史 v1
        if op == "write":
            missing = [f for f in _WRITE_FIELDS if f not in rec]
            if missing:
                self.recovery_anomalies.append(
                    f"replica {self.replica_id}: write 记录缺少字段 {missing}，"
                    f"已跳过该记录（request_id={rec.get('request_id')!r}）"
                )
                return
            self._ingest(
                rec["key"], rec["value"], rec["request_id"],
                rec["timestamp"], bool(rec.get("committed", False)),
            )
        elif op == "commit":
            state = self.data.get(rec.get("key"))
            if state is None:
                self.recovery_anomalies.append(
                    f"replica {self.replica_id}: commit 标记 request_id="
                    f"{rec.get('request_id')!r} 找不到对应 write 记录"
                    f"（key={rec.get('key')!r}，可能来自未同步的落后数据），已忽略"
                )
                return
            if state.request_id != rec["request_id"]:
                self.recovery_anomalies.append(
                    f"replica {self.replica_id}: commit 标记 request_id="
                    f"{rec.get('request_id')!r} 不属于该键当前获胜写入 "
                    f"{state.request_id}，不覆盖已确认状态，已忽略"
                )
                return
            state.committed = True
        else:
            self.recovery_anomalies.append(
                f"replica {self.replica_id}: 未知 op={op!r}（format v{version}），"
                f"已跳过"
            )

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
            raise OSError(f"replica {self.replica_id} injected write fault")
        record = {
            "v": WAL_VERSION,
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
        self.log.append({"v": WAL_VERSION, "op": "commit", "key": key, "request_id": request_id})
        state.committed = True

    # 查询 -------------------------------------------------------------
    def get(self, key: str) -> ReplicaState | None:
        if not self.alive:
            raise RuntimeError(f"replica {self.replica_id} is down")
        return self.data.get(key)

    def has_request(self, request_id: str) -> bool:
        """该副本是否已接收过去重后的某个写入（对账时判断缺口）。"""
        return request_id in self.seen_requests

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
