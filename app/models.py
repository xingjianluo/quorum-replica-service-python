"""领域模型：状态枚举与可序列化数据结构。"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass, field
from enum import Enum


class WriteStatus(str, Enum):
    """写入/查询的对外判定结论。"""

    COMMITTED = "COMMITTED"
    REJECTED_CONTENT_CONFLICT = "REJECTED_CONTENT_CONFLICT"
    FAILED_QUORUM = "FAILED_QUORUM"
    INSUFFICIENT_REPLICAS = "INSUFFICIENT_REPLICAS"
    NOT_FOUND = "NOT_FOUND"


class EntryPhase(str, Enum):
    """副本上一条写入记录的生命周期阶段。"""

    PREPARED = "PREPARED"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"


def payload_fingerprint(payload: str) -> str:
    """对写入内容计算稳定指纹，用于区分“同键不同内容”。"""
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass
class WriteRequest:
    """一次写入提交请求。"""

    idempotency_key: str
    data_key: str
    payload: str
    timestamp_ns: int
    request_id: str

    def fingerprint(self) -> str:
        return payload_fingerprint(self.payload)


@dataclass
class LogEntry:
    """副本 WAL 中的一条记录（prepare/commit/abort）。"""

    seq: int
    request_id: str
    idempotency_key: str
    data_key: str
    payload: str
    fingerprint: str
    timestamp_ns: int
    phase: EntryPhase

    def to_dict(self) -> dict:
        return {
            "seq": self.seq,
            "request_id": self.request_id,
            "idempotency_key": self.idempotency_key,
            "data_key": self.data_key,
            "payload": self.payload,
            "fingerprint": self.fingerprint,
            "timestamp_ns": self.timestamp_ns,
            "phase": self.phase.value,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "LogEntry":
        return cls(
            seq=int(raw["seq"]),
            request_id=raw["request_id"],
            idempotency_key=raw["idempotency_key"],
            data_key=raw["data_key"],
            payload=raw["payload"],
            fingerprint=raw["fingerprint"],
            timestamp_ns=int(raw["timestamp_ns"]),
            phase=EntryPhase(raw["phase"]),
        )


@dataclass
class ConflictRecord:
    """同一 data_key 上两个已提交写入之间的冲突及其确定性裁决。"""

    data_key: str
    winner_request_id: str
    winner_idempotency_key: str
    loser_request_id: str
    loser_idempotency_key: str
    winner_timestamp_ns: int
    loser_timestamp_ns: int
    rule: str

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict) -> "ConflictRecord":
        return cls(
            data_key=raw["data_key"],
            winner_request_id=raw["winner_request_id"],
            winner_idempotency_key=raw["winner_idempotency_key"],
            loser_request_id=raw["loser_request_id"],
            loser_idempotency_key=raw["loser_idempotency_key"],
            winner_timestamp_ns=int(raw["winner_timestamp_ns"]),
            loser_timestamp_ns=int(raw["loser_timestamp_ns"]),
            rule=raw["rule"],
        )


@dataclass
class WriteResult:
    """协调器对一次提交或查询的结论。"""

    status: WriteStatus
    request_id: str
    idempotency_key: str
    data_key: str | None = None
    payload: str | None = None
    timestamp_ns: int | None = None
    acked_replicas: list[str] = field(default_factory=list)
    quorum: int = 0
    total_replicas: int = 0
    alive_replicas: int = 0
    reason: str = ""
    duplicate: bool = False

    def to_dict(self) -> dict:
        return {
            "status": self.status.value,
            "request_id": self.request_id,
            "idempotency_key": self.idempotency_key,
            "data_key": self.data_key,
            "payload": self.payload,
            "timestamp_ns": self.timestamp_ns,
            "acked_replicas": list(self.acked_replicas),
            "quorum": self.quorum,
            "total_replicas": self.total_replicas,
            "alive_replicas": self.alive_replicas,
            "reason": self.reason,
            "duplicate": self.duplicate,
        }
