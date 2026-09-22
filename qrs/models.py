"""数据类型与状态码定义。"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class CommitCode(str, Enum):
    """写入提交的判定结果。"""
    COMMITTED = "COMMITTED"                      # 首次提交，达到多数派
    REPLAYED = "REPLAYED"                        # 幂等键重复提交，返回首次结果
    CONTENT_MISMATCH = "CONTENT_MISMATCH"        # 同幂等键但内容不同
    QUORUM_FAILED = "QUORUM_FAILED"              # 已尽力复制但未达多数派
    NOT_ENOUGH_REPLICAS = "NOT_ENOUGH_REPLICAS"  # 存活副本数本身不足多数派


@dataclass
class WriteEntry:
    """一条针对某个数据键的写入内容。"""
    key: str
    value: str
    request_id: str
    timestamp: int
    idem_key: str


@dataclass
class CommitOutcome:
    """协调者对一次提交请求的最终判定。"""
    idem_key: str
    code: CommitCode
    request_id: str = ""
    key: str = ""
    value: str = ""
    timestamp: int | None = None
    acked: list[str] = field(default_factory=list)
    pending: list[str] = field(default_factory=list)
    quorum: int = 0
    reason: str = ""
    replay: bool = False


@dataclass
class ConflictRecord:
    """一次冲突（迟到/并发写入）及其解决结果。"""
    key: str
    winner_request_id: str
    winner_timestamp: int
    loser_request_id: str
    loser_timestamp: int
    rule: str
    detail: str


CONFLICT_RULE = (
    "LWW(timestamp, request_id): 同一数据键上 timestamp 较大者生效；"
    "timestamp 相同则 request_id 字典序较大者生效；败方写入记入冲突历史，不静默丢弃。"
)
