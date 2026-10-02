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


class WriteStatus(str, Enum):
    """一个幂等键对应写入请求的后续收敛状态（可随恢复推进而变化）。"""
    SUCCESS = "SUCCESS"   # 已达多数派并发布 commit，最终成功
    WAITING = "WAITING"   # 尚未达多数派，条件恢复后仍可自动/手动推进收敛
    FAILED = "FAILED"     # 终态失败：已被同键更新的已提交写入按 LWW 取代，永不生效


# WAL/日志记录的落盘格式版本：
# - v1（历史格式）：write 记录无 "v" 字段，commit 记录仅 key/request_id
# - v2：所有记录带 "v": 2，重放时按版本归一化，保证旧数据可继续启动与读取
WAL_VERSION_V1 = 1
WAL_VERSION = 2


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


@dataclass
class WriteStatusView:
    """未达多数派写入的当前收敛状态视图：成功/等待/失败、卡点与确认进度。"""
    idem_key: str
    request_id: str
    key: str
    value: str
    timestamp: int | None
    status: WriteStatus
    code: CommitCode
    step: str
    acked: list[str] = field(default_factory=list)
    pending: list[str] = field(default_factory=list)
    quorum: int = 0
    readable: bool = False
    basis: str = ""
    superseded_by: str = ""


@dataclass
class ReconcileResult:
    """一次自动对账的结果：补齐了哪些缺口、推进了哪些写入。"""
    backfilled: list[str] = field(default_factory=list)   # "request_id -> replica-x"
    upgraded: list[str] = field(default_factory=list)     # 升级为 COMMITTED 的 request_id
    failed: list[str] = field(default_factory=list)       # 判定为终态 FAILED 的 request_id
    waiting: list[str] = field(default_factory=list)      # 仍在等待条件恢复的 request_id
    conflicts: int = 0

    @property
    def changed(self) -> bool:
        return bool(self.backfilled or self.upgraded or self.failed)
