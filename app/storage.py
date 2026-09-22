"""单副本本地持久化：JSON Lines WAL + 目录管理。

每个副本一个目录，日志按行追加并 fsync；重启时全量重放重建内存状态。
日志行格式：
    {"kind": "entry",    "entry":    LogEntry.to_dict()}
    {"kind": "conflict", "conflict": ConflictRecord.to_dict()}
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path

from app.models import ConflictRecord, EntryPhase, LogEntry


class ReplicaStorage:
    """单个副本的持久化日志。"""

    LOG_NAME = "wal.jsonl"

    def __init__(self, replica_dir: str | os.PathLike) -> None:
        self._dir = Path(replica_dir)
        self._lock = threading.Lock()
        self._seq = 0
        self._dir.mkdir(parents=True, exist_ok=True)
        # 重放一次以拿到当前最大 seq
        entries, _ = self.replay()
        if entries:
            self._seq = max(e.seq for e in entries)

    @property
    def log_path(self) -> Path:
        return self._dir / self.LOG_NAME

    def reset(self) -> None:
        """清空日志（测试用）。"""
        with self._lock:
            if self.log_path.exists():
                self.log_path.unlink()
            self._seq = 0

    def _append_line(self, line: dict) -> None:
        with self._lock, open(self.log_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(line, ensure_ascii=False, sort_keys=True))
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())

    def append_entry(self, entry: LogEntry) -> None:
        """追加一条 WAL 记录并落盘。"""
        self._append_line({"kind": "entry", "entry": entry.to_dict()})

    def append_conflict(self, record: ConflictRecord) -> None:
        """追加一条冲突裁决记录并落盘。"""
        self._append_line({"kind": "conflict", "conflict": record.to_dict()})

    def replay(self) -> tuple[list[LogEntry], list[ConflictRecord]]:
        """从磁盘重放全部记录，返回按 seq 排序的条目与冲突记录。

        重放天然幂等：同一幂等键的后出现阶段覆盖先出现的阶段；
        重复/乱序投递的行不会造成重复生效。
        """
        entries_by_key: dict[str, LogEntry] = {}
        conflicts: list[ConflictRecord] = []
        if not self.log_path.exists():
            return [], []
        with open(self.log_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if record["kind"] == "entry":
                    entry = LogEntry.from_dict(record["entry"])
                    self._seq = max(self._seq, entry.seq)
                    prev = entries_by_key.get(entry.idempotency_key)
                    if prev is None or entry.seq >= prev.seq:
                        entries_by_key[entry.idempotency_key] = entry
                elif record["kind"] == "conflict":
                    conflicts.append(ConflictRecord.from_dict(record["conflict"]))
        entries = sorted(entries_by_key.values(), key=lambda e: e.seq)
        return entries, conflicts

    def next_seq(self) -> int:
        with self._lock:
            self._seq += 1
            return self._seq
