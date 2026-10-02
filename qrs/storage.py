"""本地持久化：每行一个 JSON 记录的追加日志，写入即 fsync。

用独立的 JSONL 文件模拟每个节点（副本/协调者）的本地磁盘，
进程崩溃重启后通过重放日志恢复状态。
"""
from __future__ import annotations

import json
import os
from typing import Any


class JsonlLog:
    """基于 JSONL 文件的持久追加日志。"""

    def __init__(self, path: str) -> None:
        self.path = path
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        # 以追加二进制方式打开，每行一条 JSON，写完 flush + fsync。
        # 句柄按设计长期持有（destroy 时关闭），不使用 with。
        self._fh = open(path, "a+b")  # noqa: SIM115

    def append(self, record: dict[str, Any]) -> None:
        """追加一条记录并持久化（flush + fsync）。"""
        line = json.dumps(record, ensure_ascii=False, sort_keys=True)
        self._fh.write(line.encode("utf-8") + b"\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def read_all(self) -> list[dict[str, Any]]:
        """读取全部记录（跳过可能存在的半行损坏）。"""
        records, _ = self.read_all_with_errors()
        return records

    def read_all_with_errors(self) -> tuple[list[dict[str, Any]], list[str]]:
        """读取全部记录，返回 (记录列表, 被跳过的损坏行描述)。

        崩溃可能留下半行 JSON；这里跳过它们并记录行号与内容前缀，
        供启动时的恢复校验报告使用，已 fsync 的完整记录不受影响。
        """
        self._fh.flush()
        records: list[dict[str, Any]] = []
        skipped: list[str] = []
        with open(self.path, "rb") as fh:
            for lineno, raw in enumerate(fh, start=1):
                try:
                    records.append(json.loads(raw.decode("utf-8")))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    skipped.append(
                        f"{self.path}:{lineno}: 半行/损坏记录已跳过 "
                        f"({len(raw)} bytes)"
                    )
                    continue
        return records, skipped

    def append_raw_line(self, line: str) -> None:
        """直接追加一行原始文本（测试用于播种历史版本落盘数据）。"""
        self._fh.write(line.encode("utf-8") + b"\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def destroy(self) -> None:
        """关闭并删除日志文件（测试用）。"""
        try:
            self._fh.close()
        except OSError:
            pass
        if os.path.exists(self.path):
            os.remove(self.path)
