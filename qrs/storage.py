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
        self._fh = open(path, "a+b")

    def append(self, record: dict[str, Any]) -> None:
        """追加一条记录并持久化（flush + fsync）。"""
        line = json.dumps(record, ensure_ascii=False, sort_keys=True)
        self._fh.write(line.encode("utf-8") + b"\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def read_all(self) -> list[dict[str, Any]]:
        """读取全部记录（跳过可能存在的半行损坏）。"""
        self._fh.flush()
        records: list[dict[str, Any]] = []
        with open(self.path, "rb") as fh:
            for raw in fh:
                try:
                    records.append(json.loads(raw.decode("utf-8")))
                except json.JSONDecodeError:
                    # 模拟崩溃可能留下的半行：跳过，不影响已 fsync 的记录
                    continue
        return records

    def destroy(self) -> None:
        """关闭并删除日志文件（测试用）。"""
        try:
            self._fh.close()
        except OSError:
            pass
        if os.path.exists(self.path):
            os.remove(self.path)
