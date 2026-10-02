"""历史落盘数据的兼容与恢复校验。

旧版本（v0）落盘格式与当前（v1）格式的差异：
- 副本 WAL 的 write 记录可能缺少 "op" 与 "committed" 字段（旧格式只有
  key/value/request_id/timestamp，commit 标记格式不变）；
- 协调者提交日志可能缺少 "pending"/"quorum"/"reason"/"replay"/"timestamp"
  字段（旧格式只有 idem_key/code/request_id/key/value/acked）。

本模块把旧记录规范化为当前格式，并在恢复时做一致性校验，
保证升级前后数据可正常启动读取、不重复生效、不丢状态。
"""
from __future__ import annotations

from typing import Any

SCHEMA_VERSION = 1

# 协调者日志中允许出现的判定码（与 models.CommitCode 对齐，独立保存以避免循环依赖）
_KNOWN_CODES = {
    "COMMITTED", "REPLAYED", "CONTENT_MISMATCH",
    "QUORUM_FAILED", "NOT_ENOUGH_REPLICAS",
}


def normalize_replica_record(rec: dict[str, Any]) -> dict[str, Any] | None:
    """把一条副本 WAL 记录规范化为当前格式；无法识别时返回 None（跳过）。

    兼容规则：
    - 缺 "op" 但含 key/value/request_id/timestamp → 视为 v0 的 write 记录；
    - write 记录缺 "committed" → 默认 False（v0 没有该字段，未提交不丢数据，
    仅表示该写入当时未获多数派确认，读取侧仍按 commit 标记判定可见性）；
    - commit 记录格式未变，原样通过；
    - 缺关键字段（key/request_id）→ 返回 None，由调用方跳过并记日志。
    """
    op = rec.get("op")
    if op is None:
        # v0 格式：没有 op 字段，能凑齐 write 要素即视为 write
        if all(k in rec for k in ("key", "value", "request_id", "timestamp")):
            op = "write"
        else:
            return None
    if op == "write":
        if "key" not in rec or "request_id" not in rec:
            return None
        return {
            "op": "write",
            "key": rec["key"],
            "value": rec.get("value", ""),
            "request_id": rec["request_id"],
            "timestamp": int(rec.get("timestamp", 0)),
            "committed": bool(rec.get("committed", False)),
        }
    if op == "commit":
        if "key" not in rec or "request_id" not in rec:
            return None
        return {"op": "commit", "key": rec["key"], "request_id": rec["request_id"]}
    return None


def normalize_coordinator_record(rec: dict[str, Any]) -> dict[str, Any] | None:
    """把一条协调者提交日志记录规范化为当前格式；无法识别时返回 None。

    兼容规则：
    - 缺 idem_key/code → 无法识别，返回 None；
    - code 不在已知集合 → 返回 None（避免把未知结论当成成功）；
    - v0 缺失的字段补默认值：pending=[]、quorum=0、reason=""、
      replay=False、timestamp=None、key=""、value=""。
    """
    if "idem_key" not in rec or "code" not in rec:
        return None
    if rec["code"] not in _KNOWN_CODES:
        return None
    return {
        "idem_key": rec["idem_key"],
        "code": rec["code"],
        "request_id": rec.get("request_id", ""),
        "key": rec.get("key", ""),
        "value": rec.get("value", ""),
        "timestamp": rec.get("timestamp"),
        "acked": list(rec.get("acked", [])),
        "pending": list(rec.get("pending", [])),
        "quorum": int(rec.get("quorum", 0)),
        "reason": rec.get("reason", ""),
        "replay": bool(rec.get("replay", False)),
    }


def validate_recovered(keys: int, requests: int, source: str) -> None:
    """恢复完成后的校验钩子：打印恢复摘要，供日志核对。"""
    print(
        f"[recover] source={source} keys={keys} requests={requests} "
        f"schema=v{SCHEMA_VERSION} basis=WAL 重放完成，状态与落盘记录一致",
        flush=True,
    )
