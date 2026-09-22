"""协调者：幂等键管理、多数派判定、冲突解决与重启恢复。

职责：
1. 幂等：同一 idem_key 的首次判定持久化到协调者本地日志，重复提交要么返回
   首次结果（内容一致），要么被拒绝（CONTENT_MISMATCH，可区分原因）。
2. 多数派：仅当多数副本确认持久化后才写入 commit 标记并对外返回成功；
   存活不足与尽力复制后仍未达多数派返回不同错误码。
3. 冲突：复制中按 LWW(timestamp, request_id) 全序解决，冲突记录集中保存，
   败方不静默丢弃；重启后重放收敛。
"""
from __future__ import annotations

import itertools
import threading
from typing import Callable

from qrs.models import CONFLICT_RULE, CommitCode, CommitOutcome, ConflictRecord
from qrs.replica import Replica
from qrs.storage import JsonlLog

_clock_seq = itertools.count(1)


def default_clock() -> int:
    """单调逻辑时钟（仅当调用方未提供 timestamp 时使用）。"""
    return next(_clock_seq)


class Coordinator:
    """进程内协调者，管理多个副本上的多数派写入。"""

    def __init__(
        self,
        replicas: list[Replica],
        commit_log: JsonlLog,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self.replicas = replicas
        self.commit_log = commit_log
        self._clock = clock or default_clock
        self.idem_table: dict[str, CommitOutcome] = {}
        self.conflicts: list[ConflictRecord] = []
        self._lock = threading.RLock()
        self._key_locks: dict[str, threading.Lock] = {}

    # 恢复 -------------------------------------------------------------
    def recover(self) -> None:
        """从协调者本地持久化恢复幂等表，并从各副本 WAL 重建冲突历史。"""
        with self._lock:
            self.idem_table = {}
            for rec in self.commit_log.read_all():
                self.idem_table[rec["idem_key"]] = CommitOutcome(
                    idem_key=rec["idem_key"],
                    code=CommitCode(rec["code"]),
                    request_id=rec["request_id"],
                    key=rec.get("key", ""),
                    value=rec["value"],
                    acked=list(rec.get("acked", [])),
                    pending=list(rec.get("pending", [])),
                    quorum=rec.get("quorum", 0),
                    reason=rec.get("reason", ""),
                    replay=rec.get("replay", False),
                    timestamp=rec.get("timestamp"),
                )
            self.conflicts = self._rebuild_conflicts()

    def _rebuild_conflicts(self) -> list[ConflictRecord]:
        """把每个副本的 WAL 重放进一个临时副本，重新导出确定性的冲突记录。"""
        conflicts: list[ConflictRecord] = []
        for rep in self.replicas:
            # 按 WAL 顺序把 write 记录重放进临时副本，重新导出确定性冲突。
            tmp = Replica(rep.replica_id, JsonlLog(rep.log.path + ".replay-tmp"))
            try:
                for rec in rep.log.read_all():
                    if rec.get("op") != "write":
                        continue
                    conflict = tmp.apply_write(
                        rec["key"], rec["value"], rec["request_id"],
                        rec["timestamp"], rec.get("committed", False),
                    )
                    if conflict is not None:
                        conflicts.append(conflict)
            finally:
                tmp.log.destroy()
        return conflicts

    # 写入 -------------------------------------------------------------
    def commit(
        self,
        idem_key: str,
        key: str,
        value: str,
        request_id: str,
        timestamp: int | None = None,
    ) -> CommitOutcome:
        """带幂等键的写入提交（对同一 idem_key 串行化）。"""
        with self._key_lock(idem_key):
            existing = self.idem_table.get(idem_key)
            if existing is not None:
                return self._handle_duplicate(existing, key, value, request_id)

            ts = self._clock() if timestamp is None else timestamp
            quorum = self.quorum
            alive = [r for r in self.replicas if r.alive]
            print(
                f"[commit] request_id={request_id} idem_key={idem_key} key={key} "
                f"ts={ts} alive={[r.replica_id for r in alive]} quorum={quorum}",
                flush=True,
            )

            if len(alive) < quorum:
                outcome = CommitOutcome(
                    idem_key=idem_key, code=CommitCode.NOT_ENOUGH_REPLICAS,
                    request_id=request_id, key=key, value=value,
                    quorum=quorum, timestamp=ts,
                    pending=[r.replica_id for r in alive],
                    reason=(
                        f"存活副本 {len(alive)} 个 < 多数派 {quorum}，"
                        f"不具备提交条件，写入未生效为成功"
                    ),
                )
                self._persist_outcome(outcome)
                print(
                    f"[commit] request_id={request_id} verdict={outcome.code.value} "
                    f"basis={outcome.reason}", flush=True,
                )
                return outcome

            acked, pending, conflicts = self._replicate(
                alive, key, value, request_id, ts,
            )
            self._record_conflicts(conflicts)
            print(
                f"[commit] request_id={request_id} acked={acked} pending={pending} "
                f"quorum={quorum}", flush=True,
            )

            if len(acked) >= quorum:
                self._publish_commit(key, request_id, acked)
                outcome = CommitOutcome(
                    idem_key=idem_key, code=CommitCode.COMMITTED,
                    request_id=request_id, key=key, value=value, timestamp=ts,
                    acked=acked,
                    pending=pending, quorum=quorum,
                    reason=(
                        f"{len(acked)}/{len(self.replicas)} 个副本确认持久化，"
                        f"达到多数派 {quorum}，并已对确认副本写入 commit 标记"
                    ),
                )
                self._persist_outcome(outcome)
                print(
                    f"[commit] request_id={request_id} verdict=COMMITTED "
                    f"basis={outcome.reason}", flush=True,
                )
                return outcome

            outcome = CommitOutcome(
                idem_key=idem_key, code=CommitCode.QUORUM_FAILED,
                request_id=request_id, key=key, value=value, timestamp=ts,
                acked=acked,
                pending=pending, quorum=quorum,
                reason=(
                    f"仅 {len(acked)}/{len(self.replicas)} 个副本确认，"
                    f"未达多数派 {quorum}；写入可能存在于少数副本，"
                    f"不对外暴露为成功，可用 resume 补复制"
                ),
            )
            self._persist_outcome(outcome)
            print(
                f"[commit] request_id={request_id} verdict=QUORUM_FAILED "
                f"basis={outcome.reason}", flush=True,
            )
            return outcome

    def resume(self, idem_key: str) -> CommitOutcome:
        """对已部分生效但未达多数派的写入继续补复制，驱动收敛。"""
        with self._key_lock(idem_key):
            stored = self.idem_table.get(idem_key)
            if stored is None:
                raise KeyError(f"未知幂等键: {idem_key}")
            if stored.code is CommitCode.COMMITTED:
                return stored
            key = stored.key or self._stored_key(idem_key) or ""
            value = stored.value

            alive = [r for r in self.replicas if r.alive]
            acked, pending, conflicts = self._replicate(
                alive, key, value, stored.request_id, stored.timestamp or 0,
            )
            self._record_conflicts(conflicts)
            print(
                f"[resume] request_id={stored.request_id} acked={acked} "
                f"pending={pending} quorum={stored.quorum}", flush=True,
            )

            if len(acked) >= stored.quorum:
                self._publish_commit(key, stored.request_id, acked)
                upgraded = CommitOutcome(
                    idem_key=idem_key, code=CommitCode.COMMITTED,
                    request_id=stored.request_id, key=key, value=value,
                    timestamp=stored.timestamp, acked=acked,
                    pending=pending, quorum=stored.quorum,
                    reason=(
                        f"补复制后 {len(acked)}/{len(self.replicas)} 个副本确认，"
                        f"达到多数派 {stored.quorum}"
                    ),
                )
                self._persist_outcome(upgraded)
                return upgraded

            failed = CommitOutcome(
                idem_key=idem_key,
                code=(CommitCode.NOT_ENOUGH_REPLICAS
                      if len(alive) < stored.quorum
                      else CommitCode.QUORUM_FAILED),
                request_id=stored.request_id, key=key, value=value,
                timestamp=stored.timestamp, acked=acked,
                pending=pending, quorum=stored.quorum,
                reason="补复制后仍未达到多数派",
            )
            self._persist_outcome(failed)
            return failed

    # 查询 -------------------------------------------------------------
    def get(self, idem_key: str) -> CommitOutcome | None:
        """按幂等键查询首次提交结果。"""
        with self._lock:
            return self.idem_table.get(idem_key)

    def read_key(self, key: str) -> dict | None:
        """读取数据键当前值：同一 request_id 在多数存活副本上为已提交才可见。"""
        with self._lock:
            votes: dict[str, list[Replica]] = {}
            unreachable: list[str] = []
            for rep in self.replicas:
                if not rep.alive:
                    unreachable.append(rep.replica_id)
                    continue
                state = rep.data.get(key)
                if state is not None:
                    votes.setdefault(state.request_id, []).append(rep)
            if not votes:
                return None
            winner_rid, holders = max(
                votes.items(), key=lambda kv: (len(kv[1]), kv[0]),
            )
            committed_holders = [
                r.replica_id for r in holders if r.data[key].committed
            ]
            sample = holders[0].data[key]
            visible = len(committed_holders) >= self.quorum
            return {
                "key": key,
                "value": sample.value,
                "request_id": winner_rid,
                "timestamp": sample.timestamp,
                "visible": visible,
                "committed_replicas": committed_holders,
                "holder_count": len(holders),
                "unreachable_replicas": unreachable,
                "basis": (
                    f"request_id={winner_rid} 在 {len(committed_holders)}/"
                    f"{len(self.replicas)} 个副本上已提交"
                    + ("，达到多数派，读取可见" if visible
                       else "，未达多数派，读取不可见（可能是少数派残留）")
                ),
            }

    def get_conflicts(self, key: str | None = None) -> list[ConflictRecord]:
        """查询冲突及其解决结果（可按数据键过滤）。"""
        with self._lock:
            if key is None:
                return list(self.conflicts)
            return [c for c in self.conflicts if c.key == key]

    # 内部 -------------------------------------------------------------
    @property
    def quorum(self) -> int:
        return len(self.replicas) // 2 + 1

    def _key_lock(self, idem_key: str) -> threading.Lock:
        with self._lock:
            lock = self._key_locks.get(idem_key)
            if lock is None:
                lock = threading.Lock()
                self._key_locks[idem_key] = lock
            return lock

    def _handle_duplicate(
        self, existing: CommitOutcome, key: str, value: str, request_id: str
    ) -> CommitOutcome:
        if (key, value) != (existing.key, existing.value):
            mismatch = CommitOutcome(
                idem_key=existing.idem_key,
                code=CommitCode.CONTENT_MISMATCH,
                request_id=existing.request_id,
                key=existing.key,
                value=existing.value, timestamp=existing.timestamp,
                acked=list(existing.acked),
                pending=list(existing.pending), quorum=existing.quorum,
                reason=(
                    f"幂等键 {existing.idem_key} 已绑定首次提交 "
                    f"(key={existing.key}, request_id={existing.request_id})；"
                    f"本次提交 (key={key}, request_id={request_id}) 内容不同，拒绝重复生效"
                ),
            )
            print(
                f"[commit] request_id={request_id} idem_key={existing.idem_key} "
                f"verdict=CONTENT_MISMATCH basis=与首次提交内容不一致", flush=True,
            )
            return mismatch
        print(
            f"[commit] request_id={request_id} idem_key={existing.idem_key} "
            f"verdict=REPLAY({existing.code.value}) basis=与首次提交(request_id="
            f"{existing.request_id})完全一致，重放其首次结论",
            flush=True,
        )
        # 首次若失败，重复提交必须保留失败结论（绝不能以 200 掩盖未达多数派）；
        # 仅首次成功时以 REPLAYED 明确表达“幂等重放”。
        replay_code = (
            CommitCode.REPLAYED
            if existing.code is CommitCode.COMMITTED
            else existing.code
        )
        return CommitOutcome(
            idem_key=existing.idem_key, code=replay_code,
            request_id=existing.request_id, key=existing.key,
            value=existing.value, timestamp=existing.timestamp,
            acked=list(existing.acked), pending=list(existing.pending),
            quorum=existing.quorum, reason=existing.reason, replay=True,
        )

    def _replicate(
        self, targets: list[Replica], key: str, value: str,
        request_id: str, timestamp: int,
    ) -> tuple[list[str], list[str], list[ConflictRecord]]:
        acked: list[str] = []
        pending: list[str] = []
        conflicts: list[ConflictRecord] = []
        for rep in targets:
            try:
                conflict = rep.apply_write(key, value, request_id, timestamp, False)
            except (RuntimeError, IOError) as exc:
                pending.append(rep.replica_id)
                print(
                    f"[replicate] request_id={request_id} -> {rep.replica_id} "
                    f"FAILED: {exc}", flush=True,
                )
                continue
            acked.append(rep.replica_id)
            print(
                f"[replicate] request_id={request_id} -> {rep.replica_id} ACK",
                flush=True,
            )
            if conflict is not None:
                conflicts.append(conflict)
        return acked, pending, conflicts

    def _publish_commit(
        self, key: str, request_id: str, acked: list[str],
    ) -> None:
        for rep in self.replicas:
            if not rep.alive or rep.replica_id not in acked:
                continue
            try:
                rep.mark_committed(key, request_id)
            except (RuntimeError, IOError) as exc:
                print(
                    f"[commit-marker] request_id={request_id} -> "
                    f"{rep.replica_id} FAILED: {exc}", flush=True,
                )

    def _record_conflicts(self, conflicts: list[ConflictRecord]) -> None:
        with self._lock:
            self.conflicts.extend(conflicts)

    def _persist_outcome(self, outcome: CommitOutcome) -> None:
        with self._lock:
            self.commit_log.append({
                "idem_key": outcome.idem_key,
                "code": outcome.code.value,
                "request_id": outcome.request_id,
                "key": outcome.key,
                "value": outcome.value,
                "timestamp": outcome.timestamp,
                "acked": outcome.acked,
                "pending": outcome.pending,
                "quorum": outcome.quorum,
                "reason": outcome.reason,
                "replay": outcome.replay,
            })
            self.idem_table[outcome.idem_key] = outcome

    def _stored_key(self, idem_key: str) -> str | None:
        for rec in self.commit_log.read_all():
            if rec["idem_key"] == idem_key:
                return rec["key"]
        return None

