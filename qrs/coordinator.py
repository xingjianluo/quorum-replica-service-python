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
from collections.abc import Callable
from functools import cmp_to_key

from qrs.models import (
    CommitCode,
    CommitOutcome,
    ConflictRecord,
    ReconcileResult,
    WriteEntry,
    WriteStatus,
    WriteStatusView,
)
from qrs.replica import Replica
from qrs.storage import JsonlLog

_clock_seq = itertools.count(1)


def default_clock() -> int:
    """单调逻辑时钟（仅当调用方未提供时使用）。"""
    return next(_clock_seq)


def _cmp_pending(a: CommitOutcome, b: CommitOutcome) -> int:
    """同键内按 LWW 全序从新到旧；不同键按 key 稳定分组（处理顺序不影响正确性）。"""
    if a.key != b.key:
        return -1 if a.key < b.key else 1
    ra = (a.timestamp or 0, a.request_id)
    rb = (b.timestamp or 0, b.request_id)
    if ra == rb:
        return 0
    return 1 if ra < rb else -1  # 全序更靠后（更新）的排在前面


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
        # 全局写入账本：request_id -> 已持久化的写入内容（含未达多数派者），
        # 恢复时从协调者日志重建，是自动对账的缺口补齐来源。
        self.write_ledger: dict[str, WriteEntry] = {}
        self._lock = threading.RLock()
        self._key_locks: dict[str, threading.Lock] = {}

    # 恢复 -------------------------------------------------------------
    def recover(self) -> None:
        """从协调者本地持久化恢复幂等表/写入账本，并从各副本 WAL 重建冲突历史。

        旧版本（字段缺失）落盘记录按缺省值归一化，保证升级后可启动、可读取，
        不重复生效、不丢已确认状态。
        """
        with self._lock:
            self.idem_table = {}
            self.write_ledger = {}
            for rec in self.commit_log.read_all():
                outcome = CommitOutcome(
                    idem_key=rec["idem_key"],
                    code=CommitCode(rec["code"]),
                    request_id=rec["request_id"],
                    key=rec.get("key", ""),
                    value=rec.get("value", ""),
                    acked=list(rec.get("acked", [])),
                    pending=list(rec.get("pending", [])),
                    quorum=rec.get("quorum", 0),
                    reason=rec.get("reason", ""),
                    replay=rec.get("replay", False),
                    timestamp=rec.get("timestamp"),
                )
                # 同一 idem_key 的后写记录是结论升级（resume/对账）；账本以
                # request_id 去重，首次出现的顺序即历史投递顺序。
                prev = self.idem_table.get(outcome.idem_key)
                self.idem_table[outcome.idem_key] = outcome
                if (
                    outcome.request_id
                    and outcome.request_id not in self.write_ledger
                    and outcome.key
                ):
                    self.write_ledger[outcome.request_id] = WriteEntry(
                        key=outcome.key, value=outcome.value,
                        request_id=outcome.request_id,
                        timestamp=outcome.timestamp or 0,
                        idem_key=outcome.idem_key,
                    )
                if prev is not None and prev.request_id != outcome.request_id:
                    # 理论上同一 idem_key 不会换 request_id；防御性记录。
                    print(
                        f"[recover] idem_key={outcome.idem_key} 历史记录 request_id "
                        f"由 {prev.request_id} 变为 {outcome.request_id}，以最新为准",
                        flush=True,
                    )
            self.conflicts = self._rebuild_conflicts()
            print(
                f"[recover] coordinator 恢复完成：idem_keys={len(self.idem_table)} "
                f"ledger_requests={len(self.write_ledger)} "
                f"conflicts={len(self.conflicts)}",
                flush=True,
            )

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
            # 与自动对账相同的终态判断：已被更新的已提交写入 LWW 取代者不再推进。
            superseder = self._committed_superseder(stored)
            if superseder is not None:
                basis = (
                    f"request_id={stored.request_id} 已被同键更新的已提交写入 "
                    f"{superseder} 按 LWW 取代，终态 FAILED，不再补复制"
                )
                failed = CommitOutcome(
                    idem_key=idem_key, code=CommitCode.QUORUM_FAILED,
                    request_id=stored.request_id, key=key, value=value,
                    timestamp=stored.timestamp, acked=list(stored.acked),
                    pending=list(stored.pending), quorum=stored.quorum,
                    reason=basis,
                )
                self._persist_outcome(failed)
                print(
                    f"[resume] request_id={stored.request_id} "
                    f"verdict=FAILED basis={basis}", flush=True,
                )
                return failed

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

    # 自动对账 ----------------------------------------------------------
    def reconcile(self) -> ReconcileResult:
        """自动对账：补齐落后副本缺口，并把可收敛的未达多数派写入推进到底。

        - 按全局账本的历史投递顺序向所有存活副本补齐它们缺失的写入；
          重复投递在副本上以 request_id 幂等去重，不会重复生效。
        - 补齐后重新统计每个已持久化写入的确认副本数：
          达到多数派则发布 commit 标记并把判定升级为 COMMITTED（原首次
          判定若是 QUORUM_FAILED 也随之以最终结果为准，同一幂等键仍只有一个结论）；
          已被同键更新的已提交写入按 LWW 取代者判为终态 FAILED，不再等待；
          其余仍不具备条件的保持 WAITING。
        - 整个过程不向读取入口发布未确认内容：commit 标记只在凑够多数派时写入。
        """
        with self._lock:
            result = ReconcileResult()
            alive = [r for r in self.replicas if r.alive]

            # 1) 缺口补齐：账本按插入顺序（=历史投递顺序）回放给有缺口的存活副本。
            for rid, entry in self.write_ledger.items():
                for rep in alive:
                    if rep.has_request(rid):
                        continue
                    try:
                        conflict = rep.apply_write(
                            entry.key, entry.value, rid,
                            entry.timestamp, False,
                        )
                    except (OSError, RuntimeError) as exc:
                        print(
                            f"[reconcile] backfill request_id={rid} -> "
                            f"{rep.replica_id} FAILED: {exc}", flush=True,
                        )
                        continue
                    result.backfilled.append(f"{rid} -> {rep.replica_id}")
                    print(
                        f"[reconcile] backfill request_id={rid} key={entry.key} "
                        f"-> {rep.replica_id} ACK（补齐缺口）", flush=True,
                    )
                    if conflict is not None:
                        self.conflicts.append(conflict)
                        result.conflicts += 1

            # 2) 已达多数派的写入，把 commit 标记补发给刚补齐/此前漏标的存活副本
            #    （mark_committed 本身幂等，不会重复生效）。
            for o in self.idem_table.values():
                if o.code is CommitCode.COMMITTED and o.key:
                    self._publish_commit(
                        o.key, o.request_id,
                        [r.replica_id for r in alive],
                    )

            # 3) 按写入重估判定，推进未达多数派的写入。
            result = self._advance_pending(alive, result)
            print(
                f"[reconcile] done: backfilled={len(result.backfilled)} "
                f"upgraded={result.upgraded} failed={result.failed} "
                f"waiting={result.waiting}",
                flush=True,
            )
            return result

    def _advance_pending(
        self, alive: list[Replica], result: ReconcileResult,
    ) -> ReconcileResult:
        """根据补齐后的副本状态，重估每个非 COMMITTED 幂等键的最终判定。

        同一数据键上按 LWW 全序从新到旧处理：更新的写入先判定是否升级，
        旧写入随后才能识别出“已被更新的已提交写入取代”的终态 FAILED。
        """
        alive_ids = {r.replica_id for r in alive}
        pending_items = [
            o for o in self.idem_table.values()
            if o.code is not CommitCode.COMMITTED and o.request_id and o.key
        ]
        pending_items.sort(key=cmp_to_key(_cmp_pending))
        for stored in pending_items:
            idem_key = stored.idem_key
            rid = stored.request_id
            holders: list[str] = []
            committed_holders: list[str] = []
            for rep in alive:
                state = rep.data.get(stored.key)
                if state is None or state.request_id != rid:
                    continue
                holders.append(rep.replica_id)
                if state.committed:
                    committed_holders.append(rep.replica_id)

            superseder = self._committed_superseder(stored)
            if superseder is not None:
                verdict = WriteStatus.FAILED
                step = f"已被同键更新的已提交写入 {superseder} 按 LWW 取代"
                basis = (
                    f"request_id={rid} 在 LWW 全序上落败于已提交写入 "
                    f"{superseder}，判定终态 FAILED，不再补复制、不可见"
                )
                self._record_status_outcome(
                    idem_key, stored, CommitCode.QUORUM_FAILED,
                    holders, sorted(alive_ids - set(holders)), basis,
                )
                result.failed.append(rid)
            elif len(holders) >= stored.quorum:
                self._publish_commit(stored.key, rid, holders)
                committed_holders = holders
                basis = (
                    f"自动对账后 {len(holders)}/{len(self.replicas)} 个副本持有并已"
                    f"发布 commit 标记，达到多数派 {stored.quorum}，判定升级为 "
                    f"COMMITTED（历史未达多数派判定以最终结果为准）"
                )
                upgraded = CommitOutcome(
                    idem_key=idem_key, code=CommitCode.COMMITTED,
                    request_id=rid, key=stored.key, value=stored.value,
                    timestamp=stored.timestamp, acked=holders,
                    pending=sorted(alive_ids - set(holders)),
                    quorum=stored.quorum, reason=basis,
                )
                self._persist_outcome(upgraded)
                result.upgraded.append(rid)
                verdict = WriteStatus.SUCCESS
                step = "已发布 commit 标记"
            else:
                verdict = WriteStatus.WAITING
                if len(alive) < stored.quorum:
                    step = (
                        f"等待存活副本恢复：存活 {len(alive)} < 多数派 "
                        f"{stored.quorum}"
                    )
                else:
                    step = (
                        f"等待补复制确认：持有 {len(holders)}/"
                        f"{stored.quorum} 个多数派副本"
                    )
                basis = (
                    f"request_id={rid} 当前仅 {len(holders)} 个副本持有"
                    f"（多数派 {stored.quorum}），未发布 commit；读取入口不可见，"
                    f"条件恢复后自动对账继续推进"
                )
                result.waiting.append(rid)
            print(
                f"[reconcile] request_id={rid} idem_key={idem_key} "
                f"verdict={verdict.value} holders={holders} "
                f"committed={committed_holders} basis={step if verdict is not WriteStatus.SUCCESS else '已发布 commit 标记'}",
                flush=True,
            )
        return result

    def _committed_superseder(self, stored: CommitOutcome) -> str | None:
        """若同数据键上存在 LWW 全序更靠后且已达多数派的写入，返回其 request_id。"""
        for other in self.idem_table.values():
            if (
                other.code is CommitCode.COMMITTED
                and other.key == stored.key
                and other.request_id != stored.request_id
                and (other.timestamp or 0, other.request_id)
                > (stored.timestamp or 0, stored.request_id)
            ):
                return other.request_id
        return None

    def _record_status_outcome(
        self,
        idem_key: str,
        stored: CommitOutcome,
        code: CommitCode,
        acked: list[str],
        pending: list[str],
        reason: str,
    ) -> None:
        """把对账后的失败判定持久化（幂等键结论更新，request_id 不变）。"""
        updated = CommitOutcome(
            idem_key=idem_key, code=code,
            request_id=stored.request_id, key=stored.key, value=stored.value,
            timestamp=stored.timestamp, acked=acked, pending=pending,
            quorum=stored.quorum, reason=reason,
        )
        self._persist_outcome(updated)

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

    def write_status(self, idem_key: str) -> WriteStatusView | None:
        """查询某次写入当前是 SUCCESS / WAITING / FAILED、卡在哪一步、确认进度。

        状态基于各存活副本的实时持有/提交情况判定，不改变任何状态；
        WAITING 期间读取入口始终看不到未确认内容。
        """
        with self._lock:
            stored = self.idem_table.get(idem_key)
            if stored is None:
                return None
            return self._build_status(stored)

    def list_pending(self) -> list[WriteStatusView]:
        """列出所有尚未最终成功的写入状态（WAITING / FAILED）。"""
        with self._lock:
            return [
                self._build_status(o)
                for o in self.idem_table.values()
                if o.code is not CommitCode.COMMITTED
            ]

    def _build_status(self, stored: CommitOutcome) -> WriteStatusView:
        rid = stored.key and stored.request_id
        holders: list[str] = []
        committed: list[str] = []
        down: list[str] = []
        for rep in self.replicas:
            if not rep.alive:
                down.append(rep.replica_id)
                continue
            state = rep.data.get(stored.key)
            if state is not None and state.request_id == rid:
                holders.append(rep.replica_id)
                if state.committed:
                    committed.append(rep.replica_id)

        superseder = self._committed_superseder(stored) if rid else None
        alive_count = len(self.replicas) - len(down)
        if stored.code is CommitCode.COMMITTED or len(committed) >= stored.quorum:
            status = WriteStatus.SUCCESS
            step = "已达多数派并发布 commit 标记"
        elif superseder is not None:
            status = WriteStatus.FAILED
            step = f"已被更新的已提交写入 {superseder} 按 LWW 取代"
        elif alive_count < stored.quorum:
            status = WriteStatus.WAITING
            step = (
                f"卡在复制确认：存活副本 {alive_count} < 多数派 {stored.quorum}，"
                f"宕机副本 {down}"
            )
        else:
            status = WriteStatus.WAITING
            step = (
                f"卡在复制确认：未达多数派 {stored.quorum}，"
                f"{len(committed)}/{stored.quorum} 个副本已提交，"
                f"{len(holders)} 个副本持有未确认数据，宕机副本 {down}"
            )

        pending = [
            r.replica_id for r in self.replicas
            if r.alive and r.replica_id not in holders
        ] + down
        readable = len(committed) >= stored.quorum
        if status is WriteStatus.SUCCESS:
            basis = (
                f"request_id={rid} 在 {len(committed)}/{len(self.replicas)} 个副本上"
                f"带 commit 标记，达到多数派 {stored.quorum}"
            )
        elif status is WriteStatus.FAILED:
            basis = (
                f"request_id={rid} 落败于已提交写入 {superseder}（LWW），"
                f"永不生效；当前持有副本 {holders}"
            )
        else:
            basis = (
                f"request_id={rid} 尚未达多数派 {stored.quorum}："
                f"已确认提交副本 {committed}，持有未确认副本 "
                f"{[h for h in holders if h not in committed]}，"
                f"缺失/宕机副本 {pending}；读取入口不可见，恢复后自动对账推进"
            )
        return WriteStatusView(
            idem_key=stored.idem_key, request_id=rid, key=stored.key,
            value=stored.value, timestamp=stored.timestamp, status=status,
            code=stored.code, step=step, acked=committed,
            pending=pending, quorum=stored.quorum, readable=readable,
            basis=basis, superseded_by=superseder or "",
        )

    def verify(self) -> dict:
        """启动后/运行期恢复校验：检查落盘数据重放后的一致性与可解释异常。

        返回每个副本的重放异常（半行、孤儿 commit、字段缺失等）以及
        已提交写入的副本覆盖情况；异常不阻断启动，但会被如实报告。
        """
        with self._lock:
            anomalies: list[str] = []
            for rep in self.replicas:
                anomalies.extend(getattr(rep, "recovery_anomalies", []))

            committed_checks: list[dict] = []
            for o in self.idem_table.values():
                if o.code is not CommitCode.COMMITTED or not o.key:
                    continue
                holders = [
                    r for r in self.replicas
                    if r.alive
                    and (st := r.data.get(o.key)) is not None
                    and st.request_id == o.request_id
                ]
                committed_replicas = [
                    r.replica_id for r in holders if r.data[o.key].committed
                ]
                ok = len(committed_replicas) >= o.quorum
                committed_checks.append({
                    "idem_key": o.idem_key,
                    "request_id": o.request_id,
                    "key": o.key,
                    "committed_replicas": committed_replicas,
                    "holder_replicas": [r.replica_id for r in holders],
                    "quorum": o.quorum,
                    "ok": ok,
                })
                if not ok:
                    anomalies.append(
                        f"coordinator: 已提交写入 request_id={o.request_id} "
                        f"(idem_key={o.idem_key}) 当前仅 {len(committed_replicas)} "
                        f"个存活副本带 commit 标记 < 多数派 {o.quorum}"
                    )

            pending = [v.request_id for v in self.list_pending()]
            report = {
                "replica_count": len(self.replicas),
                "idem_keys": len(self.idem_table),
                "ledger_requests": len(self.write_ledger),
                "committed_checks": committed_checks,
                "pending_requests": pending,
                "anomalies": anomalies,
                "ok": all(c["ok"] for c in committed_checks),
            }
            print(
                f"[verify] idem_keys={report['idem_keys']} "
                f"committed={len(committed_checks)} pending={pending} "
                f"anomalies={len(anomalies)} ok={report['ok']}",
                flush=True,
            )
            return report

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
            except (OSError, RuntimeError) as exc:
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
            except (OSError, RuntimeError) as exc:
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
            if outcome.request_id and outcome.key:
                self.write_ledger.setdefault(
                    outcome.request_id,
                    WriteEntry(
                        key=outcome.key, value=outcome.value,
                        request_id=outcome.request_id,
                        timestamp=outcome.timestamp or 0,
                        idem_key=outcome.idem_key,
                    ),
                )

    def _stored_key(self, idem_key: str) -> str | None:
        for rec in self.commit_log.read_all():
            if rec["idem_key"] == idem_key:
                return rec["key"]
        return None

