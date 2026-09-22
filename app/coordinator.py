"""写入协调器：幂等提交、多数派确认、冲突裁决与可解释结论。"""

from __future__ import annotations

import logging
import threading
from collections import Counter, defaultdict

from app.cluster import ReplicaCluster
from app.models import (
    ConflictRecord,
    EntryPhase,
    LogEntry,
    WriteRequest,
    WriteResult,
    WriteStatus,
)
from app.replica import (
    IdempotencyContentMismatch,
    ReplicaFailure,
    compare_writes,
    make_conflict,
)

logger = logging.getLogger("quorum.coordinator")

CONFLICT_RULE = "LWW_TIMESTAMP_NS_THEN_REQUEST_ID"


class _KeyLocks:
    """按幂等键加锁，保证同一键的并发提交被串行化。"""

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}

    def get(self, key: str) -> threading.Lock:
        with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            return lock


class Coordinator:
    """在 ReplicaCluster 之上提供提交/查询/复制协调。"""

    def __init__(self, cluster: ReplicaCluster) -> None:
        self.cluster = cluster
        self._key_locks = _KeyLocks()

    # ==================================================================
    # 提交
    # ==================================================================
    def submit(
        self,
        idempotency_key: str,
        data_key: str,
        payload: str,
        timestamp_ns: int,
        request_id: str,
    ) -> WriteResult:
        """带幂等键的写入提交（同一键并发安全）。"""
        req = WriteRequest(
            idempotency_key=idempotency_key,
            data_key=data_key,
            payload=payload,
            timestamp_ns=timestamp_ns,
            request_id=request_id,
        )
        with self._key_locks.get(idempotency_key):
            return self._submit_locked(req)

    def _base_result(self, req: WriteRequest) -> dict:
        alive = len(self.cluster.alive_replicas())
        return {
            "request_id": req.request_id,
            "idempotency_key": req.idempotency_key,
            "data_key": req.data_key,
            "quorum": self.cluster.quorum(),
            "total_replicas": self.cluster.replica_count,
            "alive_replicas": alive,
        }

    def _submit_locked(self, req: WriteRequest) -> WriteResult:
        quorum = self.cluster.quorum()
        alive = self.cluster.alive_replicas()
        base = self._base_result(req)

        # ---- 快路径：已达多数派提交的同一键 ----
        chosen = self._find_chosen(req.idempotency_key)
        if chosen is not None:
            if chosen.fingerprint != req.fingerprint():
                result = WriteResult(
                    status=WriteStatus.REJECTED_CONTENT_CONFLICT,
                    reason=(
                        f"idempotency key {req.idempotency_key!r} is already bound "
                        f"to a different payload: committed fingerprint "
                        f"{chosen.fingerprint[:12]} (request {chosen.request_id}); "
                        f"submitted fingerprint {req.fingerprint()[:12]}"
                    ),
                    **base,
                )
                logger.warning(
                    "request_id=%s idempotency_key=%s basis=content_conflict "
                    "committed_request=%s decision=REJECTED",
                    req.request_id, req.idempotency_key, chosen.request_id,
                )
                return result
            acked = self._committed_replicas(req.idempotency_key)
            result = WriteResult(
                status=WriteStatus.COMMITTED,
                data_key=chosen.data_key,
                payload=chosen.payload,
                timestamp_ns=chosen.timestamp_ns,
                request_id=chosen.request_id,
                acked_replicas=acked,
                duplicate=True,
                reason=(
                    f"duplicate submission: key already committed by request "
                    f"{chosen.request_id}; replaying the identical first result"
                ),
                **{
                    k: v
                    for k, v in base.items()
                    if k not in ("request_id", "data_key")
                },
            )
            logger.info(
                "request_id=%s idempotency_key=%s basis=already_chosen "
                "acked_replicas=%s quorum=%s decision=COMMITTED duplicate=true",
                req.request_id, req.idempotency_key, acked, quorum,
            )
            return result

        # ---- 存活预检：副本数不足时不产生任何写入 ----
        if len(alive) < quorum:
            result = WriteResult(
                status=WriteStatus.INSUFFICIENT_REPLICAS,
                reason=(
                    f"only {len(alive)} replicas alive, quorum is {quorum}; "
                    "no write attempted"
                ),
                **base,
            )
            logger.warning(
                "request_id=%s idempotency_key=%s basis=alive_below_quorum "
                "alive=%s quorum=%s decision=INSUFFICIENT_REPLICAS",
                req.request_id, req.idempotency_key, len(alive), quorum,
            )
            return result

        # ---- 阶段一：prepare ----
        prepared_ids, mismatch, failed_ids = self._run_prepare(req)

        if mismatch is not None:
            result = WriteResult(
                status=WriteStatus.REJECTED_CONTENT_CONFLICT,
                reason=mismatch,
                acked_replicas=prepared_ids,
                **base,
            )
            logger.warning(
                "request_id=%s idempotency_key=%s basis=key_bound_to_other_payload "
                "prepared=%s decision=REJECTED",
                req.request_id, req.idempotency_key, prepared_ids,
            )
            return result

        if len(prepared_ids) < quorum:
            # 尽力回滚，不改变“失败”结论
            self._run_abort(req, prepared_ids)
            result = WriteResult(
                status=WriteStatus.FAILED_QUORUM,
                reason=(
                    f"prepare phase: {len(prepared_ids)} acks < quorum {quorum} "
                    f"(failed replicas: {failed_ids}); best-effort abort issued"
                ),
                acked_replicas=prepared_ids,
                **base,
            )
            logger.warning(
                "request_id=%s idempotency_key=%s basis=prepare_acks_below_quorum "
                "acked_replicas=%s failed=%s quorum=%s decision=FAILED_QUORUM",
                req.request_id, req.idempotency_key, prepared_ids, failed_ids, quorum,
            )
            return result

        # ---- 阶段二：commit ----
        committed_ids, commit_failed = self._run_commit(req, prepared_ids)
        if len(committed_ids) < quorum:
            # 已 commit 的副本不可撤销；未 commit 的尽力 abort，结论仍为失败
            rollback_ids = [r for r in prepared_ids if r not in committed_ids]
            self._run_abort(req, rollback_ids)
            result = WriteResult(
                status=WriteStatus.FAILED_QUORUM,
                reason=(
                    f"commit phase: {len(committed_ids)} durable commits < quorum "
                    f"{quorum} (failed replicas: {commit_failed}); uncommitted "
                    "replicas aborted; retry with the same key to complete it"
                ),
                acked_replicas=committed_ids,
                **base,
            )
            logger.warning(
                "request_id=%s idempotency_key=%s basis=commit_acks_below_quorum "
                "committed_replicas=%s failed=%s quorum=%s decision=FAILED_QUORUM",
                req.request_id, req.idempotency_key, committed_ids,
                commit_failed, quorum,
            )
            return result

        # ---- 成功：记录冲突并返回 ----
        conflicts = self._materialize_conflicts()
        acked = self._committed_replicas(req.idempotency_key)
        conflict_note = ""
        if conflicts:
            for rec in conflicts:
                if req.request_id in (rec.winner_request_id, rec.loser_request_id):
                    outcome = (
                        "winner" if rec.winner_request_id == req.request_id else "loser"
                    )
                    conflict_note = (
                        f"; concurrent write conflict resolved by {CONFLICT_RULE}: "
                        f"this request is the {outcome} against "
                        f"{rec.loser_request_id if outcome == 'winner' else rec.winner_request_id}"
                    )
        result = WriteResult(
            status=WriteStatus.COMMITTED,
            payload=req.payload,
            timestamp_ns=req.timestamp_ns,
            acked_replicas=acked,
            reason=f"durable commit on {len(committed_ids)} replicas >= quorum {quorum}"
            + conflict_note,
            **base,
        )
        logger.info(
            "request_id=%s idempotency_key=%s data_key=%s basis=commit_quorum_met "
            "acked_replicas=%s quorum=%s decision=COMMITTED duplicate=false "
            "new_conflicts=%s",
            req.request_id, req.idempotency_key, req.data_key,
            committed_ids, quorum,
            [(c.winner_request_id, c.loser_request_id) for c in conflicts],
        )
        return result

    # ==================================================================
    # 查询
    # ==================================================================
    def query_idempotency(self, idempotency_key: str) -> WriteResult:
        """按幂等键查询首次提交的结论（多数派读）。"""
        quorum = self.cluster.quorum()
        alive = self.cluster.alive_replicas()
        meta = {
            "request_id": "",
            "idempotency_key": idempotency_key,
            "quorum": quorum,
            "total_replicas": self.cluster.replica_count,
            "alive_replicas": len(alive),
        }
        if len(alive) < quorum:
            logger.warning(
                "idempotency_key=%s basis=alive_below_quorum alive=%s quorum=%s "
                "decision=INSUFFICIENT_REPLICAS",
                idempotency_key, len(alive), quorum,
            )
            return WriteResult(
                status=WriteStatus.INSUFFICIENT_REPLICAS,
                reason=f"only {len(alive)} replicas alive, quorum is {quorum}",
                **meta,
            )

        chosen = self._find_chosen(idempotency_key)
        if chosen is not None:
            acked = self._committed_replicas(idempotency_key)
            logger.info(
                "request_id=%s idempotency_key=%s basis=quorum_read_committed "
                "acked_replicas=%s quorum=%s decision=COMMITTED",
                chosen.request_id, idempotency_key, acked, quorum,
            )
            return WriteResult(
                status=WriteStatus.COMMITTED,
                request_id=chosen.request_id,
                data_key=chosen.data_key,
                payload=chosen.payload,
                timestamp_ns=chosen.timestamp_ns,
                acked_replicas=acked,
                reason="committed entry observed on a quorum of replicas",
                **{k: v for k, v in meta.items() if k != "request_id"},
            )

        prepared = self._phase_replicas(idempotency_key, EntryPhase.PREPARED)
        aborted = self._phase_replicas(idempotency_key, EntryPhase.ABORTED)
        reason = "no committed entry reached a quorum"
        if prepared or aborted:
            reason += (
                f"; uncommitted hints: prepared={prepared} aborted={aborted} "
                "(not exposed as success)"
            )
        logger.info(
            "idempotency_key=%s basis=quorum_read_missing prepared=%s aborted=%s "
            "quorum=%s decision=NOT_FOUND",
            idempotency_key, prepared, aborted, quorum,
        )
        return WriteResult(status=WriteStatus.NOT_FOUND, reason=reason, **meta)

    def read_data(self, data_key: str) -> WriteResult:
        """按数据键读取当前生效值（多数派读 + 冲突收敛）。"""
        quorum = self.cluster.quorum()
        alive = self.cluster.alive_replicas()
        meta = {
            "request_id": "",
            "idempotency_key": "",
            "data_key": data_key,
            "quorum": quorum,
            "total_replicas": self.cluster.replica_count,
            "alive_replicas": len(alive),
        }
        if len(alive) < quorum:
            return WriteResult(
                status=WriteStatus.INSUFFICIENT_REPLICAS,
                reason=f"only {len(alive)} replicas alive, quorum is {quorum}",
                **meta,
            )

        chosen_entries = self._chosen_entries_for_data(data_key)
        if not chosen_entries:
            return WriteResult(
                status=WriteStatus.NOT_FOUND,
                reason="no committed value for the data key reached a quorum",
                **meta,
            )

        winner = chosen_entries[0]
        for entry in chosen_entries[1:]:
            winner, _ = compare_writes(winner, entry)

        losers = [e for e in chosen_entries if e.request_id != winner.request_id]
        reason = "committed value observed on a quorum of replicas"
        if losers:
            reason += (
                f"; {len(losers)} concurrent conflicting write(s) resolved by "
                f"{CONFLICT_RULE}; loser_request_ids="
                f"{[e.request_id for e in losers]}"
            )
        acked = self._committed_replicas(winner.idempotency_key)
        logger.info(
            "read data_key=%s basis=quorum_read winner_request=%s "
            "loser_requests=%s acked_replicas=%s quorum=%s decision=COMMITTED",
            data_key, winner.request_id, [e.request_id for e in losers],
            acked, quorum,
        )
        return WriteResult(
            status=WriteStatus.COMMITTED,
            request_id=winner.request_id,
            idempotency_key=winner.idempotency_key,
            payload=winner.payload,
            timestamp_ns=winner.timestamp_ns,
            acked_replicas=acked,
            reason=reason,
            **{k: v for k, v in meta.items() if k not in ("request_id", "idempotency_key")},
        )

    # ==================================================================
    # 冲突
    # ==================================================================
    def list_conflicts(self, data_key: str | None = None) -> list[ConflictRecord]:
        records: dict[tuple, ConflictRecord] = {}
        for replica in self.cluster.all_replicas():
            if not replica.is_alive():
                continue
            for record in replica.conflicts():
                if data_key is not None and record.data_key != data_key:
                    continue
                pair = tuple(
                    sorted((record.winner_request_id, record.loser_request_id))
                )
                records.setdefault(pair, record)
        return sorted(
            records.values(),
            key=lambda r: (r.data_key, r.winner_request_id, r.loser_request_id),
        )

    # ==================================================================
    # 复制收敛
    # ==================================================================
    def repair(self) -> dict:
        """把已达多数派的提交与冲突记录同步到所有存活但落后的副本。"""
        propagated_entries: list[str] = []
        propagated_conflicts = 0
        skipped_unchosen: list[str] = []

        chosen = self._all_chosen_entries()
        alive = self.cluster.alive_replicas()
        for replica in alive:
            for entry in chosen:
                local = replica.get_entry(entry.idempotency_key)
                if local is not None and local.phase is EntryPhase.COMMITTED:
                    continue
                req = WriteRequest(
                    idempotency_key=entry.idempotency_key,
                    data_key=entry.data_key,
                    payload=entry.payload,
                    timestamp_ns=entry.timestamp_ns,
                    request_id=entry.request_id,
                )
                replica.prepare(req)
                replica.commit(entry.idempotency_key)
                propagated_entries.append(
                    f"{replica.id}:{entry.idempotency_key}"
                )
            for record in self.list_conflicts():
                pairs = {
                    tuple(sorted((c.winner_request_id, c.loser_request_id)))
                    for c in replica.conflicts()
                }
                pair = tuple(
                    sorted((record.winner_request_id, record.loser_request_id))
                )
                if pair not in pairs:
                    replica.record_conflict(record)
                    propagated_conflicts += 1

        # 统计未达多数派、不允许被提升的提交
        committed_counter = self._committed_counter()
        for key, count in committed_counter.items():
            if count < self.cluster.quorum():
                skipped_unchosen.append(f"{key}@{count}")

        summary = {
            "propagated_entries": propagated_entries,
            "propagated_conflicts": propagated_conflicts,
            "unchosen_commits_not_promoted": skipped_unchosen,
        }
        logger.info(
            "repair basis=quorum_gate propagated_entries=%s propagated_conflicts=%s "
            "unchosen_not_promoted=%s",
            propagated_entries, propagated_conflicts, skipped_unchosen,
        )
        return summary

    # ==================================================================
    # 内部：阶段执行
    # ==================================================================
    def _run_prepare(
        self, req: WriteRequest
    ) -> tuple[list[str], str | None, list[str]]:
        prepared: list[str] = []
        failed: list[str] = []
        mismatch: str | None = None
        for replica in self.cluster.alive_replicas():
            try:
                existing = replica.get_entry(req.idempotency_key)
                if (
                    existing is not None
                    and existing.fingerprint != req.fingerprint()
                ):
                    mismatch = self._mismatch_reason(existing)
                    failed.append(replica.id)
                    continue
                replica.prepare(req)
                prepared.append(replica.id)
            except IdempotencyContentMismatch as exc:
                existing = replica.get_entry(req.idempotency_key)
                mismatch = (
                    self._mismatch_reason(existing) if existing is not None else str(exc)
                )
                failed.append(replica.id)
            except ReplicaFailure:
                failed.append(replica.id)
        return sorted(prepared), mismatch, sorted(failed)

    def _run_commit(self, req: WriteRequest, targets: list[str]) -> tuple[list[str], list[str]]:
        committed: list[str] = []
        failed: list[str] = []
        for rid in targets:
            replica = self.cluster.get(rid)
            try:
                replica.commit(req.idempotency_key)
                committed.append(rid)
            except ReplicaFailure:
                failed.append(rid)
        return sorted(committed), sorted(failed)

    def _run_abort(self, req: WriteRequest, targets: list[str]) -> None:
        for rid in targets:
            replica = self.cluster.get(rid)
            try:
                replica.abort(req.idempotency_key)
            except (ReplicaFailure, KeyError):
                pass

    @staticmethod
    def _mismatch_reason(existing: LogEntry) -> str:
        phase_kind = (
            "committed"
            if existing.phase is EntryPhase.COMMITTED
            else f"uncommitted ({existing.phase.value.lower()})"
        )
        return (
            f"idempotency key {existing.idempotency_key!r} is already bound to a "
            f"different payload in a {phase_kind} record from request "
            f"{existing.request_id}; the key may not be reused with other content"
        )

    # ==================================================================
    # 内部：多数派读
    # ==================================================================
    def _committed_replicas(self, idempotency_key: str) -> list[str]:
        out: list[str] = []
        for replica in self.cluster.all_replicas():
            entry = replica.get_entry(idempotency_key) if replica.is_alive() else None
            if entry is not None and entry.phase is EntryPhase.COMMITTED:
                out.append(replica.id)
        return sorted(out)

    def _phase_replicas(self, idempotency_key: str, phase: EntryPhase) -> list[str]:
        out: list[str] = []
        for replica in self.cluster.all_replicas():
            entry = replica.get_entry(idempotency_key) if replica.is_alive() else None
            if entry is not None and entry.phase is phase:
                out.append(replica.id)
        return sorted(out)

    def _committed_counter(self) -> Counter:
        counter: Counter = Counter()
        for replica in self.cluster.all_replicas():
            if not replica.is_alive():
                continue
            for entry in replica.all_committed():
                counter[entry.idempotency_key] += 1
        return counter

    def _representative_committed(self, idempotency_key: str) -> LogEntry | None:
        for replica in self.cluster.all_replicas():
            entry = replica.get_entry(idempotency_key) if replica.is_alive() else None
            if entry is not None and entry.phase is EntryPhase.COMMITTED:
                return entry
        return None

    def _find_chosen(self, idempotency_key: str) -> LogEntry | None:
        """多数派门控：仅当某键的 COMMITTED 出现在多数副本上时视为已决。"""
        counter = 0
        for replica in self.cluster.all_replicas():
            entry = replica.get_entry(idempotency_key) if replica.is_alive() else None
            if entry is not None and entry.phase is EntryPhase.COMMITTED:
                counter += 1
        if counter >= self.cluster.quorum():
            return self._representative_committed(idempotency_key)
        return None

    def _all_chosen_entries(self) -> list[LogEntry]:
        quorum = self.cluster.quorum()
        out: list[LogEntry] = []
        for key, count in self._committed_counter().items():
            if count >= quorum:
                entry = self._representative_committed(key)
                if entry is not None:
                    out.append(entry)
        return out

    def _chosen_entries_for_data(self, data_key: str) -> list[LogEntry]:
        quorum = self.cluster.quorum()
        counter: Counter = Counter()
        representative: dict[str, LogEntry] = {}
        for replica in self.cluster.all_replicas():
            if not replica.is_alive():
                continue
            for entry in replica.committed_for_key(data_key):
                counter[entry.idempotency_key] += 1
                representative.setdefault(entry.idempotency_key, entry)
        return [
            representative[key]
            for key, count in counter.items()
            if count >= quorum
        ]

    # ==================================================================
    # 内部：冲突裁决落盘
    # ==================================================================
    def _materialize_conflicts(self) -> list[ConflictRecord]:
        """对所有已达多数派的同 data_key 写入两两裁决，并在存活副本持久化。"""
        grouped: dict[str, dict[str, LogEntry]] = defaultdict(dict)
        for entry in self._all_chosen_entries():
            grouped[entry.data_key][entry.idempotency_key] = entry

        existing_pairs = {
            tuple(sorted((r.winner_request_id, r.loser_request_id)))
            for r in self.list_conflicts()
        }
        new_records: list[ConflictRecord] = []
        for entries in grouped.values():
            keys = sorted(entries)
            for i in range(len(keys)):
                for j in range(i + 1, len(keys)):
                    a, b = entries[keys[i]], entries[keys[j]]
                    winner, loser = compare_writes(a, b)
                    record = make_conflict(winner, loser)
                    pair = tuple(sorted((a.request_id, b.request_id)))
                    if pair in existing_pairs:
                        continue
                    existing_pairs.add(pair)
                    new_records.append(record)
                    for replica in self.cluster.alive_replicas():
                        try:
                            replica.record_conflict(record)
                        except ReplicaFailure:
                            continue
        return new_records
