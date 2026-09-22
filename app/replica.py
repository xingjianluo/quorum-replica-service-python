"""进程内副本：持有本地状态机并以 ReplicaStorage 持久化。"""

from __future__ import annotations

import threading

from app.models import ConflictRecord, EntryPhase, LogEntry, WriteRequest
from app.storage import ReplicaStorage

CONFLICT_RULE = "LWW_TIMESTAMP_NS_THEN_REQUEST_ID"


class ReplicaFailure(Exception):
    """副本被注入故障时抛出（模拟节点宕机/磁盘失败）。"""


class IdempotencyContentMismatch(Exception):
    """同一幂等键已存在，但内容指纹不同。"""


def compare_writes(a: LogEntry, b: LogEntry) -> tuple[LogEntry, LogEntry]:
    """冲突裁决：timestamp_ns 较大者胜；相同时间戳下 request_id 字典序大者胜。

    返回 (winner, loser)。规则完全确定，与提交顺序、到达顺序无关。
    """
    if (a.timestamp_ns, a.request_id) > (b.timestamp_ns, b.request_id):
        return a, b
    return b, a


def make_conflict(winner: LogEntry, loser: LogEntry) -> ConflictRecord:
    return ConflictRecord(
        data_key=winner.data_key,
        winner_request_id=winner.request_id,
        winner_idempotency_key=winner.idempotency_key,
        loser_request_id=loser.request_id,
        loser_idempotency_key=loser.idempotency_key,
        winner_timestamp_ns=winner.timestamp_ns,
        loser_timestamp_ns=loser.timestamp_ns,
        rule=CONFLICT_RULE,
    )


class Replica:
    """一个本地副本节点。"""

    def __init__(self, replica_id: str, storage: ReplicaStorage) -> None:
        self.id = replica_id
        self._storage = storage
        self._lock = threading.RLock()
        self._alive = True
        # “存活但本次操作持久化失败”的故障注入（模拟磁盘错误/超时）
        self._fault_ops: set[str] = set()
        self._entries: dict[str, LogEntry] = {}
        self._by_data_key: dict[str, set[str]] = {}
        self._conflicts: dict[tuple, ConflictRecord] = {}
        self.recover()

    # -- 故障注入 -------------------------------------------------
    def set_alive(self, alive: bool) -> None:
        with self._lock:
            self._alive = alive

    def is_alive(self) -> bool:
        return self._alive

    def set_faults(self, ops: set[str] | list[str] | tuple[str, ...]) -> None:
        """设置在哪些操作上抛 ReplicaFailure（节点仍报告存活）。"""
        with self._lock:
            self._fault_ops = set(ops)

    def clear_faults(self) -> None:
        with self._lock:
            self._fault_ops.clear()

    def _require_alive(self) -> None:
        if not self._alive:
            raise ReplicaFailure(f"replica {self.id} is down")

    def _check_fault(self, op: str) -> None:
        self._require_alive()
        if op in self._fault_ops:
            raise ReplicaFailure(
                f"replica {self.id} injected fault during {op}"
            )

    # -- 恢复 -----------------------------------------------------
    def recover(self) -> None:
        """从持久化日志重放重建状态（重启入口，幂等）。"""
        with self._lock:
            entries, raw_conflicts = self._storage.replay()
            self._entries = {}
            self._by_data_key = {}
            self._conflicts = {}
            for entry in entries:
                self._index(entry)
            for record in raw_conflicts:
                self._index_conflict(record)

    def _index(self, entry: LogEntry) -> None:
        self._entries[entry.idempotency_key] = entry
        self._by_data_key.setdefault(entry.data_key, set()).add(
            entry.idempotency_key
        )

    def _index_conflict(self, record: ConflictRecord) -> None:
        key = tuple(
            sorted((record.winner_request_id, record.loser_request_id))
        )
        # 同一对写入的冲突结论只有一个；重复投递/重放不重复生效
        self._conflicts.setdefault(key, record)

    # -- 两阶段写入 ----------------------------------------------
    def prepare(self, req: WriteRequest) -> LogEntry:
        with self._lock:
            self._check_fault("prepare")
            existing = self._entries.get(req.idempotency_key)
            if existing is not None:
                if existing.fingerprint != req.fingerprint():
                    raise IdempotencyContentMismatch(
                        f"idempotency key {req.idempotency_key!r} already bound "
                        f"to a different payload"
                    )
                if existing.request_id != req.request_id:
                    # 键与内容相同但 request_id 不同：按重复提交处理，直接回放现状
                    return existing
                if existing.phase is EntryPhase.COMMITTED:
                    # 重复投递 prepare：幂等无操作
                    return existing
                if existing.phase is EntryPhase.ABORTED:
                    # 未达多数派后的同键重试：允许从 ABORTED 重新 PREPARED
                    entry = self._new_phase_entry(existing, EntryPhase.PREPARED)
                    self._storage.append_entry(entry)
                    self._index(entry)
                    return entry
                return existing
            entry = LogEntry(
                seq=self._storage.next_seq(),
                request_id=req.request_id,
                idempotency_key=req.idempotency_key,
                data_key=req.data_key,
                payload=req.payload,
                fingerprint=req.fingerprint(),
                timestamp_ns=req.timestamp_ns,
                phase=EntryPhase.PREPARED,
            )
            self._storage.append_entry(entry)
            self._index(entry)
            return entry

    def _new_phase_entry(self, entry: LogEntry, phase: EntryPhase) -> LogEntry:
        return LogEntry(
            seq=self._storage.next_seq(),
            request_id=entry.request_id,
            idempotency_key=entry.idempotency_key,
            data_key=entry.data_key,
            payload=entry.payload,
            fingerprint=entry.fingerprint,
            timestamp_ns=entry.timestamp_ns,
            phase=phase,
        )

    def commit(self, idempotency_key: str) -> LogEntry:
        with self._lock:
            self._check_fault("commit")
            entry = self._entries[idempotency_key]
            if entry.phase is EntryPhase.COMMITTED:
                # 重复投递 commit：幂等无操作
                return entry
            if entry.phase is not EntryPhase.PREPARED:
                raise ReplicaFailure(
                    f"illegal commit on {entry.phase.value} entry "
                    f"{idempotency_key!r} (must be prepared first)"
                )
            committed = self._new_phase_entry(entry, EntryPhase.COMMITTED)
            self._storage.append_entry(committed)
            self._index(committed)
            return committed

    def abort(self, idempotency_key: str) -> LogEntry:
        with self._lock:
            self._check_fault("abort")
            entry = self._entries[idempotency_key]
            if entry.phase in (EntryPhase.ABORTED, EntryPhase.COMMITTED):
                return entry
            aborted = self._new_phase_entry(entry, EntryPhase.ABORTED)
            self._storage.append_entry(aborted)
            self._index(aborted)
            return aborted

    # -- 冲突 -----------------------------------------------------
    def detect_new_conflicts(self, entry: LogEntry) -> list[ConflictRecord]:
        """entry 提交后，检查与同 data_key 上其它已提交写入的新冲突。"""
        records: list[ConflictRecord] = []
        if entry.phase is not EntryPhase.COMMITTED:
            return records
        for other in self.committed_for_key(entry.data_key):
            if other.idempotency_key == entry.idempotency_key:
                continue
            winner, loser = compare_writes(entry, other)
            record = make_conflict(winner, loser)
            pair = tuple(sorted((entry.request_id, other.request_id)))
            if pair not in self._conflicts:
                records.append(record)
                self._index_conflict(record)
        return records

    def record_conflict(self, record: ConflictRecord) -> None:
        with self._lock:
            self._storage.append_conflict(record)
            self._index_conflict(record)

    # -- 读 -------------------------------------------------------
    def get_entry(self, idempotency_key: str) -> LogEntry | None:
        with self._lock:
            return self._entries.get(idempotency_key)

    def committed_for_key(self, data_key: str) -> list[LogEntry]:
        with self._lock:
            keys = self._by_data_key.get(data_key, set())
            out = [
                self._entries[k]
                for k in keys
                if self._entries[k].phase is EntryPhase.COMMITTED
            ]
            return sorted(out, key=lambda e: e.seq)

    def all_committed(self) -> list[LogEntry]:
        with self._lock:
            return sorted(
                (e for e in self._entries.values() if e.phase is EntryPhase.COMMITTED),
                key=lambda e: e.seq,
            )

    def conflicts(self) -> list[ConflictRecord]:
        with self._lock:
            return sorted(
                self._conflicts.values(),
                key=lambda r: (r.data_key, r.winner_request_id, r.loser_request_id),
            )
