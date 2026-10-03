"""自动对账：落后副本补齐 + 未达多数派写入的自动续推。

对账循环做两件事：
1. 对幂等表中尚未 COMMITTED 的写入，条件具备（存活副本足够）时自动
   resume 推进到最终一致；条件不足则跳过并说明原因。若同数据键已有
   另一笔已提交写入，该未决写入被判 SUPERSEDED 终态失败：不改判成功、
   不补投、不在副本上留下已提交痕迹；
2. 对已提交的写入，按 LWW 全序把获胜的已提交状态补投到缺失/落后的副本：
   - 同一 request_id 重复投递是幂等 no-op（副本去重），不会重复生效；
   - 副本上已有全序更新的写入时跳过，绝不覆盖已确认的数据；
   - 已提交数据只增不丢，补投后补写 commit 标记。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from qrs.models import CommitCode


@dataclass
class ReconcileReport:
    """一次对账的结果报告。"""
    retried_requests: list[str] = field(default_factory=list)   # 自动续推的 request_id
    repaired_replicas: list[str] = field(default_factory=list)  # 被补齐的副本
    skipped: list[str] = field(default_factory=list)            # 条件不足跳过的幂等键
    superseded: list[str] = field(default_factory=list)         # 被判取代（终态失败）的幂等键
    converged: bool = False                                     # 本轮后是否全部收敛


class Reconciler:
    """挂在协调者上的对账器，可手动触发也可周期性触发。"""

    def __init__(self, coordinator) -> None:
        self.coordinator = coordinator

    def reconcile_once(self) -> ReconcileReport:
        """执行一轮对账：先续推未决写入，再补齐落后副本。"""
        report = ReconcileReport()
        print("[reconcile] === round start ===", flush=True)
        self._retry_pending(report)
        self._repair_committed(report)
        report.converged = self._check_converged()
        print(
            f"[reconcile] === round end: retried={report.retried_requests} "
            f"repaired={report.repaired_replicas} skipped={report.skipped} "
            f"superseded={report.superseded} converged={report.converged} ===",
            flush=True,
        )
        return report

    # 未决写入续推 ------------------------------------------------------
    def _retry_pending(self, report: ReconcileReport) -> None:
        coord = self.coordinator
        for idem_key, outcome in list(coord.idem_table.items()):
            # SUPERSEDED 与 COMMITTED 同为终态：不再续推
            if outcome.code in (CommitCode.COMMITTED, CommitCode.SUPERSEDED):
                continue
            alive = [r for r in coord.replicas if r.alive]
            if len(alive) < coord.quorum:
                report.skipped.append(idem_key)
                print(
                    f"[reconcile] request_id={outcome.request_id} "
                    f"idem_key={idem_key} SKIP basis=存活副本 {len(alive)} < "
                    f"多数派 {coord.quorum}，等待条件恢复",
                    flush=True,
                )
                continue
            result = coord.resume(idem_key)
            if result.code is CommitCode.SUPERSEDED:
                # 同键已有更新的已提交写入：终态失败，不得改判成功
                report.superseded.append(idem_key)
                print(
                    f"[reconcile] request_id={result.request_id} "
                    f"idem_key={idem_key} verdict=SUPERSEDED "
                    f"basis={result.reason}", flush=True,
                )
                continue
            report.retried_requests.append(result.request_id)
            print(
                f"[reconcile] request_id={result.request_id} idem_key={idem_key} "
                f"verdict={result.code.value} acked={result.acked} "
                f"basis=对账自动续推未决写入", flush=True,
            )

    # 已提交写入的副本补齐 -----------------------------------------------
    def _repair_committed(self, report: ReconcileReport) -> None:
        coord = self.coordinator
        committed = [
            o for o in coord.idem_table.values()
            if o.code is CommitCode.COMMITTED and o.key
        ]
        # 按 LWW 全序升序投递，终态收敛到全序最大者，避免无谓的反复覆盖
        committed.sort(key=lambda o: (o.timestamp or 0, o.request_id))
        for outcome in committed:
            ts = outcome.timestamp or 0
            for rep in coord.replicas:
                if not rep.alive:
                    continue
                state = rep.data.get(outcome.key)
                if state is not None and state.request_id == outcome.request_id:
                    if state.committed:
                        continue  # 该副本已持有此写入的已提交状态
                    # 同一写入只缺 commit 标记：补标记即可，无需重新投递
                    try:
                        rep.mark_committed(outcome.key, outcome.request_id)
                    except (OSError, RuntimeError) as exc:
                        print(
                            f"[reconcile] request_id={outcome.request_id} -> "
                            f"{rep.replica_id} FAILED: {exc}", flush=True,
                        )
                        continue
                    report.repaired_replicas.append(rep.replica_id)
                    print(
                        f"[reconcile] request_id={outcome.request_id} -> "
                        f"{rep.replica_id} REPAIRED key={outcome.key} "
                        f"basis=同一写入缺 commit 标记，补标记", flush=True,
                    )
                    continue
                if (state is not None
                        and (state.timestamp, state.request_id)
                        > (ts, outcome.request_id)):
                    # 副本上已有全序更新的写入：不覆盖，保护已确认数据
                    print(
                        f"[reconcile] request_id={outcome.request_id} -> "
                        f"{rep.replica_id} SKIP basis=副本已有更新写入 "
                        f"{state.request_id}@ts={state.timestamp}，LWW 不覆盖",
                        flush=True,
                    )
                    continue
                try:
                    rep.apply_write(
                        outcome.key, outcome.value,
                        outcome.request_id, ts, False,
                    )
                    rep.mark_committed(outcome.key, outcome.request_id)
                except (OSError, RuntimeError) as exc:
                    print(
                        f"[reconcile] request_id={outcome.request_id} -> "
                        f"{rep.replica_id} FAILED: {exc}", flush=True,
                    )
                    continue
                report.repaired_replicas.append(rep.replica_id)
                print(
                    f"[reconcile] request_id={outcome.request_id} -> "
                    f"{rep.replica_id} REPAIRED key={outcome.key} "
                    f"basis=补齐已提交写入并补写 commit 标记", flush=True,
                )

    # 收敛判定 ----------------------------------------------------------
    def _check_converged(self) -> bool:
        coord = self.coordinator
        outcomes = list(coord.idem_table.values())
        # SUPERSEDED 是终态失败（已被取代，无需再推进），不阻碍收敛判定
        if any(o.code not in (CommitCode.COMMITTED, CommitCode.SUPERSEDED)
               for o in outcomes):
            return False
        # 每个数据键的 LWW 获胜写入，须在所有存活副本上为已提交状态
        winners: dict[str, tuple[int, str]] = {}
        for o in outcomes:
            if not o.key:
                continue
            cur = winners.get(o.key)
            cand = (o.timestamp or 0, o.request_id)
            if cur is None or cand > cur:
                winners[o.key] = cand
        for key, (_, rid) in winners.items():
            for rep in coord.replicas:
                if not rep.alive:
                    continue
                state = rep.data.get(key)
                if (state is None or state.request_id != rid
                        or not state.committed):
                    return False
        return True
