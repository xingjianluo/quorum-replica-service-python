"""HTTP 接口层。

接口：
- POST /writes                 带幂等键的写入提交
- GET  /writes/{idem_key}      按幂等键查询首次提交结果
- POST /writes/{idem_key}/resume  对未达多数派的写入补复制收敛
- GET  /keys/{key}             读取数据键的当前已提交值（多数派视角）
- GET  /conflicts              查询冲突及解决结果（可按 key 过滤）
- POST /cluster/fail/{rid}     故障注入：副本宕机
- POST /cluster/fault/{rid}    故障注入：存活但写失败
- POST /cluster/clear-fault/{rid}
- POST /cluster/restart/{rid}  重启单个副本
- POST /cluster/restart-coordinator
- GET  /cluster                查看集群状态
"""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from qrs.cluster import Cluster
from qrs.models import CommitCode, CommitOutcome, ConflictRecord

_CODE_TO_HTTP = {
    CommitCode.COMMITTED: 200,
    CommitCode.REPLAYED: 200,
    CommitCode.CONTENT_MISMATCH: 409,
    CommitCode.QUORUM_FAILED: 504,
    CommitCode.NOT_ENOUGH_REPLICAS: 503,
}


class WriteRequest(BaseModel):
    idem_key: str = Field(..., description="客户端幂等键")
    key: str = Field(..., description="数据键")
    value: str
    request_id: str = Field(..., description="调用方请求标识，用于日志与 tie-break")
    timestamp: int | None = Field(None, description="可选逻辑时间戳；缺省由服务端单调分配")


def _outcome_dict(o: CommitOutcome) -> dict[str, Any]:
    return {
        "idem_key": o.idem_key,
        "code": o.code.value,
        "request_id": o.request_id,
        "value": o.value,
        "replay": o.replay,
        "acked_replicas": o.acked,
        "pending_replicas": o.pending,
        "quorum": o.quorum,
        "reason": o.reason,
    }


def _conflict_dict(c: ConflictRecord) -> dict[str, Any]:
    return {
        "key": c.key,
        "rule": c.rule,
        "winner": {"request_id": c.winner_request_id, "timestamp": c.winner_timestamp},
        "loser": {"request_id": c.loser_request_id, "timestamp": c.loser_timestamp},
        "detail": c.detail,
    }


def create_app(cluster: Cluster | None = None) -> FastAPI:
    """构造 FastAPI 应用，允许注入集群（测试用）。"""
    app = FastAPI(title="quorum-replica-service")
    app.state.cluster = cluster or Cluster()

    def coord():
        return app.state.cluster.coordinator

    @app.post("/writes")
    def submit_write(req: WriteRequest) -> JSONResponse:
        outcome = coord().commit(
            idem_key=req.idem_key, key=req.key, value=req.value,
            request_id=req.request_id, timestamp=req.timestamp,
        )
        return JSONResponse(
            status_code=_CODE_TO_HTTP[outcome.code],
            content=_outcome_dict(outcome),
        )

    @app.get("/writes/{idem_key}")
    def query_write(idem_key: str) -> dict[str, Any]:
        outcome = coord().get(idem_key)
        if outcome is None:
            raise HTTPException(status_code=404, detail=f"未知幂等键: {idem_key}")
        return _outcome_dict(outcome)

    @app.get("/writes/{idem_key}/status")
    def write_status(idem_key: str) -> dict[str, Any]:
        status = coord().write_status(idem_key)
        if status is None:
            raise HTTPException(status_code=404, detail=f"未知幂等键: {idem_key}")
        return status

    @app.post("/writes/{idem_key}/resume")
    def resume_write(idem_key: str) -> JSONResponse:
        try:
            outcome = coord().resume(idem_key)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return JSONResponse(
            status_code=_CODE_TO_HTTP[outcome.code],
            content=_outcome_dict(outcome),
        )

    @app.get("/keys/{key}")
    def read_key(key: str) -> dict[str, Any]:
        result = coord().read_key(key)
        if result is None:
            raise HTTPException(status_code=404, detail=f"数据键不存在: {key}")
        return result

    @app.get("/conflicts")
    def list_conflicts(key: str | None = None) -> dict[str, Any]:
        records = coord().get_conflicts(key)
        return {"count": len(records), "conflicts": [_conflict_dict(c) for c in records]}

    # --- 故障注入 / 重启入口（测试与演示用） ---------------------------
    @app.post("/cluster/fail/{replica_id}")
    def fail_replica(replica_id: str) -> dict[str, str]:
        try:
            app.state.cluster.fail_replica(replica_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"status": "failed", "replica_id": replica_id}

    @app.post("/cluster/fault/{replica_id}")
    def inject_fault(replica_id: str) -> dict[str, str]:
        try:
            app.state.cluster.inject_write_fault(replica_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"status": "write-fault", "replica_id": replica_id}

    @app.post("/cluster/clear-fault/{replica_id}")
    def clear_fault(replica_id: str) -> dict[str, str]:
        try:
            app.state.cluster.clear_write_fault(replica_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"status": "fault-cleared", "replica_id": replica_id}

    @app.post("/cluster/restart/{replica_id}")
    def restart_replica(replica_id: str) -> dict[str, Any]:
        try:
            rep = app.state.cluster.restart_replica(replica_id)
        except (KeyError, StopIteration) as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {
            "status": "restarted", "replica_id": replica_id,
            "keys": len(rep.data), "requests": len(rep.seen_requests),
        }

    @app.post("/cluster/restart-coordinator")
    def restart_coordinator() -> dict[str, Any]:
        c = app.state.cluster.restart_coordinator()
        return {
            "status": "restarted",
            "idem_keys": len(c.idem_table),
            "conflicts": len(c.conflicts),
        }

    @app.post("/cluster/reconcile")
    def reconcile() -> dict[str, Any]:
        report = app.state.cluster.reconcile()
        return {
            "retried_requests": report.retried_requests,
            "repaired_replicas": report.repaired_replicas,
            "skipped": report.skipped,
            "converged": report.converged,
        }

    @app.get("/cluster")
    def cluster_status() -> dict[str, Any]:
        cl = app.state.cluster
        return {
            "n": cl.n,
            "quorum": cl.quorum,
            "replicas": [
                {
                    "replica_id": r.replica_id,
                    "alive": r.alive,
                    "fault_write": r.fault_write,
                    "keys": len(r.data),
                }
                for r in cl.replicas
            ],
        }

    return app
