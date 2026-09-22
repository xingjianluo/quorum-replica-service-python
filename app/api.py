"""FastAPI HTTP 入口：薄封装 QuorumService，不包含业务判定逻辑。

所有判定（幂等、多数派、冲突）都在 coordinator 内完成；
HTTP 与直接调用得到的结论字段完全一致，仅多一层 HTTP 状态码映射：
COMMITTED=200 / REJECTED_CONTENT_CONFLICT=409 / 未达多数派类=503 / NOT_FOUND=404。
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.models import WriteStatus
from app.service import QuorumService

_HTTP_STATUS = {
    WriteStatus.COMMITTED: 200,
    WriteStatus.REJECTED_CONTENT_CONFLICT: 409,
    WriteStatus.FAILED_QUORUM: 503,
    WriteStatus.INSUFFICIENT_REPLICAS: 503,
    WriteStatus.NOT_FOUND: 404,
}


class SubmitBody(BaseModel):
    idempotency_key: str
    data_key: str
    payload: str
    timestamp_ns: int | None = None
    request_id: str | None = None


class AliveBody(BaseModel):
    alive: bool


class FaultsBody(BaseModel):
    ops: list[str]


class DeliverBody(BaseModel):
    op: str
    idempotency_key: str
    data_key: str | None = None
    payload: str | None = None
    timestamp_ns: int | None = None
    request_id: str | None = None


def _result_response(result) -> JSONResponse:
    return JSONResponse(
        status_code=_HTTP_STATUS[result.status],
        content=result.to_dict(),
    )


def create_app(service: QuorumService) -> FastAPI:
    app = FastAPI(title="quorum-replica-service")

    @app.get("/health")
    def health() -> dict:
        return {
            "status": "ok",
            "replicas": service.cluster.replica_count,
            "quorum": service.cluster.quorum_for(service.cluster.replica_count),
            "alive": len(service.cluster.alive_replicas()),
        }

    @app.post("/writes")
    def submit(body: SubmitBody):
        result = service.submit(
            idempotency_key=body.idempotency_key,
            data_key=body.data_key,
            payload=body.payload,
            timestamp_ns=body.timestamp_ns,
            request_id=body.request_id,
        )
        return _result_response(result)

    @app.get("/writes/{idempotency_key}")
    def query_write(idempotency_key: str):
        result = service.query_idempotency(idempotency_key)
        return _result_response(result)

    @app.get("/data/{data_key}")
    def read_data(data_key: str):
        result = service.read_data(data_key)
        return _result_response(result)

    @app.get("/conflicts")
    def list_conflicts(data_key: str | None = None):
        records = [r.to_dict() for r in service.list_conflicts(data_key)]
        return {"conflicts": records, "count": len(records)}

    @app.post("/repair")
    def repair():
        return service.repair()

    @app.post("/cluster/replicas/{replica_id}/alive")
    def set_alive(replica_id: str, body: AliveBody):
        service.cluster.set_alive(replica_id, body.alive)
        return {"replica": replica_id, "alive": body.alive}

    @app.post("/cluster/replicas/{replica_id}/faults")
    def set_faults(replica_id: str, body: FaultsBody):
        service.cluster.set_faults(replica_id, set(body.ops))
        return {"replica": replica_id, "faults": body.ops}

    @app.post("/cluster/replicas/{replica_id}/clear-faults")
    def clear_faults(replica_id: str):
        service.cluster.clear_faults(replica_id)
        return {"replica": replica_id, "faults": []}

    @app.post("/cluster/replicas/{replica_id}/restart")
    def restart(replica_id: str):
        service.cluster.restart(replica_id)
        return {"replica": replica_id, "restarted": True}

    @app.post("/cluster/restart-all")
    def restart_all():
        service.cluster.restart_all()
        return {"restarted": True}

    @app.post("/cluster/deliver/{replica_id}")
    def deliver(replica_id: str, body: DeliverBody):
        return service.cluster.deliver(replica_id, body.model_dump())

    return app
