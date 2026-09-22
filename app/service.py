"""对外业务门面：组装集群与协调器，统一入口（直接调用与 HTTP 共用）。"""

from __future__ import annotations

import logging
import time
import uuid

from app.cluster import ReplicaCluster
from app.coordinator import Coordinator
from app.models import WriteResult

_CONFIGURED = False


def configure_logging(level: int = logging.INFO) -> None:
    """配置单测与服务共用的日志格式。

    判定日志中固定带 request_id / idempotency_key / acked_replicas /
    quorum / basis / decision 等字段，便于解释每一次判定。
    """
    global _CONFIGURED
    if _CONFIGURED:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    root = logging.getLogger("quorum")
    root.handlers[:] = [handler]
    root.setLevel(level)
    root.propagate = False
    _CONFIGURED = True


class QuorumService:
    """直接调用入口；HTTP 层也是对它的薄封装，两者判定路径完全相同。"""

    def __init__(self, data_dir: str, replica_count: int = 5) -> None:
        self.data_dir = data_dir
        self.cluster = ReplicaCluster(data_dir, replica_count=replica_count)
        self.coordinator = Coordinator(self.cluster)

    def submit(
        self,
        idempotency_key: str,
        data_key: str,
        payload: str,
        timestamp_ns: int | None = None,
        request_id: str | None = None,
    ) -> WriteResult:
        return self.coordinator.submit(
            idempotency_key=idempotency_key,
            data_key=data_key,
            payload=payload,
            timestamp_ns=timestamp_ns if timestamp_ns is not None else time.time_ns(),
            request_id=request_id or self.new_request_id(),
        )

    def query_idempotency(self, idempotency_key: str) -> WriteResult:
        return self.coordinator.query_idempotency(idempotency_key)

    def read_data(self, data_key: str) -> WriteResult:
        return self.coordinator.read_data(data_key)

    def list_conflicts(self, data_key: str | None = None):
        return self.coordinator.list_conflicts(data_key)

    def repair(self) -> dict:
        return self.coordinator.repair()

    @staticmethod
    def new_request_id() -> str:
        return f"req-{uuid.uuid4().hex}"
