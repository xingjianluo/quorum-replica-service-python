"""uvicorn 启动入口：uvicorn main:app"""

from __future__ import annotations

import os

from app.api import create_app
from app.service import QuorumService, configure_logging

configure_logging()

service = QuorumService(
    data_dir=os.environ.get("QUORUM_DATA_DIR", "./.data"),
    replica_count=int(os.environ.get("QUORUM_REPLICAS", "5")),
)

app = create_app(service)
