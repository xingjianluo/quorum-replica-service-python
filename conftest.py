"""pytest 公共夹具。"""

from __future__ import annotations

import logging

import pytest

from app.service import QuorumService, configure_logging


@pytest.fixture
def service(tmp_path):
    configure_logging(logging.INFO)
    return QuorumService(data_dir=str(tmp_path / "replicas"), replica_count=5)
