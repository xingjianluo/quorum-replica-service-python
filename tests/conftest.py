"""pytest 夹具：每个测试使用独立临时目录的 3 副本集群。"""
from __future__ import annotations

import tempfile

import pytest
from fastapi.testclient import TestClient

from qrs.api import create_app
from qrs.cluster import Cluster


@pytest.fixture()
def cluster():
    # 显式关闭自动对账：这些用例需要手动控制对账/恢复时机
    base_dir = tempfile.mkdtemp(prefix="qrs-test-")
    cl = Cluster(base_dir=base_dir, n=3, auto_reconcile=False)
    yield cl
    cl.cleanup()


@pytest.fixture()
def client(cluster):
    app = create_app(cluster)
    with TestClient(app) as c:
        yield c
