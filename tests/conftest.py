"""pytest 夹具：每个测试使用独立临时目录的 3 副本集群。"""
from __future__ import annotations

import tempfile

import pytest
from fastapi.testclient import TestClient

from qrs.api import create_app
from qrs.cluster import Cluster


@pytest.fixture()
def cluster():
    base_dir = tempfile.mkdtemp(prefix="qrs-test-")
    cl = Cluster(base_dir=base_dir, n=3)
    yield cl
    cl.cleanup()


@pytest.fixture()
def client(cluster):
    app = create_app(cluster)
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def manual_cluster():
    """显式关闭自动对账的集群：用于隔离验证手动对账/续推语义本身。"""
    base_dir = tempfile.mkdtemp(prefix="qrs-test-manual-")
    cl = Cluster(base_dir=base_dir, n=3, auto_reconcile=False)
    yield cl
    cl.cleanup()


@pytest.fixture()
def manual_client(manual_cluster):
    app = create_app(manual_cluster)
    with TestClient(app) as c:
        yield c
