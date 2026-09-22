# quorum-replica-service-python

进程内多副本 + 本地 WAL 持久化模拟的多数派写入协调服务，演示：

- 幂等键提交/查询：重复提交结论一致，同键不同内容明确拒绝，并发安全；
- 多数派确认：prepare/commit 两阶段，未达多数派绝不暴露为成功，
  副本不足与部分副本失败原因可区分；
- 重启重放与冲突写入：WAL 重放不重复生效、不丢状态，冲突按
  `LWW(timestamp_ns, request_id)` 确定性裁决且可查询、不静默丢弃任何一方；
- 落后/重复复制消息经重放与 `repair` 收敛；直接调用与 HTTP 两个入口结论一致。

## 快速开始

```bash
uv sync
uv run pytest -q          # 全部单测
uv run pytest -s          # 含 request_id / 确认副本 / 判定依据 的单测日志
uv run uvicorn main:app --port 8000
```

详见 [docs/consistency.md](docs/consistency.md)。
