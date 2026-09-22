# 一致性模型、冲突解决与本地验证

本服务在**单个进程内运行多个副本实例**，每个副本拥有独立的 JSON Lines WAL
目录（append + `fsync`），用它模拟跨节点复制与节点重启。协调器
（`app/coordinator.py`）在副本集合之上实现多数派写入协调。

## 1. 架构与数据流

```
客户端 ──► QuorumService（业务门面）
              │  直接调用 与  FastAPI(/writes ...) 共用同一条判定路径
              ▼
          Coordinator ── prepare/commit/abort ──► Replica r0..r4
              │                                     各自持有 WAL + 内存状态
              └─ 多数派读 / 冲突裁决 / repair 收敛
```

- 写入采用**两阶段提交（2PC）**：先向存活副本发 `prepare`，收集到多数派
  确认后再发 `commit`；任一阶段未达多数派，结论都不是成功。
- 每个副本只接受三种阶段迁移：`PREPARED → COMMITTED`、`PREPARED → ABORTED`。
  `commit`/`abort`/`prepare` 都是**幂等操作**，重复投递返回同一状态，不重复生效。
- 每条 WAL 记录都有单调递增 `seq`；重启时全量重放，同一幂等键以 `seq`
  最大的阶段为准，因此重复或乱序行不会造成重复生效。

## 2. 幂等语义

- 幂等键（`idempotency_key`）一旦在某副本上出现，就与**内容指纹**
  （SHA-256 of payload）永久绑定。
- 同键 + 同内容的重复提交：
  - 无论是否使用新的 `request_id`，都回放**首次提交的结论**
    （`request_id`、内容、确认副本列表完全一致），并标记 `duplicate=true`。
  - 同一键的并发提交由“按键互斥锁”串行化，只有第一次真正生效，
    其余全部得到与首次一致的结果，不会出现相互矛盾的返回。
- 同键 + **不同内容**：返回 `REJECTED_CONTENT_CONFLICT`
  （HTTP 409），原因中同时给出已提交指纹与本次指纹，幂等键永不被改写。
- 失败结论不做进程内缓存：未达多数派的写入可以用**同一个键**重试并最终成功。

## 3. 多数派确认与可区分的失败

多数派 = `floor(N/2) + 1`（默认 N=5，多数派=3）。

| 状态 | 触发条件 | HTTP | 是否可能已写盘 |
|---|---|---|---|
| `COMMITTED` | `commit` 持久化确认数 ≥ 多数派 | 200 | 是，且对读可见 |
| `REJECTED_CONTENT_CONFLICT` | 同键绑定了不同内容指纹 | 409 | 首次写入不受影响 |
| `INSUFFICIENT_REPLICAS` | 预检时存活副本数 < 多数派 | 503 | **否**，完全不尝试写入 |
| `FAILED_QUORUM` | 副本都存活（过预检），但 prepare/commit 确认数 < 多数派 | 503 | prepare 残留会被 abort；commit 阶段可能有少数派落盘 |
| `NOT_FOUND` | 多数派读未发现已提交记录 | 404 | 可能存在未决（PREPARED/ABORTED）痕迹，原因中可见 |

关键不变量：

1. **未达多数派绝不暴露为成功**。少数派上已 `COMMITTED` 的记录，在
   多数派读中不可见；`query`/`read` 都以“多数派门控”判定。
2. `INSUFFICIENT_REPLICAS` 与 `FAILED_QUORUM` 在 `status` 与 `reason`
   上都可区分（前者“no write attempted”，后者给出失败阶段与失败副本列表）。
3. commit 阶段失败时，未提交的副本尽力 abort；已提交的少数派副本保留事实，
   等待同键重试或 `repair` 收敛，不会被伪装成成功。

## 4. 冲突解决规则（LWW）

不同幂等键对**同一数据键（`data_key`）**的已提交写入互为冲突。裁决规则：

1. `timestamp_ns` **较大者胜**（时间戳由客户端在请求中提供）；
2. 时间戳相同，`request_id` **字典序较大者胜**。

规则标识固定为 `LWW_TIMESTAMP_NS_THEN_REQUEST_ID`，与提交顺序、到达顺序、
重放顺序无关，因此任意副本重放后结论一致。

- 两两冲突都会生成 `ConflictRecord`（胜/败双方的 request_id、幂等键、时间戳、
  规则名），并持久化到存活副本，可通过 `GET /conflicts?data_key=...` 查询。
- **败方不被静默丢弃**：败方按自己的幂等键仍可查到“自身已提交”，
  只是在数据键读取时不生效；三方及以上冲突会记录全部两两冲突对。
- 冲突记录按“请求对”去重，重复投递/重放不会产生重复记录。
- `repair` 以多数派为门控：只传播已达多数派的提交与冲突，
  **从不提升**未达多数派的少数派提交。

## 5. 重启与复制收敛

- “重启节点”=`restart(replica_id)`：重建 Replica 并重放该副本 WAL，
  恢复阶段、数据键索引与冲突记录，无重复生效、无状态丢失。
- “落后副本”：节点离线期间错过的提交，恢复后可由
  `POST /cluster/deliver/{rid}` 手动补投，或 `POST /repair` 统一补齐。
- “重复投递”：prepare/commit 重放均为幂等操作，重放后仍只生效一次。

## 6. 可解释性与日志

协调器对每次判定输出结构化日志，固定包含：
`request_id`、`idempotency_key`、`acked_replicas`/`committed_replicas`、
`failed`、`quorum`、`basis`（判定依据）、`decision`（结论）。
运行 pytest 加 `-s` 即可在单测日志中看到这些字段。

## 7. HTTP 接口

| 方法与路径 | 说明 |
|---|---|
| `POST /writes` | 提交写入（body: idempotency_key/data_key/payload/timestamp_ns/request_id） |
| `GET /writes/{idempotency_key}` | 按幂等键查询首次结论（多数派读） |
| `GET /data/{data_key}` | 读取数据键当前生效值（多数派读，原因中含冲突裁决） |
| `GET /conflicts?data_key=` | 查询冲突及其裁决 |
| `POST /repair` | 多数派门控的复制收敛 |
| `POST /cluster/replicas/{rid}/alive` | 模拟节点宕机/恢复 |
| `POST /cluster/replicas/{rid}/faults` | 注入 prepare/commit/abort 操作级故障（节点仍存活） |
| `POST /cluster/replicas/{rid}/clear-faults` | 清除故障注入 |
| `POST /cluster/replicas/{rid}/restart`、`POST /cluster/restart-all` | 重启重放 |
| `POST /cluster/deliver/{rid}` | 手动复制消息投递（落后/重复投递场景） |

## 8. 本地验证

```bash
uv sync
uv run pytest -q                       # 运行全部单测（29 个用例）
uv run pytest -s                       # 同时查看判定日志
uv run pytest tests/test_quorum.py -q  # 只跑多数派场景
```

手动起服务：

```bash
QUORUM_DATA_DIR=./.data uv run uvicorn main:app --port 8000
curl -s localhost:8000/health
```

测试覆盖场景：并发重复提交、同键不同内容拒绝、副本不足 vs 部分失败
（prepare/commit 两阶段）、少数派不暴露、同键重试完成、重启重放、
落后/重复投递收敛、repair 不提升未决提交、两方/三方冲突的 LWW 裁决、
以及 HTTP 与直接调用两个入口的结论一致性。
