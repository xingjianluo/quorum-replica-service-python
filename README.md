# Quorum Replica Service（本地多副本写入协调）

在单个 Python 进程内用**多个副本实例 + 各自独立的本地持久化文件**模拟跨节点复制，
实现带幂等键的多数派写入协调。重点不是网络分布式，而是把重复请求、部分副本失败、
节点重启三类场景下的状态判定做成**一致、可区分、可解释**。

## 架构

```
HTTP API (FastAPI, qrs/api.py)
        │
        ▼
Coordinator (qrs/coordinator.py)
  ├─ 幂等表 idem_key → 首次判定（持久化到 coordinator.log）
  ├─ 全局写入账本 request_id → 写入内容（恢复时重建，驱动自动对账）
  ├─ 多数派判定 / commit 标记发布 / 补复制 resume / 自动对账 reconcile
  ├─ 未决写入状态视图（SUCCESS / WAITING / FAILED）与恢复校验 verify
  └─ 冲突历史收集
        │  复制写入（先 fsync WAL 再生效）
        ▼
Replica replica-0 ── replica-0.wal   (qrs/replica.py)
Replica replica-1 ── replica-1.wal
Replica replica-2 ── replica-2.wal
```

- 持久化（`qrs/storage.py`）：每条记录一行 JSON（带格式版本 `v`），追加写后
  `flush + fsync`；崩溃可能留下半行，重放时跳过并计入恢复校验报告，
  不影响已确认记录。
- 副本（`qrs/replica.py`）：WAL 有两类记录，`write`（复制写入）与
  `commit`（多数派确认标记）；v2 记录带版本字段，升级前的 v1 记录（无
  `v`）按旧语义归一化重放。重启 = 丢弃内存、重建实例、重放 WAL。
- 协调者（`qrs/coordinator.py`）：自身有独立提交日志，重启后重放幂等表，
  并从各副本 WAL 重建冲突历史。
- 集群（`qrs/cluster.py`）：默认 3 副本，多数派 = N//2+1 = 2；支持
  副本宕机、写盘故障注入、副本/协调者重启。

## 一致性模型

1. **幂等提交**
   - 同一 `idem_key` 的**首次判定**（成功或失败）立即持久化，对同一键的提交在
     协调者内按键串行化（互斥锁），并发提交只有一个首次生效。
   - 重复提交内容（数据键 + 值）与首次完全一致：重放首次结论（响应带
     `replay=true`）。首次成功则返回 `REPLAYED`（200），其
     `request_id / value / acked_replicas / quorum` 与首次结果一致；
     **首次失败（503/504）时重试仍返回同样的失败码**，绝不会把未达多数派的
     写入重放成 200 成功——需要推进收敛时显式调用 `resume`。
   - 同键但内容不同：返回 `CONTENT_MISMATCH`（HTTP 409）被拒绝，响应里带首次
     提交的键与 `request_id` 作为可区分原因；首次结果不受影响。
   - 查询入口 `GET /writes/{idem_key}` 与提交入口结论一致。

2. **多数派确认（Quorum）**
   - 协调者向所有存活副本复制；`>= N//2+1` 个副本 ACK 后，才向这些副本发布
     `commit` 标记，并返回 `COMMITTED`。
   - 读取（`GET /keys/{key}`）要求同一 `request_id` 在多数副本上带 `commit`
     标记才 `visible=true`；少数派残留即使存在也不暴露为成功值。
   - 两类失败严格区分：
     | 情形 | code | HTTP | 含义 |
     | --- | --- | --- | --- |
     | 存活副本数本身 < 多数派 | `NOT_ENOUGH_REPLICAS` | 503 | 不具备提交条件 |
     | 存活够但 ACK 数 < 多数派 | `QUORUM_FAILED` | 504 | 尽力复制后仍未达多数派 |
   - 未达多数派的写入只可能残留在少数副本，状态被记录，可在故障恢复后
     **自动对账**（副本重启/故障解除/协调者重启时触发）或手动
     `POST /writes/{idem_key}/resume` 补复制；达到多数派后判定升级为
     `COMMITTED`，历史判定以最终结果为准（同一幂等键仍只有一个结论）。
     若该写入已被同键更新的已提交写入按 LWW 取代，则不再等待，状态为 `FAILED`。

3. **重放收敛**
   - 同一 `request_id` 的写入重复投递是幂等 no-op（副本以 `request_id` 去重）。
   - 迟到/旧序写入按冲突规则落败，不覆盖已生效状态；更新的写入覆盖生效。
   - 副本重启只信任 WAL：重复记录、乱序记录重放后都收敛到同一状态。

4. **自动对账与故障恢复收敛**
   - 协调者维护全局写入账本（`request_id → 写入内容`，随判定持久化、重启重建）。
   - 以下时机自动执行一次对账（`reconcile`），无需人工介入、无需重新发起写入：
     副本重启回来、写盘故障注入解除、协调者重启、使用已有数据目录启动。
   - 对账按账本的**历史投递顺序**把缺失写入补到落后副本（重复投递在副本侧
     幂等去重，**不重复生效**）；随后给所有存活副本补发已确认写入的
     `commit` 标记，最终各副本上已提交数据状态一致。
   - 对账只补齐、不覆盖：已确认的写入不会被旧投递盖掉；落后副本上的乱序/
     重复写入仍走 LWW 与去重规则，败方照旧进入冲突历史。
   - 也可随时手动触发：`POST /writes/reconcile`（返回补齐/升级/终态/等待明细）。

5. **未达多数派写入的状态查询与后续收敛**
   - 一次写入没凑够多数派时，响应仍明确返回 `503/504`；同时可通过
     `GET /writes/{idem_key}/status` 查询当前处于：
     | status | 含义 |
     | --- | --- |
     | `SUCCESS` | 已达多数派并发布 commit（首次失败的判定随之以最终结果为准） |
     | `WAITING` | 尚未达多数派，条件恢复后可继续推进；响应给出卡在哪一步、已提交/持有/缺失的副本 |
     | `FAILED` | 终态：被同数据键上更新的已提交写入按 LWW 取代，不再等待、resume 也无法救回 |
   - `GET /writes` 列出所有仍处于 `WAITING/FAILED` 的写入。
   - 状态查询返回 `acked_replicas`（带 commit 的副本）、`pending_replicas`
     （缺失或宕机副本）、`quorum`、`step`（卡点）、`basis`（判定依据）。
   - 整个 WAITING 期间 `GET /keys/{key}` 始终 `visible=false`，读不到未确认内容。

6. **历史落盘数据兼容与恢复校验**
   - WAL 记录带格式版本（当前 `v=2`）；升级前的 **v1 历史数据**（无版本字段）
     重启后按 v1 语义归一化重放：可正常启动、读取、幂等重放、冲突判定，
     不重复生效、不丢已确认状态。
   - 崩溃半行、找不到 write 的孤儿 `commit` 标记、字段缺失等异常**不阻断启动**，
     而是记录到副本 `recovery_anomalies` 并由 `GET /cluster/verify` 如实报告。
   - `GET /cluster/verify` 汇总：已提交写入的副本覆盖是否仍达多数派、
     仍在等待的 request、重放异常列表、总体 `ok`；启动（含协调者重启）时自动执行。

## 冲突解决规则

对同一数据键的不同写入，按确定的全序解决（LWW）：

```
先比较 timestamp，较大者胜；timestamp 相同则 request_id 字典序较大者胜。
```

- 败方**不会被静默丢弃**：每次覆盖/落败都会生成 `ConflictRecord`，记录
  胜方与败方的 `request_id / timestamp`、适用规则与说明（含观察到冲突的副本）。
- 冲突可通过 `GET /conflicts?key=...` 查询；协调者重启后从副本 WAL 重建该历史。
- `timestamp` 可由调用方提供（便于确定性测试），缺省由服务端单调时钟分配。

## HTTP 接口

| 方法 路径 | 说明 |
| --- | --- |
| `POST /writes` | 提交写入，body: `idem_key,key,value,request_id,timestamp?` |
| `GET  /writes` | 列出所有未决写入（WAITING/FAILED）及其卡点与确认副本 |
| `GET  /writes/{idem_key}` | 查询幂等键的首次提交结果（结论会随收敛升级） |
| `GET  /writes/{idem_key}/status` | 查询收敛状态：SUCCESS/WAITING/FAILED、卡点、acked/pending |
| `POST /writes/{idem_key}/resume` | 对未达多数派的写入手动补复制 |
| `POST /writes/reconcile` | 手动触发全量自动对账，返回补齐/升级/终态/等待明细 |
| `GET  /keys/{key}` | 多数派视角读取数据键（WAITING 期间 visible=false） |
| `GET  /conflicts?key=...` | 查询冲突与解决结果 |
| `GET  /cluster/verify` | 恢复校验：已提交覆盖、未决请求、重放异常、ok |
| `POST /cluster/fail/{rid}` | 副本宕机注入 |
| `POST /cluster/fault/{rid}` | 副本存活但写盘失败注入 |
| `POST /cluster/clear-fault/{rid}` | 解除写盘故障（自动触发对账） |
| `POST /cluster/restart/{rid}` | 重启单个副本（WAL 重放，随后自动对账） |
| `POST /cluster/restart-coordinator` | 重启协调者（恢复+校验+对账） |
| `GET  /cluster` | 集群与各副本状态 |

`COMMITTED/REPLAYED → 200`，`CONTENT_MISMATCH → 409`，
`NOT_ENOUGH_REPLICAS → 503`，`QUORUM_FAILED → 504`。

## 日志

单测/运行日志会打印判定所需三要素：**请求标识**（`request_id`）、
**确认副本**（逐副本 `ACK/FAILED` 及汇总 `acked/pending`）、
**判定依据**（`verdict=... basis=...`）。自动对账时额外打印
`[reconcile] backfill request_id=... -> replica-x`（补齐动作）与每个写入的
`verdict=SUCCESS/FAILED/WAITING` 及持有者副本，便于追踪收敛过程。

## 本地验证

```bash
uv sync
uv run pytest                 # 全部 32 个用例
uv run pytest -s              # 同时查看逐请求协调/对账日志
uv run uvicorn main:app --reload
# 然后：
# curl -X POST localhost:8000/writes -H 'content-type: application/json' \
#   -d '{"idem_key":"k1","key":"account-1","value":"100","request_id":"r1","timestamp":1}'
# curl localhost:8000/keys/account-1
```

手动演示“未达多数派 → 查询等待 → 恢复后自动收敛”：

```bash
# 两个副本写盘故障：写入得到 504
curl -X POST localhost:8000/cluster/fault/replica-1
curl -X POST localhost:8000/cluster/fault/replica-2
curl -X POST localhost:8000/writes -H 'content-type: application/json' \
  -d '{"idem_key":"ik-1","key":"account-1","value":"100","request_id":"r1","timestamp":1}'
# 查询当前状态：WAITING，给出卡点与确认/缺失副本；此时读取仍不可见
curl localhost:8000/writes/ik-1/status
curl localhost:8000/keys/account-1
# 恢复一个副本（解除即自动对账）：状态变 SUCCESS，读取可见
curl -X POST localhost:8000/cluster/clear-fault/replica-1
curl localhost:8000/writes/ik-1/status
curl localhost:8000/cluster/verify
```

测试覆盖（`tests/`）：

- `test_idempotency.py`：首次/重复提交一致、同键异内容拒绝、16 路并发重复提交
  只生效一次且响应不矛盾、并发 REPLAYED 与 MISMATCH 混杂。
- `test_quorum.py`：单副本失败仍成功、双写故障 504、存活不足 503、**失败结论
  的重复提交仍返回 504/503 不被掩盖为成功**、失败状态跨协调者重启保持、
  `resume` 补复制收敛、重复/落后投递重放收敛。
- `test_recovery_conflict.py`：副本重启不重复生效不丢状态、协调者重启恢复幂等表
  与冲突历史、timestamp/request_id 两级 tie-break、同数据键并发冲突写入确定解决、
宕机副本重启后补复制收敛、少数派残留在条件未恢复时仍不可见、恢复后自动收敛。
- `test_consistency.py`：HTTP 各入口结论一致；日志含请求标识、确认副本、判定依据；
  两类失败原因在日志中可区分。
- `test_reconcile.py`（本轮新增，9 个用例）：
  - 落后/宕机副本回归后自动补齐缺口、已提交写入补发 commit 标记、三副本最终一致；
  - 滞后副本重复投递幂等 no-op、乱序旧写入落败、重启后仍不重复生效；
  - 协调者/副本重启后的自动收敛与 `verify` 恢复校验；
  - 未达多数派写入的 `WAITING` 状态查询（卡点、acked/pending、读不可见）与
    恢复后自动 `SUCCESS`；
  - 未达多数派旧写入被更新的已提交写入 LWW 取代后判 `FAILED` 终态（resume
    无法救回、幂等与冲突语义保持、败方可查）；
  - 4 路并发冲突写入叠加落后副本回归时按 LWW 确定收敛、各幂等键结论互不覆盖；
  - 升级前 v1 历史 WAL（含崩溃半行、孤儿 commit 标记、落后空副本）可直接启动、
    读取、自动补齐收敛，幂等重放/内容冲突规则不变；
  - FAILED/WAITING 跨协调者与副本重启保持；
  - 对账日志含请求标识、确认副本、补齐动作与 `verdict` 判定依据。
