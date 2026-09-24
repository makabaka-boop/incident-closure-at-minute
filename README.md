# raincut — 暴雨管网最小关管成本与污染传播事件

暴雨污染从供水入口（sources）沿管网流向取水口（sinks）。本服务提供三类能力：

1. **最小关管成本**：计算阻断所有 source → sink 有向路径所需的最小关管总成本
   （有向图最小 s-t 割，`POST /mincut`）。
2. **污染传播事件**：给定管网（管道有向通行分钟）、多个污染源与各自释放分钟、
   重点取水口与截止分钟，事件时钟可分段推进，返回本次首次到达节点、累计最早
   到达分钟与状态（`POST /incidents`、`POST /incidents/{id}/advance`）。
3. **关管预演**：在事件推进到当前分钟时，只读地预演"立即关闭若干条管道"
   能否保护重点取水口，不改写已经发生的传播
   （`POST /incidents/{id}/simulate`）。

## 算法

- 自建**超级源**与**超级汇**：超级源以容量 `总边费 + 1` 连向每个 source，
  每个 sink 以同样容量连向超级汇。因为任何只含原始边的割至多为总边费，
  最优割绝不会切断超级边，故最大流的值恰等于最小关管成本
  （最大流最小割定理）。
- 最大流使用**自实现的 Dinic 算法**（BFS 分层图 + 当前弧优化的 DFS 增广），
  容量全程使用 64 位整数。无外部求解器，不枚举割集。
- 平行边各自独立计费；自环永远不会跨越任何割，自然不影响结果；
  原本无通路时最大流为 0，返回 0。

## 快速开始（Docker Compose）

```bash
# 启动 API（默认宿主端口 8080，可用 API_PORT 覆盖）
API_PORT=9000 docker compose up --build api

# 一次性验收：先跑 Go 测试，再对真实 API 跑全部黑盒检查
docker compose up --build --exit-code-from verify
docker compose down
```

`verify` 服务的退出码即验收结果（0 = 全部通过）。

## API

### `POST /mincut`

请求体（仅普通 JSON 基础类型，数值必须为整数）：

| 字段      | 类型    | 约束                                             |
| --------- | ------- | ------------------------------------------------ |
| `n`       | integer | 节点数，编号 `0..n-1`，`2 ≤ n ≤ 20000`           |
| `edges`   | array   | 至多 100000 条；`from`/`to` 为合法节点，`1 ≤ cost ≤ 10^9` |
| `sources` | array   | 非空节点数组，与 `sinks` 互斥                     |
| `sinks`   | array   | 非空节点数组，与 `sources` 互斥                    |

成功响应 `200`：

```json
{"minimum_shutdown_cost": 13}
```

请求示例：

```bash
curl -s -X POST http://localhost:${API_PORT:-8080}/mincut \
  -H 'Content-Type: application/json' \
  -d '{
        "n": 4,
        "edges": [
          {"from": 0, "to": 1, "cost": 5},
          {"from": 0, "to": 2, "cost": 8},
          {"from": 1, "to": 3, "cost": 5},
          {"from": 2, "to": 3, "cost": 8}
        ],
        "sources": [0],
        "sinks": [3]
      }'
# => {"minimum_shutdown_cost":13}
```

### 错误

任何非法输入（越界、非法引用、空端点组、端点重叠、非整数数值、畸形
JSON 等）都不会进入求解，统一返回 `422` 与稳定错误结构：

```bash
curl -s -X POST http://localhost:8080/mincut \
  -d '{"n":1,"edges":[],"sources":[0],"sinks":[1]}'
# HTTP 422
# {"error":{"code":"invalid_input","message":"n must satisfy 2 <= n <= 20000, got 1"}}
```

### `GET /healthz`

健康检查，返回 `{"status":"ok"}`。排空（draining）期间保持 `200`。

### `GET /readyz`

就绪探针。正常服务时返回 `200 {"status":"accepting"}`；收到停止信号进入
排空后返回 `503 {"status":"draining"}`，编排系统应停止向本实例调度新请求。

## 优雅停机（两阶段排空）

进程收到 `SIGTERM` / `SIGINT` 后进入两阶段排空：

1. **屏障（accepting → draining）**：共享准入器（`internal/lifecycle`）在同一把
   锁下裁决"状态切换"与"租约授予"，二者构成同一线性化点。屏障前已取得租约的
   请求**完整执行到底**（即使请求体尚未发完）；屏障后的业务请求
   （`/mincut`、`/incidents`、`/incidents/{id}/advance`、
   `/incidents/{id}/simulate`）不再进入处理器、不改变
   任何事件，稳定返回 `503 {"error":{"code":"draining",...}}`。`/healthz` 与
   `/readyz` 旁路准入器。
2. **归零 → 关闭（draining → stopped）**：在 `DRAIN_TIMEOUT`（Go 时长或裸秒数，
   默认 `5s`）内等待存量租约归零；归零则仅调用一次 `Shutdown` 并以退出码 `0`
   结束；超时则 `Close` 强制关闭并以非零退出。重复信号被吞掉，不会触发第二次
   关闭，也不会死锁。

```bash
DRAIN_TIMEOUT=10s API_PORT=8080 go run ./cmd/api
```

## 污染传播事件

### `POST /incidents`

创建一个独立的污染传播事件，返回事件标识 `id`（`inc_` 前缀的随机串）与初始
快照。请求体（数值均为整数）：

| 字段       | 类型    | 约束                                                       |
| ---------- | ------- | ---------------------------------------------------------- |
| `n`        | integer | 节点数，编号 `0..n-1`，`2 ≤ n ≤ 20000`                     |
| `pipes`    | array   | 至多 100000 条；`from`/`to` 为合法节点，`1 ≤ minutes ≤ 10^9` |
| `releases` | array   | 非空；`{node, at}`，`0 ≤ at ≤ deadline`                    |
| `intakes`  | array   | 非空重点取水口节点数组                                     |
| `deadline` | integer | 截止分钟，`0 ≤ deadline ≤ 10^9`                            |

管道是**有向**的：只有 `from → to` 方向传播。平行管道各自可通行，最终取更早
路径；**自环不插入传播图、反向边不会造成回流**。

成功响应 `200`（创建后时钟在分钟 0、状态 `scheduled`、无任何到达）：

```json
{
  "id": "inc_ac6eebc1e5cfe95811a429aa172ca8ec",
  "snapshot": {"current_minute": 0, "status": "scheduled", "earliest_arrivals": {}}
}
```

### `POST /incidents/{id}/advance`

把事件时钟推进到请求分钟（请求体 `{"minute": 5}`），原子地提交**时钟校验、
到达增量与状态跃迁**，返回本次首次到达的节点（`new_arrivals`，按到达分钟再按
节点排序）与最新快照：

```json
{
  "new_arrivals": [{"node": 0, "at_minute": 1}, {"node": 1, "at_minute": 3}],
  "snapshot": {
    "current_minute": 5,
    "status": "breached",
    "earliest_arrivals": {"0": 1, "1": 3, "2": 5}
  }
}
```

`earliest_arrivals` 是截至当前时钟、每个节点的**累计最早到达分钟**（不可达
节点不出现）。

**到达计算**：创建事件时以每个污染源的释放分钟为该节点初值，沿有向管道做
**多源 Dijkstra 松弛**（边权为通行分钟）；多个污染源取更早释放。平行管自然
取更早路径，自环与反向边不产生传播。

**状态机**：

- `scheduled`：时钟尚未越过首个污染源的释放分钟；
- `propagating`：首个污染源已释放，且未触达取水口、未到截止；
- `breached`（终态）：在 `deadline` 及之前有重点取水口被触达（恰在截止分钟
  到达也算 breached）；
- `contained`（终态）：时钟到达截止分钟且没有任何取水口被触达。

**推进语义**：

- 推进到**当前分钟**是幂等重试：返回空 `new_arrivals` 与相同快照（终态后同样
  适用，仍是 200）；
- **倒退**（目标分钟小于已提交时钟）、**越过截止**（目标分钟大于 `deadline`）、
  **终态后再推进**均返回稳定 `409`；
- 并发推进由每事件互斥锁串行化：各请求只能按提交顺序单调生效，已被更新的时钟
  拒绝；所有失败都不会改变快照。

错误码：非法拓扑/非法请求体为稳定 `422`（`invalid_input` / `invalid_json`）；
未知事件为 `404`（`not_found`）；冲突为稳定 `409`（`clock_regression` /
`past_deadline` / `incident_terminal`）。错误体均为统一的
`{"error":{"code":...,"message":...}}`。

```bash
# 创建
curl -s -X POST http://localhost:${API_PORT:-8080}/incidents \
  -H 'Content-Type: application/json' \
  -d '{
        "n": 4,
        "pipes": [
          {"from": 0, "to": 1, "minutes": 2},
          {"from": 1, "to": 2, "minutes": 3},
          {"from": 2, "to": 3, "minutes": 2}
        ],
        "releases": [{"node": 0, "at": 1}],
        "intakes": [3],
        "deadline": 20
      }'

# 推进到分钟 8（节点 0@1、1@3、2@6 首次到达；取水口节点 3 要到 @8 才触达）
curl -s -X POST http://localhost:8080/incidents/<id>/advance \
  -H 'Content-Type: application/json' -d '{"minute": 7}'
# => new_arrivals 0@1/1@3/2@6，status=propagating
curl -s -X POST http://localhost:8080/incidents/<id>/advance \
  -H 'Content-Type: application/json' -d '{"minute": 8}'
# => new_arrivals 3@8，status=breached
```

### `POST /incidents/{id}/simulate`

**只读关管预演**：在事件推进到的当前分钟，预演"立即关闭若干条管道"能否
保护重点取水口。请求体 `closed_pipes` 为创建事件时的管道**下标**（0 起，
按 `pipes` 数组顺序；重复或越界下标返回 `422`）：

```json
{"closed_pipes": [1, 3]}
```

预演规则：

- 以响应中 `event_minute`（事件当前时钟）为关管生效时刻；预演在**同一
  事件快照**下记录当前分钟、已到达节点与传播状态（`snapshot` /
  `arrived_nodes`），并发推进不会把两个状态拼进一份预演；
- 关管只阻止**当前分钟及之后开始**的通行；当前分钟之前已进入管道的污染
  仍在途，按原通行时间抵达（恰在当前分钟出发的通行被阻止）；
- 当前分钟及之后的释放源照常生效；平行管按各自下标独立关闭；自环关闭
  无效果；
- 接口完全只读：事件时钟、首次到达记录与 `advance` 行为均不受任何影响。

成功响应 `200`（示例：链 `0-2→1-2→2`，释放 `0@0`，取水口 `2`，截止 `10`，
时钟在分钟 `2` 时关闭管道 `1`）：

```json
{
  "event_minute": 2,
  "status": "propagating",
  "arrived_nodes": {"0": 0, "1": 2},
  "snapshot": {"current_minute": 2, "status": "propagating",
               "earliest_arrivals": {"0": 0, "1": 2}},
  "closed_pipes": [1],
  "baseline_arrivals": {"0": 0, "1": 2, "2": 4},
  "shutdown_arrivals": {"0": 0, "1": 2},
  "changes": [{"node": 2, "baseline": 4, "shutdown": null}],
  "intakes": [{"node": 2, "baseline_at": 4, "shutdown_at": null,
               "baseline_breached": true, "shutdown_breached": false}],
  "baseline_status": "breached",
  "shutdown_status": "contained",
  "protected": true
}
```

- `baseline_arrivals` / `shutdown_arrivals`：原方案与关管方案在**截止分钟
  及之前**的各节点最早到达（不可达节点不出现）；
- `changes`：到达时间发生变化的节点，按节点编号升序；`baseline` /
  `shutdown` 为 `null` 表示该方案在截止前不可达；
- `intakes`：每个重点取水口的到达分钟与是否被触达结论；
- `baseline_status` / `shutdown_status`：两方案的截止结论（`breached` /
  `contained`）；`protected=true` 表示关管方案下所有取水口在截止前均未
  被触达。

错误：未知事件 `404 not_found`；重复/越界下标 `422 invalid_input`（畸形
JSON 为 `422 invalid_json`）；事件已到终态（`breached` / `contained`）
返回 `409 incident_terminal`。所有拒绝都不会改变事件。

```bash
curl -s -X POST http://localhost:8080/incidents/<id>/simulate \
  -H 'Content-Type: application/json' -d '{"closed_pipes": [1]}'
```

## 验收内容（verify 服务）

`cmd/verify` 是黑盒验收客户端，只调用真实 HTTP 接口（无假接口、无固定
结果），检查：

- `/healthz` 可用、`/mincut` 平行边各自计费、自环不影响结果、有向性、
  多源多汇等**精确代价**，原本无通路时返回 0；
- 各类非法图返回 `422` 且错误结构稳定（`error.code` / `error.message`）；
- **20000 节点 / 100000 边**的最小割大图与传播事件大图均在 **10 秒请求
  超时**内返回精确结果；
- 污染事件：创建返回标识与初始快照，**分段推进**的精确到达分钟、首次到达
  增量与累计最早到达分钟；平行管取更早路径、自环与反向边不传播；
- 同分钟重试返回空增量与相同快照；倒退 / 越过截止 / 终态推进均为稳定 `409`
  且失败不改变快照；未知事件为 `404`；
- **并发推进**：15 个乱序目标分钟并发提交只能单调串行生效，20 个同分钟
  并发请求只有一个提交携带增量；
- `breached`（截止前触达取水口，恰在截止分钟也算）与 `contained`（到截止
  未触达）两类终态；
- **关管预演**：平行管按下标独立关闭与改道、在途污染按原通行时间抵达、
  恰在关闭分钟的出发被阻止、未来释放源照常生效、截止边界（恰在截止分钟
  到达仍算触达）、自环关闭无效果；预演只读且基于同一事件快照；重复/越界
  下标 `422`、终态事件 `409`、未知事件 `404`；
- **优雅停机**：以真实子进程、半发送请求与真实 `SIGTERM`/`SIGINT` 复现正常与
  超时路径——屏障前取得租约的半发送请求补全请求体后完整执行并返回精确结果；
  屏障后业务请求稳定 `503 draining` 且不进入处理器；`/readyz` 在排空时返回
  `503 draining`、`/healthz` 保持 `200`；存量归零后退出码 `0`，租约卡死超过
  `DRAIN_TIMEOUT` 则强制关闭并非零退出；重复信号无重复关闭、无死锁；
- `API_PORT` / `API_BASE_URL` 两种寻址方式都可用。

verify 容器启动时先执行 `go test ./...`（含对小图与暴力枚举割的对拍、
传播事件的状态机 / 幂等 / 并发单元测试、关管预演与逐分钟模拟预言机的
随机小图对拍，以及同规模大图测试），全部通过后再发起 HTTP 检查。

## 本地开发

```bash
go test ./...        # 单元测试
API_PORT=8080 go run ./cmd/api   # 本地启动（API_PORT 或 PORT，默认 8080）
API_BASE_URL=http://localhost:8080 go run ./cmd/verify   # 对本地实例验收
API_PORT=8080 go run ./cmd/verify                        # 等价寻址方式
```

## 项目结构

```
cmd/api/       HTTP 服务入口（信号驱动的两阶段排空）
cmd/verify/    一次性黑盒验收客户端（含排空验收：子进程 + 半发送请求 + 真实信号）
internal/api/  路由、请求校验、错误结构（mincut + incidents + simulate）、准入屏障与探针
internal/lifecycle/ 共享准入器：accepting → draining → stopped 与租约线性化裁决
internal/flow/ 自实现 Dinic 最大流 / 最小割求解器
internal/incident/ 多源最早到达（Dijkstra）、事件状态机、关管预演与并发安全存储
Dockerfile     多阶段构建（api 运行镜像 + verify 验收镜像）
docker-compose.yml  api 与 verify 服务编排（API_PORT 控制宿主端口）
```
