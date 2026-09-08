# redis-rto-rpo

测量云Redis集群版Proxy 接入在主节点故障切换场景下的 **RTO**（业务中断时长）与 **RPO**（数据丢失量）的命令行工具，为容灾演练与 SLA 评估提供实测数据。

## 工作原理

工具启动后会持续制造负载并实时探测可用性：

- **写入器（Writer）**：以 `--write-interval` 间隔持续写入带自增序号的 key，格式为 `<prefix>:<test-id>:seq:N`，值为 `<时间戳>|<序号>`，并记录客户端视角「最后成功写入」的序号与时间戳。
- **探针器（Probe）**：以 `--probe-interval` 间隔写入探针 key。连续失败达到 `--fail-threshold` 次即判定**故障开始**；连续成功达到 `--recover-n` 次即判定**业务恢复**。阈值机制用于去抖，避免单次网络抖动造成误判。
- **扫描器（Scanner）**：故障确认时冻结「故障前最后成功序号」快照；测试结束后扫描存活且序号 ≤ 快照的 seq key，以「快照序号 − 存活最大序号」估算故障窗口内的数据丢失量（恢复后的写入不计入，避免丢失被掩盖为 0）。

## 指标口径

| 指标 | 计算方式 | 含义 |
| --- | --- | --- |
| **RTO（推荐）** | 首次失败时刻 → 业务恢复判定时刻 | 真实业务不可用时长 |
| **RTO（参考）** | 用户标记的故障注入时刻 T0 → 业务恢复判定时刻 | 含人工操作延迟 |
| **RPO** | 故障前最后成功序号快照（`seq_at_fault`）− 存活最大序号（`max_recovered_seq`，≤快照），及对应时间窗口（秒） | 故障窗口内的数据丢失量 |

## 构建

需要 Go 1.26+（见 [go.mod](go.mod)）。

```bash
# 编译二进制（产物为当前目录下的 redis-rto-rpo）
go build -o redis-rto-rpo ./cmd/redis-rto-rpo

# 或直接运行
go run ./cmd/redis-rto-rpo --help
```

## 快速开始

```bash
# 连接本地 Redis，使用默认参数
./redis-rto-rpo --addr 127.0.0.1:6379

# 连接云 Redis 集群（密码格式为 实例ID:密码），并将结果保存为 JSON
./redis-rto-rpo --addr 192.168.0.2:6379 --password 'crs-xxxx:yourpass' --output-json result.json
```

### 使用流程

1. 启动工具，等待终端出现 `>>> 持续负载已启动` 提示。
2. 通过云控制台或 API 触发主节点重启 / 故障切换。
3. 触发后**立即**回到终端按【回车】，标记故障注入时刻 T0。
4. 工具自动检测故障开始与恢复，恢复后继续观察 `--post-recover-observe` 时长，然后生成报告。

## 参数说明

### 连接参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--addr` | string | `127.0.0.1:6379` | Redis 连接地址，格式 `host:port`，云集群填 Proxy 的 VIP 地址 |
| `--password` | string | 空 | 连接密码，云 Redis 默认账号格式为 `实例ID:密码`；留空时读取环境变量 `REDIS_PASSWORD`（推荐，避免明文入 shell history） |
| `--pool-size` | int | `16` | 连接池大小，测量场景为串行探针，默认值已足够 |
| `--prefix` | string | `rto_rpo_test` | 测试 key 的统一前缀，用于隔离测试数据与清理 |
| `--test-id` | string | 自动毫秒时间戳 | 本次测试唯一标识，用于隔离命名空间；仅允许字母、数字、`_`、`-`、`.`。启动前检查该 test-id 下是否已有遗留 seq key，有则拒绝启动（防止旧数据污染 RPO），需更换 test-id 或加 `--clean` |

### 测量精度参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--write-interval` | duration | `20ms` | 写入间隔，越小则 RPO 采样精度越高 |
| `--probe-interval` | duration | `80ms` | 探测间隔，越小则故障时刻判定越精确 |
| `--op-timeout` | duration | `500ms` | 单次探测/写入命令的超时上限，是故障时刻判定的**实际精度边界**；超过 `--probe-interval` 的 10 倍时启动横幅给出精度告警（单次阻塞会主导探测间隔） |
| `--fail-threshold` | int | `3` | 连续失败多少次判定故障开始（去抖） |
| `--recover-n` | int | `6` | 连续成功多少次判定业务恢复（去抖） |
| `--probe-keys` | int | `6` | 探针 key 数量，集群下建议设为分片数以覆盖所有分片 |

### 运行控制参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--max-run` | duration | `30m` | 最长运行时间，超时后自动停止并生成报告 |
| `--post-recover-observe` | duration | `4s` | 判定恢复后继续观察的时长；观察期内探针再现连续失败会**撤销恢复判定**并继续等待稳定恢复，撤销次数记入 `post_recover_invalidations` |
| `--status-interval` | duration | `3s` | 运行期间实时状态打印间隔 |
| `--output-json` | string | 空 | 将结果额外保存为 JSON 文件的路径，留空则不保存 |

### 扫描与清理参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--scan-method` | string | `scan` | 扫描方式：`scan` 用游标渐进扫描（推荐）；`keys` 一次性返回，集群或大 key 量慎用 |
| `--skip-scan-check` | bool | `false` | 跳过启动时的 SCAN 完整性检查，集群场景不建议 |
| `--clean` | bool | `false` | 启动前清理历史遗留测试 key，开启会删除 `<prefix>:*` 全部数据 |

## 输出说明

### 终端报告

运行结束后打印结构化报告，包含测试标识、时间线、RTO、RPO 及错误分类统计。错误分类按数量降序排列，每个分类下列出去重后的错误样本（错误文本 + 次数 + Go 类型）。当出现 `other` 或 `proto_error` 桶时，可据样本判断是云厂商自定义错误码还是分类器遗漏。

### JSON 输出

指定 `--output-json <path>` 后，完整结果写入该文件。关键字段：

| 字段 | 含义 |
| --- | --- |
| `test_id` | 本次测试唯一标识 |
| `rto_from_first_fail_sec` | RTO（推荐），首次失败到恢复，单位秒。**字段缺失表示未测量**（缺少首次失败或恢复时间），`0` 表示实测瞬时恢复 |
| `rto_from_inject_sec` | RTO（参考），故障注入到恢复，单位秒。**字段缺失表示未测量**（缺少故障注入或恢复时间），`0` 表示实测瞬时恢复 |
| `lost_ops` | 估算丢失的写操作数量（= `seq_at_fault` − `max_recovered_seq`） |
| `seq_at_fault` | 故障确认时刻冻结的「故障前最后成功序号」快照，RPO 计算基准；0 表示故障未确认或故障前无成功写入 |
| `ts_at_fault` | 快照对应的 Unix 毫秒时间戳 |
| `rpo_valid` | RPO 结果是否可信。扫描失败（SCAN/KEYS/GET 报错或超时）或故障前无成功写入时为 `false`，此时 `lost_ops`/`rpo_time_sec` 未计算 |
| `rpo_invalid_reason` | `rpo_valid=false` 时的原因说明，有效时省略该字段 |
| `post_recover_invalidations` | 恢复判定在观察期内被撤销的次数（瞬时抖动恢复），0 表示一次判定即稳定 |
| `rpo_time_sec` | RPO 时间窗口，单位秒。**字段缺失表示无法精确计算**（无成功写入或扫描未取回时间戳），`0` 表示实测零数据丢失窗口 |
| `error_stats` | 按类型归类的错误计数，key 统一小写。协议错误：`loading`/`readonly`/`moved`/`ask`/`clusterdown`/`noreplicas`/`masterdown`/`tryagain`/`maxclients`/`auth`/`noperm`/`execabort`/`oom`/`nil_key`，未被 go-redis 类型化的 RESP 错误归 `proto_error`；连接层错误：`timeout`/`connection_refused`/`connection_reset`/`broken_pipe`/`eof`/`conn_closed`；客户端语义：`ctx_canceled`/`ctx_deadline`/`pool_timeout`；无法归类者落 `other` |
| `error_samples` | 每个分类桶的错误样本，按 Go 类型与文本去重、每桶最多 5 条。字段：`text` 错误文本（超 256 字符截断）、`count` 出现次数、`type` Go 错误类型 |

完整字段定义见 [result.go](internal/app/result.go)。

## 注意事项

- **RPO 准确性依赖 SCAN 全局性**：RPO 计算需 Proxy 的 `SCAN`/`KEYS` 能返回**全部分片**的 key，否则存活最大序号偏低、RPO 被高估。启动时的 SCAN 完整性检查即用于验证此点，集群场景请勿使用 `--skip-scan-check`。
- **禁用底层重试以保证时间戳准确**：客户端固定 `MaxRetries=-1`，使每次失败原样暴露给探针逻辑，去抖交给 `--fail-threshold`，避免重试稀释故障时刻。
- **测试数据带 TTL**：seq key TTL 随本次运行时长派生（`--max-run` + 30 分钟余量，保证长运行下故障前的 key 在扫描前不过期），探针 key 为 1 分钟，到期自动过期；也可用 `--clean` 主动清理。
- **故障判定精度上限由 `--op-timeout` 决定**：故障期间单次探测最长阻塞 `op-timeout`，连续 `fail-threshold` 次失败才判定故障开始，最坏情况阻塞 `fail-threshold × op-timeout`（默认 3×500ms=1.5s）后才打出故障时刻。启动横幅会打印该上限；追求精确时刻请调小 `--op-timeout`（超过 `--probe-interval` 10 倍时横幅告警）。探测时间戳取的是**发起时刻**而非阻塞返回时刻，不会被 socket 等待放大。
- **测量客户端不预热连接**：`MinIdleConns=0` 且启用 `ContextTimeoutEnabled`，每次探测都走确定的建连/失败路径，无后台重连流量干扰测量。
- **`--clean` 假设单进程独占实例**：清理范围为 `prefix:*`（跨 TestID），同一 Redis 实例上**不可**以相同 prefix 并行运行多个本工具进程，否则后启动者的 `--clean` 会误删在跑进程的测量数据。
- **生产环境慎用**：工具会持续写入并可能触发清理，请在测试实例或可接受写入的环境中使用。
