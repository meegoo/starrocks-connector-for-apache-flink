# StarRocks Flink Connector — Multi-Table Transaction Stream Load 用户文档

## 1. 适用场景

当一个 Flink 作业在一个处理周期内写入同一 StarRocks 数据库的多张表时，开启 multi-table transaction 可以保证：

- **跨表原子提交**：同一 commit 周期内写入 `orders` 和 `order_items` 的数据要么全部可见，要么全部不可见。
- **源事务完整性**：来自上游（如 Kafka）的一个完整事务不会被拆分到两个 StarRocks 事务中。
- **秒级数据新鲜度**：数据持续通过 `/api/transaction/load` 流入 StarRocks，以 `sink.buffer-flush.interval-ms` 为周期 commit。

典型场景：

- 订单主表 `orders` 与明细表 `order_items` 同步写入
- 一条事件根据路由规则写入不同分表（如 `events_202601`、`events_202602`）
- 一个作业同时维护多张互相关联的下游结果表

> **前提条件**：StarRocks >= 4.0（支持 Transaction Stream Load 的多表事务），Connector >= 1.2.9。

## 2. 核心能力

| 能力 | 说明 |
|---|---|
| 跨表原子提交 | 同一 flush 周期内所有表共享一个 StarRocks 事务 label，统一 prepare + commit |
| 源事务不撕裂 | 通过 `transactionEnd` 标志位控制 commit 时机，只在完整源事务边界 commit |
| 秒级数据可见 | 数据定时 flush 到 StarRocks（`/api/transaction/load`），`transactionEnd` + 定时条件满足时 commit |
| N:1 事务映射 | 多个源事务可以累积在同一个 StarRocks 事务中，不要求一对一 |
| partition 内有序 | 通过 `keyBy(sourcePartition)` 保证同一 partition 的事务在同一 sink subtask 中顺序处理 |

## 3. 配置项

### 3.1 新增配置

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `sink.transaction.multi-table.enabled` | Boolean | `false` | 开启多表原子事务模式 |

### 3.2 关联配置

| 参数 | 推荐值 | 说明 |
|---|---|---|
| `sink.version` | `V2` | 必须为 V2，V1 不支持 transaction stream load |
| `sink.semantic` | `at-least-once` | 当前 multi-table 模式仅支持 at-least-once |
| `database-name` | `*` | 通配符，开启动态多表路由 |
| `table-name` | `*` | 通配符，开启动态多表路由 |
| `sink.buffer-flush.interval-ms` | `1000` | 控制 commit 周期（秒级新鲜度设 1000） |
| `sink.properties.format` | `json` | 数据格式 |
| `sink.properties.strip_outer_array` | `true` | JSON 数组解析 |

## 4. 接口定义

### 4.1 StarRocksRowData

```java
public interface StarRocksRowData {
    String getUniqueKey();    // 用于 region 路由（可为 null，自动按 database.table 路由）
    String getDatabase();     // 目标数据库
    String getTable();        // 目标表
    String getRow();          // JSON 格式的行数据

    /**
     * 标记这是源事务的最后一行。connector 仅在最近一次 write 带有此标记时
     * 才允许 commit，确保不会提交不完整的源事务。
     */
    default boolean isTransactionEnd() {
        return false;
    }
}
```

### 4.2 DefaultStarRocksRowData

```java
public class DefaultStarRocksRowData implements StarRocksRowData {
    // 基本字段
    private String uniqueKey;
    private String database;
    private String table;
    private String row;

    // 多表事务相关
    private boolean transactionEnd;  // 源事务结束标记
    private int sourcePartition;     // 源 partition 编号（用于 keyBy 保序）

    // 构造方法
    public DefaultStarRocksRowData(String uniqueKey, String database, String table, String row);

    // setter
    public void setTransactionEnd(boolean transactionEnd);
    public void setSourcePartition(int sourcePartition);

    // getter（继承自 StarRocksRowData + 新增）
    public boolean isTransactionEnd();
    public int getSourcePartition();
}
```

### 4.3 用户需要实现的组件

用户需要实现一个 `KeyedProcessFunction`（本文称为 **TransactionAssembler**），负责：

1. 按源 partition keyBy，缓存事务内的数据行
2. 仅在源事务闭合（如收到 `TXN_END`）时发射所有行
3. 在最后一行设置 `transactionEnd=true`
4. 在每行设置 `sourcePartition`

Connector 提供的标准 API（`SinkFunctionFactory.createSinkFunction()`）无需自定义 SinkFunction。

## 5. 完整示例

### 5.1 StarRocks 建表

```sql
CREATE DATABASE `test`;

CREATE TABLE `test`.`orders` (
    `order_id` BIGINT NOT NULL,
    `customer_id` BIGINT NOT NULL,
    `total_amount` DECIMAL(10,2) DEFAULT "0",
    `order_status` VARCHAR(32) DEFAULT ""
) ENGINE=OLAP PRIMARY KEY(`order_id`)
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES("replication_num" = "1");

CREATE TABLE `test`.`order_items` (
    `item_id` BIGINT NOT NULL,
    `order_id` BIGINT NOT NULL,
    `product_name` VARCHAR(128) DEFAULT "",
    `quantity` INT DEFAULT "0",
    `price` DECIMAL(10,2) DEFAULT "0"
) ENGINE=OLAP PRIMARY KEY(`item_id`)
DISTRIBUTED BY HASH(`item_id`)
PROPERTIES("replication_num" = "1");
```

### 5.2 Flink 作业代码

```java
import com.starrocks.connector.flink.table.data.DefaultStarRocksRowData;
import com.starrocks.connector.flink.table.sink.SinkFunctionFactory;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class WriteMultipleTablesWithTransaction {

    // =====================================================
    // 1. 源事件模型（根据实际业务定义）
    // =====================================================

    enum EventType { TXN_BEGIN, DATA, TXN_END }

    static class TxnEvent implements java.io.Serializable {
        int partition;
        String txnId;
        EventType type;
        String database;
        String table;
        String json;
        // ... 构造方法省略
    }

    // =====================================================
    // 2. TransactionAssembler — 核心用户组件
    // =====================================================

    /**
     * 按 partition 缓存数据行，仅在 TXN_END 时发射完整事务的所有行。
     *
     * 关键约定：
     *   - 最后一行必须设置 transactionEnd=true
     *   - 每行必须设置 sourcePartition
     *   - processElement 内同步发射所有行（Flink 保证不会被 checkpoint 打断）
     */
    static class TransactionAssembler
            extends KeyedProcessFunction<Integer, TxnEvent, DefaultStarRocksRowData> {

        private transient ListState<TxnEvent> pendingRows;

        @Override
        public void open(Configuration parameters) throws Exception {
            pendingRows = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("pending-txn-rows", Types.POJO(TxnEvent.class)));
        }

        @Override
        public void processElement(TxnEvent event, Context ctx,
                                   Collector<DefaultStarRocksRowData> out) throws Exception {
            switch (event.type) {
                case TXN_BEGIN:
                    pendingRows.clear();
                    break;

                case DATA:
                    pendingRows.add(event);
                    break;

                case TXN_END:
                    List<TxnEvent> rows = new ArrayList<>();
                    for (TxnEvent row : pendingRows.get()) {
                        rows.add(row);
                    }
                    int partition = ctx.getCurrentKey();

                    for (int i = 0; i < rows.size(); i++) {
                        TxnEvent row = rows.get(i);
                        DefaultStarRocksRowData rowData = new DefaultStarRocksRowData(
                                null, row.database, row.table, row.json);
                        rowData.setSourcePartition(partition);
                        // 最后一行标记为源事务结束
                        if (i == rows.size() - 1) {
                            rowData.setTransactionEnd(true);
                        }
                        out.collect(rowData);
                    }
                    pendingRows.clear();
                    break;
            }
        }
    }

    // =====================================================
    // 3. 主程序
    // =====================================================

    public static void main(String[] args) throws Exception {
        String jdbcUrl  = "jdbc:mysql://127.0.0.1:9030";
        String loadUrl  = "127.0.0.1:8030";
        String userName = "root";
        String password = "";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);  // checkpoint 仅用于故障恢复，不影响 commit 周期

        // --- Source ---
        // 替换为实际的 Kafka Source，反序列化为 TxnEvent
        DataStream<TxnEvent> events = env.addSource(/* KafkaSource */);

        // --- Step 1: 按 partition 组装完整事务 ---
        DataStream<DefaultStarRocksRowData> rows = events
                .keyBy(e -> e.partition)
                .process(new TransactionAssembler());

        // --- Step 2: 按 sourcePartition 保序路由到 sink ---
        // 保证同一 partition 的数据始终到同一个 sink subtask，维护事务顺序
        DataStream<DefaultStarRocksRowData> partitionedRows = rows
                .keyBy(DefaultStarRocksRowData::getSourcePartition);

        // --- Step 3: 配置 connector ---
        StarRocksSinkOptions options = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", jdbcUrl)
                .withProperty("load-url", loadUrl)
                .withProperty("database-name", "*")               // 通配，动态多表路由
                .withProperty("table-name", "*")                  // 通配，动态多表路由
                .withProperty("username", userName)
                .withProperty("password", password)
                .withProperty("sink.version", "V2")               // 必须 V2
                .withProperty("sink.semantic", "at-least-once")
                .withProperty("sink.transaction.multi-table.enabled", "true")  // 开启多表事务
                .withProperty("sink.buffer-flush.interval-ms", "1000")         // ~1s 数据新鲜度
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .build();

        // 可选：为特定表设置独立的 stream load 属性
        StreamLoadTableProperties orderItemsProps = StreamLoadTableProperties.builder()
                .database("test")
                .table("order_items")
                .addProperty("format", "json")
                .addProperty("strip_outer_array", "true")
                .addProperty("ignore_json_size", "true")
                .build();
        options.addTableProperties(orderItemsProps);

        // --- Step 4: 创建并挂载 sink ---
        SinkFunction<DefaultStarRocksRowData> sink = SinkFunctionFactory.createSinkFunction(options);
        partitionedRows.addSink(sink);

        env.execute("WriteMultipleTablesWithTransaction");
    }
}
```

### 5.3 数据流拓扑图

```
Kafka (60 partitions)
  │
  ▼
keyBy(partition) ──── 确保同 partition 事件到同一 subtask
  │
  ▼
TransactionAssembler (KeyedProcessFunction)
  │  缓存 DATA 事件
  │  TXN_END 时发射所有行（最后一行 transactionEnd=true）
  │  每行携带 sourcePartition
  │
  ▼
keyBy(sourcePartition) ──── 确保同 partition 行到同一 sink subtask（保序）
  │
  ▼
StarRocksDynamicSinkFunctionV2 (SinkFunctionFactory.createSinkFunction)
  │
  │  ┌─────────── StreamLoadManagerV2 (multi-table txn mode) ───────────┐
  │  │                                                                   │
  │  │  write(row)                                                       │
  │  │    → ensureSharedTxnBegun (首次写入: begin 共享事务)               │
  │  │    → region.write (写入 active chunk)                             │
  │  │    → setCommitAllowed(row.isTransactionEnd())                     │
  │  │        ↓ true + timer ready + !commitInFlight?                    │
  │  │        → triggerCommitFromTaskThread()                            │
  │  │            switchChunk all regions (~1ms, task 线程, 无并发 write) │
  │  │            commitInFlight = true                                  │
  │  │            signal manager thread                                  │
  │  │                                                                   │
  │  │  Manager 线程:                                                    │
  │  │    commitInFlight=true:  等 load 完成 → prepare → commit          │
  │  │    commitInFlight=false: 定时 buffer flush (/api/transaction/load)│
  │  └───────────────────────────────────────────────────────────────────┘
  │
  ▼
StarRocks (test.orders + test.order_items, 原子可见)
```

## 6. 工作原理

### 6.1 两阶段异步 Commit

**Phase 1（task 线程，~1ms，非阻塞）：**

当 `transactionEnd=true` 的行被写入且距上次 commit 已过 `sink.buffer-flush.interval-ms` 时：
1. 对所有 region 执行 switchChunk（将 active chunk 移入 inactive queue）
2. 设置 `commitInFlight=true`，阻止新的 commit 触发
3. 唤醒 manager 线程
4. 立即返回，task 线程继续处理新行

**Phase 2（manager 线程，异步）：**

1. 等待所有 inactive chunk 的 HTTP load 完成
2. 调用 `/api/transaction/prepare`
3. 调用 `/api/transaction/commit`（原子提交所有表）
4. 重置 label 和事务状态，`commitInFlight=false`

### 6.2 安全性保证

| 保证 | 实现机制 |
|---|---|
| switchChunk 不切割源事务 | switchChunk 在 task 线程执行，Flink 单线程模型保证无并发 processElement |
| commit 不包含不完整源事务 | 仅在 `transactionEnd=true`（源事务最后一行）时触发 commit |
| 多表原子提交 | 所有表共享同一 StarRocks 事务 label，统一 prepare + commit |
| partition 内顺序 | `keyBy(sourcePartition)` 保证同一 partition 的数据路由到同一 sink subtask |
| task 线程不阻塞 | switchChunk ~1ms，HTTP 等待由 manager 线程异步完成 |
| 定时 flush 持续工作 | manager 线程的 buffer management flush 在 `!commitInFlight` 时正常运行 |

### 6.3 N:1 事务映射

多个源事务可以累积在同一个 StarRocks 事务中：

```
源事务 K1 (3 rows) → write → buffer
源事务 K2 (2 rows) → write → buffer   ← 此时 transactionEnd=true + timer ready
                                        → switchChunk → commit
                                        → K1 + K2 一起原子提交到 StarRocks
```

## 7. 能力限制

1. **仅支持 `sink.version=V2`**：V1 不支持 transaction stream load。

2. **仅支持 `at-least-once`**：失败重试后可能重复写入。multi-table 保证同批次多表共同成功/失败，不提供全局 exactly-once。对于 PRIMARY KEY 表，重复写入是幂等的（upsert）。

3. **所有表必须在同一 database**：StarRocks 多表事务是 database 级别的，不支持跨 database。

4. **事务范围是 sink subtask 级别**：每个 sink subtask 独立维护自己的 StarRocks 事务。原子性范围是"同一 subtask + 同一 commit 周期内的所有表"。

5. **依赖 StarRocks 集群事务能力配置**：需关注 running txn 上限、prepared timeout（默认 600s）、label 保留时长等参数。确保 `sink.buffer-flush.interval-ms` 远小于 StarRocks 事务超时时间。

6. **commitInFlight 期间暂停 buffer flush**：async commit 进行中（通常 30-200ms）时，新数据暂存在 active chunk 不会被 flush。极端高吞吐场景下如果 `maxWriteBlockCacheBytes` 被填满，task 线程会短暂阻塞直到 commit 完成。

## 8. 监控与排障

推荐监控：
- **Flink**：checkpoint 成功率、checkpoint duration、sink flush/commit 耗时
- **StarRocks**：running/prepared txns 数量、txn timeout、label 冲突

常见问题：

| 错误 | 原因 | 解决方案 |
|---|---|---|
| `transaction not existed` | StarRocks 事务超时 | 检查 prepared timeout 是否过短，或 flush interval 是否过大 |
| `too many running txns` | 并发事务过多 | 降低 sink 并行度，或调大 StarRocks `max_running_txn_num_per_db` |
| `Transaction start failed` | beginTransaction HTTP 调用失败 | 检查 load-url 连通性和 StarRocks 版本（需 >= 4.0） |
| 数据不可见延迟过高 | commit 条件不满足 | 确认上游数据有正确的 `transactionEnd=true` 标记 |

## 9. 最佳实践

1. **TransactionAssembler 约定**：
   - 仅在源事务完整闭合后发射行
   - 最后一行必须 `setTransactionEnd(true)`
   - 每行必须 `setSourcePartition(partition)`
   - 在同一个 `processElement()` 内同步发射所有行

2. **Sink 前 keyBy**：`rows.keyBy(DefaultStarRocksRowData::getSourcePartition).addSink(sink)` — 不可省略，否则 partition 内事务顺序无法保证。

3. **Checkpoint 与 commit 解耦**：checkpoint 间隔可以设较大值（如 60s）用于故障恢复；数据可见性由 `sink.buffer-flush.interval-ms`（如 1000ms）控制。

4. **路由策略尽量稳定**：避免单事务写入过多不同的表，增加事务时长与失败概率。

5. **上线前故障注入测试**：kill taskmanager / 网络抖动，验证从 checkpoint 恢复后的数据正确性。
