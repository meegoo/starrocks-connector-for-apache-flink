# StarRocks Flink Connector — Multi-Table Transaction Stream Load

## 1. Use Cases

When a single Flink job writes to multiple tables within the same StarRocks database in one processing cycle, enabling multi-table transaction guarantees:

- **Cross-table atomic commit**: Data written to `orders` and `order_items` within the same commit cycle becomes visible atomically — all or nothing.
- **Source transaction integrity**: A complete upstream transaction (e.g., from Kafka) is never split across two StarRocks transactions.
- **Sub-second data freshness**: Data continuously flows into StarRocks via `/api/transaction/load`, and is committed at the interval configured by `sink.buffer-flush.interval-ms`.

Typical scenarios:

- Synchronous writes to a master table `orders` and a detail table `order_items`
- Event routing to different partition tables (e.g., `events_202601`, `events_202602`)
- A single job maintaining multiple interrelated downstream result tables

> **Prerequisites**: StarRocks >= 4.0 (multi-table Transaction Stream Load support), Connector >= 1.2.9.

## 2. Core Capabilities

| Capability | Description |
|---|---|
| Cross-table atomic commit | All tables within the same flush cycle share one StarRocks transaction label; unified prepare + commit |
| Source transaction integrity | Commit timing is controlled by the `transactionEnd` flag; commit only occurs at complete source transaction boundaries |
| Sub-second data visibility | Data is periodically flushed to StarRocks (`/api/transaction/load`); committed when `transactionEnd` + timer conditions are met |
| N:1 transaction mapping | Multiple source transactions can accumulate in a single StarRocks transaction; no 1:1 requirement |
| Within-partition ordering | `keyBy(sourcePartition)` ensures transactions from the same partition are processed in order within the same sink subtask |

## 3. Configuration

### 3.1 New Configuration

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sink.transaction.multi-table.enabled` | Boolean | `false` | Enable multi-table atomic transaction mode |

### 3.2 Related Configuration

| Parameter | Recommended Value | Description |
|---|---|---|
| `sink.version` | `V2` | Required. V1 does not support transaction stream load |
| `sink.semantic` | `at-least-once` | Multi-table mode currently supports at-least-once only |
| `database-name` | `*` | Wildcard to enable dynamic multi-table routing |
| `table-name` | `*` | Wildcard to enable dynamic multi-table routing |
| `sink.buffer-flush.interval-ms` | `1000` | Controls the commit cycle (set to 1000 for ~1s freshness) |
| `sink.properties.format` | `json` | Data format |
| `sink.properties.strip_outer_array` | `true` | JSON array parsing |

## 4. Interface Definitions

### 4.1 StarRocksRowData

```java
public interface StarRocksRowData {
    String getUniqueKey();    // Region routing key (nullable; auto-derived from database.table)
    String getDatabase();     // Target database
    String getTable();        // Target table
    String getRow();          // Row data in JSON format

    /**
     * Marks this as the last row of a source transaction batch.
     * The connector only allows a commit when the most recent write
     * has this flag set, ensuring no partial source transaction is committed.
     */
    default boolean isTransactionEnd() {
        return false;
    }
}
```

### 4.2 DefaultStarRocksRowData

```java
public class DefaultStarRocksRowData implements StarRocksRowData {
    // Basic fields
    private String uniqueKey;
    private String database;
    private String table;
    private String row;

    // Multi-table transaction fields
    private boolean transactionEnd;  // Source transaction end marker
    private int sourcePartition;     // Source partition ID (for keyBy ordering)

    // Constructor
    public DefaultStarRocksRowData(String uniqueKey, String database, String table, String row);

    // Setters
    public void setTransactionEnd(boolean transactionEnd);
    public void setSourcePartition(int sourcePartition);

    // Getters (inherited from StarRocksRowData + new)
    public boolean isTransactionEnd();
    public int getSourcePartition();
}
```

### 4.3 User-Implemented Component

Users need to implement a `KeyedProcessFunction` (referred to as **TransactionAssembler** in this document) that:

1. Keys by source partition and buffers data rows within a transaction
2. Emits all rows only when the source transaction is closed (e.g., upon receiving `TXN_END`)
3. Sets `transactionEnd=true` on the last row
4. Sets `sourcePartition` on every row

No custom SinkFunction is needed — the standard connector API (`SinkFunctionFactory.createSinkFunction()`) handles everything.

## 5. Complete Example

### 5.1 StarRocks Table DDL

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

### 5.2 Flink Job Code

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
    // 1. Source Event Model (define per your business logic)
    // =====================================================

    enum EventType { TXN_BEGIN, DATA, TXN_END }

    static class TxnEvent implements java.io.Serializable {
        int partition;
        String txnId;
        EventType type;
        String database;
        String table;
        String json;
        // ... constructors omitted
    }

    // =====================================================
    // 2. TransactionAssembler — Core User Component
    // =====================================================

    /**
     * Buffers data rows per partition. Emits all rows of a complete
     * transaction only upon receiving TXN_END.
     *
     * Contract:
     *   - The last row must have transactionEnd=true
     *   - Every row must have sourcePartition set
     *   - All rows are emitted synchronously within a single
     *     processElement() call (Flink guarantees no checkpoint
     *     barrier can interrupt this)
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
                        // Mark the last row as transaction end
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
    // 3. Main Program
    // =====================================================

    public static void main(String[] args) throws Exception {
        String jdbcUrl  = "jdbc:mysql://127.0.0.1:9030";
        String loadUrl  = "127.0.0.1:8030";
        String userName = "root";
        String password = "";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);  // Checkpoint is for recovery only; does not affect commit cycle

        // --- Source ---
        // Replace with an actual Kafka Source that deserializes into TxnEvent
        DataStream<TxnEvent> events = env.addSource(/* KafkaSource */);

        // --- Step 1: Assemble complete transactions per partition ---
        DataStream<DefaultStarRocksRowData> rows = events
                .keyBy(e -> e.partition)
                .process(new TransactionAssembler());

        // --- Step 2: Partition-affinity routing to sink ---
        // Ensures all data from the same source partition always reaches
        // the same sink subtask, preserving within-partition transaction order.
        DataStream<DefaultStarRocksRowData> partitionedRows = rows
                .keyBy(DefaultStarRocksRowData::getSourcePartition);

        // --- Step 3: Configure the connector ---
        StarRocksSinkOptions options = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", jdbcUrl)
                .withProperty("load-url", loadUrl)
                .withProperty("database-name", "*")               // Wildcard for dynamic multi-table routing
                .withProperty("table-name", "*")                  // Wildcard for dynamic multi-table routing
                .withProperty("username", userName)
                .withProperty("password", password)
                .withProperty("sink.version", "V2")               // Required: V2
                .withProperty("sink.semantic", "at-least-once")
                .withProperty("sink.transaction.multi-table.enabled", "true")  // Enable multi-table txn
                .withProperty("sink.buffer-flush.interval-ms", "1000")         // ~1s data freshness
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .build();

        // Optional: per-table stream load properties
        StreamLoadTableProperties orderItemsProps = StreamLoadTableProperties.builder()
                .database("test")
                .table("order_items")
                .addProperty("format", "json")
                .addProperty("strip_outer_array", "true")
                .addProperty("ignore_json_size", "true")
                .build();
        options.addTableProperties(orderItemsProps);

        // --- Step 4: Create and attach the sink ---
        SinkFunction<DefaultStarRocksRowData> sink = SinkFunctionFactory.createSinkFunction(options);
        partitionedRows.addSink(sink);

        env.execute("WriteMultipleTablesWithTransaction");
    }
}
```

### 5.3 Data Flow Topology

```
Kafka (60 partitions)
  |
  v
keyBy(partition) ———— Ensures same-partition events go to the same subtask
  |
  v
TransactionAssembler (KeyedProcessFunction)
  |  Buffers DATA events
  |  On TXN_END: emits all rows (last row has transactionEnd=true)
  |  Every row carries sourcePartition
  |
  v
keyBy(sourcePartition) ———— Ensures same-partition rows go to the same sink subtask (ordering)
  |
  v
StarRocksDynamicSinkFunctionV2 (via SinkFunctionFactory.createSinkFunction)
  |
  |  +——————————— StreamLoadManagerV2 (multi-table txn mode) ———————————+
  |  |                                                                   |
  |  |  write(row)                                                       |
  |  |    -> ensureSharedTxnBegun (first write: begin shared txn)        |
  |  |    -> region.write (write to active chunk)                        |
  |  |    -> setCommitAllowed(row.isTransactionEnd())                    |
  |  |        | true + timer ready + !commitInFlight?                    |
  |  |        -> triggerCommitFromTaskThread()                           |
  |  |            switchChunk all regions (~1ms, task thread, no race)   |
  |  |            commitInFlight = true                                  |
  |  |            signal manager thread                                  |
  |  |                                                                   |
  |  |  Manager thread:                                                  |
  |  |    commitInFlight=true:  wait for loads -> prepare -> commit      |
  |  |    commitInFlight=false: periodic buffer flush (txn/load API)     |
  |  +———————————————————————————————————————————————————————————————————+
  |
  v
StarRocks (test.orders + test.order_items, atomically visible)
```

## 6. How It Works

### 6.1 Two-Phase Async Commit

**Phase 1 (task thread, ~1ms, non-blocking):**

When a row with `transactionEnd=true` is written and `sink.buffer-flush.interval-ms` has elapsed since the last commit:
1. Execute switchChunk on all regions (move active chunks to inactive queue)
2. Set `commitInFlight=true` to prevent new commit triggers
3. Wake up the manager thread
4. Return immediately — the task thread continues processing new rows

**Phase 2 (manager thread, async):**

1. Wait for all HTTP loads of inactive chunks to complete
2. Call `/api/transaction/prepare`
3. Call `/api/transaction/commit` (atomically commits all tables)
4. Reset labels and transaction state; set `commitInFlight=false`

### 6.2 Safety Guarantees

| Guarantee | Mechanism |
|---|---|
| switchChunk does not split source transactions | switchChunk runs on the task thread; Flink's single-threaded model ensures no concurrent processElement |
| Commit never includes partial source transactions | Commit is only triggered when `transactionEnd=true` (last row of a source transaction) |
| Multi-table atomic commit | All tables share one StarRocks transaction label; unified prepare + commit |
| Within-partition ordering | `keyBy(sourcePartition)` ensures data from the same partition is routed to the same sink subtask |
| Task thread is non-blocking | switchChunk takes ~1ms; HTTP wait is handled asynchronously by the manager thread |
| Periodic flush continues working | Manager thread buffer-management flushes run normally when `!commitInFlight` |

### 6.3 N:1 Transaction Mapping

Multiple source transactions can accumulate in a single StarRocks transaction:

```
Source txn K1 (3 rows) -> write -> buffer
Source txn K2 (2 rows) -> write -> buffer   <-- transactionEnd=true + timer ready
                                              -> switchChunk -> commit
                                              -> K1 + K2 atomically committed to StarRocks
```

## 7. Limitations

1. **Requires `sink.version=V2`**: V1 does not support transaction stream load.

2. **At-least-once only**: Failed retries may produce duplicate writes. Multi-table mode guarantees all tables within the same batch succeed or fail together, but does not provide global exactly-once. For PRIMARY KEY tables, duplicate writes are idempotent (upsert).

3. **All tables must be in the same database**: StarRocks multi-table transactions are database-scoped; cross-database transactions are not supported.

4. **Transaction scope is per sink subtask**: Each sink subtask maintains its own StarRocks transaction independently. The atomicity scope is "same subtask + same commit cycle + all tables."

5. **Depends on StarRocks cluster transaction settings**: Monitor running txn limits, prepared timeout (default 600s), and label retention. Ensure `sink.buffer-flush.interval-ms` is significantly shorter than the StarRocks transaction timeout.

6. **Buffer flushes paused during commitInFlight**: While the async commit is in progress (typically 30-200ms), new data stays in active chunks and is not flushed. Under extreme throughput, if `maxWriteBlockCacheBytes` is reached, the task thread may briefly block until the commit completes.

## 8. Monitoring and Troubleshooting

Recommended metrics:
- **Flink**: Checkpoint success rate, checkpoint duration, sink flush/commit latency
- **StarRocks**: Running/prepared txn count, txn timeout occurrences, label conflicts

Common issues:

| Error | Cause | Solution |
|---|---|---|
| `transaction not existed` | StarRocks transaction timeout | Check if prepared timeout is too short or flush interval is too large |
| `too many running txns` | Too many concurrent transactions | Reduce sink parallelism or increase StarRocks `max_running_txn_num_per_db` |
| `Transaction start failed` | beginTransaction HTTP call failed | Verify load-url connectivity and StarRocks version (requires >= 4.0) |
| High data visibility latency | Commit conditions not met | Verify upstream data has correct `transactionEnd=true` markers |

## 9. Best Practices

1. **TransactionAssembler contract**:
   - Emit rows only after the source transaction is fully closed
   - The last row must have `setTransactionEnd(true)`
   - Every row must have `setSourcePartition(partition)`
   - All rows must be emitted synchronously within a single `processElement()` call

2. **keyBy before sink is mandatory**: `rows.keyBy(DefaultStarRocksRowData::getSourcePartition).addSink(sink)` — omitting this breaks within-partition transaction ordering.

3. **Checkpoint is decoupled from commit**: Checkpoint interval can be set to a large value (e.g., 60s) for fault recovery; data visibility is governed by `sink.buffer-flush.interval-ms` (e.g., 1000ms).

4. **Keep routing strategies stable**: Avoid single transactions writing to an excessive number of distinct tables, which increases transaction duration and failure probability.

5. **Fault injection testing before production**: Kill TaskManagers / introduce network jitter and verify data correctness after checkpoint recovery.
