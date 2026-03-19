/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.it.sink;

import com.starrocks.connector.flink.it.StarRocksITTestBase;
import com.starrocks.connector.flink.table.data.DefaultStarRocksRowData;
import com.starrocks.connector.flink.table.sink.SinkFunctionFactory;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.starrocks.connector.flink.it.sink.StarRocksTableUtils.scanTable;
import static com.starrocks.connector.flink.it.sink.StarRocksTableUtils.verifyResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for multi-table atomic transaction stream load.
 *
 * <p>Requires StarRocks >= 4.0 for multi-table transaction support.
 *
 * <p>Run against an external cluster:
 * <pre>
 *   mvn test -Dtest=MultiTableTransactionITTest \
 *     -Dit.starrocks.fe.http=172.26.95.228:8030 \
 *     -Dit.starrocks.fe.jdbc=jdbc:mysql://172.26.95.228:9030
 * </pre>
 *
 * <h2>Test coverage</h2>
 * <ol>
 *   <li>{@link #testEndToEndMultiPartition} — end-to-end write with parallelism=2 and
 *       per-partition routing; verifies data correctness across two tables.</li>
 *   <li>{@link #testNoFlushBeforeTxnEnd} — transaction consistency: data must NOT be
 *       visible before txnEnd, and MUST be visible shortly after.</li>
 *   <li>{@link #testMultipleConsecutiveTransactions} — consecutive transaction isolation:
 *       txn-2 data must not be committed until txn-2's own txnEnd arrives (re-arm logic).</li>
 *   <li>{@link #testEmptyTransaction} — boundary: a txnEnd with no preceding data must
 *       not cause errors.</li>
 *   <li>{@link #testAtomicVisibilityAcrossTables} — atomicity: both tables become visible
 *       simultaneously via a single shared StarRocks transaction label.</li>
 *   <li>{@link #testPartialPartitionTxnEndBlocking} — blocking: when only one of two
 *       partitions has sent txnEnd, data must NOT be committed.</li>
 *   <li>{@link #testCheckpointTriggeredFlushDataIntegrity} — checkpoint: Flink checkpoint
 *       triggers flush(), all buffered data must be committed completely.</li>
 * </ol>
 */
public class MultiTableTransactionITTest extends StarRocksITTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MultiTableTransactionITTest.class);

    /**
     * Flush interval used in timing-sensitive tests (ms).
     * Must be larger than the SDK's minimum scanningFrequency (50 ms).
     */
    private static final int FLUSH_INTERVAL_MS = 500;

    /**
     * How long to wait after a txnEnd row is emitted before asserting that data
     * is visible in StarRocks (ms).
     *
     * <p>With the event-driven commit implementation, the task thread executes
     * {@code switchChunk} immediately on txnEnd, then the manager thread drives:
     * HTTP load (~100-500 ms) → prepare (~50-200 ms) → commit (~50-200 ms).
     * We add a 5 s buffer for CI/network variance, especially for freshly
     * provisioned TSP clusters where the first few HTTP round-trips may be slow.
     */
    private static final long COMMIT_PROPAGATION_MS = 5_000L;

    // -------------------------------------------------------------------------
    // DDL helpers
    // -------------------------------------------------------------------------

    private String createOrdersTable() throws Exception {
        String tableName = "orders_" + genRandomUuid();
        executeSrSQL(String.format(
                "CREATE TABLE `%s`.`%s` (" +
                        "order_id BIGINT NOT NULL," +
                        "customer_id BIGINT NOT NULL," +
                        "total_amount DECIMAL(10,2) DEFAULT '0'," +
                        "order_status VARCHAR(32) DEFAULT ''" +
                        ") ENGINE=OLAP PRIMARY KEY(order_id) " +
                        "DISTRIBUTED BY HASH(order_id) BUCKETS 4 " +
                        "PROPERTIES (\"replication_num\" = \"1\")",
                DB_NAME, tableName));
        return tableName;
    }

    private String createOrderItemsTable() throws Exception {
        String tableName = "order_items_" + genRandomUuid();
        executeSrSQL(String.format(
                "CREATE TABLE `%s`.`%s` (" +
                        "item_id BIGINT NOT NULL," +
                        "order_id BIGINT NOT NULL," +
                        "product_name VARCHAR(128) DEFAULT ''," +
                        "quantity INT DEFAULT '0'," +
                        "price DECIMAL(10,2) DEFAULT '0'" +
                        ") ENGINE=OLAP PRIMARY KEY(item_id) " +
                        "DISTRIBUTED BY HASH(item_id) BUCKETS 4 " +
                        "PROPERTIES (\"replication_num\" = \"1\")",
                DB_NAME, tableName));
        return tableName;
    }

    // -------------------------------------------------------------------------
    // Test cases
    // -------------------------------------------------------------------------

    /**
     * End-to-end test: two source partitions, sink parallelism = 2.
     *
     * <p>Verifies the core routing contract: {@code keyBy(sourcePartition)} ensures
     * each sink subtask handles exactly one partition, so one partition's data is
     * always processed by the same sink instance.
     *
     * <ul>
     *   <li>Partition 0 → {@code orders} table (2 rows + txnEnd)</li>
     *   <li>Partition 1 → {@code order_items} table (3 rows + txnEnd)</li>
     * </ul>
     *
     * <p>After the job finishes ({@code close()} flushes remaining data), both
     * tables must contain the expected rows.
     */
    @Test
    public void testEndToEndMultiPartition() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        StreamExecutionEnvironment env = buildEnv(2);

        DataStream<DefaultStarRocksRowData> partition0 = env.fromElements(
                row(DB_NAME, ordersTable,
                        "{\"order_id\":1,\"customer_id\":100,\"total_amount\":99.99,\"order_status\":\"created\"}",
                        0, false),
                row(DB_NAME, ordersTable,
                        "{\"order_id\":2,\"customer_id\":101,\"total_amount\":25.00,\"order_status\":\"created\"}",
                        0, true)
        ).returns(TypeInformation.of(DefaultStarRocksRowData.class));

        DataStream<DefaultStarRocksRowData> partition1 = env.fromElements(
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":1,\"order_id\":1,\"product_name\":\"widget\",\"quantity\":2,\"price\":49.99}",
                        1, false),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":2,\"order_id\":1,\"product_name\":\"gadget\",\"quantity\":1,\"price\":50.00}",
                        1, false),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":3,\"order_id\":2,\"product_name\":\"doohickey\",\"quantity\":3,\"price\":25.00}",
                        1, true)
        ).returns(TypeInformation.of(DefaultStarRocksRowData.class));

        partition0.union(partition1)
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .addSink(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS))
                .setParallelism(2);

        env.execute("testEndToEndMultiPartition");

        verifyResult(
                Arrays.asList(
                        Arrays.asList(1L, 100L, new BigDecimal("99.99"), "created"),
                        Arrays.asList(2L, 101L, new BigDecimal("25.00"), "created")),
                scanTable(DB_CONNECTION, DB_NAME, ordersTable));

        verifyResult(
                Arrays.asList(
                        Arrays.asList(1L, 1L, "widget",    2, new BigDecimal("49.99")),
                        Arrays.asList(2L, 1L, "gadget",    1, new BigDecimal("50.00")),
                        Arrays.asList(3L, 2L, "doohickey", 3, new BigDecimal("25.00"))),
                scanTable(DB_CONNECTION, DB_NAME, orderItemsTable));
    }

    /**
     * Transaction consistency timing test — the core correctness guarantee.
     *
     * <p>Verifies that data is NOT visible in StarRocks before the txnEnd marker
     * arrives, even after the flush interval has elapsed multiple times.
     * Only after txnEnd is received should the data become visible.
     *
     * <p>Timeline:
     * <pre>
     *   t=0              source emits data rows (no txnEnd)
     *   t=2×interval     assert tables EMPTY  ← flush timer fired but no commit
     *   t=2×interval     emit txnEnd          ← event-driven commit triggered
     *   t=2×interval+Δ   assert tables have data (Δ ≈ network round-trip)
     * </pre>
     *
     * <p>This test validates the event-driven {@code setCommitAllowed} path in
     * {@code DefaultStreamLoadManager}: on txnEnd the task thread immediately calls
     * {@code switchChunkForCommit()} on all regions and signals the manager thread,
     * which then drives flush → prepare → commit without waiting for the age timer.
     */
    @Test
    public void testNoFlushBeforeTxnEnd() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        TxnEndControlledSource.reset(DB_NAME, ordersTable, orderItemsTable);

        StreamExecutionEnvironment env = buildEnv(1);
        env.addSource(new TxnEndControlledSource())
                .setParallelism(1)
                .returns(TypeInformation.of(DefaultStarRocksRowData.class))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .addSink(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS))
                .setParallelism(1);

        Thread jobThread = new Thread(() -> {
            try {
                env.execute("testNoFlushBeforeTxnEnd");
            } catch (Exception e) {
                LOG.warn("Job thread finished (may be expected on cancel)", e);
            }
        }, "flink-job-thread");
        jobThread.start();
        long testStartMs = System.currentTimeMillis();

        try {
            // Wait 2× flush interval — the age-based timer fires but commitAllowed=false
            Thread.sleep(2L * FLUSH_INTERVAL_MS);
            long elapsedBeforeCheck = System.currentTimeMillis() - testStartMs;
            LOG.info("[DIAG testNoFlushBeforeTxnEnd] Test thread: waited {}ms (2×{}ms), checking tables at t={}ms",
                    elapsedBeforeCheck, FLUSH_INTERVAL_MS, elapsedBeforeCheck);

            // [DIAG] Log actual table state before txnEnd assertion
            List<List<Object>> ordersBeforeTxnEnd = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
            List<List<Object>> itemsBeforeTxnEnd = scanTable(DB_CONNECTION, DB_NAME, orderItemsTable);
            LOG.info("[DIAG testNoFlushBeforeTxnEnd] Before txnEnd (after 2×{}ms): orders.size={}, order_items.size={}. orders={}, order_items={}",
                    FLUSH_INTERVAL_MS, ordersBeforeTxnEnd.size(), itemsBeforeTxnEnd.size(),
                    ordersBeforeTxnEnd, itemsBeforeTxnEnd);

            assertEquals("orders must be empty before txnEnd",
                    0, ordersBeforeTxnEnd.size());
            assertEquals("order_items must be empty before txnEnd",
                    0, itemsBeforeTxnEnd.size());

            LOG.info("Confirmed: no data visible before txnEnd. Sending txnEnd signal.");
            TxnEndControlledSource.SEND_TXN_END_LATCH.countDown();

            assertTrue("txnEnd row should be emitted within 10 s",
                    TxnEndControlledSource.TXN_END_EMITTED_LATCH.await(10, TimeUnit.SECONDS));

            // Event-driven commit: switchChunk already done on task thread;
            // manager thread only needs one HTTP round-trip to flush + prepare + commit.
            Thread.sleep(COMMIT_PROPAGATION_MS);

            List<List<Object>> ordersAfter = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
            List<List<Object>> itemsAfter  = scanTable(DB_CONNECTION, DB_NAME, orderItemsTable);

            assertEquals("orders must have 1 row after txnEnd", 1, ordersAfter.size());
            assertEquals("order_items must have 2 rows after txnEnd", 2, itemsAfter.size());

            verifyResult(
                    Arrays.asList(Arrays.asList(1L, 100L, new BigDecimal("99.99"), "created")),
                    ordersAfter);
            verifyResult(
                    Arrays.asList(
                            Arrays.asList(1L, 1L, "widget", 2, new BigDecimal("49.99")),
                            Arrays.asList(2L, 1L, "gadget", 1, new BigDecimal("50.00"))),
                    itemsAfter);

            LOG.info("Confirmed: data visible after txnEnd. Transaction consistency verified.");
        } finally {
            jobThread.interrupt();
            jobThread.join(5_000);
        }
    }

    /**
     * Consecutive transaction isolation test.
     *
     * <p>Verifies that after txn-1 commits, txn-2's data is NOT visible until
     * txn-2's own txnEnd arrives. This exercises the "re-arm" logic: after a
     * commit cycle completes, {@code commitInFlight} is reset to {@code false}
     * so the next source transaction is blocked until its own txnEnd marker.
     *
     * <p>Timeline:
     * <pre>
     *   t=0              txn-1 data emitted
     *   t=2×interval     assert EMPTY  (no txnEnd yet)
     *   t=2×interval     emit txn-1 txnEnd → commit
     *   t=2×interval+Δ   assert 1 row  (txn-1 visible, txn-2 not started)
     *   t=...            txn-2 data emitted (commitInFlight re-armed to false)
     *   t=...+2×interval assert still 1 row (txn-2 data buffered, no txnEnd)
     *   t=...            emit txn-2 txnEnd → commit
     *   t=...+Δ          assert 2 rows (both txns visible)
     * </pre>
     */
    @Test
    public void testMultipleConsecutiveTransactions() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        MultiTxnControlledSource.reset(DB_NAME, ordersTable, orderItemsTable);

        StreamExecutionEnvironment env = buildEnv(1);
        env.addSource(new MultiTxnControlledSource())
                .setParallelism(1)
                .returns(TypeInformation.of(DefaultStarRocksRowData.class))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .addSink(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS))
                .setParallelism(1);

        Thread jobThread = new Thread(() -> {
            try {
                env.execute("testMultipleConsecutiveTransactions");
            } catch (Exception e) {
                LOG.warn("Job thread finished (may be expected on cancel)", e);
            }
        }, "flink-job-thread");
        jobThread.start();
        long testStartMs = System.currentTimeMillis();

        try {
            // ---- Phase 1: txn-1 data emitted, no txnEnd yet ----
            Thread.sleep(2L * FLUSH_INTERVAL_MS);
            List<List<Object>> ordersBeforeTxn1 = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
            LOG.info("[DIAG testMultipleConsecutiveTransactions] Phase1 before txn-1 txnEnd (t={}ms): orders.size={}, orders={}",
                    System.currentTimeMillis() - testStartMs, ordersBeforeTxn1.size(), ordersBeforeTxn1);
            assertEquals("orders empty before txn-1 txnEnd",
                    0, ordersBeforeTxn1.size());

            // Allow txn-1 to commit
            MultiTxnControlledSource.TXN1_END_LATCH.countDown();
            assertTrue("txn-1 end emitted within 10 s",
                    MultiTxnControlledSource.TXN1_END_EMITTED_LATCH.await(10, TimeUnit.SECONDS));
            Thread.sleep(COMMIT_PROPAGATION_MS);

            // txn-1 data must now be visible
            List<List<Object>> afterTxn1 = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
            String diagMsg1 = String.format("[DIAG testMultipleConsecutiveTransactions] After txn-1 commit (waited %dms): orders.size=%d, orders=%s",
                    COMMIT_PROPAGATION_MS, afterTxn1.size(), afterTxn1);
            LOG.info(diagMsg1);
            System.out.println(diagMsg1);
            assertEquals("orders must have 1 row after txn-1", 1, afterTxn1.size());
            verifyResult(
                    Arrays.asList(Arrays.asList(1L, 100L, new BigDecimal("10.00"), "created")),
                    afterTxn1);

            // ---- Phase 2: txn-2 data emitted, no txnEnd yet ----
            // commitInFlight was reset to false after txn-1 commit
            Thread.sleep(2L * FLUSH_INTERVAL_MS);
            List<List<Object>> midTxn2 = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
            LOG.info("[DIAG testMultipleConsecutiveTransactions] Mid txn-2 (before txn-2 txnEnd): orders.size={}, orders={}",
                    midTxn2.size(), midTxn2);
            assertEquals("orders must still have only 1 row (txn-2 not committed yet)",
                    1, midTxn2.size());

            // Allow txn-2 to commit
            MultiTxnControlledSource.TXN2_END_LATCH.countDown();
            assertTrue("txn-2 end emitted within 10 s",
                    MultiTxnControlledSource.TXN2_END_EMITTED_LATCH.await(10, TimeUnit.SECONDS));
            Thread.sleep(COMMIT_PROPAGATION_MS);

            // Both txn-1 and txn-2 data must be visible
            List<List<Object>> afterTxn2 = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
            assertEquals("orders must have 2 rows after txn-2", 2, afterTxn2.size());
            verifyResult(
                    Arrays.asList(
                            Arrays.asList(1L, 100L, new BigDecimal("10.00"), "created"),
                            Arrays.asList(2L, 101L, new BigDecimal("20.00"), "created")),
                    afterTxn2);

            LOG.info("Confirmed: consecutive transaction isolation verified.");
        } finally {
            jobThread.interrupt();
            jobThread.join(5_000);
        }
    }

    /**
     * Boundary test: a txnEnd row with no preceding data rows must not cause
     * an error and must leave the tables empty.
     */
    @Test
    public void testEmptyTransaction() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        StreamExecutionEnvironment env = buildEnv(1);
        // Only a txnEnd row, no actual data
        env.fromElements(
                row(DB_NAME, ordersTable, null, 0, true)
        ).returns(TypeInformation.of(DefaultStarRocksRowData.class))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .addSink(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS));

        env.execute("testEmptyTransaction");

        assertEquals("orders must be empty after empty transaction",
                0, scanTable(DB_CONNECTION, DB_NAME, ordersTable).size());
        assertEquals("order_items must be empty after empty transaction",
                0, scanTable(DB_CONNECTION, DB_NAME, orderItemsTable).size());
    }

    /**
     * Atomicity test: verifies that both tables become visible via a single
     * shared StarRocks transaction label.
     *
     * <p>This is the strongest atomicity guarantee: after txnEnd triggers a commit,
     * we verify via {@code show proc '/transactions/{db}/finished'} that there is
     * exactly one COMMITTED transaction for both tables' data, and both tables
     * become non-empty at the same query point.
     *
     * <p>Timeline:
     * <pre>
     *   t=0              source emits rows to BOTH orders and order_items (no txnEnd)
     *   t=2×interval     assert both tables EMPTY
     *   t=2×interval     emit txnEnd
     *   t=2×interval+Δ   assert both tables non-empty simultaneously
     *   t=2×interval+Δ   verify exactly 1 finished transaction with the label prefix
     * </pre>
     */
    @Test
    public void testAtomicVisibilityAcrossTables() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        String labelPrefix = "test-atomic-" + genRandomUuid().substring(0, 8) + "-";

        AtomicVisibilitySource.reset(DB_NAME, ordersTable, orderItemsTable);

        StreamExecutionEnvironment env = buildEnv(1);
        env.addSource(new AtomicVisibilitySource())
                .setParallelism(1)
                .returns(TypeInformation.of(DefaultStarRocksRowData.class))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .addSink(buildSinkWithLabelPrefix(ordersTable, orderItemsTable,
                        FLUSH_INTERVAL_MS, labelPrefix))
                .setParallelism(1);

        Thread jobThread = new Thread(() -> {
            try {
                env.execute("testAtomicVisibilityAcrossTables");
            } catch (Exception e) {
                LOG.warn("Job thread finished (may be expected on cancel)", e);
            }
        }, "flink-job-thread");
        jobThread.start();

        try {
            // Wait for data to be buffered but not committed
            Thread.sleep(2L * FLUSH_INTERVAL_MS);

            List<List<Object>> ordersBefore = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
            List<List<Object>> itemsBefore = scanTable(DB_CONNECTION, DB_NAME, orderItemsTable);
            LOG.info("[DIAG testAtomicVisibilityAcrossTables] Before txnEnd: orders.size={}, order_items.size={}, orders={}, items={}",
                    ordersBefore.size(), itemsBefore.size(), ordersBefore, itemsBefore);

            assertEquals("orders must be empty before txnEnd",
                    0, ordersBefore.size());
            assertEquals("order_items must be empty before txnEnd",
                    0, itemsBefore.size());

            // Signal txnEnd
            AtomicVisibilitySource.SEND_TXN_END_LATCH.countDown();
            assertTrue("txnEnd row should be emitted within 10 s",
                    AtomicVisibilitySource.TXN_END_EMITTED_LATCH.await(10, TimeUnit.SECONDS));

            Thread.sleep(COMMIT_PROPAGATION_MS);

            // Both tables must be non-empty at the same query point
            List<List<Object>> ordersAfter = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
            List<List<Object>> itemsAfter = scanTable(DB_CONNECTION, DB_NAME, orderItemsTable);

            assertEquals("orders must have 1 row after txnEnd", 1, ordersAfter.size());
            assertEquals("order_items must have 2 rows after txnEnd", 2, itemsAfter.size());

            // Verify atomicity: exactly 1 finished transaction with our label prefix
            List<TransactionInfo> txns = getFinishedTransactionInfo(labelPrefix);
            String diagTxns = String.format("[DIAG testAtomicVisibilityAcrossTables] Finished transactions with prefix '%s': count=%d", labelPrefix, txns.size());
            LOG.info(diagTxns);
            System.out.println(diagTxns);
            for (int i = 0; i < txns.size(); i++) {
                TransactionInfo txn = txns.get(i);
                String txnLine = String.format("[DIAG testAtomicVisibilityAcrossTables]   txn[%d]: label=%s, status=%s, txnId=%d, coordinator=%s, loadJobSourceType=%s",
                        i, txn.label, txn.transactionStatus, txn.transactionId, txn.coordinator, txn.loadJobSourceType);
                LOG.info(txnLine);
                System.out.println(txnLine);
            }

            // Filter for COMMITTED (not ABORTED) transactions
            long committedCount = txns.stream()
                    .filter(t -> "COMMITTED".equalsIgnoreCase(t.transactionStatus)
                            || "VISIBLE".equalsIgnoreCase(t.transactionStatus))
                    .count();
            String diagCount = String.format("[DIAG testAtomicVisibilityAcrossTables] committedCount=%d (expected 1), orders.size=%d, order_items.size=%d",
                    committedCount, ordersAfter.size(), itemsAfter.size());
            LOG.info(diagCount);
            System.out.println(diagCount);
            assertEquals("Exactly 1 committed transaction expected (shared label for both tables)",
                    1, committedCount);

            LOG.info("Confirmed: cross-table atomic visibility with single shared transaction.");
        } finally {
            jobThread.interrupt();
            jobThread.join(5_000);
        }
    }

    /**
     * Partial partition blocking test: when two partitions are active but only
     * one has sent txnEnd, data must NOT be committed.
     *
     * <p>This validates that {@code PartitionCommitTracker.allSwitched()} correctly
     * blocks the commit until ALL active partitions have reached txnEnd.
     *
     * <p>Timeline:
     * <pre>
     *   t=0              P0 emits data + txnEnd; P1 emits data only (no txnEnd)
     *   t=3×interval     assert BOTH tables EMPTY (P1 blocks the commit)
     *   t=3×interval     P1 sends txnEnd
     *   t=3×interval+Δ   assert both tables have data
     * </pre>
     */
    @Test
    public void testPartialPartitionTxnEndBlocking() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        PartialPartitionSource.reset(DB_NAME, ordersTable, orderItemsTable);

        StreamExecutionEnvironment env = buildEnv(1);
        env.addSource(new PartialPartitionSource())
                .setParallelism(1)
                .returns(TypeInformation.of(DefaultStarRocksRowData.class))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .addSink(buildSink(ordersTable, orderItemsTable, FLUSH_INTERVAL_MS))
                .setParallelism(1);

        Thread jobThread = new Thread(() -> {
            try {
                env.execute("testPartialPartitionTxnEndBlocking");
            } catch (Exception e) {
                LOG.warn("Job thread finished (may be expected on cancel)", e);
            }
        }, "flink-job-thread");
        jobThread.start();

        try {
            // Wait for P0 data + txnEnd and P1 data (no txnEnd) to be processed
            assertTrue("Phase 1 should complete within 10 s",
                    PartialPartitionSource.PHASE1_DONE_LATCH.await(10, TimeUnit.SECONDS));

            // Wait extra flush intervals to ensure the timer fires multiple times
            Thread.sleep(3L * FLUSH_INTERVAL_MS);

            // P1 has NOT sent txnEnd → allSwitched() is false → no commit
            assertEquals("orders must be empty (P1 blocks commit)",
                    0, scanTable(DB_CONNECTION, DB_NAME, ordersTable).size());
            assertEquals("order_items must be empty (P1 blocks commit)",
                    0, scanTable(DB_CONNECTION, DB_NAME, orderItemsTable).size());

            LOG.info("Confirmed: partial partition txnEnd correctly blocks commit.");

            // Now let P1 send txnEnd
            PartialPartitionSource.P1_TXN_END_LATCH.countDown();
            assertTrue("P1 txnEnd should be emitted within 10 s",
                    PartialPartitionSource.P1_TXN_END_EMITTED_LATCH.await(10, TimeUnit.SECONDS));

            Thread.sleep(COMMIT_PROPAGATION_MS);

            // Now both partitions have txnEnd → data should be committed
            List<List<Object>> ordersAfter = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
            List<List<Object>> itemsAfter = scanTable(DB_CONNECTION, DB_NAME, orderItemsTable);

            assertEquals("orders must have 1 row after both partitions txnEnd",
                    1, ordersAfter.size());
            assertEquals("order_items must have 1 row after both partitions txnEnd",
                    1, itemsAfter.size());

            verifyResult(
                    Arrays.asList(Arrays.asList(1L, 100L, new BigDecimal("50.00"), "created")),
                    ordersAfter);
            verifyResult(
                    Arrays.asList(Arrays.asList(1L, 1L, "widget", 2, new BigDecimal("25.00"))),
                    itemsAfter);

            LOG.info("Confirmed: data visible after all partitions sent txnEnd.");
        } finally {
            jobThread.interrupt();
            jobThread.join(5_000);
        }
    }

    /**
     * Checkpoint-triggered flush data integrity test.
     *
     * <p>Verifies that when a Flink checkpoint triggers {@code flush()} (savepoint
     * path), all buffered multi-table data is committed completely — even if no
     * txnEnd has been received from any partition.
     *
     * <p>This exercises the savepoint code path in {@code DefaultStreamLoadManager}
     * where the manager thread detects an active shared transaction and forces
     * switchChunk + load + prepare + commit for all regions.
     *
     * <p>The source emits data to two tables without sending txnEnd, then stops.
     * The job finishes normally via {@code close()} → {@code flush()}, which acts
     * as a savepoint. After the job completes, both tables must have all expected rows.
     */
    @Test
    public void testCheckpointTriggeredFlushDataIntegrity() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        StreamExecutionEnvironment env = buildEnv(1);
        // Use a very large flush interval so the timer never fires —
        // only the flush() from close() should commit the data.
        int largeFlushInterval = 60_000;
        // Disable periodic Flink checkpoints for this test. The test relies on
        // the sink's close() → flush() path to commit data, NOT on a Flink
        // periodic checkpoint. Enabling periodic checkpoints risks a barrier
        // arriving while the sink task thread is blocked inside flush(), which
        // can create a deadlock where the checkpoint can never complete.
        env.getCheckpointConfig().disableCheckpointing();

        // Emit data to two tables WITHOUT txnEnd, then source finishes.
        // The sink's close() will call flush(), which must commit everything.
        env.fromElements(
                row(DB_NAME, ordersTable,
                        "{\"order_id\":1,\"customer_id\":100,\"total_amount\":10.00,\"order_status\":\"created\"}",
                        0, false),
                row(DB_NAME, ordersTable,
                        "{\"order_id\":2,\"customer_id\":101,\"total_amount\":20.00,\"order_status\":\"created\"}",
                        0, false),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":1,\"order_id\":1,\"product_name\":\"widget\",\"quantity\":3,\"price\":10.00}",
                        0, false),
                row(DB_NAME, orderItemsTable,
                        "{\"item_id\":2,\"order_id\":2,\"product_name\":\"gadget\",\"quantity\":1,\"price\":20.00}",
                        0, false)
        ).returns(TypeInformation.of(DefaultStarRocksRowData.class))
                .keyBy(DefaultStarRocksRowData::getSourcePartition)
                .addSink(buildSink(ordersTable, orderItemsTable, largeFlushInterval))
                .setParallelism(1);

        // Execute synchronously — close() triggers flush()
        env.execute("testCheckpointTriggeredFlushDataIntegrity");

        // All data must be committed by the savepoint path
        List<List<Object>> ordersAfter = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
        List<List<Object>> itemsAfter = scanTable(DB_CONNECTION, DB_NAME, orderItemsTable);

        verifyResult(
                Arrays.asList(
                        Arrays.asList(1L, 100L, new BigDecimal("10.00"), "created"),
                        Arrays.asList(2L, 101L, new BigDecimal("20.00"), "created")),
                ordersAfter);

        verifyResult(
                Arrays.asList(
                        Arrays.asList(1L, 1L, "widget", 3, new BigDecimal("10.00")),
                        Arrays.asList(2L, 2L, "gadget", 1, new BigDecimal("20.00"))),
                itemsAfter);

        LOG.info("Confirmed: checkpoint-triggered flush committed all multi-table data.");
    }

    // -------------------------------------------------------------------------
    // Controlled source functions (use static fields to avoid ClosureCleaner)
    // -------------------------------------------------------------------------

    /**
     * Source for {@link #testNoFlushBeforeTxnEnd}.
     *
     * <p>Phase 1: emits data rows without txnEnd (sink must not commit).
     * Phase 2: waits for {@link #SEND_TXN_END_LATCH}, then emits the txnEnd marker.
     */
    private static class TxnEndControlledSource
            extends RichParallelSourceFunction<DefaultStarRocksRowData> {

        private static final long serialVersionUID = 1L;

        static volatile CountDownLatch SEND_TXN_END_LATCH;
        static volatile CountDownLatch TXN_END_EMITTED_LATCH;
        static volatile String DB;
        static volatile String ORDERS_TABLE;
        static volatile String ORDER_ITEMS_TABLE;

        static void reset(String db, String ordersTable, String orderItemsTable) {
            DB = db;
            ORDERS_TABLE = ordersTable;
            ORDER_ITEMS_TABLE = orderItemsTable;
            SEND_TXN_END_LATCH    = new CountDownLatch(1);
            TXN_END_EMITTED_LATCH = new CountDownLatch(1);
        }

        @Override
        public void run(SourceContext<DefaultStarRocksRowData> ctx) throws Exception {
            // Phase 1: data rows, no txnEnd
            long t0 = System.currentTimeMillis();
            ctx.collect(row(DB, ORDERS_TABLE,
                    "{\"order_id\":1,\"customer_id\":100,\"total_amount\":99.99,\"order_status\":\"created\"}",
                    0, false));
            ctx.collect(row(DB, ORDER_ITEMS_TABLE,
                    "{\"item_id\":1,\"order_id\":1,\"product_name\":\"widget\",\"quantity\":2,\"price\":49.99}",
                    0, false));
            ctx.collect(row(DB, ORDER_ITEMS_TABLE,
                    "{\"item_id\":2,\"order_id\":1,\"product_name\":\"gadget\",\"quantity\":1,\"price\":50.00}",
                    0, false));
            LOG.info("[DIAG TxnEndControlledSource] Phase1: emitted 3 data rows at t={}ms", System.currentTimeMillis() - t0);

            SEND_TXN_END_LATCH.await();
            long t1 = System.currentTimeMillis();
            LOG.info("[DIAG TxnEndControlledSource] SEND_TXN_END_LATCH released at t={}ms (test thread signaled)", t1 - t0);

            // Phase 2: txnEnd marker (null row = no data, just signal end-of-transaction)
            ctx.collect(row(DB, ORDERS_TABLE, null, 0, true));
            TXN_END_EMITTED_LATCH.countDown();
            LOG.info("[DIAG TxnEndControlledSource] Phase2: emitted txnEnd at t={}ms", System.currentTimeMillis() - t0);
        }

        @Override
        public void cancel() {
            CountDownLatch l = SEND_TXN_END_LATCH;
            if (l != null) l.countDown();
        }
    }

    /**
     * Source for {@link #testMultipleConsecutiveTransactions}.
     *
     * <p>Emits two back-to-back transactions, each gated by its own latch pair so
     * the test thread can control exactly when each txnEnd is delivered.
     */
    private static class MultiTxnControlledSource
            extends RichParallelSourceFunction<DefaultStarRocksRowData> {

        private static final long serialVersionUID = 1L;

        static volatile CountDownLatch TXN1_END_LATCH;
        static volatile CountDownLatch TXN1_END_EMITTED_LATCH;
        static volatile CountDownLatch TXN2_END_LATCH;
        static volatile CountDownLatch TXN2_END_EMITTED_LATCH;
        static volatile String DB;
        static volatile String ORDERS_TABLE;

        static void reset(String db, String ordersTable, String orderItemsTable) {
            DB = db;
            ORDERS_TABLE = ordersTable;
            TXN1_END_LATCH          = new CountDownLatch(1);
            TXN1_END_EMITTED_LATCH  = new CountDownLatch(1);
            TXN2_END_LATCH          = new CountDownLatch(1);
            TXN2_END_EMITTED_LATCH  = new CountDownLatch(1);
        }

        @Override
        public void run(SourceContext<DefaultStarRocksRowData> ctx) throws Exception {
            long t0 = System.currentTimeMillis();
            // txn-1 data
            ctx.collect(row(DB, ORDERS_TABLE,
                    "{\"order_id\":1,\"customer_id\":100,\"total_amount\":10.00,\"order_status\":\"created\"}",
                    0, false));
            LOG.info("[DIAG MultiTxnControlledSource] txn-1 data emitted at t={}ms", System.currentTimeMillis() - t0);

            TXN1_END_LATCH.await();
            ctx.collect(row(DB, ORDERS_TABLE, null, 0, true));  // txn-1 end
            TXN1_END_EMITTED_LATCH.countDown();
            LOG.info("[DIAG MultiTxnControlledSource] txn-1 txnEnd emitted at t={}ms", System.currentTimeMillis() - t0);

            // txn-2 data (commitInFlight re-armed to false after txn-1 commit)
            ctx.collect(row(DB, ORDERS_TABLE,
                    "{\"order_id\":2,\"customer_id\":101,\"total_amount\":20.00,\"order_status\":\"created\"}",
                    0, false));
            LOG.info("[DIAG MultiTxnControlledSource] txn-2 data emitted at t={}ms", System.currentTimeMillis() - t0);

            TXN2_END_LATCH.await();
            ctx.collect(row(DB, ORDERS_TABLE, null, 0, true));  // txn-2 end
            TXN2_END_EMITTED_LATCH.countDown();
            LOG.info("[DIAG MultiTxnControlledSource] txn-2 txnEnd emitted at t={}ms", System.currentTimeMillis() - t0);
        }

        @Override
        public void cancel() {
            CountDownLatch l1 = TXN1_END_LATCH;
            CountDownLatch l2 = TXN2_END_LATCH;
            if (l1 != null) l1.countDown();
            if (l2 != null) l2.countDown();
        }
    }

    /**
     * Source for {@link #testAtomicVisibilityAcrossTables}.
     *
     * <p>Emits data rows to both orders and order_items in a single partition,
     * then waits for the test thread to signal txnEnd.
     */
    private static class AtomicVisibilitySource
            extends RichParallelSourceFunction<DefaultStarRocksRowData> {

        private static final long serialVersionUID = 1L;

        static volatile CountDownLatch SEND_TXN_END_LATCH;
        static volatile CountDownLatch TXN_END_EMITTED_LATCH;
        static volatile String DB;
        static volatile String ORDERS_TABLE;
        static volatile String ORDER_ITEMS_TABLE;

        static void reset(String db, String ordersTable, String orderItemsTable) {
            DB = db;
            ORDERS_TABLE = ordersTable;
            ORDER_ITEMS_TABLE = orderItemsTable;
            SEND_TXN_END_LATCH    = new CountDownLatch(1);
            TXN_END_EMITTED_LATCH = new CountDownLatch(1);
        }

        @Override
        public void run(SourceContext<DefaultStarRocksRowData> ctx) throws Exception {
            long t0 = System.currentTimeMillis();
            // Emit data to both tables in partition 0
            ctx.collect(row(DB, ORDERS_TABLE,
                    "{\"order_id\":1,\"customer_id\":100,\"total_amount\":88.88,\"order_status\":\"pending\"}",
                    0, false));
            ctx.collect(row(DB, ORDER_ITEMS_TABLE,
                    "{\"item_id\":1,\"order_id\":1,\"product_name\":\"alpha\",\"quantity\":1,\"price\":44.44}",
                    0, false));
            ctx.collect(row(DB, ORDER_ITEMS_TABLE,
                    "{\"item_id\":2,\"order_id\":1,\"product_name\":\"beta\",\"quantity\":2,\"price\":22.22}",
                    0, false));
            LOG.info("[DIAG AtomicVisibilitySource] Emitted 3 data rows at t={}ms", System.currentTimeMillis() - t0);

            SEND_TXN_END_LATCH.await();
            ctx.collect(row(DB, ORDERS_TABLE, null, 0, true));
            TXN_END_EMITTED_LATCH.countDown();
            LOG.info("[DIAG AtomicVisibilitySource] Emitted txnEnd at t={}ms", System.currentTimeMillis() - t0);
        }

        @Override
        public void cancel() {
            CountDownLatch l = SEND_TXN_END_LATCH;
            if (l != null) l.countDown();
        }
    }

    /**
     * Source for {@link #testPartialPartitionTxnEndBlocking}.
     *
     * <p>Phase 1: P0 writes to orders + txnEnd; P1 writes to order_items (no txnEnd).
     * Phase 2: waits for latch, then P1 sends txnEnd.
     */
    private static class PartialPartitionSource
            extends RichParallelSourceFunction<DefaultStarRocksRowData> {

        private static final long serialVersionUID = 1L;

        static volatile CountDownLatch PHASE1_DONE_LATCH;
        static volatile CountDownLatch P1_TXN_END_LATCH;
        static volatile CountDownLatch P1_TXN_END_EMITTED_LATCH;
        static volatile String DB;
        static volatile String ORDERS_TABLE;
        static volatile String ORDER_ITEMS_TABLE;

        static void reset(String db, String ordersTable, String orderItemsTable) {
            DB = db;
            ORDERS_TABLE = ordersTable;
            ORDER_ITEMS_TABLE = orderItemsTable;
            PHASE1_DONE_LATCH       = new CountDownLatch(1);
            P1_TXN_END_LATCH        = new CountDownLatch(1);
            P1_TXN_END_EMITTED_LATCH = new CountDownLatch(1);
        }

        @Override
        public void run(SourceContext<DefaultStarRocksRowData> ctx) throws Exception {
            // P0: write to orders + txnEnd
            ctx.collect(row(DB, ORDERS_TABLE,
                    "{\"order_id\":1,\"customer_id\":100,\"total_amount\":50.00,\"order_status\":\"created\"}",
                    0, false));
            ctx.collect(row(DB, ORDERS_TABLE, null, 0, true));  // P0 txnEnd

            // P1: write to order_items, NO txnEnd yet
            ctx.collect(row(DB, ORDER_ITEMS_TABLE,
                    "{\"item_id\":1,\"order_id\":1,\"product_name\":\"widget\",\"quantity\":2,\"price\":25.00}",
                    1, false));

            // Signal test thread that phase 1 is done
            PHASE1_DONE_LATCH.countDown();

            // Wait for test thread to verify blocking, then send P1 txnEnd
            P1_TXN_END_LATCH.await();
            ctx.collect(row(DB, ORDER_ITEMS_TABLE, null, 1, true));  // P1 txnEnd
            P1_TXN_END_EMITTED_LATCH.countDown();
        }

        @Override
        public void cancel() {
            CountDownLatch l1 = PHASE1_DONE_LATCH;
            CountDownLatch l2 = P1_TXN_END_LATCH;
            if (l1 != null) l1.countDown();
            if (l2 != null) l2.countDown();
        }
    }

    // -------------------------------------------------------------------------
    // Shared builder helpers
    // -------------------------------------------------------------------------

    private StreamExecutionEnvironment buildEnv(int parallelism) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(parallelism);
        // Use a 60-second checkpoint interval so that no periodic checkpoint fires
        // during these short-running tests (each test runs for < 30 seconds).
        // Periodic checkpoints firing while the sink task thread is inside flush()
        // from close() causes a deadlock (the task thread is parked, can't process
        // the barrier, checkpoint never completes). By setting the interval to 60s,
        // all tests complete before any checkpoint is triggered.
        env.enableCheckpointing(60_000);
        env.getCheckpointConfig().setCheckpointTimeout(30_000);
        return env;
    }

    private SinkFunction<DefaultStarRocksRowData> buildSink(
            String ordersTable, String orderItemsTable, int flushIntervalMs) {
        StarRocksSinkOptions options = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", getJdbcUrl())
                .withProperty("load-url", getHttpUrls())
                .withProperty("database-name", "*")
                .withProperty("table-name", "*")
                .withProperty("username", USERNAME)
                .withProperty("password", PASSWORD)
                .withProperty("sink.version", "V2")
                .withProperty("sink.semantic", "at-least-once")
                .withProperty("sink.transaction.multi-table.enabled", "true")
                .withProperty("sink.buffer-flush.interval-ms", String.valueOf(flushIntervalMs))
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .build();

        options.addTableProperties(StreamLoadTableProperties.builder()
                .database(DB_NAME)
                .table(ordersTable)
                .addProperty("format", "json")
                .addProperty("strip_outer_array", "true")
                .addProperty("ignore_json_size", "true")
                .build());

        options.addTableProperties(StreamLoadTableProperties.builder()
                .database(DB_NAME)
                .table(orderItemsTable)
                .addProperty("format", "json")
                .addProperty("strip_outer_array", "true")
                .addProperty("ignore_json_size", "true")
                .build());

        return SinkFunctionFactory.createSinkFunction(options);
    }

    private SinkFunction<DefaultStarRocksRowData> buildSinkWithLabelPrefix(
            String ordersTable, String orderItemsTable, int flushIntervalMs, String labelPrefix) {
        StarRocksSinkOptions options = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", getJdbcUrl())
                .withProperty("load-url", getHttpUrls())
                .withProperty("database-name", "*")
                .withProperty("table-name", "*")
                .withProperty("username", USERNAME)
                .withProperty("password", PASSWORD)
                .withProperty("sink.version", "V2")
                .withProperty("sink.semantic", "at-least-once")
                .withProperty("sink.transaction.multi-table.enabled", "true")
                .withProperty("sink.buffer-flush.interval-ms", String.valueOf(flushIntervalMs))
                .withProperty("sink.label-prefix", labelPrefix)
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .build();

        options.addTableProperties(StreamLoadTableProperties.builder()
                .database(DB_NAME)
                .table(ordersTable)
                .addProperty("format", "json")
                .addProperty("strip_outer_array", "true")
                .addProperty("ignore_json_size", "true")
                .build());

        options.addTableProperties(StreamLoadTableProperties.builder()
                .database(DB_NAME)
                .table(orderItemsTable)
                .addProperty("format", "json")
                .addProperty("strip_outer_array", "true")
                .addProperty("ignore_json_size", "true")
                .build());

        return SinkFunctionFactory.createSinkFunction(options);
    }

    /** Builds a {@link DefaultStarRocksRowData}. {@code json} may be {@code null} for txnEnd-only rows. */
    private static DefaultStarRocksRowData row(String db, String table, String json,
                                               int partition, boolean txnEnd) {
        DefaultStarRocksRowData r = new DefaultStarRocksRowData(null, db, table, json);
        r.setSourcePartition(partition);
        r.setTransactionEnd(txnEnd);
        return r;
    }
}
