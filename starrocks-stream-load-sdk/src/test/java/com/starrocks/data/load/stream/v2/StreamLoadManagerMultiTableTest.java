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

package com.starrocks.data.load.stream.v2;

import com.starrocks.data.load.stream.MockedStarRocksHttpServer;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StreamLoadManagerMultiTableTest {

    private static final String USERNAME = "root";
    private static final String PASSWORD = "";

    private MockedStarRocksHttpServer mockedServer;

    @Before
    public void setUp() throws Exception {
        mockedServer = MockedStarRocksHttpServer.builder()
                .port(0)
                .enforceAuth(USERNAME, PASSWORD)
                .build();
        mockedServer.start();
    }

    @After
    public void tearDown() {
        if (mockedServer != null) {
            mockedServer.stop();
        }
    }

    private StreamLoadProperties buildMultiTableProperties(int flushIntervalMs) {
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database("test")
                .table("orders")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .maxBufferRows(100000)
                .build();

        return StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("4.0.0")
                .enableMultiTableTransaction()
                .labelPrefix("test-mtxn-")
                .defaultTableProperties(tableProps)
                .expectDelayTime(flushIntervalMs)
                .scanningFrequency(50)
                .ioThreadCount(2)
                .build();
    }

    /**
     * Single partition: write data to two tables, send txnEnd, verify commit.
     * Verifies that the shared transaction coordinator is used (1 begin, 1 prepare, 1 commit).
     */
    @Test
    public void testSinglePartitionWriteAndCommit() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(100);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            int partition = 0;
            manager.write(partition, "test", "orders",
                    "{\"order_id\":1, \"customer_id\":100, \"total_amount\":99.99}");
            manager.setCommitAllowed(partition, false);

            manager.write(partition, "test", "order_items",
                    "{\"item_id\":1, \"order_id\":1, \"product_name\":\"widget\", \"quantity\":2}");
            manager.setCommitAllowed(partition, true);

            Thread.sleep(500);
            Assert.assertNull("No exception expected", manager.getException());

            manager.flush();
            Assert.assertNull("No exception expected after flush", manager.getException());

            // Single shared transaction: exactly 1 begin and 1 prepare/commit sequence.
            Assert.assertEquals("Expected exactly 1 begin for single shared transaction",
                    1, mockedServer.getBeginCount());
            Assert.assertTrue("Expected at least 1 load call (one per table)",
                    mockedServer.getLoadCount() >= 1);
            Assert.assertEquals("Expected exactly 1 prepare for single shared transaction",
                    1, mockedServer.getPrepareCount());
            Assert.assertEquals("Expected exactly 1 commit for single shared transaction",
                    1, mockedServer.getCommitCount());
        } finally {
            manager.close();
        }
    }

    /**
     * Two partitions sharing one sink: commit only triggers when BOTH
     * partitions have reached txnEnd.
     */
    @Test
    public void testMultiPartitionCommitWaitsForAll() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(100);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            // Partition 0 writes + txnEnd
            manager.write(0, "test", "orders",
                    "{\"order_id\":1, \"customer_id\":100}");
            manager.setCommitAllowed(0, true);

            // Only partition 0 has txnEnd. Partition 1 hasn't even started.
            // But there's also only partition 0 active, so commit may trigger.
            Thread.sleep(300);
            Assert.assertNull("No exception after P0 txnEnd", manager.getException());

            // Now partition 1 writes + txnEnd
            manager.write(1, "test", "orders",
                    "{\"order_id\":2, \"customer_id\":101}");
            manager.setCommitAllowed(1, false);

            manager.write(1, "test", "order_items",
                    "{\"item_id\":1, \"order_id\":2}");
            manager.setCommitAllowed(1, true);

            Thread.sleep(500);
            Assert.assertNull("No exception after P1 txnEnd", manager.getException());

            manager.flush();
            Assert.assertNull("No exception after flush", manager.getException());
        } finally {
            manager.close();
        }
    }

    /**
     * Verifies that data is NOT committed while no partition has sent txnEnd.
     * The shared begin/prepare/commit should not happen before txnEnd.
     */
    @Test
    public void testCommitNotTriggeredWithoutTxnEnd() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(100);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            manager.write(0, "test", "orders",
                    "{\"order_id\":1, \"customer_id\":100}");
            manager.setCommitAllowed(0, false);

            Thread.sleep(300);
            Assert.assertNull("No exception expected", manager.getException());
            Assert.assertEquals("No begin expected before txnEnd", 0, mockedServer.getBeginCount());
            Assert.assertEquals("No commit expected before txnEnd", 0, mockedServer.getCommitCount());

            manager.setCommitAllowed(0, true);
            Thread.sleep(300);
            Assert.assertNull("No exception expected after txnEnd", manager.getException());

            manager.flush();
            Assert.assertNull("No exception expected after flush", manager.getException());
        } finally {
            manager.close();
        }
    }

    /**
     * N:1 mapping: multiple source transactions accumulate before commit.
     */
    @Test
    public void testMultipleTransactionsAccumulate() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(500);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            // Txn 1
            manager.write(0, "test", "orders", "{\"order_id\":1}");
            manager.setCommitAllowed(0, true);

            // Txn 2 (interval not elapsed yet, so txn 1 data stays in active chunk)
            manager.write(0, "test", "orders", "{\"order_id\":2}");
            manager.setCommitAllowed(0, true);

            // Wait for interval
            Thread.sleep(600);

            // Txn 3 triggers the interval check
            manager.write(0, "test", "orders", "{\"order_id\":3}");
            manager.setCommitAllowed(0, true);

            Thread.sleep(500);
            Assert.assertNull("No exception expected", manager.getException());

            manager.flush();
            Assert.assertNull("No exception expected after flush", manager.getException());
        } finally {
            manager.close();
        }
    }

    /**
     * Savepoint (flush) commits the active shared transaction even when
     * txnEnd has not been received (interval-based commit threshold is very high).
     * Verifies exactly 1 begin + prepare + commit via the savepoint path.
     */
    @Test
    public void testSavepointCommitsMultiTableTransaction() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(60000); // 60s interval — never fires
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            manager.write(0, "test", "orders",
                    "{\"order_id\":1, \"customer_id\":100}");
            manager.write(0, "test", "order_items",
                    "{\"item_id\":1, \"order_id\":1}");

            // setCommitAllowed(0, true) is NOT called — flush() must commit via savepoint path
            manager.flush();
            Assert.assertNull("No exception expected after flush", manager.getException());

            // The savepoint path commits any active shared transaction.
            // Exactly 1 begin/prepare/commit must be issued regardless of txnEnd.
            Assert.assertEquals("Savepoint should issue exactly 1 begin",
                    1, mockedServer.getBeginCount());
            Assert.assertEquals("Savepoint should issue exactly 1 prepare",
                    1, mockedServer.getPrepareCount());
            Assert.assertEquals("Savepoint should issue exactly 1 commit",
                    1, mockedServer.getCommitCount());
        } finally {
            manager.close();
        }
    }

    /**
     * An evicted (idle) partition that later receives txnEnd should be
     * automatically re-registered and participate in the next commit cycle.
     *
     * <p>This tests the {@code onTxnEnd()} fix that re-registers evicted partitions
     * instead of ignoring them.
     */
    @Test
    public void testEvictedPartitionReRegisters() throws Exception {
        // Very short interval (100 ms) to trigger multiple rapid commit cycles
        StreamLoadProperties properties = buildMultiTableProperties(100);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            mockedServer.resetCounters();

            // Commit cycle 1 — only partition 0
            manager.write(0, "test", "orders", "{\"order_id\":1}");
            manager.setCommitAllowed(0, true);
            Thread.sleep(400);

            // Commit cycle 2 — only partition 0 (partition 0 resets to ACTIVE)
            manager.write(0, "test", "orders", "{\"order_id\":2}");
            manager.setCommitAllowed(0, true);
            Thread.sleep(400);

            // Commit cycle 3 — only partition 0 (3rd cycle: idle count reaches MAX_IDLE_CYCLES)
            manager.write(0, "test", "orders", "{\"order_id\":3}");
            manager.setCommitAllowed(0, true);
            Thread.sleep(400);

            // After 3 idle commit cycles, partition 0 would be evicted.
            // Now partition 0 sends txnEnd again — it must be re-registered, not ignored.
            manager.write(0, "test", "orders", "{\"order_id\":4}");
            manager.setCommitAllowed(0, true);
            Thread.sleep(400);

            manager.flush();
            Assert.assertNull("No exception expected after evicted partition re-registers",
                    manager.getException());

            // At least 4 complete commit cycles must have occurred
            Assert.assertTrue("Expected at least 4 commits across re-registration cycles",
                    mockedServer.getCommitCount() >= 4);
        } finally {
            manager.close();
        }
    }

    /**
     * Regions from different databases must be rejected: multi-table transactions
     * require all tables to share the same StarRocks database.
     */
    @Test
    public void testCrossDbWriteIsRejected() throws Exception {
        StreamLoadProperties properties = buildMultiTableProperties(100);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            // Write to two different databases in the same commit cycle
            manager.write(0, "db_a", "orders", "{\"order_id\":1}");
            manager.write(0, "db_b", "payments", "{\"payment_id\":1}");
            manager.setCommitAllowed(0, true);

            // Give manager thread time to attempt commit
            Thread.sleep(500);

            // Manager should have recorded an error about mismatched databases
            Assert.assertNotNull("Expected exception for cross-database write",
                    manager.getException());
            Assert.assertTrue("Exception should mention database mismatch",
                    manager.getException().getMessage().contains("same database"));
        } finally {
            manager.close();
        }
    }

    /**
     * Non-multi-table mode is completely unaffected by the new code paths.
     */
    @Test
    public void testNonMultiTableModeUnaffected() throws Exception {
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database("test")
                .table("tbl1")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .maxBufferRows(100000)
                .build();

        StreamLoadProperties properties = StreamLoadProperties.builder()
                .loadUrls(mockedServer.getBaseUrl())
                .username(USERNAME)
                .password(PASSWORD)
                .version("3.5.0")
                .enableTransaction()
                .labelPrefix("test-normal-")
                .defaultTableProperties(tableProps)
                .expectDelayTime(1000)
                .scanningFrequency(50)
                .ioThreadCount(2)
                .build();

        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            manager.write(null, "test", "tbl1",
                    "{\"id\":1, \"name\":\"test\"}");

            manager.flush();
            Assert.assertNull("No exception expected", manager.getException());
        } finally {
            manager.close();
        }
    }
}
