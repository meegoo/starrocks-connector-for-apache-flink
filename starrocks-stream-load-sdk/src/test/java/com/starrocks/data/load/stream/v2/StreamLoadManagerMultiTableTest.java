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

    private StreamLoadProperties buildProperties(int flushIntervalMs) {
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
                .enableTransaction()
                .enableMultiTableTransaction()
                .labelPrefix("test-mtxn-")
                .defaultTableProperties(tableProps)
                .expectDelayTime(flushIntervalMs)
                .scanningFrequency(50)
                .ioThreadCount(2)
                .build();
    }

    @Test
    public void testMultiTableWriteAndCommit() throws Exception {
        StreamLoadProperties properties = buildProperties(100);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            manager.write(null, "test", "orders",
                    "{\"order_id\":1, \"customer_id\":100, \"total_amount\":99.99}");
            manager.setCommitAllowed(false);

            manager.write(null, "test", "order_items",
                    "{\"item_id\":1, \"order_id\":1, \"product_name\":\"widget\", \"quantity\":2}");
            manager.setCommitAllowed(false);

            manager.write(null, "test", "order_items",
                    "{\"item_id\":2, \"order_id\":1, \"product_name\":\"gadget\", \"quantity\":1}");
            // last row of source transaction
            manager.setCommitAllowed(true);

            // timer not ready yet (just started), so no commit triggered
            // wait for flush interval to elapse
            Thread.sleep(200);

            // write another transaction
            manager.write(null, "test", "orders",
                    "{\"order_id\":2, \"customer_id\":101, \"total_amount\":49.99}");
            manager.setCommitAllowed(false);

            manager.write(null, "test", "order_items",
                    "{\"item_id\":3, \"order_id\":2, \"product_name\":\"doohickey\", \"quantity\":3}");
            manager.setCommitAllowed(true);
            // This should trigger commit (timer elapsed + txnEnd)

            // wait for async commit to complete
            Thread.sleep(500);

            // Verify no exceptions
            Assert.assertNull("No exception expected", manager.getException());

        } finally {
            manager.close();
        }
    }

    @Test
    public void testSavepointCommitsMultiTableTransaction() throws Exception {
        StreamLoadProperties properties = buildProperties(60000);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            manager.write(null, "test", "orders",
                    "{\"order_id\":1, \"customer_id\":100}");
            manager.setCommitAllowed(false);

            manager.write(null, "test", "order_items",
                    "{\"item_id\":1, \"order_id\":1}");
            manager.setCommitAllowed(true);

            // flush() triggers savepoint which commits in multi-table mode
            manager.flush();

            Assert.assertNull("No exception expected after flush", manager.getException());
        } finally {
            manager.close();
        }
    }

    @Test
    public void testCommitNotTriggeredWithoutTxnEnd() throws Exception {
        StreamLoadProperties properties = buildProperties(100);
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(properties, true);
        manager.init();

        try {
            manager.write(null, "test", "orders",
                    "{\"order_id\":1, \"customer_id\":100}");
            manager.setCommitAllowed(false);

            // wait longer than flush interval
            Thread.sleep(300);

            // No txnEnd was set, so no commit should have been triggered.
            // Data is in the buffer but not committed.
            // Verify no errors
            Assert.assertNull("No exception expected", manager.getException());

            // Now flush via savepoint — this WILL commit regardless of commitAllowed
            manager.flush();
            Assert.assertNull("No exception expected after flush", manager.getException());
        } finally {
            manager.close();
        }
    }

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

            // setCommitAllowed is no-op in non-multi-table mode
            manager.setCommitAllowed(true);

            // flush via savepoint — uses original per-region commit
            manager.flush();

            Assert.assertNull("No exception expected", manager.getException());
        } finally {
            manager.close();
        }
    }
}
