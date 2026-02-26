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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.starrocks.connector.flink.it.sink.StarRocksTableUtils.scanTable;
import static com.starrocks.connector.flink.it.sink.StarRocksTableUtils.verifyResult;

/**
 * Integration test for multi-table atomic transaction stream load.
 *
 * <p>Requires StarRocks >= 4.0 for multi-table transaction support. If running
 * against an older version, the test will fail at the begin transaction step.
 *
 * <p>Uses the same Testcontainers-based infrastructure as other IT tests.
 * Override the StarRocks image with:
 * <pre>
 *   mvn test -Dtest=MultiTableTransactionITTest -Dit.starrocks.image=starrocks/allin1-ubuntu:4.0.4
 * </pre>
 */
public class MultiTableTransactionITTest extends StarRocksITTestBase {

    private String createOrdersTable() throws Exception {
        String tableName = "orders_" + genRandomUuid();
        String sql = String.format(
                "CREATE TABLE `%s`.`%s` (" +
                        "order_id BIGINT NOT NULL," +
                        "customer_id BIGINT NOT NULL," +
                        "total_amount DECIMAL(10,2) DEFAULT '0'," +
                        "order_status VARCHAR(32) DEFAULT ''" +
                        ") ENGINE=OLAP PRIMARY KEY(order_id) " +
                        "DISTRIBUTED BY HASH(order_id) BUCKETS 4 " +
                        "PROPERTIES (\"replication_num\" = \"1\")",
                DB_NAME, tableName);
        executeSrSQL(sql);
        return tableName;
    }

    private String createOrderItemsTable() throws Exception {
        String tableName = "order_items_" + genRandomUuid();
        String sql = String.format(
                "CREATE TABLE `%s`.`%s` (" +
                        "item_id BIGINT NOT NULL," +
                        "order_id BIGINT NOT NULL," +
                        "product_name VARCHAR(128) DEFAULT ''," +
                        "quantity INT DEFAULT '0'," +
                        "price DECIMAL(10,2) DEFAULT '0'" +
                        ") ENGINE=OLAP PRIMARY KEY(item_id) " +
                        "DISTRIBUTED BY HASH(item_id) BUCKETS 4 " +
                        "PROPERTIES (\"replication_num\" = \"1\")",
                DB_NAME, tableName);
        executeSrSQL(sql);
        return tableName;
    }

    /**
     * Verifies that data written to multiple tables via the multi-table transaction
     * mode is atomically visible. Simulates two source transactions, each writing
     * to both orders and order_items tables.
     */
    @Test
    public void testMultiTableAtomicWrite() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        List<DefaultStarRocksRowData> rows = new ArrayList<>();

        // Source transaction 1: one order with two items
        rows.add(buildRow(DB_NAME, ordersTable,
                "{\"order_id\":1,\"customer_id\":100,\"total_amount\":99.99,\"order_status\":\"created\"}", 0, false));
        rows.add(buildRow(DB_NAME, orderItemsTable,
                "{\"item_id\":1,\"order_id\":1,\"product_name\":\"widget\",\"quantity\":2,\"price\":49.99}", 0, false));
        rows.add(buildRow(DB_NAME, orderItemsTable,
                "{\"item_id\":2,\"order_id\":1,\"product_name\":\"gadget\",\"quantity\":1,\"price\":50.00}", 0, true));

        // Source transaction 2: one order with one item
        rows.add(buildRow(DB_NAME, ordersTable,
                "{\"order_id\":2,\"customer_id\":101,\"total_amount\":25.00,\"order_status\":\"created\"}", 0, false));
        rows.add(buildRow(DB_NAME, orderItemsTable,
                "{\"item_id\":3,\"order_id\":2,\"product_name\":\"doohickey\",\"quantity\":3,\"price\":25.00}", 0, true));

        executeMultiTableLoad(ordersTable, orderItemsTable, rows);

        // Verify orders
        List<List<Object>> expectedOrders = Arrays.asList(
                Arrays.asList(1L, 100L, new java.math.BigDecimal("99.99"), "created"),
                Arrays.asList(2L, 101L, new java.math.BigDecimal("25.00"), "created")
        );
        List<List<Object>> actualOrders = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
        verifyResult(expectedOrders, actualOrders);

        // Verify order_items
        List<List<Object>> expectedItems = Arrays.asList(
                Arrays.asList(1L, 1L, "widget", 2, new java.math.BigDecimal("49.99")),
                Arrays.asList(2L, 1L, "gadget", 1, new java.math.BigDecimal("50.00")),
                Arrays.asList(3L, 2L, "doohickey", 3, new java.math.BigDecimal("25.00"))
        );
        List<List<Object>> actualItems = scanTable(DB_CONNECTION, DB_NAME, orderItemsTable);
        verifyResult(expectedItems, actualItems);
    }

    /**
     * Verifies that PRIMARY KEY table upsert works correctly across multiple
     * source transactions in multi-table mode.
     */
    @Test
    public void testMultiTableUpsert() throws Exception {
        String ordersTable = createOrdersTable();
        String orderItemsTable = createOrderItemsTable();

        List<DefaultStarRocksRowData> rows = new ArrayList<>();

        // Transaction 1: insert
        rows.add(buildRow(DB_NAME, ordersTable,
                "{\"order_id\":1,\"customer_id\":100,\"total_amount\":99.99,\"order_status\":\"created\"}", 0, false));
        rows.add(buildRow(DB_NAME, orderItemsTable,
                "{\"item_id\":1,\"order_id\":1,\"product_name\":\"widget\",\"quantity\":2,\"price\":49.99}", 0, true));

        // Transaction 2: update order status
        rows.add(buildRow(DB_NAME, ordersTable,
                "{\"order_id\":1,\"customer_id\":100,\"total_amount\":99.99,\"order_status\":\"paid\"}", 0, true));

        executeMultiTableLoad(ordersTable, orderItemsTable, rows);

        // Verify: order_status should be "paid" (upsert)
        List<List<Object>> expectedOrders = Arrays.asList(
                Arrays.asList(1L, 100L, new java.math.BigDecimal("99.99"), "paid")
        );
        List<List<Object>> actualOrders = scanTable(DB_CONNECTION, DB_NAME, ordersTable);
        verifyResult(expectedOrders, actualOrders);
    }

    private void executeMultiTableLoad(String ordersTable, String orderItemsTable,
                                       List<DefaultStarRocksRowData> rows) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(5_000);

        DataStream<DefaultStarRocksRowData> source = env.fromCollection(rows);

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
                .withProperty("sink.buffer-flush.interval-ms", "1000")
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .build();

        StreamLoadTableProperties itemsProps = StreamLoadTableProperties.builder()
                .database(DB_NAME)
                .table(orderItemsTable)
                .addProperty("format", "json")
                .addProperty("strip_outer_array", "true")
                .addProperty("ignore_json_size", "true")
                .build();
        options.addTableProperties(itemsProps);

        SinkFunction<DefaultStarRocksRowData> sink = SinkFunctionFactory.createSinkFunction(options);
        source.keyBy(DefaultStarRocksRowData::getSourcePartition)
                .addSink(sink);

        env.execute("MultiTableTransactionITTest");
    }

    private static DefaultStarRocksRowData buildRow(String db, String table, String json,
                                                     int partition, boolean txnEnd) {
        DefaultStarRocksRowData row = new DefaultStarRocksRowData(null, db, table, json);
        row.setSourcePartition(partition);
        row.setTransactionEnd(txnEnd);
        return row;
    }
}
