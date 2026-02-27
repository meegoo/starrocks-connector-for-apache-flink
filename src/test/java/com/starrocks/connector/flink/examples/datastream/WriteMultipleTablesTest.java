/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.examples.datastream;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.mock.MockFeHttpServer;
import com.starrocks.connector.flink.table.data.DefaultStarRocksRowData;
import com.starrocks.connector.flink.table.sink.SinkFunctionFactory;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for WriteMultipleTables example (examples/datastream/WriteMultipleTables.java).
 * Tests the multi-table sink pattern: buildRow, StarRocksSinkOptions with database-name/table-name
 * as "*", addTableProperties for table-specific config, and SinkFunctionFactory.createSinkFunction.
 */
public class WriteMultipleTablesTest {

    private static DefaultStarRocksRowData buildRow(String db, String table, String data) {
        return new DefaultStarRocksRowData(null, db, table, data);
    }

    @Test
    public void testBuildRow() {
        DefaultStarRocksRowData row = buildRow("test", "tbl1", "{\"id\":1, \"name\":\"starrocks\", \"score\":100}");
        assertEquals("test", row.getDatabase());
        assertEquals("tbl1", row.getTable());
        assertEquals("{\"id\":1, \"name\":\"starrocks\", \"score\":100}", row.getRow());
        assertEquals(null, row.getUniqueKey());
    }

    @Test
    public void testBuildRowWithPartialColumns() {
        DefaultStarRocksRowData row = buildRow("test", "tbl2", "{\"order_id\":2, \"order_state\":2}");
        assertEquals("test", row.getDatabase());
        assertEquals("tbl2", row.getTable());
        assertEquals("{\"order_id\":2, \"order_state\":2}", row.getRow());
    }

    @Test
    public void testStarRocksSinkOptionsWithMultipleTables() {
        StarRocksSinkOptions options = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
                .withProperty("load-url", "127.0.0.1:8030")
                .withProperty("database-name", "*")
                .withProperty("table-name", "*")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .withProperty("sink.properties.ignore_json_size", "true")
                .build();

        assertEquals("*", options.getDatabaseName());
        assertEquals("*", options.getTableName());
    }

    @Test
    public void testAddTableProperties() {
        StarRocksSinkOptions options = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
                .withProperty("load-url", "127.0.0.1:8030")
                .withProperty("database-name", "*")
                .withProperty("table-name", "*")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .withProperty("sink.properties.ignore_json_size", "true")
                .build();

        StreamLoadTableProperties tbl2Properties = StreamLoadTableProperties.builder()
                .database("test")
                .table("tbl2")
                .addProperty("format", "json")
                .addProperty("strip_outer_array", "true")
                .addProperty("ignore_json_size", "true")
                .addProperty("partial_update", "true")
                .addProperty("columns", "`order_id`,`order_state`")
                .build();
        options.addTableProperties(tbl2Properties);

        assertEquals("*", options.getDatabaseName());
        assertEquals("*", options.getTableName());
    }

    @Test
    public void testCreateSinkFunctionWithMultipleTables() throws Exception {
        try (MockFeHttpServer httpServer = new MockFeHttpServer()) {
            httpServer.start();
            int port = httpServer.getListenPort();

            JSONObject probeResponse = new JSONObject();
            probeResponse.put("status", "INVALID_ARGUMENT");
            probeResponse.put("msg", "empty label");
            httpServer.addJsonResponse(probeResponse.toJSONString());

            StarRocksSinkOptions options = StarRocksSinkOptions.builder()
                    .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
                    .withProperty("load-url", "127.0.0.1:" + port)
                    .withProperty("database-name", "*")
                    .withProperty("table-name", "*")
                    .withProperty("username", "root")
                    .withProperty("password", "")
                    .withProperty("sink.properties.format", "json")
                    .withProperty("sink.properties.strip_outer_array", "true")
                    .withProperty("sink.properties.ignore_json_size", "true")
                    .withProperty("sink.connect.timeout-ms", "2000")
                    .build();

            StreamLoadTableProperties tbl2Properties = StreamLoadTableProperties.builder()
                    .database("test")
                    .table("tbl2")
                    .addProperty("format", "json")
                    .addProperty("strip_outer_array", "true")
                    .addProperty("ignore_json_size", "true")
                    .addProperty("partial_update", "true")
                    .addProperty("columns", "`order_id`,`order_state`")
                    .build();
            options.addTableProperties(tbl2Properties);

            SinkFunction<DefaultStarRocksRowData> sink = SinkFunctionFactory.createSinkFunction(options);
            assertNotNull(sink);
        }
    }

    @Test
    public void testRecordsStructureMatchesExample() {
        DefaultStarRocksRowData[] records = new DefaultStarRocksRowData[]{
                buildRow("test", "tbl1", "{\"id\":1, \"name\":\"starrocks-json\", \"score\":100}"),
                buildRow("test", "tbl1", "{\"id\":2, \"name\":\"flink-json\"}"),
                buildRow("test", "tbl2", "{\"order_id\":1, \"order_state\":1, \"total_price\":100}"),
                buildRow("test", "tbl2", "{\"order_id\":2, \"order_state\":2}")
        };

        assertEquals(4, records.length);
        assertEquals("test", records[0].getDatabase());
        assertEquals("tbl1", records[0].getTable());
        assertEquals("test", records[2].getDatabase());
        assertEquals("tbl2", records[2].getTable());
    }
}
