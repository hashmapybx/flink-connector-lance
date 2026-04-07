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

package org.apache.flink.connector.lance;

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.connector.lance.table.LanceDynamicTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Lance DELETE support.
 *
 * <p>Validates the LanceDeleteExecutor, LanceSink DELETE handling,
 * and LanceDynamicTableSink changelog mode support.
 */
public class LanceDeleteTest {

    @TempDir
    Path tempDir;

    // ==================== LanceDeleteExecutor Tests ====================

    @Test
    public void testDeleteExecutorCreation() {
        String path = tempDir.resolve("test_dataset").toString();
        try (LanceDeleteExecutor executor = new LanceDeleteExecutor(path)) {
            assertEquals(path, executor.getDatasetPath());
        } catch (IOException e) {
            fail("Should not throw exception on creation: " + e.getMessage());
        }
    }

    @Test
    public void testDeleteExecutorNullPath() {
        assertThrows(IllegalArgumentException.class, () -> {
            new LanceDeleteExecutor(null);
        });
    }

    @Test
    public void testDeleteExecutorEmptyPath() {
        assertThrows(IllegalArgumentException.class, () -> {
            new LanceDeleteExecutor("");
        });
    }

    @Test
    public void testDeleteExecutorNullPredicate() {
        String path = tempDir.resolve("test_dataset").toString();
        try (LanceDeleteExecutor executor = new LanceDeleteExecutor(path)) {
            assertThrows(IllegalArgumentException.class, () -> {
                executor.delete(null);
            });
        } catch (IOException e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testDeleteExecutorEmptyPredicate() {
        String path = tempDir.resolve("test_dataset").toString();
        try (LanceDeleteExecutor executor = new LanceDeleteExecutor(path)) {
            assertThrows(IllegalArgumentException.class, () -> {
                executor.delete("");
            });
        } catch (IOException e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testDeleteExecutorBlankPredicate() {
        String path = tempDir.resolve("test_dataset").toString();
        try (LanceDeleteExecutor executor = new LanceDeleteExecutor(path)) {
            assertThrows(IllegalArgumentException.class, () -> {
                executor.delete("   ");
            });
        } catch (IOException e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testDeleteAndCountNullPredicate() {
        String path = tempDir.resolve("test_dataset").toString();
        try (LanceDeleteExecutor executor = new LanceDeleteExecutor(path)) {
            assertThrows(IllegalArgumentException.class, () -> {
                executor.deleteAndCount(null);
            });
        } catch (IOException e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }

    // ==================== LanceDynamicTableSink ChangelogMode Tests ====================

    @Test
    public void testSinkSupportsInsertOnly() {
        LanceOptions options = LanceOptions.builder()
                .path(tempDir.resolve("test_dataset").toString())
                .build();

        RowType rowType = RowType.of(new IntType(), new VarCharType());
        DataType dataType = TypeConversions.fromLogicalToDataType(rowType);

        LanceDynamicTableSink sink = new LanceDynamicTableSink(options, dataType);

        // When only INSERT is requested
        ChangelogMode insertOnly = ChangelogMode.insertOnly();
        ChangelogMode result = sink.getChangelogMode(insertOnly);

        assertTrue(result.contains(RowKind.INSERT));
        assertFalse(result.contains(RowKind.DELETE));
    }

    @Test
    public void testSinkSupportsDeleteWhenRequested() {
        LanceOptions options = LanceOptions.builder()
                .path(tempDir.resolve("test_dataset").toString())
                .build();

        RowType rowType = RowType.of(new IntType(), new VarCharType());
        DataType dataType = TypeConversions.fromLogicalToDataType(rowType);

        LanceDynamicTableSink sink = new LanceDynamicTableSink(options, dataType);

        // When DELETE is requested
        ChangelogMode withDelete = ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .build();
        ChangelogMode result = sink.getChangelogMode(withDelete);

        assertTrue(result.contains(RowKind.INSERT));
        assertTrue(result.contains(RowKind.DELETE));
    }

    @Test
    public void testSinkCopy() {
        LanceOptions options = LanceOptions.builder()
                .path(tempDir.resolve("test_dataset").toString())
                .build();

        RowType rowType = RowType.of(new IntType(), new VarCharType());
        DataType dataType = TypeConversions.fromLogicalToDataType(rowType);

        LanceDynamicTableSink sink = new LanceDynamicTableSink(options, dataType);
        DynamicTableSink copy = sink.copy();

        assertNotSame(sink, copy);
        assertTrue(copy instanceof LanceDynamicTableSink);
    }

    @Test
    public void testSinkSummary() {
        LanceOptions options = LanceOptions.builder()
                .path(tempDir.resolve("test_dataset").toString())
                .build();

        RowType rowType = RowType.of(new IntType(), new VarCharType());
        DataType dataType = TypeConversions.fromLogicalToDataType(rowType);

        LanceDynamicTableSink sink = new LanceDynamicTableSink(options, dataType);
        assertEquals("Lance Table Sink", sink.asSummaryString());
    }

    // ==================== LanceSink DELETE Configuration Tests ====================

    @Test
    public void testLanceSinkBuilder() {
        RowType rowType = RowType.of(
                new LogicalType[]{new BigIntType(), new VarCharType()},
                new String[]{"id", "name"});

        LanceSink sink = LanceSink.builder()
                .path(tempDir.resolve("test_dataset").toString())
                .rowType(rowType)
                .batchSize(512)
                .build();

        assertNotNull(sink);
        assertEquals(rowType, sink.getRowType());
    }

    @Test
    public void testLanceSinkBuilderNullPath() {
        RowType rowType = RowType.of(new IntType());

        assertThrows(IllegalArgumentException.class, () -> {
            LanceSink.builder()
                    .rowType(rowType)
                    .build();
        });
    }

    @Test
    public void testLanceSinkBuilderNullRowType() {
        assertThrows(IllegalArgumentException.class, () -> {
            LanceSink.builder()
                    .path(tempDir.resolve("test_dataset").toString())
                    .build();
        });
    }

    @Test
    public void testDeleteExecutorNullAllocator() {
        assertThrows(IllegalArgumentException.class, () -> {
            new LanceDeleteExecutor(tempDir.resolve("test_dataset").toString(), null);
        });
    }
}
