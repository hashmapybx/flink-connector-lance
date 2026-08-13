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

package org.apache.flink.connector.lance.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the Arrow IPC schema stream that {@link LanceNamespaceCatalog} sends with
 * {@code createTable}.
 *
 * <p>These exercise the serialization in isolation, without a live namespace, by invoking the
 * private helper reflectively. The end-to-end path is covered by {@code
 * LanceNamespaceCatalogITCase}.
 */
@DisplayName("Lance Namespace Catalog Schema Serialization Tests")
class LanceNamespaceCatalogSchemaTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        if (allocator != null) {
            allocator.close();
        }
    }

    @Test
    @DisplayName("Schema is serialized as a readable Arrow IPC stream")
    void testSchemaRoundTrip() throws Exception {
        CatalogTable table = tableWith(
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("name", DataTypes.STRING())
                        .column("score", DataTypes.DOUBLE())
                        .build());

        byte[] ipc = LanceNamespaceCatalog.toArrowIpcSchema(table, allocator);

        assertThat(ipc).isNotEmpty();

        try (ArrowStreamReader reader =
                new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            org.apache.arrow.vector.types.pojo.Schema arrowSchema = reader.getVectorSchemaRoot()
                    .getSchema();
            assertThat(arrowSchema.getFields())
                    .extracting(org.apache.arrow.vector.types.pojo.Field::getName)
                    .containsExactly("id", "name", "score");

            // The stream carries one batch of zero rows: the namespace rejects a
            // schema-only stream, and an empty table must not gain phantom rows.
            assertThat(reader.loadNextBatch()).isTrue();
            assertThat(reader.getVectorSchemaRoot().getRowCount()).isZero();
            assertThat(reader.loadNextBatch()).isFalse();
        }
    }

    @Test
    @DisplayName("Computed and metadata columns are excluded from the stored schema")
    void testOnlyPhysicalColumnsAreSerialized() throws Exception {
        CatalogTable table = tableWith(
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .columnByExpression("id_doubled", "id * 2")
                        .build());

        byte[] ipc = LanceNamespaceCatalog.toArrowIpcSchema(table, allocator);

        try (ArrowStreamReader reader =
                new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            assertThat(reader.getVectorSchemaRoot().getSchema().getFields())
                    .extracting(org.apache.arrow.vector.types.pojo.Field::getName)
                    .containsExactly("id");
        }
    }

    @Test
    @DisplayName("A schema with no physical columns is rejected with a clear message")
    void testNoPhysicalColumnsRejected() {
        CatalogTable table = tableWith(Schema.newBuilder().build());

        assertThatThrownBy(() -> LanceNamespaceCatalog.toArrowIpcSchema(table, allocator))
                .isInstanceOf(org.apache.flink.table.catalog.exceptions.CatalogException.class)
                .hasMessageContaining("without physical columns");
    }

    private static CatalogTable tableWith(Schema schema) {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "lance");
        options.put("path", "/tmp/does-not-need-to-exist");
        return CatalogTable.of(schema, "", Collections.emptyList(), options);
    }
}
