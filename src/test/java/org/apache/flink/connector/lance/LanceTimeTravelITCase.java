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
import org.apache.flink.connector.lance.util.LanceOpener;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.Version;
import org.lance.WriteParams;
import org.lance.operation.Append;
import org.lance.operation.Overwrite;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end integration test for time-travel reads (issue #5).
 *
 * <p>Strategy:
 * <ol>
 *   <li>Write 3 versions to a fresh dataset with distinct row counts (10, 20, 30).</li>
 *   <li>Open each historical version via {@link LanceOpener#open(String, BufferAllocator, LanceOptions)}
 *       driven by {@code read.version} and assert row counts.</li>
 *   <li>Resolve a version via {@code read.as-of-timestamp} using a future timestamp
 *       (should return the latest version) and a very-past timestamp (should reject).</li>
 * </ol>
 *
 * <p>This test intentionally uses the Lance Java API directly for writes so the failure surface
 * is confined to {@code LanceOpener} + {@code LanceOptions} — the two units this issue actually
 * touches. Sink-driven end-to-end tests belong to the SinkV2 IT (issue #49).
 */
class LanceTimeTravelITCase {

    @TempDir
    Path tempDir;

    @BeforeAll
    static void ensureArrowNettyLoaded() {
        // Force Arrow to use Netty allocator, matching the pattern used by
        // LanceNamespaceCatalogITCase to avoid classloader-related SPI issues.
        System.setProperty("arrow.memory.allocator.type", "Netty");
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            // allocator created and closed successfully
        }
    }

    @Test
    @DisplayName("read.version returns the row count of the requested historical version")
    void testReadByVersion() throws Exception {
        String datasetPath = tempDir.resolve("tt_by_version").toString();
        Schema schema = simpleSchema();

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            long v1 = writeVersion(datasetPath, schema, allocator, 0, 10, /* overwrite */ true);
            long v2 = writeVersion(datasetPath, schema, allocator, 10, 20, false);
            long v3 = writeVersion(datasetPath, schema, allocator, 30, 30, false);

            assertThat(v1).isLessThan(v2);
            assertThat(v2).isLessThan(v3);

            // Latest read (no time-travel options) should return v1+v2+v3 = 60 rows.
            LanceOptions latest = LanceOptions.builder().path(datasetPath).build();
            try (Dataset ds = LanceOpener.open(datasetPath, allocator, latest)) {
                assertThat(ds.countRows()).isEqualTo(60L);
                assertThat(ds.version()).isEqualTo(v3);
            }

            // read.version=v1  → only the first 10 rows.
            LanceOptions atV1 = LanceOptions.builder().path(datasetPath).readVersion(v1).build();
            try (Dataset ds = LanceOpener.open(datasetPath, allocator, atV1)) {
                assertThat(ds.countRows()).isEqualTo(10L);
                assertThat(ds.version()).isEqualTo(v1);
            }

            // read.version=v2  → 30 rows (v1 append + v2 append).
            LanceOptions atV2 = LanceOptions.builder().path(datasetPath).readVersion(v2).build();
            try (Dataset ds = LanceOpener.open(datasetPath, allocator, atV2)) {
                assertThat(ds.countRows()).isEqualTo(30L);
                assertThat(ds.version()).isEqualTo(v2);
            }
        }
    }

    @Test
    @DisplayName("read.as-of-timestamp resolves to the newest version whose data time <= target")
    void testReadByAsOfTimestamp() throws Exception {
        String datasetPath = tempDir.resolve("tt_by_timestamp").toString();
        Schema schema = simpleSchema();

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            long v1 = writeVersion(datasetPath, schema, allocator, 0, 5, true);
            long v2 = writeVersion(datasetPath, schema, allocator, 5, 5, false);

            // Grab v1's data time so we can build both a "== v1 time" and a "far future" probe.
            ZonedDateTime v1Time;
            try (Dataset ds = Dataset.open(datasetPath, allocator)) {
                List<Version> versions = ds.listVersions();
                v1Time = versions.stream()
                        .filter(v -> v.getId() == v1)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("v1 not found in listVersions()"))
                        .getDataTime();
                assertThat(v1Time).isNotNull();
            }

            // Far-future timestamp → should resolve to the latest version (v2).
            String farFuture = "2999-01-01T00:00:00Z";
            LanceOptions asOfFuture = LanceOptions.builder()
                    .path(datasetPath)
                    .readAsOfTimestamp(farFuture)
                    .build();
            try (Dataset ds = LanceOpener.open(datasetPath, allocator, asOfFuture)) {
                assertThat(ds.version()).isEqualTo(v2);
                assertThat(ds.countRows()).isEqualTo(10L);
            }

            // Exact v1 time → should resolve to v1 (newest version whose time <= target).
            LanceOptions asOfV1 = LanceOptions.builder()
                    .path(datasetPath)
                    .readAsOfTimestamp(v1Time.toInstant().toString())
                    .build();
            try (Dataset ds = LanceOpener.open(datasetPath, allocator, asOfV1)) {
                assertThat(ds.version()).isEqualTo(v1);
                assertThat(ds.countRows()).isEqualTo(5L);
            }

            // Timestamp that predates every version → informative error.
            LanceOptions asOfAncient = LanceOptions.builder()
                    .path(datasetPath)
                    .readAsOfTimestamp("1970-01-01T00:00:00Z")
                    .build();
            assertThatThrownBy(() -> LanceOpener.open(datasetPath, allocator, asOfAncient).close())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("predates the oldest version");
        }
    }

    @Test
    @DisplayName("read.version takes precedence over read.as-of-timestamp when both are set")
    void testReadVersionWinsOverTimestamp() throws Exception {
        String datasetPath = tempDir.resolve("tt_precedence").toString();
        Schema schema = simpleSchema();

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            long v1 = writeVersion(datasetPath, schema, allocator, 0, 3, true);
            long v2 = writeVersion(datasetPath, schema, allocator, 3, 3, false);

            // read.version wins even though the timestamp would resolve to v2.
            LanceOptions both = LanceOptions.builder()
                    .path(datasetPath)
                    .readVersion(v1)
                    .readAsOfTimestamp("2999-01-01T00:00:00Z")
                    .build();
            try (Dataset ds = LanceOpener.open(datasetPath, allocator, both)) {
                assertThat(ds.version()).isEqualTo(v1);
                assertThat(ds.countRows()).isEqualTo(3L);
            }
            // Suppress unused var warning: v2 is intentional (proves dataset has multiple versions).
            assertThat(v2).isGreaterThan(v1);
        }
    }

    // ============================ helpers ============================

    private static Schema simpleSchema() {
        return new Schema(Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
        ));
    }

    /**
     * Write {@code count} rows starting at {@code idBase} to {@code datasetPath} as either the
     * initial Overwrite commit or an Append commit. Returns the resulting dataset version.
     */
    private static long writeVersion(String datasetPath, Schema schema, BufferAllocator allocator,
                                     long idBase, int count, boolean overwrite) throws Exception {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BigIntVector idVec = (BigIntVector) root.getVector("id");
            VarCharVector nameVec = (VarCharVector) root.getVector("name");
            idVec.allocateNew(count);
            nameVec.allocateNew();
            for (int i = 0; i < count; i++) {
                idVec.setSafe(i, idBase + i);
                nameVec.setSafe(i, ("row-" + (idBase + i)).getBytes(StandardCharsets.UTF_8));
            }
            root.setRowCount(count);

            WriteParams writeParams = new WriteParams.Builder().withMaxRowsPerFile(1_000_000).build();
            List<FragmentMetadata> fragments = Fragment.write()
                    .datasetUri(datasetPath)
                    .allocator(allocator)
                    .data(root)
                    .writeParams(writeParams)
                    .execute();

            CommitBuilder builder;
            Transaction txn;
            if (overwrite) {
                builder = new CommitBuilder(datasetPath, allocator).writeParams(Collections.emptyMap());
                txn = new Transaction.Builder()
                        .operation(Overwrite.builder().fragments(fragments).schema(schema).build())
                        .build();
            } else {
                builder = new CommitBuilder(datasetPath, allocator);
                txn = new Transaction.Builder()
                        .operation(Append.builder().fragments(fragments).build())
                        .build();
            }

            try (Transaction toClose = txn;
                 Dataset ds = builder.execute(toClose)) {
                return ds.version();
            }
        }
    }
}
