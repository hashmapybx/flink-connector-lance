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
package org.apache.flink.connector.lance.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for split LanceOptions: LanceSourceOptions, LanceSinkOptions, etc. */
class LanceOptionsRefactorTest {

  // ==================== LanceSourceOptions Tests ====================

  @Nested
  @DisplayName("LanceSourceOptions")
  class SourceOptionsTests {

    @Test
    @DisplayName("Build with all fields")
    void testBuildAllFields() {
      LanceSourceOptions opts =
          LanceSourceOptions.builder()
              .path("/data/my_dataset")
              .batchSize(512)
              .limit(100L)
              .columns(Arrays.asList("id", "name"))
              .filter("id > 10")
              .build();

      assertThat(opts.getPath()).isEqualTo("/data/my_dataset");
      assertThat(opts.getBatchSize()).isEqualTo(512);
      assertThat(opts.getLimit()).isEqualTo(100L);
      assertThat(opts.getColumns()).containsExactly("id", "name");
      assertThat(opts.getFilter()).isEqualTo("id > 10");
    }

    @Test
    @DisplayName("Default values are correct")
    void testDefaults() {
      LanceSourceOptions opts = LanceSourceOptions.builder().path("/data").build();

      assertThat(opts.getBatchSize()).isEqualTo(1024);
      assertThat(opts.getLimit()).isNull();
      assertThat(opts.getColumns()).isEmpty();
      assertThat(opts.getFilter()).isNull();
    }

    @Test
    @DisplayName("Columns list is unmodifiable")
    void testColumnsImmutable() {
      LanceSourceOptions opts =
          LanceSourceOptions.builder().path("/data").columns(Arrays.asList("a", "b")).build();

      assertThatThrownBy(() -> opts.getColumns().add("c"))
          .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName("toBuilder creates a modified copy")
    void testToBuilder() {
      LanceSourceOptions original =
          LanceSourceOptions.builder().path("/data").batchSize(256).filter("id > 5").build();

      LanceSourceOptions modified = original.toBuilder().limit(50L).build();

      // Original is unchanged
      assertThat(original.getLimit()).isNull();
      // Modified has new limit but retains other fields
      assertThat(modified.getLimit()).isEqualTo(50L);
      assertThat(modified.getPath()).isEqualTo("/data");
      assertThat(modified.getBatchSize()).isEqualTo(256);
      assertThat(modified.getFilter()).isEqualTo("id > 5");
    }

    @Test
    @DisplayName("Validation rejects invalid batch size")
    void testInvalidBatchSize() {
      assertThatThrownBy(() -> LanceSourceOptions.builder().path("/data").batchSize(0).build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("batch-size");
    }

    @Test
    @DisplayName("Validation rejects negative limit")
    void testNegativeLimit() {
      assertThatThrownBy(() -> LanceSourceOptions.builder().path("/data").limit(-1L).build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("limit");
    }

    @Test
    @DisplayName("Null columns defaults to empty list")
    void testNullColumns() {
      LanceSourceOptions opts = LanceSourceOptions.builder().path("/data").columns(null).build();
      assertThat(opts.getColumns()).isEmpty();
    }

    @Test
    @DisplayName("equals and hashCode")
    void testEqualsHashCode() {
      LanceSourceOptions a =
          LanceSourceOptions.builder()
              .path("/data")
              .batchSize(512)
              .limit(10L)
              .columns(Arrays.asList("x"))
              .filter("x > 1")
              .build();
      LanceSourceOptions b =
          LanceSourceOptions.builder()
              .path("/data")
              .batchSize(512)
              .limit(10L)
              .columns(Arrays.asList("x"))
              .filter("x > 1")
              .build();
      LanceSourceOptions c = LanceSourceOptions.builder().path("/other").build();

      assertThat(a).isEqualTo(b);
      assertThat(a.hashCode()).isEqualTo(b.hashCode());
      assertThat(a).isNotEqualTo(c);
    }

    @Test
    @DisplayName("toString contains key fields")
    void testToString() {
      LanceSourceOptions opts = LanceSourceOptions.builder().path("/data").batchSize(256).build();
      assertThat(opts.toString()).contains("/data").contains("256");
    }
  }

  // ==================== LanceSinkOptions Tests ====================

  @Nested
  @DisplayName("LanceSinkOptions")
  class SinkOptionsTests {

    @Test
    @DisplayName("Build with all fields")
    void testBuildAllFields() {
      LanceSinkOptions opts =
          LanceSinkOptions.builder()
              .path("/data/sink")
              .batchSize(512)
              .writeMode(LanceOptions.WriteMode.OVERWRITE)
              .maxRowsPerFile(500000)
              .build();

      assertThat(opts.getPath()).isEqualTo("/data/sink");
      assertThat(opts.getBatchSize()).isEqualTo(512);
      assertThat(opts.getWriteMode()).isEqualTo(LanceOptions.WriteMode.OVERWRITE);
      assertThat(opts.getMaxRowsPerFile()).isEqualTo(500000);
    }

    @Test
    @DisplayName("Default values are correct")
    void testDefaults() {
      LanceSinkOptions opts = LanceSinkOptions.builder().path("/data").build();

      assertThat(opts.getBatchSize()).isEqualTo(1024);
      assertThat(opts.getWriteMode()).isEqualTo(LanceOptions.WriteMode.APPEND);
      assertThat(opts.getMaxRowsPerFile()).isEqualTo(1000000);
    }

    @Test
    @DisplayName("Validation rejects invalid batch size")
    void testInvalidBatchSize() {
      assertThatThrownBy(() -> LanceSinkOptions.builder().path("/data").batchSize(-1).build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("batch-size");
    }

    @Test
    @DisplayName("Validation rejects invalid max rows per file")
    void testInvalidMaxRows() {
      assertThatThrownBy(() -> LanceSinkOptions.builder().path("/data").maxRowsPerFile(0).build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("max-rows-per-file");
    }

    @Test
    @DisplayName("toBuilder creates a modified copy")
    void testToBuilder() {
      LanceSinkOptions original =
          LanceSinkOptions.builder()
              .path("/data")
              .batchSize(256)
              .writeMode(LanceOptions.WriteMode.APPEND)
              .build();

      LanceSinkOptions modified =
          original.toBuilder().writeMode(LanceOptions.WriteMode.OVERWRITE).build();

      assertThat(original.getWriteMode()).isEqualTo(LanceOptions.WriteMode.APPEND);
      assertThat(modified.getWriteMode()).isEqualTo(LanceOptions.WriteMode.OVERWRITE);
      assertThat(modified.getPath()).isEqualTo("/data");
      assertThat(modified.getBatchSize()).isEqualTo(256);
    }

    @Test
    @DisplayName("equals and hashCode")
    void testEqualsHashCode() {
      LanceSinkOptions a =
          LanceSinkOptions.builder()
              .path("/data")
              .batchSize(512)
              .writeMode(LanceOptions.WriteMode.OVERWRITE)
              .maxRowsPerFile(100)
              .build();
      LanceSinkOptions b =
          LanceSinkOptions.builder()
              .path("/data")
              .batchSize(512)
              .writeMode(LanceOptions.WriteMode.OVERWRITE)
              .maxRowsPerFile(100)
              .build();

      assertThat(a).isEqualTo(b);
      assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }
  }

  // ==================== LanceIndexOptions Tests ====================

  @Nested
  @DisplayName("LanceIndexOptions")
  class IndexOptionsTests {

    @Test
    @DisplayName("Build with all fields")
    void testBuildAllFields() {
      LanceIndexOptions opts =
          LanceIndexOptions.builder()
              .columnName("embedding")
              .indexType(LanceOptions.IndexType.IVF_HNSW)
              .metricType(LanceOptions.MetricType.COSINE)
              .numPartitions(128)
              .numSubVectors(32)
              .numBits(4)
              .maxLevel(5)
              .maxEdges(32)
              .efConstruction(200)
              .build();

      assertThat(opts.getColumnName()).isEqualTo("embedding");
      assertThat(opts.getIndexType()).isEqualTo(LanceOptions.IndexType.IVF_HNSW);
      assertThat(opts.getMetricType()).isEqualTo(LanceOptions.MetricType.COSINE);
      assertThat(opts.getNumPartitions()).isEqualTo(128);
      assertThat(opts.getNumSubVectors()).isEqualTo(32);
      assertThat(opts.getNumBits()).isEqualTo(4);
      assertThat(opts.getMaxLevel()).isEqualTo(5);
      assertThat(opts.getM()).isEqualTo(32);
      assertThat(opts.getEfConstruction()).isEqualTo(200);
    }

    @Test
    @DisplayName("Default values are correct")
    void testDefaults() {
      LanceIndexOptions opts = LanceIndexOptions.builder().build();

      assertThat(opts.getIndexType()).isEqualTo(LanceOptions.IndexType.IVF_PQ);
      assertThat(opts.getMetricType()).isEqualTo(LanceOptions.MetricType.L2);
      assertThat(opts.getNumPartitions()).isEqualTo(256);
      assertThat(opts.getNumSubVectors()).isNull();
      assertThat(opts.getNumBits()).isEqualTo(8);
      assertThat(opts.getMaxLevel()).isEqualTo(7);
      assertThat(opts.getM()).isEqualTo(16);
      assertThat(opts.getEfConstruction()).isEqualTo(100);
    }

    @Test
    @DisplayName("Validation rejects invalid num partitions")
    void testInvalidNumPartitions() {
      assertThatThrownBy(() -> LanceIndexOptions.builder().numPartitions(0).build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("num-partitions");
    }

    @Test
    @DisplayName("Validation rejects invalid num bits")
    void testInvalidNumBits() {
      assertThatThrownBy(() -> LanceIndexOptions.builder().numBits(17).build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("num-bits");
    }

    @Test
    @DisplayName("Validation rejects invalid M")
    void testInvalidM() {
      assertThatThrownBy(() -> LanceIndexOptions.builder().maxEdges(0).build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("index.m");
    }

    @Test
    @DisplayName("equals and hashCode")
    void testEqualsHashCode() {
      LanceIndexOptions a =
          LanceIndexOptions.builder()
              .columnName("vec")
              .indexType(LanceOptions.IndexType.IVF_FLAT)
              .numPartitions(64)
              .build();
      LanceIndexOptions b =
          LanceIndexOptions.builder()
              .columnName("vec")
              .indexType(LanceOptions.IndexType.IVF_FLAT)
              .numPartitions(64)
              .build();

      assertThat(a).isEqualTo(b);
      assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }
  }

  // ==================== LanceVectorSearchOptions Tests ====================

  @Nested
  @DisplayName("LanceVectorSearchOptions")
  class VectorSearchOptionsTests {

    @Test
    @DisplayName("Build with all fields")
    void testBuildAllFields() {
      LanceVectorSearchOptions opts =
          LanceVectorSearchOptions.builder()
              .columnName("embedding")
              .metricType(LanceOptions.MetricType.DOT)
              .nprobes(40)
              .ef(200)
              .refineFactor(5)
              .build();

      assertThat(opts.getColumnName()).isEqualTo("embedding");
      assertThat(opts.getMetricType()).isEqualTo(LanceOptions.MetricType.DOT);
      assertThat(opts.getNprobes()).isEqualTo(40);
      assertThat(opts.getEf()).isEqualTo(200);
      assertThat(opts.getRefineFactor()).isEqualTo(5);
    }

    @Test
    @DisplayName("Default values are correct")
    void testDefaults() {
      LanceVectorSearchOptions opts = LanceVectorSearchOptions.builder().build();

      assertThat(opts.getMetricType()).isEqualTo(LanceOptions.MetricType.L2);
      assertThat(opts.getNprobes()).isEqualTo(20);
      assertThat(opts.getEf()).isEqualTo(100);
      assertThat(opts.getRefineFactor()).isNull();
    }

    @Test
    @DisplayName("Validation rejects invalid nprobes")
    void testInvalidNprobes() {
      assertThatThrownBy(() -> LanceVectorSearchOptions.builder().nprobes(0).build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("nprobes");
    }

    @Test
    @DisplayName("Validation rejects invalid ef")
    void testInvalidEf() {
      assertThatThrownBy(() -> LanceVectorSearchOptions.builder().ef(-1).build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("ef");
    }

    @Test
    @DisplayName("Validation rejects invalid refine factor")
    void testInvalidRefineFactor() {
      assertThatThrownBy(() -> LanceVectorSearchOptions.builder().refineFactor(0).build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("refine-factor");
    }

    @Test
    @DisplayName("equals and hashCode")
    void testEqualsHashCode() {
      LanceVectorSearchOptions a =
          LanceVectorSearchOptions.builder()
              .columnName("vec")
              .metricType(LanceOptions.MetricType.COSINE)
              .nprobes(10)
              .ef(50)
              .build();
      LanceVectorSearchOptions b =
          LanceVectorSearchOptions.builder()
              .columnName("vec")
              .metricType(LanceOptions.MetricType.COSINE)
              .nprobes(10)
              .ef(50)
              .build();

      assertThat(a).isEqualTo(b);
      assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }
  }

  // ==================== LanceOptions Conversion Tests ====================

  @Nested
  @DisplayName("LanceOptions sub-options conversion")
  class ConversionTests {

    @Test
    @DisplayName("toSourceOptions preserves read fields")
    void testToSourceOptions() {
      LanceOptions full =
          LanceOptions.builder()
              .path("/data/ds")
              .readBatchSize(512)
              .readLimit(100L)
              .readColumns(Arrays.asList("id", "name"))
              .readFilter("id > 5")
              .writeBatchSize(256)
              .build();

      LanceSourceOptions source = full.toSourceOptions();

      assertThat(source.getPath()).isEqualTo("/data/ds");
      assertThat(source.getBatchSize()).isEqualTo(512);
      assertThat(source.getLimit()).isEqualTo(100L);
      assertThat(source.getColumns()).containsExactly("id", "name");
      assertThat(source.getFilter()).isEqualTo("id > 5");
    }

    @Test
    @DisplayName("toSinkOptions preserves write fields")
    void testToSinkOptions() {
      LanceOptions full =
          LanceOptions.builder()
              .path("/data/ds")
              .writeBatchSize(256)
              .writeMode(LanceOptions.WriteMode.OVERWRITE)
              .writeMaxRowsPerFile(500000)
              .readBatchSize(1024)
              .build();

      LanceSinkOptions sink = full.toSinkOptions();

      assertThat(sink.getPath()).isEqualTo("/data/ds");
      assertThat(sink.getBatchSize()).isEqualTo(256);
      assertThat(sink.getWriteMode()).isEqualTo(LanceOptions.WriteMode.OVERWRITE);
      assertThat(sink.getMaxRowsPerFile()).isEqualTo(500000);
    }

    @Test
    @DisplayName("toIndexOptions preserves index fields")
    void testToIndexOptions() {
      LanceOptions full =
          LanceOptions.builder()
              .path("/data/ds")
              .indexColumn("embedding")
              .indexType(LanceOptions.IndexType.IVF_HNSW)
              .indexNumPartitions(128)
              .indexNumSubVectors(32)
              .indexNumBits(4)
              .indexMaxLevel(5)
              .indexM(32)
              .indexEfConstruction(200)
              .build();

      LanceIndexOptions idx = full.toIndexOptions();

      assertThat(idx.getColumnName()).isEqualTo("embedding");
      assertThat(idx.getIndexType()).isEqualTo(LanceOptions.IndexType.IVF_HNSW);
      assertThat(idx.getNumPartitions()).isEqualTo(128);
      assertThat(idx.getNumSubVectors()).isEqualTo(32);
      assertThat(idx.getNumBits()).isEqualTo(4);
      assertThat(idx.getMaxLevel()).isEqualTo(5);
      assertThat(idx.getM()).isEqualTo(32);
      assertThat(idx.getEfConstruction()).isEqualTo(200);
    }

    @Test
    @DisplayName("toVectorSearchOptions preserves vector search fields")
    void testToVectorSearchOptions() {
      LanceOptions full =
          LanceOptions.builder()
              .path("/data/ds")
              .vectorColumn("embedding")
              .vectorMetric(LanceOptions.MetricType.DOT)
              .vectorNprobes(40)
              .vectorEf(200)
              .vectorRefineFactor(5)
              .build();

      LanceVectorSearchOptions vs = full.toVectorSearchOptions();

      assertThat(vs.getColumnName()).isEqualTo("embedding");
      assertThat(vs.getMetricType()).isEqualTo(LanceOptions.MetricType.DOT);
      assertThat(vs.getNprobes()).isEqualTo(40);
      assertThat(vs.getEf()).isEqualTo(200);
      assertThat(vs.getRefineFactor()).isEqualTo(5);
    }

    @Test
    @DisplayName("Round-trip: LanceOptions -> sub-options preserve semantics")
    void testRoundTrip() {
      LanceOptions original =
          LanceOptions.builder()
              .path("/data/ds")
              .readBatchSize(512)
              .readLimit(50L)
              .readColumns(Collections.singletonList("id"))
              .readFilter("id > 0")
              .writeBatchSize(256)
              .writeMode(LanceOptions.WriteMode.OVERWRITE)
              .writeMaxRowsPerFile(100)
              .build();

      // Source round-trip
      LanceSourceOptions source = original.toSourceOptions();
      assertThat(source.getPath()).isEqualTo(original.getPath());
      assertThat(source.getBatchSize()).isEqualTo(original.getReadBatchSize());
      assertThat(source.getLimit()).isEqualTo(original.getReadLimit());
      assertThat(source.getColumns()).isEqualTo(original.getReadColumns());
      assertThat(source.getFilter()).isEqualTo(original.getReadFilter());

      // Sink round-trip
      LanceSinkOptions sink = original.toSinkOptions();
      assertThat(sink.getPath()).isEqualTo(original.getPath());
      assertThat(sink.getBatchSize()).isEqualTo(original.getWriteBatchSize());
      assertThat(sink.getWriteMode()).isEqualTo(original.getWriteMode());
      assertThat(sink.getMaxRowsPerFile()).isEqualTo(original.getWriteMaxRowsPerFile());
    }
  }
}
