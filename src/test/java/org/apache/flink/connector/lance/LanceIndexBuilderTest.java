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
package org.apache.flink.connector.lance;

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.config.LanceOptions.IndexType;
import org.apache.flink.connector.lance.config.LanceOptions.MetricType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** LanceIndexBuilder unit tests. */
class LanceIndexBuilderTest {

  @TempDir Path tempDir;

  private String datasetPath;

  @BeforeEach
  void setUp() {
    datasetPath = tempDir.resolve("test_index_dataset").toString();
  }

  @Test
  @DisplayName("Test IVF_PQ index configuration build")
  void testIvfPqIndexConfiguration() {
    LanceIndexBuilder builder =
        LanceIndexBuilder.builder()
            .datasetPath(datasetPath)
            .columnName("embedding")
            .indexType(IndexType.IVF_PQ)
            .numPartitions(128)
            .numSubVectors(16)
            .numBits(8)
            .metricType(MetricType.L2)
            .build();

    // Verify configuration - by successful build
    assertThat(builder).isNotNull();
  }

  @Test
  @DisplayName("Test IVF_HNSW index configuration build")
  void testIvfHnswIndexConfiguration() {
    LanceIndexBuilder builder =
        LanceIndexBuilder.builder()
            .datasetPath(datasetPath)
            .columnName("embedding")
            .indexType(IndexType.IVF_HNSW)
            .numPartitions(64)
            .maxLevel(5)
            .m(24)
            .efConstruction(200)
            .metricType(MetricType.COSINE)
            .build();

    assertThat(builder).isNotNull();
  }

  @Test
  @DisplayName("Test IVF_FLAT index configuration build")
  void testIvfFlatIndexConfiguration() {
    LanceIndexBuilder builder =
        LanceIndexBuilder.builder()
            .datasetPath(datasetPath)
            .columnName("embedding")
            .indexType(IndexType.IVF_FLAT)
            .numPartitions(256)
            .metricType(MetricType.DOT)
            .build();

    assertThat(builder).isNotNull();
  }

  @Test
  @DisplayName("Test index type enum")
  void testIndexTypeEnum() {
    assertThat(IndexType.fromValue("IVF_PQ")).isEqualTo(IndexType.IVF_PQ);
    assertThat(IndexType.fromValue("ivf_pq")).isEqualTo(IndexType.IVF_PQ);
    assertThat(IndexType.fromValue("IVF_HNSW")).isEqualTo(IndexType.IVF_HNSW);
    assertThat(IndexType.fromValue("IVF_FLAT")).isEqualTo(IndexType.IVF_FLAT);

    assertThat(IndexType.IVF_PQ.getValue()).isEqualTo("IVF_PQ");
    assertThat(IndexType.IVF_HNSW.getValue()).isEqualTo("IVF_HNSW");
    assertThat(IndexType.IVF_FLAT.getValue()).isEqualTo("IVF_FLAT");
  }

  @Test
  @DisplayName("Test invalid index type")
  void testInvalidIndexType() {
    assertThatThrownBy(() -> IndexType.fromValue("INVALID"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported index type");
  }

  @Test
  @DisplayName("Test metric type enum")
  void testMetricTypeEnum() {
    assertThat(MetricType.fromValue("L2")).isEqualTo(MetricType.L2);
    assertThat(MetricType.fromValue("l2")).isEqualTo(MetricType.L2);
    assertThat(MetricType.fromValue("Cosine")).isEqualTo(MetricType.COSINE);
    assertThat(MetricType.fromValue("cosine")).isEqualTo(MetricType.COSINE);
    assertThat(MetricType.fromValue("Dot")).isEqualTo(MetricType.DOT);
    assertThat(MetricType.fromValue("dot")).isEqualTo(MetricType.DOT);

    assertThat(MetricType.L2.getValue()).isEqualTo("L2");
    assertThat(MetricType.COSINE.getValue()).isEqualTo("Cosine");
    assertThat(MetricType.DOT.getValue()).isEqualTo("Dot");
  }

  @Test
  @DisplayName("Test invalid metric type")
  void testInvalidMetricType() {
    assertThatThrownBy(() -> MetricType.fromValue("INVALID"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported metric type");
  }

  @Test
  @DisplayName("Test exception when missing dataset path")
  void testMissingDatasetPath() {
    assertThatThrownBy(
            () ->
                LanceIndexBuilder.builder()
                    .columnName("embedding")
                    .indexType(IndexType.IVF_PQ)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Dataset path cannot be empty");
  }

  @Test
  @DisplayName("Test exception when missing column name")
  void testMissingColumnName() {
    assertThatThrownBy(
            () ->
                LanceIndexBuilder.builder()
                    .datasetPath(datasetPath)
                    .indexType(IndexType.IVF_PQ)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Column name cannot be empty");
  }

  @Test
  @DisplayName("Test invalid number of partitions")
  void testInvalidNumPartitions() {
    assertThatThrownBy(
            () ->
                LanceIndexBuilder.builder()
                    .datasetPath(datasetPath)
                    .columnName("embedding")
                    .numPartitions(0)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Number of partitions must be greater than 0");
  }

  @Test
  @DisplayName("Test invalid number of sub-vectors")
  void testInvalidNumSubVectors() {
    assertThatThrownBy(
            () ->
                LanceIndexBuilder.builder()
                    .datasetPath(datasetPath)
                    .columnName("embedding")
                    .numSubVectors(-1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Number of sub-vectors must be greater than 0");
  }

  @Test
  @DisplayName("Test invalid number of quantization bits")
  void testInvalidNumBits() {
    assertThatThrownBy(
            () ->
                LanceIndexBuilder.builder()
                    .datasetPath(datasetPath)
                    .columnName("embedding")
                    .numBits(0)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Quantization bits must be between 1 and 16");

    assertThatThrownBy(
            () ->
                LanceIndexBuilder.builder()
                    .datasetPath(datasetPath)
                    .columnName("embedding")
                    .numBits(17)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Quantization bits must be between 1 and 16");
  }

  @Test
  @DisplayName("Test default index configuration values")
  void testDefaultIndexConfiguration() {
    LanceOptions options =
        LanceOptions.builder().path(datasetPath).indexColumn("embedding").build();

    // Verify default values
    assertThat(options.getIndexType()).isEqualTo(IndexType.IVF_PQ);
    assertThat(options.getIndexNumPartitions()).isEqualTo(256);
    assertThat(options.getIndexNumBits()).isEqualTo(8);
    assertThat(options.getIndexMaxLevel()).isEqualTo(7);
    assertThat(options.getIndexM()).isEqualTo(16);
    assertThat(options.getIndexEfConstruction()).isEqualTo(100);
  }

  @Test
  @DisplayName("Test creating index builder from LanceOptions")
  void testFromOptions() {
    LanceOptions options =
        LanceOptions.builder()
            .path(datasetPath)
            .indexColumn("embedding")
            .indexType(IndexType.IVF_HNSW)
            .indexNumPartitions(64)
            .vectorMetric(MetricType.COSINE)
            .build();

    LanceIndexBuilder builder = LanceIndexBuilder.fromOptions(options);

    assertThat(builder).isNotNull();
  }

  @Test
  @DisplayName("Test index build result")
  void testIndexBuildResult() {
    LanceIndexBuilder.IndexBuildResult result =
        new LanceIndexBuilder.IndexBuildResult(
            true, IndexType.IVF_PQ, "embedding", datasetPath, 1000, null);

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.getIndexType()).isEqualTo(IndexType.IVF_PQ);
    assertThat(result.getColumnName()).isEqualTo("embedding");
    assertThat(result.getDatasetPath()).isEqualTo(datasetPath);
    assertThat(result.getDurationMillis()).isEqualTo(1000);
    assertThat(result.getErrorMessage()).isNull();
  }

  @Test
  @DisplayName("Test index build failure result")
  void testIndexBuildFailureResult() {
    LanceIndexBuilder.IndexBuildResult result =
        new LanceIndexBuilder.IndexBuildResult(
            false, IndexType.IVF_PQ, "embedding", datasetPath, 500, "Column does not exist");

    assertThat(result.isSuccess()).isFalse();
    assertThat(result.getErrorMessage()).isEqualTo("Column does not exist");
  }

  @Test
  @DisplayName("Test replace index option")
  void testReplaceIndexOption() {
    LanceIndexBuilder builder =
        LanceIndexBuilder.builder()
            .datasetPath(datasetPath)
            .columnName("embedding")
            .indexType(IndexType.IVF_PQ)
            .replace(true)
            .build();

    assertThat(builder).isNotNull();
  }
}
