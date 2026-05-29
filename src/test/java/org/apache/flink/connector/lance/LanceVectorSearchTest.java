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
import org.apache.flink.connector.lance.config.LanceOptions.MetricType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** LanceVectorSearch unit tests. */
class LanceVectorSearchTest {

  @TempDir Path tempDir;

  private String datasetPath;

  @BeforeEach
  void setUp() {
    datasetPath = tempDir.resolve("test_search_dataset").toString();
  }

  @Test
  @DisplayName("Test vector search configuration build")
  void testVectorSearchConfiguration() {
    LanceVectorSearch search =
        LanceVectorSearch.builder()
            .datasetPath(datasetPath)
            .columnName("embedding")
            .metricType(MetricType.L2)
            .nprobes(20)
            .ef(100)
            .refineFactor(10)
            .build();

    assertThat(search).isNotNull();
  }

  @Test
  @DisplayName("Test different metric types")
  void testDifferentMetricTypes() {
    // L2 distance
    LanceVectorSearch l2Search =
        LanceVectorSearch.builder()
            .datasetPath(datasetPath)
            .columnName("embedding")
            .metricType(MetricType.L2)
            .build();
    assertThat(l2Search).isNotNull();

    // Cosine similarity
    LanceVectorSearch cosineSearch =
        LanceVectorSearch.builder()
            .datasetPath(datasetPath)
            .columnName("embedding")
            .metricType(MetricType.COSINE)
            .build();
    assertThat(cosineSearch).isNotNull();

    // Dot product
    LanceVectorSearch dotSearch =
        LanceVectorSearch.builder()
            .datasetPath(datasetPath)
            .columnName("embedding")
            .metricType(MetricType.DOT)
            .build();
    assertThat(dotSearch).isNotNull();
  }

  @Test
  @DisplayName("Test exception when missing dataset path")
  void testMissingDatasetPath() {
    assertThatThrownBy(
            () ->
                LanceVectorSearch.builder()
                    .columnName("embedding")
                    .metricType(MetricType.L2)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Dataset path cannot be empty");
  }

  @Test
  @DisplayName("Test exception when missing column name")
  void testMissingColumnName() {
    assertThatThrownBy(
            () ->
                LanceVectorSearch.builder()
                    .datasetPath(datasetPath)
                    .metricType(MetricType.L2)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Column name cannot be empty");
  }

  @Test
  @DisplayName("Test invalid nprobes value")
  void testInvalidNprobes() {
    assertThatThrownBy(
            () ->
                LanceVectorSearch.builder()
                    .datasetPath(datasetPath)
                    .columnName("embedding")
                    .nprobes(0)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nprobes must be greater than 0");
  }

  @Test
  @DisplayName("Test default vector search configuration values")
  void testDefaultVectorSearchConfiguration() {
    LanceOptions options =
        LanceOptions.builder().path(datasetPath).vectorColumn("embedding").build();

    // Verify default values
    assertThat(options.getVectorMetric()).isEqualTo(MetricType.L2);
    assertThat(options.getVectorNprobes()).isEqualTo(20);
    assertThat(options.getVectorEf()).isEqualTo(100);
    assertThat(options.getVectorRefineFactor()).isNull();
  }

  @Test
  @DisplayName("Test creating vector searcher from LanceOptions")
  void testFromOptions() {
    LanceOptions options =
        LanceOptions.builder()
            .path(datasetPath)
            .vectorColumn("embedding")
            .vectorMetric(MetricType.COSINE)
            .vectorNprobes(30)
            .vectorEf(150)
            .vectorRefineFactor(5)
            .build();

    LanceVectorSearch search = LanceVectorSearch.fromOptions(options);

    assertThat(search).isNotNull();
  }

  @Test
  @DisplayName("Test search result")
  void testSearchResult() {
    // Create mock RowData
    LanceVectorSearch.SearchResult result = new LanceVectorSearch.SearchResult(null, 0.5);

    assertThat(result.getDistance()).isEqualTo(0.5);
    assertThat(result.getSimilarity()).isGreaterThan(0);
    assertThat(result.getSimilarity()).isLessThanOrEqualTo(1.0);
  }

  @Test
  @DisplayName("Test search result similarity calculation")
  void testSearchResultSimilarity() {
    // Distance 0 should have similarity 1.0
    LanceVectorSearch.SearchResult perfectMatch = new LanceVectorSearch.SearchResult(null, 0.0);
    assertThat(perfectMatch.getSimilarity()).isEqualTo(1.0);

    // Distance 1 should have similarity 0.5
    LanceVectorSearch.SearchResult halfMatch = new LanceVectorSearch.SearchResult(null, 1.0);
    assertThat(halfMatch.getSimilarity()).isEqualTo(0.5);

    // Greater distance means lower similarity
    LanceVectorSearch.SearchResult farResult = new LanceVectorSearch.SearchResult(null, 10.0);
    assertThat(farResult.getSimilarity()).isLessThan(0.5);
  }

  @Test
  @DisplayName("Test search result equality")
  void testSearchResultEquality() {
    LanceVectorSearch.SearchResult result1 = new LanceVectorSearch.SearchResult(null, 0.5);
    LanceVectorSearch.SearchResult result2 = new LanceVectorSearch.SearchResult(null, 0.5);
    LanceVectorSearch.SearchResult result3 = new LanceVectorSearch.SearchResult(null, 1.0);

    assertThat(result1).isEqualTo(result2);
    assertThat(result1.hashCode()).isEqualTo(result2.hashCode());
    assertThat(result1).isNotEqualTo(result3);
  }

  @Test
  @DisplayName("Test configuration validation - invalid vector search params")
  void testInvalidVectorSearchParams() {
    assertThatThrownBy(
            () ->
                LanceOptions.builder()
                    .path(datasetPath)
                    .vectorColumn("embedding")
                    .vectorNprobes(0)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nprobes");

    assertThatThrownBy(
            () ->
                LanceOptions.builder()
                    .path(datasetPath)
                    .vectorColumn("embedding")
                    .vectorEf(0)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ef");

    assertThatThrownBy(
            () ->
                LanceOptions.builder()
                    .path(datasetPath)
                    .vectorColumn("embedding")
                    .vectorRefineFactor(0)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("refine-factor");
  }

  @Test
  @DisplayName("Test metric type values")
  void testMetricTypeValues() {
    assertThat(MetricType.L2.getValue()).isEqualTo("L2");
    assertThat(MetricType.COSINE.getValue()).isEqualTo("Cosine");
    assertThat(MetricType.DOT.getValue()).isEqualTo("Dot");
  }

  @Test
  @DisplayName("Test search result toString")
  void testSearchResultToString() {
    LanceVectorSearch.SearchResult result = new LanceVectorSearch.SearchResult(null, 0.5);
    String str = result.toString();

    assertThat(str).contains("SearchResult");
    assertThat(str).contains("distance=0.5");
  }
}
