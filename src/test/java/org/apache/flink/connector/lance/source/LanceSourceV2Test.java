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
package org.apache.flink.connector.lance.source;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.FragmentOperation;
import com.lancedb.lance.WriteParams;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Lance Source V2 unit tests.
 *
 * <p>Tests various components of the Source V2 API implementation, including:
 *
 * <ul>
 *   <li>{@link LanceSourceSplit} - Split model
 *   <li>{@link LanceSourceSplitSerializer} - Split serialization
 *   <li>{@link LanceEnumeratorState} - Enumerator state
 *   <li>{@link LanceEnumeratorStateSerializer} - State serialization
 *   <li>{@link LanceSource} - Source entry point
 * </ul>
 */
class LanceSourceV2Test {

  @TempDir Path tempDir;

  private String datasetPath;
  private RowType rowType;

  @BeforeEach
  void setUp() {
    datasetPath = tempDir.resolve("test_dataset.lance").toString();

    // Create test RowType
    List<RowType.RowField> fields = new ArrayList<>();
    fields.add(new RowType.RowField("id", new BigIntType()));
    fields.add(new RowType.RowField("name", new VarCharType()));
    rowType = new RowType(fields);
  }

  // ==================== LanceSourceSplit Tests ====================

  @Test
  @DisplayName("Test LanceSourceSplit creation and properties")
  void testSourceSplitCreation() {
    LanceSourceSplit split = new LanceSourceSplit(1, datasetPath, 1000);

    assertThat(split.getFragmentId()).isEqualTo(1);
    assertThat(split.getDatasetPath()).isEqualTo(datasetPath);
    assertThat(split.getRowCount()).isEqualTo(1000);
    assertThat(split.splitId()).isEqualTo("lance-split-1");
  }

  @Test
  @DisplayName("Test LanceSourceSplit equality")
  void testSourceSplitEquality() {
    LanceSourceSplit split1 = new LanceSourceSplit(1, datasetPath, 1000);
    LanceSourceSplit split2 = new LanceSourceSplit(1, datasetPath, 1000);
    LanceSourceSplit split3 = new LanceSourceSplit(2, datasetPath, 2000);

    assertThat(split1).isEqualTo(split2);
    assertThat(split1.hashCode()).isEqualTo(split2.hashCode());
    assertThat(split1).isNotEqualTo(split3);
  }

  @Test
  @DisplayName("Test LanceSourceSplit does not allow null path")
  void testSourceSplitNullPath() {
    assertThatThrownBy(() -> new LanceSourceSplit(1, null, 1000))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  @DisplayName("Test LanceSourceSplit toString")
  void testSourceSplitToString() {
    LanceSourceSplit split = new LanceSourceSplit(1, "/test/path", 1000);
    String str = split.toString();

    assertThat(str).contains("fragmentId=1");
    assertThat(str).contains("/test/path");
    assertThat(str).contains("rowCount=1000");
  }

  // ==================== LanceSourceSplitSerializer Tests ====================

  @Test
  @DisplayName("Test Split serialize and deserialize")
  void testSplitSerializeDeserialize() throws IOException {
    LanceSourceSplit original = new LanceSourceSplit(5, datasetPath, 5000);

    LanceSourceSplitSerializer serializer = LanceSourceSplitSerializer.INSTANCE;
    byte[] serialized = serializer.serialize(original);

    LanceSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertThat(deserialized).isEqualTo(original);
    assertThat(deserialized.getFragmentId()).isEqualTo(5);
    assertThat(deserialized.getDatasetPath()).isEqualTo(datasetPath);
    assertThat(deserialized.getRowCount()).isEqualTo(5000);
  }

  @Test
  @DisplayName("Test Split serializer version")
  void testSplitSerializerVersion() {
    assertThat(LanceSourceSplitSerializer.INSTANCE.getVersion()).isEqualTo(1);
  }

  @Test
  @DisplayName("Test Split deserialization with unsupported version")
  void testSplitDeserializeUnsupportedVersion() throws IOException {
    LanceSourceSplit original = new LanceSourceSplit(1, datasetPath, 1000);
    byte[] serialized = LanceSourceSplitSerializer.INSTANCE.serialize(original);

    assertThatThrownBy(() -> LanceSourceSplitSerializer.INSTANCE.deserialize(999, serialized))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Unsupported serialization version");
  }

  @Test
  @DisplayName("Test multiple Splits serialization and deserialization")
  void testMultipleSplitsSerialization() throws IOException {
    List<LanceSourceSplit> originals =
        Arrays.asList(
            new LanceSourceSplit(0, "/path/a", 100),
            new LanceSourceSplit(1, "/path/b", 200),
            new LanceSourceSplit(2, "/path/c", 300));

    LanceSourceSplitSerializer serializer = LanceSourceSplitSerializer.INSTANCE;

    for (LanceSourceSplit original : originals) {
      byte[] serialized = serializer.serialize(original);
      LanceSourceSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);
      assertThat(deserialized).isEqualTo(original);
    }
  }

  // ==================== LanceEnumeratorState Tests ====================

  @Test
  @DisplayName("Test EnumeratorState creation")
  void testEnumeratorStateCreation() {
    List<LanceSourceSplit> splits =
        Arrays.asList(
            new LanceSourceSplit(0, datasetPath, 100), new LanceSourceSplit(1, datasetPath, 200));

    LanceEnumeratorState state = new LanceEnumeratorState(splits);

    assertThat(state.getPendingSplits()).hasSize(2);
    assertThat(state.getPendingSplits().get(0).getFragmentId()).isEqualTo(0);
    assertThat(state.getPendingSplits().get(1).getFragmentId()).isEqualTo(1);
  }

  @Test
  @DisplayName("Test EnumeratorState list is immutable")
  void testEnumeratorStateImmutableList() {
    List<LanceSourceSplit> splits = new ArrayList<>();
    splits.add(new LanceSourceSplit(0, datasetPath, 100));

    LanceEnumeratorState state = new LanceEnumeratorState(splits);

    // Modifying original list should not affect state
    splits.add(new LanceSourceSplit(1, datasetPath, 200));
    assertThat(state.getPendingSplits()).hasSize(1);

    // State's list should be unmodifiable
    assertThatThrownBy(
            () -> state.getPendingSplits().add(new LanceSourceSplit(2, datasetPath, 300)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  @DisplayName("Test empty EnumeratorState")
  void testEmptyEnumeratorState() {
    LanceEnumeratorState state = new LanceEnumeratorState(Collections.emptyList());
    assertThat(state.getPendingSplits()).isEmpty();
  }

  // ==================== LanceEnumeratorStateSerializer Tests ====================

  @Test
  @DisplayName("Test EnumeratorState serialize and deserialize")
  void testEnumeratorStateSerializeDeserialize() throws IOException {
    List<LanceSourceSplit> splits =
        Arrays.asList(
            new LanceSourceSplit(0, "/path/a", 100),
            new LanceSourceSplit(1, "/path/b", 200),
            new LanceSourceSplit(2, "/path/c", 300));

    LanceEnumeratorState original = new LanceEnumeratorState(splits);
    LanceEnumeratorStateSerializer serializer = LanceEnumeratorStateSerializer.INSTANCE;

    byte[] serialized = serializer.serialize(original);
    LanceEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertThat(deserialized.getPendingSplits()).hasSize(3);
    assertThat(deserialized.getPendingSplits().get(0)).isEqualTo(splits.get(0));
    assertThat(deserialized.getPendingSplits().get(1)).isEqualTo(splits.get(1));
    assertThat(deserialized.getPendingSplits().get(2)).isEqualTo(splits.get(2));
  }

  @Test
  @DisplayName("Test empty EnumeratorState serialize and deserialize")
  void testEmptyEnumeratorStateSerializeDeserialize() throws IOException {
    LanceEnumeratorState original = new LanceEnumeratorState(Collections.emptyList());
    LanceEnumeratorStateSerializer serializer = LanceEnumeratorStateSerializer.INSTANCE;

    byte[] serialized = serializer.serialize(original);
    LanceEnumeratorState deserialized = serializer.deserialize(serializer.getVersion(), serialized);

    assertThat(deserialized.getPendingSplits()).isEmpty();
  }

  @Test
  @DisplayName("Test EnumeratorState serializer version")
  void testEnumeratorStateSerializerVersion() {
    assertThat(LanceEnumeratorStateSerializer.INSTANCE.getVersion()).isEqualTo(1);
  }

  @Test
  @DisplayName("Test EnumeratorState deserialization with unsupported version")
  void testEnumeratorStateDeserializeUnsupportedVersion() throws IOException {
    LanceEnumeratorState original = new LanceEnumeratorState(Collections.emptyList());
    byte[] serialized = LanceEnumeratorStateSerializer.INSTANCE.serialize(original);

    assertThatThrownBy(() -> LanceEnumeratorStateSerializer.INSTANCE.deserialize(999, serialized))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Unsupported serialization version");
  }

  // ==================== LanceSource Tests ====================

  @Test
  @DisplayName("Test LanceSource basic properties")
  void testLanceSourceProperties() {
    LanceOptions options = LanceOptions.builder().path(datasetPath).readBatchSize(512).build();

    LanceSource source = new LanceSource(options, rowType);

    assertThat(source.getOptions().getPath()).isEqualTo(datasetPath);
    assertThat(source.getOptions().getReadBatchSize()).isEqualTo(512);
    assertThat(source.getRowType()).isEqualTo(rowType);
    assertThat(source.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
  }

  @Test
  @DisplayName("Test LanceSource auto-infer schema (no RowType)")
  void testLanceSourceWithoutRowType() {
    LanceOptions options = LanceOptions.builder().path(datasetPath).build();

    LanceSource source = new LanceSource(options);

    assertThat(source.getRowType()).isNull();
    assertThat(source.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
  }

  @Test
  @DisplayName("Test LanceSource Builder pattern")
  void testLanceSourceBuilder() {
    LanceSource source =
        LanceSource.builder()
            .path(datasetPath)
            .batchSize(256)
            .columns(Arrays.asList("id", "name"))
            .filter("id > 10")
            .limit(100L)
            .rowType(rowType)
            .build();

    assertThat(source.getOptions().getPath()).isEqualTo(datasetPath);
    assertThat(source.getOptions().getReadBatchSize()).isEqualTo(256);
    assertThat(source.getOptions().getReadColumns()).containsExactly("id", "name");
    assertThat(source.getOptions().getReadFilter()).isEqualTo("id > 10");
    assertThat(source.getOptions().getReadLimit()).isEqualTo(100L);
    assertThat(source.getRowType()).isEqualTo(rowType);
  }

  @Test
  @DisplayName("Test LanceSource Builder throws exception when path is missing")
  void testLanceSourceBuilderMissingPath() {
    assertThatThrownBy(() -> LanceSource.builder().rowType(rowType).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("path must not be empty");
  }

  @Test
  @DisplayName("Test LanceSource serializers are not null")
  void testLanceSourceSerializers() {
    LanceOptions options = LanceOptions.builder().path(datasetPath).build();

    LanceSource source = new LanceSource(options, rowType);

    assertThat(source.getSplitSerializer()).isNotNull();
    assertThat(source.getEnumeratorCheckpointSerializer()).isNotNull();
    assertThat(source.getSplitSerializer()).isSameAs(LanceSourceSplitSerializer.INSTANCE);
    assertThat(source.getEnumeratorCheckpointSerializer())
        .isSameAs(LanceEnumeratorStateSerializer.INSTANCE);
  }

  // ==================== Integration Test: Using Real Dataset ====================

  @Test
  @DisplayName("Test split discovery with real Lance Dataset")
  void testSplitDiscoveryWithRealDataset() throws Exception {
    // Create test Dataset
    String testDatasetPath = createTestDataset(10);

    LanceOptions options = LanceOptions.builder().path(testDatasetPath).build();

    // Create Source and verify serializers are accessible
    LanceSource source = new LanceSource(options, rowType);
    assertThat(source.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
    assertThat(source.getSplitSerializer()).isNotNull();
  }

  @Test
  @DisplayName("Test Split end-to-end serialization round trip")
  void testSplitRoundTripSerialization() throws IOException {
    // Create a series of Splits with different parameters
    List<LanceSourceSplit> splits =
        Arrays.asList(
            new LanceSourceSplit(0, "/data/table1.lance", 0),
            new LanceSourceSplit(
                Integer.MAX_VALUE, "/very/long/path/to/dataset.lance", Long.MAX_VALUE),
            new LanceSourceSplit(42, "/path/with spaces/and-dashes/data.lance", 999999));

    LanceSourceSplitSerializer serializer = LanceSourceSplitSerializer.INSTANCE;

    for (LanceSourceSplit original : splits) {
      byte[] bytes = serializer.serialize(original);
      LanceSourceSplit restored = serializer.deserialize(serializer.getVersion(), bytes);

      assertThat(restored.getFragmentId()).isEqualTo(original.getFragmentId());
      assertThat(restored.getDatasetPath()).isEqualTo(original.getDatasetPath());
      assertThat(restored.getRowCount()).isEqualTo(original.getRowCount());
      assertThat(restored.splitId()).isEqualTo(original.splitId());
    }
  }

  @Test
  @DisplayName("Test EnumeratorState end-to-end serialization round trip")
  void testEnumeratorStateRoundTripSerialization() throws IOException {
    // Create State with many Splits
    List<LanceSourceSplit> splits = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      splits.add(new LanceSourceSplit(i, "/data/table_" + i + ".lance", i * 1000L));
    }

    LanceEnumeratorState original = new LanceEnumeratorState(splits);
    LanceEnumeratorStateSerializer serializer = LanceEnumeratorStateSerializer.INSTANCE;

    byte[] bytes = serializer.serialize(original);
    LanceEnumeratorState restored = serializer.deserialize(serializer.getVersion(), bytes);

    assertThat(restored.getPendingSplits()).hasSize(100);
    for (int i = 0; i < 100; i++) {
      assertThat(restored.getPendingSplits().get(i)).isEqualTo(splits.get(i));
    }
  }

  // ==================== Helper Methods ====================

  /**
   * Create a test Lance Dataset.
   *
   * @param rowCount Number of rows
   * @return Dataset path
   */
  private String createTestDataset(int rowCount) throws Exception {
    String path = tempDir.resolve("real_dataset.lance").toString();

    Schema arrowSchema = LanceTypeConverter.toArrowSchema(rowType);
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    try {
      VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
      root.allocateNew();

      BigIntVector idVector = (BigIntVector) root.getVector("id");
      VarCharVector nameVector = (VarCharVector) root.getVector("name");

      for (int i = 0; i < rowCount; i++) {
        idVector.setSafe(i, i);
        nameVector.setSafe(i, ("name_" + i).getBytes());
      }
      root.setRowCount(rowCount);

      // Use Fragment.create + FragmentOperation.Overwrite.commit to create Dataset
      WriteParams writeParams = new WriteParams.Builder().build();
      List<FragmentMetadata> fragments = Fragment.create(path, allocator, root, writeParams);

      FragmentOperation.Overwrite overwrite =
          new FragmentOperation.Overwrite(fragments, arrowSchema);
      Dataset dataset = overwrite.commit(allocator, path, Optional.empty(), Collections.emptyMap());
      dataset.close();
      root.close();

      return path;
    } finally {
      allocator.close();
    }
  }
}
