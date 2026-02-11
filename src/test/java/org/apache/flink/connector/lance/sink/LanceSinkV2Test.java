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
package org.apache.flink.connector.lance.sink;

import com.lancedb.lance.Dataset;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Lance Sink V2 unit tests.
 *
 * <p>Tests various components of the Sink V2 API implementation, including:
 *
 * <ul>
 *   <li>{@link LanceSink} - Sink entry point
 *   <li>{@link LanceSinkWriter} - Data writer
 *   <li>Write verification - Validate data integrity by reading back from Dataset
 * </ul>
 */
class LanceSinkV2Test {

  @TempDir Path tempDir;

  private RowType rowType;

  @BeforeEach
  void setUp() {
    List<RowType.RowField> fields = new ArrayList<>();
    fields.add(new RowType.RowField("id", new BigIntType()));
    fields.add(new RowType.RowField("name", new VarCharType()));
    rowType = new RowType(fields);
  }

  // ==================== LanceSink Tests ====================

  @Test
  @DisplayName("Test LanceSink basic properties")
  void testLanceSinkProperties() {
    String datasetPath = tempDir.resolve("test_dataset.lance").toString();
    LanceOptions options =
        LanceOptions.builder()
            .path(datasetPath)
            .writeBatchSize(512)
            .writeMode(LanceOptions.WriteMode.APPEND)
            .writeMaxRowsPerFile(500000)
            .build();

    LanceSink sink = new LanceSink(options, rowType);

    assertThat(sink.getOptions().getPath()).isEqualTo(datasetPath);
    assertThat(sink.getOptions().getWriteBatchSize()).isEqualTo(512);
    assertThat(sink.getOptions().getWriteMode()).isEqualTo(LanceOptions.WriteMode.APPEND);
    assertThat(sink.getOptions().getWriteMaxRowsPerFile()).isEqualTo(500000);
    assertThat(sink.getRowType()).isEqualTo(rowType);
  }

  @Test
  @DisplayName("Test LanceSink Builder pattern")
  void testLanceSinkBuilder() {
    String datasetPath = tempDir.resolve("test_dataset.lance").toString();
    LanceSink sink =
        LanceSink.builder()
            .path(datasetPath)
            .batchSize(256)
            .writeMode(LanceOptions.WriteMode.OVERWRITE)
            .maxRowsPerFile(100000)
            .rowType(rowType)
            .build();

    assertThat(sink.getOptions().getPath()).isEqualTo(datasetPath);
    assertThat(sink.getOptions().getWriteBatchSize()).isEqualTo(256);
    assertThat(sink.getOptions().getWriteMode()).isEqualTo(LanceOptions.WriteMode.OVERWRITE);
    assertThat(sink.getOptions().getWriteMaxRowsPerFile()).isEqualTo(100000);
  }

  @Test
  @DisplayName("Test LanceSink Builder throws exception when path is missing")
  void testLanceSinkBuilderMissingPath() {
    assertThatThrownBy(() -> LanceSink.builder().rowType(rowType).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("path must not be empty");
  }

  @Test
  @DisplayName("Test LanceSink Builder throws exception when RowType is missing")
  void testLanceSinkBuilderMissingRowType() {
    assertThatThrownBy(
            () -> LanceSink.builder().path(tempDir.resolve("test.lance").toString()).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("RowType");
  }

  @Test
  @DisplayName("Test LanceSink createWriter")
  void testLanceSinkCreateWriter() throws IOException {
    String datasetPath = tempDir.resolve("test_writer.lance").toString();
    LanceOptions options = LanceOptions.builder().path(datasetPath).build();

    LanceSink sink = new LanceSink(options, rowType);

    // createWriter should not throw exceptions
    SinkWriter<RowData> writer = sink.createWriter(null);
    assertThat(writer).isNotNull();
    assertThat(writer).isInstanceOf(LanceSinkWriter.class);

    // Close writer
    try {
      writer.close();
    } catch (Exception e) {
      // ignore
    }
  }

  // ==================== LanceSinkWriter Write Tests ====================

  @Test
  @DisplayName("Test writing a single row and verification")
  void testWriteSingleRow() throws Exception {
    String datasetPath = tempDir.resolve("single_row.lance").toString();
    LanceOptions options = LanceOptions.builder().path(datasetPath).writeBatchSize(10).build();

    LanceSinkWriter writer = new LanceSinkWriter(options, rowType);

    // Write one row
    GenericRowData row = new GenericRowData(2);
    row.setField(0, 1L);
    row.setField(1, StringData.fromString("hello"));
    writer.write(row, null);

    // Flush and close
    writer.flush(true);
    writer.close();

    assertThat(writer.getTotalWrittenRows()).isEqualTo(1);

    // Verify written data
    verifyDataset(datasetPath, 1);
  }

  @Test
  @DisplayName("Test writing multiple rows and verification")
  void testWriteMultipleRows() throws Exception {
    String datasetPath = tempDir.resolve("multi_rows.lance").toString();
    LanceOptions options = LanceOptions.builder().path(datasetPath).writeBatchSize(100).build();

    LanceSinkWriter writer = new LanceSinkWriter(options, rowType);

    // Write 50 rows
    for (int i = 0; i < 50; i++) {
      GenericRowData row = new GenericRowData(2);
      row.setField(0, (long) i);
      row.setField(1, StringData.fromString("name_" + i));
      writer.write(row, null);
    }

    // Flush and close
    writer.flush(true);
    writer.close();

    assertThat(writer.getTotalWrittenRows()).isEqualTo(50);

    // Verify written data
    verifyDataset(datasetPath, 50);
  }

  @Test
  @DisplayName("Test auto flush on batch size")
  void testAutoFlushOnBatchSize() throws Exception {
    String datasetPath = tempDir.resolve("auto_flush.lance").toString();
    int batchSize = 10;
    LanceOptions options =
        LanceOptions.builder().path(datasetPath).writeBatchSize(batchSize).build();

    LanceSinkWriter writer = new LanceSinkWriter(options, rowType);

    // Write 25 rows (triggers 2 auto flushes + 1 final flush)
    for (int i = 0; i < 25; i++) {
      GenericRowData row = new GenericRowData(2);
      row.setField(0, (long) i);
      row.setField(1, StringData.fromString("auto_" + i));
      writer.write(row, null);
    }

    writer.flush(true);
    writer.close();

    assertThat(writer.getTotalWrittenRows()).isEqualTo(25);
    verifyDataset(datasetPath, 25);
  }

  @Test
  @DisplayName("Test empty flush does not throw errors")
  void testEmptyFlush() throws Exception {
    String datasetPath = tempDir.resolve("empty_flush.lance").toString();
    LanceOptions options = LanceOptions.builder().path(datasetPath).build();

    LanceSinkWriter writer = new LanceSinkWriter(options, rowType);

    // Flush without writing any data
    writer.flush(false);
    writer.flush(true);
    writer.close();

    assertThat(writer.getTotalWrittenRows()).isEqualTo(0);
  }

  @Test
  @DisplayName("Test overwrite mode")
  void testOverwriteMode() throws Exception {
    String datasetPath = tempDir.resolve("overwrite.lance").toString();

    // First write: 10 rows
    LanceOptions options1 =
        LanceOptions.builder()
            .path(datasetPath)
            .writeBatchSize(100)
            .writeMode(LanceOptions.WriteMode.APPEND)
            .build();

    LanceSinkWriter writer1 = new LanceSinkWriter(options1, rowType);
    for (int i = 0; i < 10; i++) {
      GenericRowData row = new GenericRowData(2);
      row.setField(0, (long) i);
      row.setField(1, StringData.fromString("first_" + i));
      writer1.write(row, null);
    }
    writer1.flush(true);
    writer1.close();

    verifyDataset(datasetPath, 10);

    // Second write: 5 rows in overwrite mode
    LanceOptions options2 =
        LanceOptions.builder()
            .path(datasetPath)
            .writeBatchSize(100)
            .writeMode(LanceOptions.WriteMode.OVERWRITE)
            .build();

    LanceSinkWriter writer2 = new LanceSinkWriter(options2, rowType);
    for (int i = 0; i < 5; i++) {
      GenericRowData row = new GenericRowData(2);
      row.setField(0, (long) (100 + i));
      row.setField(1, StringData.fromString("second_" + i));
      writer2.write(row, null);
    }
    writer2.flush(true);
    writer2.close();

    // Overwrite mode should have only 5 rows
    verifyDataset(datasetPath, 5);
  }

  @Test
  @DisplayName("Test append mode")
  void testAppendMode() throws Exception {
    String datasetPath = tempDir.resolve("append.lance").toString();

    // First write: 10 rows
    LanceOptions options1 =
        LanceOptions.builder()
            .path(datasetPath)
            .writeBatchSize(100)
            .writeMode(LanceOptions.WriteMode.APPEND)
            .build();

    LanceSinkWriter writer1 = new LanceSinkWriter(options1, rowType);
    for (int i = 0; i < 10; i++) {
      GenericRowData row = new GenericRowData(2);
      row.setField(0, (long) i);
      row.setField(1, StringData.fromString("first_" + i));
      writer1.write(row, null);
    }
    writer1.flush(true);
    writer1.close();

    verifyDataset(datasetPath, 10);

    // Second write: append 5 rows
    LanceOptions options2 =
        LanceOptions.builder()
            .path(datasetPath)
            .writeBatchSize(100)
            .writeMode(LanceOptions.WriteMode.APPEND)
            .build();

    LanceSinkWriter writer2 = new LanceSinkWriter(options2, rowType);
    for (int i = 0; i < 5; i++) {
      GenericRowData row = new GenericRowData(2);
      row.setField(0, (long) (100 + i));
      row.setField(1, StringData.fromString("second_" + i));
      writer2.write(row, null);
    }
    writer2.flush(true);
    writer2.close();

    // Append mode should have 15 rows
    verifyDataset(datasetPath, 15);
  }

  @Test
  @DisplayName("Test write and read content correctness")
  void testWriteAndReadContent() throws Exception {
    String datasetPath = tempDir.resolve("content_verify.lance").toString();
    LanceOptions options = LanceOptions.builder().path(datasetPath).writeBatchSize(100).build();

    LanceSinkWriter writer = new LanceSinkWriter(options, rowType);

    // Write 3 rows
    for (int i = 0; i < 3; i++) {
      GenericRowData row = new GenericRowData(2);
      row.setField(0, (long) (i + 1));
      row.setField(1, StringData.fromString("item_" + (i + 1)));
      writer.write(row, null);
    }
    writer.flush(true);
    writer.close();

    // Read and verify data content
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    try {
      Dataset dataset = Dataset.open(datasetPath, allocator);
      try {
        assertThat(dataset.countRows()).isEqualTo(3);

        // Read all data through Scanner
        ArrowReader reader = dataset.newScan().scanBatches();
        RowDataConverter converter = new RowDataConverter(rowType);
        List<RowData> allRows = new ArrayList<>();

        while (reader.loadNextBatch()) {
          VectorSchemaRoot root = reader.getVectorSchemaRoot();
          allRows.addAll(converter.toRowDataList(root));
        }
        reader.close();

        assertThat(allRows).hasSize(3);

        // Verify content
        for (int i = 0; i < 3; i++) {
          RowData row = allRows.get(i);
          assertThat(row.getLong(0)).isEqualTo(i + 1);
          assertThat(row.getString(1).toString()).isEqualTo("item_" + (i + 1));
        }
      } finally {
        dataset.close();
      }
    } finally {
      allocator.close();
    }
  }

  @Test
  @DisplayName("Test checkpoint flush")
  void testCheckpointFlush() throws Exception {
    String datasetPath = tempDir.resolve("checkpoint.lance").toString();
    LanceOptions options =
        LanceOptions.builder()
            .path(datasetPath)
            .writeBatchSize(1000) // Set a large batch to ensure no auto flush
            .build();

    LanceSinkWriter writer = new LanceSinkWriter(options, rowType);

    // Write 5 rows
    for (int i = 0; i < 5; i++) {
      GenericRowData row = new GenericRowData(2);
      row.setField(0, (long) i);
      row.setField(1, StringData.fromString("cp_" + i));
      writer.write(row, null);
    }

    // Simulate checkpoint flush (endOfInput = false)
    writer.flush(false);

    assertThat(writer.getTotalWrittenRows()).isEqualTo(5);

    // Write 3 more rows
    for (int i = 5; i < 8; i++) {
      GenericRowData row = new GenericRowData(2);
      row.setField(0, (long) i);
      row.setField(1, StringData.fromString("cp_" + i));
      writer.write(row, null);
    }

    // Final flush (endOfInput = true)
    writer.flush(true);
    writer.close();

    assertThat(writer.getTotalWrittenRows()).isEqualTo(8);
    verifyDataset(datasetPath, 8);
  }

  // ==================== Helper Methods ====================

  /** Verify the row count of a Dataset. */
  private void verifyDataset(String datasetPath, long expectedRowCount) throws Exception {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    try {
      Dataset dataset = Dataset.open(datasetPath, allocator);
      try {
        long actualRowCount = dataset.countRows();
        assertThat(actualRowCount).isEqualTo(expectedRowCount);
      } finally {
        dataset.close();
      }
    } finally {
      allocator.close();
    }
  }
}
