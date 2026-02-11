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
import com.lancedb.lance.Fragment;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.FragmentOperation;
import com.lancedb.lance.WriteParams;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.lance.config.LanceDatasetFactory;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Data writer for Lance Sink V2.
 *
 * <p>Receives Flink {@link RowData}, buffers them and writes to Lance Dataset when the batch size
 * is reached.
 *
 * <p>Main responsibilities:
 *
 * <ul>
 *   <li>Receive data and buffer
 *   <li>Auto flush when batch size is reached
 *   <li>Convert RowData to Arrow VectorSchemaRoot
 *   <li>Write to Lance Dataset via Fragment.create + FragmentOperation
 *   <li>Support APPEND and OVERWRITE write modes
 * </ul>
 */
public class LanceSinkWriter implements SinkWriter<RowData> {

  private static final Logger LOG = LoggerFactory.getLogger(LanceSinkWriter.class);

  private final LanceOptions options;
  private final RowType rowType;

  private transient BufferAllocator allocator;
  private transient RowDataConverter converter;
  private transient Schema arrowSchema;
  private transient List<RowData> buffer;
  private transient long totalWrittenRows;
  private transient boolean datasetExists;
  private transient boolean isFirstWrite;

  /**
   * Create a LanceSinkWriter.
   *
   * @param options Lance configuration options
   * @param rowType Flink RowType
   */
  public LanceSinkWriter(LanceOptions options, RowType rowType) {
    this.options = options;
    this.rowType = rowType;

    initialize();
  }

  /** Initialize writer resources. */
  private void initialize() {
    LOG.info("Initializing LanceSinkWriter: {}", options.getPath());

    this.allocator = new RootAllocator(Long.MAX_VALUE);
    this.buffer = new ArrayList<>(options.getWriteBatchSize());
    this.totalWrittenRows = 0;
    this.isFirstWrite = true;

    // Initialize converter and schema
    this.converter = new RowDataConverter(rowType);
    this.arrowSchema = LanceTypeConverter.toArrowSchema(rowType);

    // Check dataset path
    String datasetPath = options.getPath();
    if (datasetPath == null || datasetPath.isEmpty()) {
      throw new IllegalArgumentException("Lance dataset path must not be empty");
    }

    // Determine if dataset already exists (supports both local and remote paths)
    this.datasetExists = checkDatasetExists(datasetPath);

    // If overwrite mode and dataset already exists, handle accordingly
    if (datasetExists && options.getWriteMode() == LanceOptions.WriteMode.OVERWRITE) {
      if (!isRemotePath(datasetPath)) {
        LOG.info("Overwrite mode, deleting existing local dataset: {}", datasetPath);
        try {
          deleteDirectory(Paths.get(datasetPath));
        } catch (IOException e) {
          throw new RuntimeException("Failed to delete existing dataset: " + datasetPath, e);
        }
      } else {
        LOG.info(
            "Overwrite mode for remote dataset: {} (will overwrite on first write)", datasetPath);
      }
      this.datasetExists = false;
    }

    LOG.info("LanceSinkWriter initialized, schema: {}", rowType);
  }

  @Override
  public void write(RowData element, Context context) throws IOException, InterruptedException {
    buffer.add(element);

    // Flush when buffer reaches batch size
    if (buffer.size() >= options.getWriteBatchSize()) {
      doFlush();
    }
  }

  @Override
  public void flush(boolean endOfInput) throws IOException, InterruptedException {
    // Flush all buffered data on checkpoint or end of input
    doFlush();

    if (endOfInput) {
      LOG.info("End of input, total rows written: {}", totalWrittenRows);
    }
  }

  /** Perform the actual flush operation, writing buffered data to Lance Dataset. */
  private void doFlush() throws IOException {
    if (buffer.isEmpty()) {
      return;
    }

    LOG.debug("Flushing buffer, row count: {}", buffer.size());

    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
      // Convert RowData to VectorSchemaRoot
      converter.toVectorSchemaRoot(buffer, root);

      String datasetPath = options.getPath();

      // Build write params
      WriteParams writeParams =
          new WriteParams.Builder().withMaxRowsPerFile(options.getWriteMaxRowsPerFile()).build();

      // Create fragments
      List<FragmentMetadata> fragments = Fragment.create(datasetPath, allocator, root, writeParams);

      Dataset dataset = null;
      try {
        if (!datasetExists) {
          // Create new dataset
          FragmentOperation.Overwrite overwrite =
              new FragmentOperation.Overwrite(fragments, arrowSchema);
          dataset =
              overwrite.commit(allocator, datasetPath, Optional.empty(), Collections.emptyMap());
          datasetExists = true;
          isFirstWrite = false;
          LOG.info("Created new dataset: {}", datasetPath);
        } else {
          if (isFirstWrite && options.getWriteMode() == LanceOptions.WriteMode.OVERWRITE) {
            // First write in overwrite mode
            FragmentOperation.Overwrite overwrite =
                new FragmentOperation.Overwrite(fragments, arrowSchema);
            dataset =
                overwrite.commit(allocator, datasetPath, Optional.empty(), Collections.emptyMap());
            isFirstWrite = false;
          } else {
            // Append mode: need to get the current dataset version
            Dataset existingDataset = LanceDatasetFactory.open(datasetPath, allocator);
            long readVersion;
            try {
              readVersion = existingDataset.version();
            } finally {
              existingDataset.close();
            }

            FragmentOperation.Append append = new FragmentOperation.Append(fragments);
            dataset =
                append.commit(
                    allocator, datasetPath, Optional.of(readVersion), Collections.emptyMap());
          }
        }

        totalWrittenRows += buffer.size();
        LOG.debug("Wrote {} rows, total: {} rows", buffer.size(), totalWrittenRows);

        buffer.clear();
      } finally {
        if (dataset != null) {
          try {
            dataset.close();
          } catch (Exception e) {
            LOG.warn("Failed to close dataset", e);
          }
        }
      }
    } catch (Exception e) {
      throw new IOException("Failed to write to Lance dataset", e);
    }
  }

  @Override
  public void close() throws Exception {
    LOG.info("Closing LanceSinkWriter");

    // Flush remaining data
    try {
      doFlush();
    } catch (Exception e) {
      LOG.warn("Failed to flush data on close", e);
    }

    if (allocator != null) {
      try {
        allocator.close();
      } catch (Exception e) {
        LOG.warn("Failed to close allocator", e);
      }
      allocator = null;
    }

    LOG.info("LanceSinkWriter closed, total rows written: {}", totalWrittenRows);
  }

  /** Get total written row count. */
  public long getTotalWrittenRows() {
    return totalWrittenRows;
  }

  /**
   * Check whether a dataset exists at the given path. Supports both local filesystem paths and
   * remote storage URIs (S3, GCS, etc.).
   */
  private boolean checkDatasetExists(String datasetPath) {
    if (isRemotePath(datasetPath)) {
      // For remote storage, try to open the dataset to check existence
      try {
        Dataset dataset = LanceDatasetFactory.open(datasetPath, allocator);
        dataset.close();
        return true;
      } catch (Exception e) {
        LOG.debug("Dataset does not exist at remote path: {}", datasetPath);
        return false;
      }
    } else {
      // Local filesystem check
      Path path = Paths.get(datasetPath);
      return Files.exists(path);
    }
  }

  /** Detect whether a path is a remote storage URI. */
  private static boolean isRemotePath(String path) {
    if (path == null) {
      return false;
    }
    String lower = path.toLowerCase();
    return lower.startsWith("s3://")
        || lower.startsWith("s3a://")
        || lower.startsWith("gs://")
        || lower.startsWith("az://")
        || lower.startsWith("https://")
        || lower.startsWith("http://");
  }

  /** Recursively delete a directory. */
  private void deleteDirectory(Path path) throws IOException {
    if (Files.isDirectory(path)) {
      Files.list(path)
          .forEach(
              child -> {
                try {
                  deleteDirectory(child);
                } catch (IOException e) {
                  LOG.warn("Failed to delete file: {}", child, e);
                }
              });
    }
    Files.deleteIfExists(path);
  }
}
