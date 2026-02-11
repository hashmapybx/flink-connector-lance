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
import com.lancedb.lance.ipc.LanceScanner;
import com.lancedb.lance.ipc.ScanOptions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.lance.config.LanceDatasetFactory;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * Data reader for Lance Source.
 *
 * <p>Reads data from assigned {@link LanceSourceSplit}s and converts Arrow data to Flink {@link
 * RowData}. Similar to the PageSource role in Trino.
 *
 * <p>Main responsibilities:
 *
 * <ul>
 *   <li>Receive Splits assigned by SplitEnumerator
 *   <li>Open Fragment Scanner to read data
 *   <li>Convert Arrow data to RowData
 *   <li>Support column pruning, filter push-down and limit push-down
 * </ul>
 */
public class LanceSourceReader implements SourceReader<RowData, LanceSourceSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(LanceSourceReader.class);

  private final SourceReaderContext readerContext;
  private final LanceOptions options;
  private final RowType rowType;
  private final String[] selectedColumns;
  private final Long readLimit;

  /** Queue of pending Splits to process */
  private final Queue<LanceSourceSplit> pendingSplits;

  /** Current reading resources */
  private transient BufferAllocator allocator;

  private transient Dataset currentDataset;
  private transient LanceScanner currentScanner;
  private transient ArrowReader currentReader;
  private transient Iterator<RowData> currentBatchIterator;
  private transient RowDataConverter converter;
  private transient LanceSourceSplit currentSplit;

  /** Whether there are no more Splits */
  private boolean noMoreSplits;

  /** Number of emitted rows (for Limit) */
  private long emittedCount;

  /** Future for available data notification */
  private CompletableFuture<Void> availableFuture;

  /**
   * Create a LanceSourceReader.
   *
   * @param readerContext Reader context
   * @param options Lance configuration
   * @param rowType Row type (nullable, auto-inferred)
   */
  public LanceSourceReader(
      SourceReaderContext readerContext, LanceOptions options, @Nullable RowType rowType) {
    this.readerContext = readerContext;
    this.options = options;
    this.rowType = rowType;
    this.pendingSplits = new ArrayDeque<>();
    this.noMoreSplits = false;
    this.emittedCount = 0;

    List<String> columns = options.getReadColumns();
    this.selectedColumns =
        columns != null && !columns.isEmpty() ? columns.toArray(new String[0]) : null;
    this.readLimit = options.getReadLimit();
  }

  @Override
  public void start() {
    LOG.info("Starting LanceSourceReader, subtask: {}", readerContext.getIndexOfSubtask());
    // Request the first Split
    readerContext.sendSplitRequest();
  }

  @Override
  public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
    // Check if Limit has been reached
    if (isLimitReached()) {
      return InputStatus.END_OF_INPUT;
    }

    // Try to read data from current batch
    if (currentBatchIterator != null && currentBatchIterator.hasNext()) {
      RowData row = currentBatchIterator.next();
      output.collect(row);
      emittedCount++;
      if (isLimitReached()) {
        closeCurrentSplit();
        return InputStatus.END_OF_INPUT;
      }
      return InputStatus.MORE_AVAILABLE;
    }

    // Current batch exhausted, try to load next batch
    if (currentReader != null) {
      try {
        if (currentReader.loadNextBatch()) {
          VectorSchemaRoot root = currentReader.getVectorSchemaRoot();
          List<RowData> rows = converter.toRowDataList(root);
          currentBatchIterator = rows.iterator();
          return InputStatus.MORE_AVAILABLE;
        }
      } catch (Exception e) {
        throw new IOException("Failed to load data batch", e);
      }

      // Current Split reading completed
      closeCurrentSplit();
      LOG.info("Split {} 读取完成", currentSplit != null ? currentSplit.splitId() : "unknown");
      currentSplit = null;
    }

    // Try to open the next Split
    if (!pendingSplits.isEmpty()) {
      LanceSourceSplit split = pendingSplits.poll();
      openSplit(split);
      return InputStatus.MORE_AVAILABLE;
    }

    // No more pending Splits
    if (noMoreSplits) {
      LOG.info("All Splits read, total rows emitted: {}", emittedCount);
      return InputStatus.END_OF_INPUT;
    }

    // More Splits may be coming, wait
    return InputStatus.NOTHING_AVAILABLE;
  }

  /** Open a Split and start reading. */
  private void openSplit(LanceSourceSplit split) throws IOException {
    LOG.info("Opening Split: {}", split);
    this.currentSplit = split;

    try {
      // Initialize allocator (if not already initialized)
      if (allocator == null) {
        allocator = new RootAllocator(Long.MAX_VALUE);
      }

      // Open Dataset
      String datasetPath = split.getDatasetPath();
      currentDataset = LanceDatasetFactory.open(datasetPath, allocator);

      // Initialize converter (if not already initialized)
      if (converter == null) {
        RowType actualRowType = this.rowType;
        if (actualRowType == null) {
          Schema arrowSchema = currentDataset.getSchema();
          actualRowType = LanceTypeConverter.toFlinkRowType(arrowSchema);
        }
        converter = new RowDataConverter(actualRowType);
      }

      // Find the target Fragment
      List<Fragment> fragments = currentDataset.getFragments();
      Fragment targetFragment = null;
      for (Fragment fragment : fragments) {
        if (fragment.getId() == split.getFragmentId()) {
          targetFragment = fragment;
          break;
        }
      }

      if (targetFragment == null) {
        throw new IOException("Fragment not found: " + split.getFragmentId());
      }

      // Build scan options
      ScanOptions.Builder scanOptionsBuilder = new ScanOptions.Builder();
      scanOptionsBuilder.batchSize(options.getReadBatchSize());

      if (selectedColumns != null && selectedColumns.length > 0) {
        scanOptionsBuilder.columns(Arrays.asList(selectedColumns));
      }

      // Fragment level does not support filter, filter is only supported at Dataset level
      // filter has been pushed down in LanceFilterSplitEnumerator (can be extended later if needed)

      ScanOptions scanOptions = scanOptionsBuilder.build();

      // Create Scanner and read data
      currentScanner = targetFragment.newScan(scanOptions);
      currentReader = currentScanner.scanBatches();

      // Load first batch of data
      if (currentReader.loadNextBatch()) {
        VectorSchemaRoot root = currentReader.getVectorSchemaRoot();
        List<RowData> rows = converter.toRowDataList(root);
        currentBatchIterator = rows.iterator();
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Failed to open Split: " + split, e);
    }
  }

  /** Close the resources of the currently reading Split. */
  private void closeCurrentSplit() {
    if (currentReader != null) {
      try {
        currentReader.close();
      } catch (Exception e) {
        LOG.warn("Failed to close Reader", e);
      }
      currentReader = null;
    }

    if (currentScanner != null) {
      try {
        currentScanner.close();
      } catch (Exception e) {
        LOG.warn("Failed to close Scanner", e);
      }
      currentScanner = null;
    }

    if (currentDataset != null) {
      try {
        currentDataset.close();
      } catch (Exception e) {
        LOG.warn("Failed to close Dataset", e);
      }
      currentDataset = null;
    }

    currentBatchIterator = null;
  }

  @Override
  public List<LanceSourceSplit> snapshotState(long checkpointId) {
    List<LanceSourceSplit> state = new ArrayList<>(pendingSplits);
    // If there's a currently processing Split, save it too
    if (currentSplit != null) {
      state.add(0, currentSplit);
    }
    LOG.debug("Checkpoint {} snapshot, saving {} Splits", checkpointId, state.size());
    return state;
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    if (!pendingSplits.isEmpty() || currentBatchIterator != null || currentReader != null) {
      return CompletableFuture.completedFuture(null);
    }

    if (availableFuture == null || availableFuture.isDone()) {
      availableFuture = new CompletableFuture<>();
    }
    return availableFuture;
  }

  @Override
  public void addSplits(List<LanceSourceSplit> splits) {
    LOG.info("Received {} new Splits", splits.size());
    pendingSplits.addAll(splits);

    // Notify that new data is available
    if (availableFuture != null && !availableFuture.isDone()) {
      availableFuture.complete(null);
    }
  }

  @Override
  public void notifyNoMoreSplits() {
    LOG.info("Notified no more Splits");
    this.noMoreSplits = true;

    // Notify of state change
    if (availableFuture != null && !availableFuture.isDone()) {
      availableFuture.complete(null);
    }
  }

  @Override
  public void close() throws Exception {
    LOG.info("Closing LanceSourceReader, total rows emitted: {}", emittedCount);
    closeCurrentSplit();

    if (allocator != null) {
      try {
        allocator.close();
      } catch (Exception e) {
        LOG.warn("Failed to close allocator", e);
      }
      allocator = null;
    }
  }

  /** Check if Limit has been reached. */
  private boolean isLimitReached() {
    return readLimit != null && emittedCount >= readLimit;
  }
}
