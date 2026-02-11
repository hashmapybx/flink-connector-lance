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

import com.lancedb.lance.Dataset;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Unified factory for opening and managing Lance {@link Dataset} instances.
 *
 * <p>Eliminates duplicated Dataset.open / allocator management logic scattered across the codebase.
 * Every class that needs to open a Lance dataset should go through this factory.
 *
 * <p>Two usage patterns are supported:
 *
 * <ul>
 *   <li><b>Auto-managed</b>: Use {@link #openManaged(String)} to get a {@link ManagedDataset} that
 *       owns both the allocator and the dataset. Close it when done.
 *   <li><b>External allocator</b>: Use {@link #open(String, BufferAllocator)} when the caller owns
 *       the allocator lifecycle.
 * </ul>
 *
 * <p>Example:
 *
 * <pre>{@code
 * // Auto-managed (recommended for short-lived usage)
 * try (LanceDatasetFactory.ManagedDataset md = LanceDatasetFactory.openManaged("/data/ds")) {
 *     Dataset ds = md.getDataset();
 *     // use ds...
 * }
 *
 * // External allocator
 * BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
 * Dataset ds = LanceDatasetFactory.open("/data/ds", alloc);
 * // caller is responsible for closing ds and alloc
 * }</pre>
 */
public final class LanceDatasetFactory {

  private static final Logger LOG = LoggerFactory.getLogger(LanceDatasetFactory.class);

  private LanceDatasetFactory() {
    // Utility class — no instantiation
  }

  /**
   * Open a Lance Dataset with the given allocator.
   *
   * @param datasetPath Path to the Lance dataset (local or remote)
   * @param allocator Arrow BufferAllocator to use
   * @return Opened Dataset
   * @throws IOException if the path is invalid or the dataset cannot be opened
   */
  public static Dataset open(String datasetPath, BufferAllocator allocator) throws IOException {
    validatePath(datasetPath);

    try {
      Dataset dataset = Dataset.open(datasetPath, allocator);
      LOG.debug("Opened Lance dataset: {}", datasetPath);
      return dataset;
    } catch (Exception e) {
      throw new IOException("Failed to open Lance dataset: " + datasetPath, e);
    }
  }

  /**
   * Open a Lance Dataset with a self-managed allocator.
   *
   * <p>The returned {@link ManagedDataset} owns both the allocator and the dataset; closing it
   * releases both resources.
   *
   * @param datasetPath Path to the Lance dataset
   * @return A {@link ManagedDataset} wrapping the dataset and its allocator
   * @throws IOException if the dataset cannot be opened
   */
  public static ManagedDataset openManaged(String datasetPath) throws IOException {
    validatePath(datasetPath);

    BufferAllocator allocator = createAllocator();
    try {
      Dataset dataset = Dataset.open(datasetPath, allocator);
      LOG.debug("Opened managed Lance dataset: {}", datasetPath);
      return new ManagedDataset(dataset, allocator);
    } catch (Exception e) {
      // Clean up allocator on failure
      closeQuietly(allocator);
      throw new IOException("Failed to open Lance dataset: " + datasetPath, e);
    }
  }

  /**
   * Create a new {@link RootAllocator} with unbounded capacity.
   *
   * @return A new BufferAllocator
   */
  public static BufferAllocator createAllocator() {
    return new RootAllocator(Long.MAX_VALUE);
  }

  /**
   * Quietly close a {@link Dataset}, logging but not propagating exceptions.
   *
   * @param dataset Dataset to close (nullable)
   */
  public static void closeQuietly(Dataset dataset) {
    if (dataset != null) {
      try {
        dataset.close();
      } catch (Exception e) {
        LOG.warn("Failed to close dataset", e);
      }
    }
  }

  /**
   * Quietly close a {@link BufferAllocator}, logging but not propagating exceptions.
   *
   * @param allocator Allocator to close (nullable)
   */
  public static void closeQuietly(BufferAllocator allocator) {
    if (allocator != null) {
      try {
        allocator.close();
      } catch (Exception e) {
        LOG.warn("Failed to close allocator", e);
      }
    }
  }

  /**
   * Validate a dataset path.
   *
   * @param datasetPath The path to validate
   * @throws IllegalArgumentException if the path is null or empty
   */
  public static void validatePath(String datasetPath) {
    if (datasetPath == null || datasetPath.isEmpty()) {
      throw new IllegalArgumentException("Lance dataset path must not be null or empty");
    }
  }

  /**
   * A {@link Closeable} wrapper that owns both a {@link Dataset} and its {@link BufferAllocator}.
   *
   * <p>Closing a ManagedDataset will close the dataset first, then the allocator.
   */
  public static final class ManagedDataset implements Closeable {
    private final Dataset dataset;
    private final BufferAllocator allocator;

    ManagedDataset(Dataset dataset, BufferAllocator allocator) {
      this.dataset = dataset;
      this.allocator = allocator;
    }

    /** Get the underlying Dataset. */
    public Dataset getDataset() {
      return dataset;
    }

    /** Get the underlying BufferAllocator. */
    public BufferAllocator getAllocator() {
      return allocator;
    }

    @Override
    public void close() throws IOException {
      closeQuietly(dataset);
      closeQuietly(allocator);
    }
  }
}
