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

import com.lancedb.lance.Dataset;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

/**
 * Executor for DELETE operations on Lance datasets.
 *
 * <p>Wraps the Lance SDK's {@code Dataset.delete(String predicate)} method to provide
 * a clean interface for deleting rows from a Lance dataset based on a SQL-like predicate.
 *
 * <p>Usage example:
 * <pre>{@code
 * try (LanceDeleteExecutor executor = new LanceDeleteExecutor("/path/to/dataset")) {
 *     long deletedCount = executor.delete("id = 1");
 *     // or delete with complex predicates
 *     long deletedCount2 = executor.delete("age > 30 AND status = 'inactive'");
 * }
 * }</pre>
 *
 * <p>This implementation is inspired by the lance-spark DELETE support.
 * The predicate syntax follows Lance's SQL-like filter syntax.
 */
public class LanceDeleteExecutor implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(LanceDeleteExecutor.class);

    private final String datasetPath;
    private BufferAllocator allocator;

    /**
     * Create a LanceDeleteExecutor for the given dataset path.
     *
     * @param datasetPath path to the Lance dataset
     */
    public LanceDeleteExecutor(String datasetPath) {
        if (datasetPath == null || datasetPath.isEmpty()) {
            throw new IllegalArgumentException("Dataset path cannot be null or empty");
        }
        this.datasetPath = datasetPath;
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    /**
     * Create a LanceDeleteExecutor with a custom allocator.
     *
     * @param datasetPath path to the Lance dataset
     * @param allocator   Arrow memory allocator
     */
    public LanceDeleteExecutor(String datasetPath, BufferAllocator allocator) {
        if (datasetPath == null || datasetPath.isEmpty()) {
            throw new IllegalArgumentException("Dataset path cannot be null or empty");
        }
        if (allocator == null) {
            throw new IllegalArgumentException("Allocator cannot be null");
        }
        this.datasetPath = datasetPath;
        this.allocator = allocator;
    }

    /**
     * Delete rows from the dataset matching the given predicate.
     *
     * <p>The predicate uses SQL-like syntax supported by Lance, for example:
     * <ul>
     *   <li>{@code "id = 1"}</li>
     *   <li>{@code "age > 30"}</li>
     *   <li>{@code "name = 'Alice' AND status = 'active'"}</li>
     *   <li>{@code "id IN (1, 2, 3)"}</li>
     * </ul>
     *
     * @param predicate the filter predicate for rows to delete
     * @throws IOException if the delete operation fails
     * @throws IllegalArgumentException if the predicate is null or empty
     */
    public void delete(String predicate) throws IOException {
        if (predicate == null || predicate.trim().isEmpty()) {
            throw new IllegalArgumentException("Delete predicate cannot be null or empty");
        }

        LOG.info("Executing DELETE on dataset: {} with predicate: {}", datasetPath, predicate);

        try (Dataset dataset = Dataset.open(datasetPath, allocator)) {
            long countBefore = dataset.countRows();
            dataset.delete(predicate);
            LOG.info("DELETE completed on dataset: {}. Rows before: {}", datasetPath, countBefore);
        } catch (Exception e) {
            throw new IOException(
                    String.format("Failed to delete from dataset '%s' with predicate '%s'",
                            datasetPath, predicate), e);
        }
    }

    /**
     * Delete rows from the dataset matching the given predicate and return the count of remaining rows.
     *
     * @param predicate the filter predicate for rows to delete
     * @return the number of rows remaining after deletion
     * @throws IOException if the delete operation fails
     */
    public long deleteAndCount(String predicate) throws IOException {
        if (predicate == null || predicate.trim().isEmpty()) {
            throw new IllegalArgumentException("Delete predicate cannot be null or empty");
        }

        LOG.info("Executing DELETE on dataset: {} with predicate: {}", datasetPath, predicate);

        try (Dataset dataset = Dataset.open(datasetPath, allocator)) {
            long countBefore = dataset.countRows();
            dataset.delete(predicate);
            LOG.info("DELETE completed. Rows before: {}", countBefore);
        } catch (Exception e) {
            throw new IOException(
                    String.format("Failed to delete from dataset '%s' with predicate '%s'",
                            datasetPath, predicate), e);
        }

        // Re-open to get the count after deletion
        try (Dataset dataset = Dataset.open(datasetPath, allocator)) {
            long countAfter = dataset.countRows();
            LOG.info("Rows after deletion: {}", countAfter);
            return countAfter;
        } catch (Exception e) {
            throw new IOException("Failed to count rows after deletion", e);
        }
    }

    /**
     * Get the current row count of the dataset.
     *
     * @return the number of rows in the dataset
     * @throws IOException if the operation fails
     */
    public long countRows() throws IOException {
        try (Dataset dataset = Dataset.open(datasetPath, allocator)) {
            return dataset.countRows();
        } catch (Exception e) {
            throw new IOException("Failed to count rows in dataset: " + datasetPath, e);
        }
    }

    /**
     * Get the current row count matching a filter.
     *
     * @param filter the filter predicate
     * @return the number of rows matching the filter
     * @throws IOException if the operation fails
     */
    public long countRows(String filter) throws IOException {
        try (Dataset dataset = Dataset.open(datasetPath, allocator)) {
            return dataset.countRows(filter);
        } catch (Exception e) {
            throw new IOException("Failed to count rows with filter in dataset: " + datasetPath, e);
        }
    }

    /**
     * Get the dataset path.
     *
     * @return the dataset path
     */
    public String getDatasetPath() {
        return datasetPath;
    }

    @Override
    public void close() throws IOException {
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                LOG.warn("Failed to close allocator", e);
            }
            allocator = null;
        }
    }
}
