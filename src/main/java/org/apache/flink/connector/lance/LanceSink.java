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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.FragmentOperation;
import com.lancedb.lance.WriteParams;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
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
 * Lance Sink implementation.
 * 
 * <p>Writes Flink RowData to Lance dataset, supports batch writing, Checkpoint, and DELETE operations.
 * 
 * <p>Usage example:
 * <pre>{@code
 * LanceOptions options = LanceOptions.builder()
 *     .path("/path/to/lance/dataset")
 *     .writeBatchSize(1024)
 *     .writeMode(WriteMode.APPEND)
 *     .build();
 * 
 * LanceSink sink = new LanceSink(options, rowType);
 * dataStream.addSink(sink);
 * }</pre>
 */
public class LanceSink extends RichSinkFunction<RowData> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceSink.class);

    private final LanceOptions options;
    private final RowType rowType;

    private transient BufferAllocator allocator;
    private transient Dataset dataset;
    private transient RowDataConverter converter;
    private transient Schema arrowSchema;
    private transient List<RowData> buffer;
    private transient List<RowData> deleteBuffer;
    private transient long totalWrittenRows;
    private transient long totalDeletedRows;
    private transient boolean datasetExists;
    private transient boolean isFirstWrite;

    /**
     * Create LanceSink
     *
     * @param options Lance configuration options
     * @param rowType Flink RowType
     */
    public LanceSink(LanceOptions options, RowType rowType) {
        this.options = options;
        this.rowType = rowType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        LOG.info("Opening Lance Sink: {}", options.getPath());
        
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.buffer = new ArrayList<>(options.getWriteBatchSize());
        this.deleteBuffer = new ArrayList<>();
        this.totalWrittenRows = 0;
        this.totalDeletedRows = 0;
        this.isFirstWrite = true;
        
        // Initialize converter and Schema
        this.converter = new RowDataConverter(rowType);
        this.arrowSchema = LanceTypeConverter.toArrowSchema(rowType);
        
        // Check if dataset exists
        String datasetPath = options.getPath();
        if (datasetPath == null || datasetPath.isEmpty()) {
            throw new IllegalArgumentException("Lance dataset path cannot be empty");
        }
        
        Path path = Paths.get(datasetPath);
        this.datasetExists = Files.exists(path);
        
        // If overwrite mode and dataset exists, delete first
        if (datasetExists && options.getWriteMode() == LanceOptions.WriteMode.OVERWRITE) {
            LOG.info("Overwrite mode, deleting existing dataset: {}", datasetPath);
            deleteDirectory(path);
            this.datasetExists = false;
        }
        
        LOG.info("Lance Sink opened, Schema: {}", rowType);
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        RowKind rowKind = value.getRowKind();
        
        if (rowKind == RowKind.DELETE) {
            // Buffer DELETE rows for batch processing
            deleteBuffer.add(value);
            
            // Process deletes when buffer reaches batch size
            if (deleteBuffer.size() >= options.getWriteBatchSize()) {
                flushDeletes();
            }
        } else if (rowKind == RowKind.INSERT || rowKind == RowKind.UPDATE_AFTER) {
            buffer.add(value);
            
            // When buffer reaches batch size, execute write
            if (buffer.size() >= options.getWriteBatchSize()) {
                flush();
            }
        }
        // Ignore UPDATE_BEFORE rows
    }

    /**
     * Flush buffer, write data to Lance dataset
     */
    public void flush() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        
        LOG.debug("Flushing buffer, row count: {}", buffer.size());
        
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            // Convert RowData to VectorSchemaRoot
            converter.toVectorSchemaRoot(buffer, root);
            
            String datasetPath = options.getPath();
            
            // Build write parameters
            WriteParams writeParams = new WriteParams.Builder()
                    .withMaxRowsPerFile(options.getWriteMaxRowsPerFile())
                    .build();
            
            // Create Fragment
            List<FragmentMetadata> fragments = Fragment.create(
                    datasetPath,
                    allocator,
                    root,
                    writeParams
            );
            
            if (!datasetExists) {
                // Create new dataset (using Overwrite operation)
                FragmentOperation.Overwrite overwrite = new FragmentOperation.Overwrite(fragments, arrowSchema);
                dataset = overwrite.commit(allocator, datasetPath, Optional.empty(), Collections.emptyMap());
                datasetExists = true;
                isFirstWrite = false;
                LOG.info("Created new dataset: {}", datasetPath);
            } else {
                // Append data
                if (isFirstWrite && options.getWriteMode() == LanceOptions.WriteMode.OVERWRITE) {
                    // First write and overwrite mode
                    FragmentOperation.Overwrite overwrite = new FragmentOperation.Overwrite(fragments, arrowSchema);
                    dataset = overwrite.commit(allocator, datasetPath, Optional.empty(), Collections.emptyMap());
                    isFirstWrite = false;
                } else {
                    // Append mode
                    FragmentOperation.Append append = new FragmentOperation.Append(fragments);
                    dataset = append.commit(allocator, datasetPath, Optional.empty(), Collections.emptyMap());
                }
            }
            
            totalWrittenRows += buffer.size();
            LOG.debug("Written {} rows, total: {} rows", buffer.size(), totalWrittenRows);
            
            buffer.clear();
        } catch (Exception e) {
            throw new IOException("Failed to write Lance dataset", e);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing Lance Sink");
        // Flush remaining data
        try {
            flushDeletes();
            flush();
        } catch (Exception e) {
            LOG.warn("Failed to flush data on close", e);
        }
        if (dataset != null) {
            try {
                dataset.close();
            } catch (Exception e) {
                LOG.warn("Failed to close dataset", e);
            }
            dataset = null;
        }
        
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                LOG.warn("Failed to close allocator", e);
            }
            allocator = null;
        }
        
        LOG.info("Lance Sink closed, total written {} rows, total deleted {} rows", totalWrittenRows, totalDeletedRows);
        
        super.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.debug("Snapshot state, checkpointId: {}", context.getCheckpointId());
        
        // Flush all buffered data at Checkpoint
        flushDeletes();
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.debug("Initialize state, isRestored: {}", context.isRestored());
        // State initialization (if recovery needed)
    }

    /**
     * Get RowType
     */
    public RowType getRowType() {
        return rowType;
    }

    /**
     * Get configuration options
     */
    public LanceOptions getOptions() {
        return options;
    }

    /**
     * Get total written row count
     */
    public long getTotalWrittenRows() {
        return totalWrittenRows;
    }

    /**
     * Get total deleted row count
     */
    public long getTotalDeletedRows() {
        return totalDeletedRows;
    }

    /**
     * Flush buffered DELETE operations.
     * 
     * <p>Builds a predicate from the buffered DELETE rows and executes
     * the delete operation using Lance's native predicate-based deletion.
     */
    private void flushDeletes() throws IOException {
        if (deleteBuffer.isEmpty()) {
            return;
        }
        
        LOG.debug("Flushing delete buffer, row count: {}", deleteBuffer.size());
        
        String datasetPath = options.getPath();
        if (!datasetExists) {
            LOG.warn("Cannot delete from non-existent dataset: {}", datasetPath);
            deleteBuffer.clear();
            return;
        }
        
        try {
            // Build delete predicate from buffered rows
            String predicate = buildDeletePredicate(deleteBuffer);
            
            if (predicate != null && !predicate.isEmpty()) {
                LOG.info("Executing DELETE with predicate: {}", predicate);
                
                try (Dataset deleteDataset = Dataset.open(datasetPath, allocator)) {
                    deleteDataset.delete(predicate);
                }
                
                totalDeletedRows += deleteBuffer.size();
                LOG.debug("Deleted {} rows, total deleted: {} rows", deleteBuffer.size(), totalDeletedRows);
            }
            
            deleteBuffer.clear();
        } catch (Exception e) {
            throw new IOException("Failed to execute DELETE on Lance dataset", e);
        }
    }

    /**
     * Build a delete predicate from a list of RowData.
     * 
     * <p>Constructs an OR-combined predicate where each row contributes
     * an AND-combined equality condition on all fields.
     * For example, for rows with fields (id=1, name='Alice') and (id=2, name='Bob'),
     * the predicate would be:
     * {@code (id = 1 AND name = 'Alice') OR (id = 2 AND name = 'Bob')}
     *
     * @param rows the rows to build the predicate from
     * @return the combined predicate string
     */
    private String buildDeletePredicate(List<RowData> rows) {
        List<String> rowPredicates = new ArrayList<>();
        List<String> fieldNames = rowType.getFieldNames();
        List<LogicalType> fieldTypes = new ArrayList<>();
        for (RowType.RowField field : rowType.getFields()) {
            fieldTypes.add(field.getType());
        }
        
        for (RowData row : rows) {
            List<String> fieldPredicates = new ArrayList<>();
            for (int i = 0; i < fieldNames.size(); i++) {
                String fieldName = fieldNames.get(i);
                String value = extractFieldValue(row, i, fieldTypes.get(i));
                if (value != null) {
                    fieldPredicates.add(fieldName + " = " + value);
                }
            }
            if (!fieldPredicates.isEmpty()) {
                rowPredicates.add("(" + String.join(" AND ", fieldPredicates) + ")");
            }
        }
        
        if (rowPredicates.isEmpty()) {
            return null;
        }
        
        return String.join(" OR ", rowPredicates);
    }

    /**
     * Extract a field value from RowData as a string suitable for a predicate.
     *
     * @param row       the RowData
     * @param fieldIndex the field index
     * @param type      the logical type of the field
     * @return the field value as a predicate string, or null if the field is null
     */
    private String extractFieldValue(RowData row, int fieldIndex, LogicalType type) {
        if (row.isNullAt(fieldIndex)) {
            return null;
        }
        
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return String.valueOf(row.getBoolean(fieldIndex));
            case TINYINT:
                return String.valueOf(row.getByte(fieldIndex));
            case SMALLINT:
                return String.valueOf(row.getShort(fieldIndex));
            case INTEGER:
                return String.valueOf(row.getInt(fieldIndex));
            case BIGINT:
                return String.valueOf(row.getLong(fieldIndex));
            case FLOAT:
                return String.valueOf(row.getFloat(fieldIndex));
            case DOUBLE:
                return String.valueOf(row.getDouble(fieldIndex));
            case CHAR:
            case VARCHAR:
                String strValue = row.getString(fieldIndex).toString();
                return "'" + strValue.replace("'", "''") + "'";
            default:
                // For unsupported types, skip this field in the predicate
                LOG.debug("Unsupported type for delete predicate: {}", type);
                return null;
        }
    }

    /**
     * Recursively delete directory
     */
    private void deleteDirectory(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            Files.list(path).forEach(child -> {
                try {
                    deleteDirectory(child);
                } catch (IOException e) {
                    LOG.warn("Failed to delete file: {}", child, e);
                }
            });
        }
        Files.deleteIfExists(path);
    }

    /**
     * Builder pattern constructor
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * LanceSink Builder
     */
    public static class Builder {
        private String path;
        private int batchSize = 1024;
        private LanceOptions.WriteMode writeMode = LanceOptions.WriteMode.APPEND;
        private int maxRowsPerFile = 1000000;
        private RowType rowType;

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder writeMode(LanceOptions.WriteMode writeMode) {
            this.writeMode = writeMode;
            return this;
        }

        public Builder maxRowsPerFile(int maxRowsPerFile) {
            this.maxRowsPerFile = maxRowsPerFile;
            return this;
        }

        public Builder rowType(RowType rowType) {
            this.rowType = rowType;
            return this;
        }

        public LanceSink build() {
            if (path == null || path.isEmpty()) {
                throw new IllegalArgumentException("Dataset path cannot be empty");
            }
            
            if (rowType == null) {
                throw new IllegalArgumentException("RowType cannot be null");
            }

            LanceOptions options = LanceOptions.builder()
                    .path(path)
                    .writeBatchSize(batchSize)
                    .writeMode(writeMode)
                    .writeMaxRowsPerFile(maxRowsPerFile)
                    .build();

            return new LanceSink(options, rowType);
        }
    }
}
