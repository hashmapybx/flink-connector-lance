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

package org.apache.flink.connector.lance.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Lance Source V2 implementation (based on FLIP-27).
 *
 * <p>Top-level entry point for Flink Source V2 API, coordinates the creation of
 * {@link LanceSplitEnumerator} (split coordinator) and {@link LanceSourceReader} (data reader).
 *
 * <p>Lance Dataset is a bounded data source, so it only supports batch mode.
 *
 * <p>Usage example:
 * <pre>{@code
 * LanceOptions options = LanceOptions.builder()
 *     .path("/path/to/lance/dataset")
 *     .readBatchSize(1024)
 *     .readLimit(100L)
 *     .build();
 *
 * LanceSource source = new LanceSource(options, rowType);
 * DataStream<RowData> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "lance-source");
 * }</pre>
 */
public class LanceSource implements Source<RowData, LanceSourceSplit, LanceEnumeratorState> {

    private static final long serialVersionUID = 1L;

    private final LanceOptions options;
    private final RowType rowType;

    /**
     * Create a LanceSource.
     *
     * @param options Lance configuration options
     * @param rowType Flink RowType (nullable, auto-inferred from Dataset Schema)
     */
    public LanceSource(LanceOptions options, @Nullable RowType rowType) {
        this.options = options;
        this.rowType = rowType;
    }

    /**
     * Create a LanceSource (auto-infer schema).
     *
     * @param options Lance configuration options
     */
    public LanceSource(LanceOptions options) {
        this(options, null);
    }

    @Override
    public Boundedness getBoundedness() {
        // Lance Dataset is a bounded data source
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<LanceSourceSplit, LanceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<LanceSourceSplit> enumContext) throws Exception {
        return new LanceSplitEnumerator(enumContext, options);
    }

    @Override
    public SplitEnumerator<LanceSourceSplit, LanceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<LanceSourceSplit> enumContext,
            LanceEnumeratorState checkpoint) throws Exception {
        return new LanceSplitEnumerator(enumContext, options, checkpoint.getPendingSplits());
    }

    @Override
    public SimpleVersionedSerializer<LanceSourceSplit> getSplitSerializer() {
        return LanceSourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<LanceEnumeratorState> getEnumeratorCheckpointSerializer() {
        return LanceEnumeratorStateSerializer.INSTANCE;
    }

    @Override
    public SourceReader<RowData, LanceSourceSplit> createReader(
            SourceReaderContext readerContext) throws Exception {
        return new LanceSourceReader(readerContext, options, rowType);
    }

    /**
     * Get RowType.
     */
    public RowType getRowType() {
        return rowType;
    }

    /**
     * Get configuration options.
     */
    public LanceOptions getOptions() {
        return options;
    }

    /**
     * Builder pattern constructor.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * LanceSource Builder
     */
    public static class Builder {
        private String path;
        private int batchSize = 1024;
        private List<String> columns;
        private String filter;
        private Long limit;
        private RowType rowType;

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder columns(List<String> columns) {
            this.columns = columns;
            return this;
        }

        public Builder filter(String filter) {
            this.filter = filter;
            return this;
        }

        public Builder limit(Long limit) {
            this.limit = limit;
            return this;
        }

        public Builder rowType(RowType rowType) {
            this.rowType = rowType;
            return this;
        }

        public LanceSource build() {
            if (path == null || path.isEmpty()) {
                throw new IllegalArgumentException("Dataset path must not be empty");
            }

            LanceOptions options = LanceOptions.builder()
                    .path(path)
                    .readBatchSize(batchSize)
                    .readColumns(columns)
                    .readFilter(filter)
                    .readLimit(limit)
                    .build();

            return new LanceSource(options, rowType);
        }
    }
}
