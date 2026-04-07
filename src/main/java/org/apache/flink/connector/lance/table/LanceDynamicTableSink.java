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

package org.apache.flink.connector.lance.table;

import org.apache.flink.connector.lance.LanceDeleteExecutor;
import org.apache.flink.connector.lance.LanceSink;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/**
 * Lance dynamic table sink.
 * 
 * <p>Implements DynamicTableSink interface, supports writing Flink data to Lance dataset.
 * <p>Supports INSERT and DELETE operations. DELETE is handled via Lance's native
 * predicate-based deletion using {@link LanceDeleteExecutor}.
 */
public class LanceDynamicTableSink implements DynamicTableSink {

    private final LanceOptions options;
    private final DataType physicalDataType;

    public LanceDynamicTableSink(LanceOptions options, DataType physicalDataType) {
        this.options = options;
        this.physicalDataType = physicalDataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // Lance supports INSERT and DELETE operations
        ChangelogMode.Builder builder = ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT);
        
        // Add DELETE support if requested
        if (requestedMode.contains(RowKind.DELETE)) {
            builder.addContainedKind(RowKind.DELETE);
        }
        
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        RowType rowType = (RowType) physicalDataType.getLogicalType();

        // Create LanceSink
        LanceSink lanceSink = new LanceSink(options, rowType);

        return SinkFunctionProvider.of(lanceSink);
    }

    @Override
    public DynamicTableSink copy() {
        return new LanceDynamicTableSink(options, physicalDataType);
    }

    @Override
    public String asSummaryString() {
        return "Lance Table Sink";
    }

    /**
     * Get configuration options
     */
    public LanceOptions getOptions() {
        return options;
    }

    /**
     * Get physical data type
     */
    public DataType getPhysicalDataType() {
        return physicalDataType;
    }
}
