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

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.sink.LanceSink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/**
 * Lance dynamic table Sink.
 *
 * <p>Implements DynamicTableSink interface, writes Flink data to Lance Dataset using Sink V2 API (FLIP-143).
 * <p>Provides runtime Sink through {@link SinkV2Provider}.
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
        // Lance only supports INSERT operations
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        RowType rowType = (RowType) physicalDataType.getLogicalType();

        // Use Sink V2 API (FLIP-143) SinkV2Provider
        LanceSink lanceSink = new LanceSink(options, rowType);

        return SinkV2Provider.of(lanceSink);
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
