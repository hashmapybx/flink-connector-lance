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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;

/**
 * Lance Sink V2 implementation (based on FLIP-143).
 *
 * <p>Top-level entry point for Flink Sink V2 API, responsible for creating {@link LanceSinkWriter}.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * LanceOptions options = LanceOptions.builder()
 *     .path("/path/to/lance/dataset")
 *     .writeBatchSize(1024)
 *     .writeMode(WriteMode.APPEND)
 *     .build();
 *
 * LanceSink sink = new LanceSink(options, rowType);
 * dataStream.sinkTo(sink);
 * }</pre>
 */
public class LanceSink implements Sink<RowData> {

  private static final long serialVersionUID = 1L;

  private final LanceOptions options;
  private final RowType rowType;

  /**
   * Create a LanceSink.
   *
   * @param options Lance configuration options
   * @param rowType Flink RowType
   */
  public LanceSink(LanceOptions options, RowType rowType) {
    this.options = options;
    this.rowType = rowType;
  }

  @Override
  public SinkWriter<RowData> createWriter(InitContext context) throws IOException {
    return new LanceSinkWriter(options, rowType);
  }

  /** Get RowType. */
  public RowType getRowType() {
    return rowType;
  }

  /** Get configuration options. */
  public LanceOptions getOptions() {
    return options;
  }

  /** Builder pattern constructor. */
  public static Builder builder() {
    return new Builder();
  }

  /** LanceSink Builder */
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
        throw new IllegalArgumentException("Dataset path must not be empty");
      }

      if (rowType == null) {
        throw new IllegalArgumentException("RowType must not be null");
      }

      LanceOptions options =
          LanceOptions.builder()
              .path(path)
              .writeBatchSize(batchSize)
              .writeMode(writeMode)
              .writeMaxRowsPerFile(maxRowsPerFile)
              .build();

      return new LanceSink(options, rowType);
    }
  }
}
