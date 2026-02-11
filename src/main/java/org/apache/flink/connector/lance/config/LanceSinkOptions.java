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

import java.io.Serializable;
import java.util.Objects;

/**
 * Immutable configuration for Lance Sink (write side).
 *
 * <p>Contains all write-related options: dataset path, batch size, write mode, and max rows per
 * file.
 *
 * <p>Use {@link Builder} to construct instances.
 */
public final class LanceSinkOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String path;
  private final int batchSize;
  private final LanceOptions.WriteMode writeMode;
  private final int maxRowsPerFile;

  private LanceSinkOptions(Builder builder) {
    this.path = builder.path;
    this.batchSize = builder.batchSize;
    this.writeMode = builder.writeMode;
    this.maxRowsPerFile = builder.maxRowsPerFile;
  }

  /** Lance dataset path. */
  public String getPath() {
    return path;
  }

  /** Batch size for writing. */
  public int getBatchSize() {
    return batchSize;
  }

  /** Write mode: APPEND or OVERWRITE. */
  public LanceOptions.WriteMode getWriteMode() {
    return writeMode;
  }

  /** Maximum rows per data file. */
  public int getMaxRowsPerFile() {
    return maxRowsPerFile;
  }

  /** Create a new Builder. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Create a new Builder pre-populated with this instance's values, useful for creating a modified
   * copy.
   */
  public Builder toBuilder() {
    return new Builder()
        .path(path)
        .batchSize(batchSize)
        .writeMode(writeMode)
        .maxRowsPerFile(maxRowsPerFile);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LanceSinkOptions that = (LanceSinkOptions) o;
    return batchSize == that.batchSize
        && maxRowsPerFile == that.maxRowsPerFile
        && Objects.equals(path, that.path)
        && writeMode == that.writeMode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, batchSize, writeMode, maxRowsPerFile);
  }

  @Override
  public String toString() {
    return "LanceSinkOptions{"
        + "path='"
        + path
        + '\''
        + ", batchSize="
        + batchSize
        + ", writeMode="
        + writeMode
        + ", maxRowsPerFile="
        + maxRowsPerFile
        + '}';
  }

  /** Builder for {@link LanceSinkOptions}. */
  public static class Builder {
    private String path;
    private int batchSize = 1024;
    private LanceOptions.WriteMode writeMode = LanceOptions.WriteMode.APPEND;
    private int maxRowsPerFile = 1000000;

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

    /** Build with validation. */
    public LanceSinkOptions build() {
      if (batchSize <= 0) {
        throw new IllegalArgumentException(
            "write.batch-size must be greater than 0, current value: " + batchSize);
      }
      if (maxRowsPerFile <= 0) {
        throw new IllegalArgumentException(
            "write.max-rows-per-file must be > 0, current value: " + maxRowsPerFile);
      }
      return new LanceSinkOptions(this);
    }
  }
}
