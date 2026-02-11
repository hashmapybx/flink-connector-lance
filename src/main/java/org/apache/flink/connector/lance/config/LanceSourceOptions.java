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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable configuration for Lance Source (read side).
 *
 * <p>Contains all read-related options: dataset path, batch size, column projection, filter
 * conditions, and read limit.
 *
 * <p>Use {@link Builder} to construct instances.
 */
public final class LanceSourceOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String path;
  private final int batchSize;
  private final Long limit;
  private final List<String> columns;
  private final String filter;

  private LanceSourceOptions(Builder builder) {
    this.path = builder.path;
    this.batchSize = builder.batchSize;
    this.limit = builder.limit;
    this.columns =
        builder.columns != null
            ? Collections.unmodifiableList(builder.columns)
            : Collections.emptyList();
    this.filter = builder.filter;
  }

  /** Lance dataset path. */
  public String getPath() {
    return path;
  }

  /** Batch size for reading. */
  public int getBatchSize() {
    return batchSize;
  }

  /** Maximum number of rows to read (for Limit push-down), null means no limit. */
  public Long getLimit() {
    return limit;
  }

  /** Columns to read; empty list means all columns. */
  public List<String> getColumns() {
    return columns;
  }

  /** Data filter condition (SQL WHERE clause syntax), null means no filter. */
  public String getFilter() {
    return filter;
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
        .limit(limit)
        .columns(columns)
        .filter(filter);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LanceSourceOptions that = (LanceSourceOptions) o;
    return batchSize == that.batchSize
        && Objects.equals(path, that.path)
        && Objects.equals(limit, that.limit)
        && Objects.equals(columns, that.columns)
        && Objects.equals(filter, that.filter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, batchSize, limit, columns, filter);
  }

  @Override
  public String toString() {
    return "LanceSourceOptions{"
        + "path='"
        + path
        + '\''
        + ", batchSize="
        + batchSize
        + ", limit="
        + limit
        + ", columns="
        + columns
        + ", filter='"
        + filter
        + '\''
        + '}';
  }

  /** Builder for {@link LanceSourceOptions}. */
  public static class Builder {
    private String path;
    private int batchSize = 1024;
    private Long limit;
    private List<String> columns = Collections.emptyList();
    private String filter;

    public Builder path(String path) {
      this.path = path;
      return this;
    }

    public Builder batchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder limit(Long limit) {
      this.limit = limit;
      return this;
    }

    public Builder columns(List<String> columns) {
      this.columns = columns != null ? columns : Collections.emptyList();
      return this;
    }

    public Builder filter(String filter) {
      this.filter = filter;
      return this;
    }

    /** Build with validation. */
    public LanceSourceOptions build() {
      if (batchSize <= 0) {
        throw new IllegalArgumentException(
            "read.batch-size must be greater than 0, current value: " + batchSize);
      }
      if (limit != null && limit < 0) {
        throw new IllegalArgumentException("read.limit must be >= 0, current value: " + limit);
      }
      return new LanceSourceOptions(this);
    }
  }
}
