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
 * Immutable configuration for Lance vector search.
 *
 * <p>Contains all vector search options: column name, distance metric, nprobes, ef, refine factor.
 *
 * <p>Use {@link Builder} to construct instances.
 */
public final class LanceVectorSearchOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String columnName;
  private final LanceOptions.MetricType metricType;
  private final int nprobes;
  private final int ef;
  private final Integer refineFactor;

  private LanceVectorSearchOptions(Builder builder) {
    this.columnName = builder.columnName;
    this.metricType = builder.metricType;
    this.nprobes = builder.nprobes;
    this.ef = builder.ef;
    this.refineFactor = builder.refineFactor;
  }

  /** Vector search column name. */
  public String getColumnName() {
    return columnName;
  }

  /** Distance metric type (L2, Cosine, Dot). */
  public LanceOptions.MetricType getMetricType() {
    return metricType;
  }

  /** IVF search probe count. */
  public int getNprobes() {
    return nprobes;
  }

  /** HNSW search width ef. */
  public int getEf() {
    return ef;
  }

  /** Refine factor for improving recall, null means not set. */
  public Integer getRefineFactor() {
    return refineFactor;
  }

  /** Create a new Builder. */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LanceVectorSearchOptions that = (LanceVectorSearchOptions) o;
    return nprobes == that.nprobes
        && ef == that.ef
        && Objects.equals(columnName, that.columnName)
        && metricType == that.metricType
        && Objects.equals(refineFactor, that.refineFactor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName, metricType, nprobes, ef, refineFactor);
  }

  @Override
  public String toString() {
    return "LanceVectorSearchOptions{"
        + "columnName='"
        + columnName
        + '\''
        + ", metricType="
        + metricType
        + ", nprobes="
        + nprobes
        + ", ef="
        + ef
        + ", refineFactor="
        + refineFactor
        + '}';
  }

  /** Builder for {@link LanceVectorSearchOptions}. */
  public static class Builder {
    private String columnName;
    private LanceOptions.MetricType metricType = LanceOptions.MetricType.L2;
    private int nprobes = 20;
    private int ef = 100;
    private Integer refineFactor;

    public Builder columnName(String columnName) {
      this.columnName = columnName;
      return this;
    }

    public Builder metricType(LanceOptions.MetricType metricType) {
      this.metricType = metricType;
      return this;
    }

    public Builder nprobes(int nprobes) {
      this.nprobes = nprobes;
      return this;
    }

    public Builder ef(int ef) {
      this.ef = ef;
      return this;
    }

    public Builder refineFactor(Integer refineFactor) {
      this.refineFactor = refineFactor;
      return this;
    }

    /** Build with validation. */
    public LanceVectorSearchOptions build() {
      if (nprobes <= 0) {
        throw new IllegalArgumentException("vector.nprobes must be > 0, current value: " + nprobes);
      }
      if (ef <= 0) {
        throw new IllegalArgumentException(
            "vector.ef must be greater than 0, current value: " + ef);
      }
      if (refineFactor != null && refineFactor <= 0) {
        throw new IllegalArgumentException(
            "vector.refine-factor must be > 0, current value: " + refineFactor);
      }
      return new LanceVectorSearchOptions(this);
    }
  }
}
