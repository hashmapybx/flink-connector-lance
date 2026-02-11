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
 * Immutable configuration for Lance vector index building.
 *
 * <p>Contains all index-related options: index type, column, partition count, PQ/HNSW parameters.
 *
 * <p>Use {@link Builder} to construct instances.
 */
public final class LanceIndexOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String columnName;
  private final LanceOptions.IndexType indexType;
  private final LanceOptions.MetricType metricType;
  private final int numPartitions;
  private final Integer numSubVectors;
  private final int numBits;
  private final int maxLevel;
  private final int m;
  private final int efConstruction;

  private LanceIndexOptions(Builder builder) {
    this.columnName = builder.columnName;
    this.indexType = builder.indexType;
    this.metricType = builder.metricType;
    this.numPartitions = builder.numPartitions;
    this.numSubVectors = builder.numSubVectors;
    this.numBits = builder.numBits;
    this.maxLevel = builder.maxLevel;
    this.m = builder.m;
    this.efConstruction = builder.efConstruction;
  }

  /** Index column name. */
  public String getColumnName() {
    return columnName;
  }

  /** Vector index type (IVF_PQ, IVF_HNSW, IVF_FLAT). */
  public LanceOptions.IndexType getIndexType() {
    return indexType;
  }

  /** Distance metric type (L2, Cosine, Dot). */
  public LanceOptions.MetricType getMetricType() {
    return metricType;
  }

  /** Number of IVF partitions. */
  public int getNumPartitions() {
    return numPartitions;
  }

  /** Number of PQ sub-vectors (null for auto-calculation). */
  public Integer getNumSubVectors() {
    return numSubVectors;
  }

  /** PQ quantization bits. */
  public int getNumBits() {
    return numBits;
  }

  /** HNSW max level. */
  public int getMaxLevel() {
    return maxLevel;
  }

  /** HNSW connections per level M. */
  public int getM() {
    return m;
  }

  /** HNSW construction search width ef_construction. */
  public int getEfConstruction() {
    return efConstruction;
  }

  /** Create a new Builder. */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LanceIndexOptions that = (LanceIndexOptions) o;
    return numPartitions == that.numPartitions
        && numBits == that.numBits
        && maxLevel == that.maxLevel
        && m == that.m
        && efConstruction == that.efConstruction
        && Objects.equals(columnName, that.columnName)
        && indexType == that.indexType
        && metricType == that.metricType
        && Objects.equals(numSubVectors, that.numSubVectors);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        columnName,
        indexType,
        metricType,
        numPartitions,
        numSubVectors,
        numBits,
        maxLevel,
        m,
        efConstruction);
  }

  @Override
  public String toString() {
    return "LanceIndexOptions{"
        + "columnName='"
        + columnName
        + '\''
        + ", indexType="
        + indexType
        + ", metricType="
        + metricType
        + ", numPartitions="
        + numPartitions
        + ", numSubVectors="
        + numSubVectors
        + ", numBits="
        + numBits
        + ", maxLevel="
        + maxLevel
        + ", m="
        + m
        + ", efConstruction="
        + efConstruction
        + '}';
  }

  /** Builder for {@link LanceIndexOptions}. */
  public static class Builder {
    private String columnName;
    private LanceOptions.IndexType indexType = LanceOptions.IndexType.IVF_PQ;
    private LanceOptions.MetricType metricType = LanceOptions.MetricType.L2;
    private int numPartitions = 256;
    private Integer numSubVectors;
    private int numBits = 8;
    private int maxLevel = 7;
    private int m = 16;
    private int efConstruction = 100;

    public Builder columnName(String columnName) {
      this.columnName = columnName;
      return this;
    }

    public Builder indexType(LanceOptions.IndexType indexType) {
      this.indexType = indexType;
      return this;
    }

    public Builder metricType(LanceOptions.MetricType metricType) {
      this.metricType = metricType;
      return this;
    }

    public Builder numPartitions(int numPartitions) {
      this.numPartitions = numPartitions;
      return this;
    }

    public Builder numSubVectors(Integer numSubVectors) {
      this.numSubVectors = numSubVectors;
      return this;
    }

    public Builder numBits(int numBits) {
      this.numBits = numBits;
      return this;
    }

    public Builder maxLevel(int maxLevel) {
      this.maxLevel = maxLevel;
      return this;
    }

    public Builder maxEdges(int m) {
      this.m = m;
      return this;
    }

    public Builder efConstruction(int efConstruction) {
      this.efConstruction = efConstruction;
      return this;
    }

    /** Build with validation. */
    public LanceIndexOptions build() {
      if (numPartitions <= 0) {
        throw new IllegalArgumentException(
            "index.num-partitions must be > 0, current value: " + numPartitions);
      }
      if (numSubVectors != null && numSubVectors <= 0) {
        throw new IllegalArgumentException(
            "index.num-sub-vectors must be > 0, current value: " + numSubVectors);
      }
      if (numBits <= 0 || numBits > 16) {
        throw new IllegalArgumentException(
            "index.num-bits must be between 1 and 16, current value: " + numBits);
      }
      if (maxLevel <= 0) {
        throw new IllegalArgumentException(
            "index.max-level must be > 0, current value: " + maxLevel);
      }
      if (m <= 0) {
        throw new IllegalArgumentException("index.m must be greater than 0, current value: " + m);
      }
      if (efConstruction <= 0) {
        throw new IllegalArgumentException(
            "index.ef-construction must be > 0, current value: " + efConstruction);
      }
      return new LanceIndexOptions(this);
    }
  }
}
