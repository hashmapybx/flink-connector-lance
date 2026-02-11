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
package org.apache.flink.connector.lance.table;

import org.apache.flink.connector.lance.aggregate.AggregateInfo;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable handle that carries push-down information from the Table API planner to the runtime.
 *
 * <p>Captures the results of:
 *
 * <ul>
 *   <li>Projection push-down (selected column indices)
 *   <li>Filter push-down (Lance SQL filter strings)
 *   <li>Limit push-down (max rows)
 *   <li>Aggregate push-down ({@link AggregateInfo})
 * </ul>
 *
 * <p>Instances are created via the {@link Builder} and are fully immutable after construction.
 */
public final class LanceSourceHandle implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Empty handle with no push-down applied. */
  public static final LanceSourceHandle EMPTY = builder().build();

  @Nullable private final int[] projectedFields;
  private final List<String> filters;
  @Nullable private final Long limit;
  @Nullable private final AggregateInfo aggregateInfo;

  private LanceSourceHandle(Builder builder) {
    this.projectedFields = builder.projectedFields != null ? builder.projectedFields.clone() : null;
    this.filters =
        builder.filters != null
            ? Collections.unmodifiableList(builder.filters)
            : Collections.emptyList();
    this.limit = builder.limit;
    this.aggregateInfo = builder.aggregateInfo;
  }

  /** Column indices selected by projection push-down, null means all columns. */
  @Nullable
  public int[] getProjectedFields() {
    return projectedFields != null ? projectedFields.clone() : null;
  }

  /** Lance SQL filter strings accepted by filter push-down. */
  public List<String> getFilters() {
    return filters;
  }

  /** Maximum rows from limit push-down, null means no limit. */
  @Nullable
  public Long getLimit() {
    return limit;
  }

  /** Aggregate push-down information, null means not applied. */
  @Nullable
  public AggregateInfo getAggregateInfo() {
    return aggregateInfo;
  }

  /** Whether any push-down is active. */
  public boolean hasPushDown() {
    return projectedFields != null || !filters.isEmpty() || limit != null || aggregateInfo != null;
  }

  /** Create a new builder. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Create a new builder pre-populated with this handle's values. Useful for incremental push-down:
   * the planner calls multiple push-down methods sequentially, each time creating a new handle with
   * the accumulated state.
   */
  public Builder toBuilder() {
    return new Builder()
        .projectedFields(projectedFields)
        .filters(filters)
        .limit(limit)
        .aggregateInfo(aggregateInfo);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LanceSourceHandle that = (LanceSourceHandle) o;
    return java.util.Arrays.equals(projectedFields, that.projectedFields)
        && Objects.equals(filters, that.filters)
        && Objects.equals(limit, that.limit)
        && Objects.equals(aggregateInfo, that.aggregateInfo);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(filters, limit, aggregateInfo);
    result = 31 * result + java.util.Arrays.hashCode(projectedFields);
    return result;
  }

  @Override
  public String toString() {
    return "LanceSourceHandle{"
        + "projectedFields="
        + java.util.Arrays.toString(projectedFields)
        + ", filters="
        + filters
        + ", limit="
        + limit
        + ", aggregateInfo="
        + aggregateInfo
        + '}';
  }

  /** Builder for {@link LanceSourceHandle}. */
  public static class Builder {
    private int[] projectedFields;
    private List<String> filters;
    private Long limit;
    private AggregateInfo aggregateInfo;

    public Builder projectedFields(@Nullable int[] projectedFields) {
      this.projectedFields = projectedFields;
      return this;
    }

    public Builder filters(List<String> filters) {
      this.filters = filters;
      return this;
    }

    public Builder limit(@Nullable Long limit) {
      this.limit = limit;
      return this;
    }

    public Builder aggregateInfo(@Nullable AggregateInfo aggregateInfo) {
      this.aggregateInfo = aggregateInfo;
      return this;
    }

    public LanceSourceHandle build() {
      return new LanceSourceHandle(this);
    }
  }
}
