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
package org.apache.flink.connector.lance.aggregate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Aggregate information encapsulation class.
 *
 * <p>Encapsulates information needed for aggregate push-down, including aggregate functions, target
 * columns and group by columns.
 */
public class AggregateInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Supported aggregate function types */
  public enum AggregateFunction {
    /** COUNT(*) or COUNT(column) */
    COUNT,
    /** COUNT(DISTINCT column) */
    COUNT_DISTINCT,
    /** SUM(column) */
    SUM,
    /** AVG(column) */
    AVG,
    /** MIN(column) */
    MIN,
    /** MAX(column) */
    MAX
  }

  /** Single aggregate call information */
  public static class AggregateCall implements Serializable {
    private static final long serialVersionUID = 1L;

    private final AggregateFunction function;
    private final String column; // null means COUNT(*)
    private final String alias; // alias for aggregate result

    public AggregateCall(AggregateFunction function, String column, String alias) {
      this.function = function;
      this.column = column;
      this.alias = alias;
    }

    public AggregateFunction getFunction() {
      return function;
    }

    public String getColumn() {
      return column;
    }

    public String getAlias() {
      return alias;
    }

    /** Whether is COUNT(*) */
    public boolean isCountStar() {
      return function == AggregateFunction.COUNT && column == null;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AggregateCall that = (AggregateCall) o;
      return function == that.function
          && Objects.equals(column, that.column)
          && Objects.equals(alias, that.alias);
    }

    @Override
    public int hashCode() {
      return Objects.hash(function, column, alias);
    }

    @Override
    public String toString() {
      if (isCountStar()) {
        return "COUNT(*)";
      }
      return function.name() + "(" + column + ")";
    }
  }

  private final List<AggregateCall> aggregateCalls;
  private final List<String> groupByColumns;
  private final int[] groupByFieldIndices;

  private AggregateInfo(Builder builder) {
    this.aggregateCalls = Collections.unmodifiableList(new ArrayList<>(builder.aggregateCalls));
    this.groupByColumns = Collections.unmodifiableList(new ArrayList<>(builder.groupByColumns));
    this.groupByFieldIndices =
        builder.groupByFieldIndices != null ? builder.groupByFieldIndices.clone() : new int[0];
  }

  public List<AggregateCall> getAggregateCalls() {
    return aggregateCalls;
  }

  public List<String> getGroupByColumns() {
    return groupByColumns;
  }

  public int[] getGroupByFieldIndices() {
    return groupByFieldIndices;
  }

  /** Whether has group by */
  public boolean hasGroupBy() {
    return !groupByColumns.isEmpty();
  }

  /** Whether is simple COUNT(*) query (no group by) */
  public boolean isSimpleCountStar() {
    return aggregateCalls.size() == 1 && aggregateCalls.get(0).isCountStar() && !hasGroupBy();
  }

  /** Get all required columns (aggregate columns + group by columns) */
  public List<String> getRequiredColumns() {
    List<String> columns = new ArrayList<>(groupByColumns);
    for (AggregateCall call : aggregateCalls) {
      if (call.getColumn() != null && !columns.contains(call.getColumn())) {
        columns.add(call.getColumn());
      }
    }
    return columns;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AggregateInfo that = (AggregateInfo) o;
    return Objects.equals(aggregateCalls, that.aggregateCalls)
        && Objects.equals(groupByColumns, that.groupByColumns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(aggregateCalls, groupByColumns);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("AggregateInfo{");
    sb.append("aggregates=").append(aggregateCalls);
    if (hasGroupBy()) {
      sb.append(", groupBy=").append(groupByColumns);
    }
    sb.append("}");
    return sb.toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  /** AggregateInfo builder */
  public static class Builder {
    private final List<AggregateCall> aggregateCalls = new ArrayList<>();
    private final List<String> groupByColumns = new ArrayList<>();
    private int[] groupByFieldIndices;

    public Builder addAggregateCall(AggregateFunction function, String column, String alias) {
      aggregateCalls.add(new AggregateCall(function, column, alias));
      return this;
    }

    public Builder addAggregateCall(AggregateCall call) {
      aggregateCalls.add(call);
      return this;
    }

    public Builder addCountStar(String alias) {
      return addAggregateCall(AggregateFunction.COUNT, null, alias);
    }

    public Builder addCount(String column, String alias) {
      return addAggregateCall(AggregateFunction.COUNT, column, alias);
    }

    public Builder addSum(String column, String alias) {
      return addAggregateCall(AggregateFunction.SUM, column, alias);
    }

    public Builder addAvg(String column, String alias) {
      return addAggregateCall(AggregateFunction.AVG, column, alias);
    }

    public Builder addMin(String column, String alias) {
      return addAggregateCall(AggregateFunction.MIN, column, alias);
    }

    public Builder addMax(String column, String alias) {
      return addAggregateCall(AggregateFunction.MAX, column, alias);
    }

    public Builder groupBy(List<String> columns) {
      this.groupByColumns.addAll(columns);
      return this;
    }

    public Builder groupBy(String... columns) {
      Collections.addAll(this.groupByColumns, columns);
      return this;
    }

    public Builder groupByFieldIndices(int[] indices) {
      this.groupByFieldIndices = indices;
      return this;
    }

    public AggregateInfo build() {
      if (aggregateCalls.isEmpty()) {
        throw new IllegalArgumentException("At least one aggregate function is required");
      }
      return new AggregateInfo(this);
    }
  }
}
