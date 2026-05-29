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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Aggregate executor.
 *
 * <p>Executes aggregate calculations at data source side, supports COUNT, SUM, AVG, MIN, MAX and
 * other aggregate functions.
 */
public class AggregateExecutor implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(AggregateExecutor.class);

  private final AggregateInfo aggregateInfo;
  private final RowType sourceRowType;

  // Aggregate state (by group key)
  private transient Map<GroupKey, AggregateState> aggregateStates;
  private transient boolean initialized;

  public AggregateExecutor(AggregateInfo aggregateInfo, RowType sourceRowType) {
    this.aggregateInfo = aggregateInfo;
    this.sourceRowType = sourceRowType;
  }

  /** Initialize aggregate executor */
  public void init() {
    this.aggregateStates = new HashMap<>();
    this.initialized = true;
    LOG.info("Initialized aggregate executor: {}", aggregateInfo);
  }

  /** Accumulate a row to aggregate state */
  public void accumulate(RowData row) {
    if (!initialized) {
      init();
    }

    // Extract group key
    GroupKey groupKey = extractGroupKey(row);

    // Get or create aggregate state
    AggregateState state =
        aggregateStates.computeIfAbsent(
            groupKey, k -> new AggregateState(aggregateInfo.getAggregateCalls().size()));

    // Update state for each aggregate function
    List<AggregateInfo.AggregateCall> calls = aggregateInfo.getAggregateCalls();
    for (int i = 0; i < calls.size(); i++) {
      AggregateInfo.AggregateCall call = calls.get(i);
      accumulateCall(state, i, call, row);
    }
  }

  /** Accumulate single aggregate function */
  private void accumulateCall(
      AggregateState state, int index, AggregateInfo.AggregateCall call, RowData row) {
    switch (call.getFunction()) {
      case COUNT:
        if (call.isCountStar()) {
          // COUNT(*)
          state.incrementCount(index);
        } else {
          // COUNT(column) - only count non-NULL values
          int fieldIndex = getFieldIndex(call.getColumn());
          if (fieldIndex >= 0 && !row.isNullAt(fieldIndex)) {
            state.incrementCount(index);
          }
        }
        break;

      case COUNT_DISTINCT:
        if (call.getColumn() != null) {
          int fieldIndex = getFieldIndex(call.getColumn());
          if (fieldIndex >= 0 && !row.isNullAt(fieldIndex)) {
            Object value = extractValue(row, fieldIndex);
            state.addDistinctValue(index, value);
          }
        }
        break;

      case SUM:
        if (call.getColumn() != null) {
          int fieldIndex = getFieldIndex(call.getColumn());
          if (fieldIndex >= 0 && !row.isNullAt(fieldIndex)) {
            Number value = extractNumericValue(row, fieldIndex);
            if (value != null) {
              state.addSum(index, value.doubleValue());
            }
          }
        }
        break;

      case AVG:
        if (call.getColumn() != null) {
          int fieldIndex = getFieldIndex(call.getColumn());
          if (fieldIndex >= 0 && !row.isNullAt(fieldIndex)) {
            Number value = extractNumericValue(row, fieldIndex);
            if (value != null) {
              state.addForAvg(index, value.doubleValue());
            }
          }
        }
        break;

      case MIN:
        if (call.getColumn() != null) {
          int fieldIndex = getFieldIndex(call.getColumn());
          if (fieldIndex >= 0 && !row.isNullAt(fieldIndex)) {
            Comparable<?> value = extractComparableValue(row, fieldIndex);
            if (value != null) {
              state.updateMin(index, value);
            }
          }
        }
        break;

      case MAX:
        if (call.getColumn() != null) {
          int fieldIndex = getFieldIndex(call.getColumn());
          if (fieldIndex >= 0 && !row.isNullAt(fieldIndex)) {
            Comparable<?> value = extractComparableValue(row, fieldIndex);
            if (value != null) {
              state.updateMax(index, value);
            }
          }
        }
        break;
    }
  }

  /** Get aggregate results */
  public List<RowData> getResults() {
    if (!initialized || aggregateStates.isEmpty()) {
      // If no data, return default aggregate result
      return getDefaultResults();
    }

    List<RowData> results = new ArrayList<>();
    List<AggregateInfo.AggregateCall> calls = aggregateInfo.getAggregateCalls();
    List<String> groupByCols = aggregateInfo.getGroupByColumns();

    for (Map.Entry<GroupKey, AggregateState> entry : aggregateStates.entrySet()) {
      GroupKey groupKey = entry.getKey();
      AggregateState state = entry.getValue();

      // Create result row: group columns + aggregate columns
      int totalFields = groupByCols.size() + calls.size();
      GenericRowData resultRow = new GenericRowData(totalFields);

      // Fill group columns
      for (int i = 0; i < groupByCols.size(); i++) {
        resultRow.setField(i, groupKey.getValues()[i]);
      }

      // Fill aggregate results
      for (int i = 0; i < calls.size(); i++) {
        AggregateInfo.AggregateCall call = calls.get(i);
        Object aggResult = getAggregateResult(state, i, call);
        resultRow.setField(groupByCols.size() + i, aggResult);
      }

      results.add(resultRow);
    }

    LOG.info("Aggregate execution completed, generated {} result rows", results.size());
    return results;
  }

  /** Get default aggregate result when no data */
  private List<RowData> getDefaultResults() {
    // If has GROUP BY, no data means no result
    if (aggregateInfo.hasGroupBy()) {
      return new ArrayList<>();
    }

    // No GROUP BY, return default aggregate values
    List<AggregateInfo.AggregateCall> calls = aggregateInfo.getAggregateCalls();
    GenericRowData resultRow = new GenericRowData(calls.size());

    for (int i = 0; i < calls.size(); i++) {
      AggregateInfo.AggregateCall call = calls.get(i);
      switch (call.getFunction()) {
        case COUNT:
        case COUNT_DISTINCT:
          resultRow.setField(i, 0L);
          break;
        default:
          resultRow.setField(i, null);
          break;
      }
    }

    List<RowData> results = new ArrayList<>();
    results.add(resultRow);
    return results;
  }

  /** Get single aggregate function result */
  private Object getAggregateResult(
      AggregateState state, int index, AggregateInfo.AggregateCall call) {
    switch (call.getFunction()) {
      case COUNT:
        return state.getCount(index);
      case COUNT_DISTINCT:
        return (long) state.getDistinctCount(index);
      case SUM:
        Double sum = state.getSum(index);
        return sum != null ? sum : null;
      case AVG:
        Double avg = state.getAvg(index);
        return avg != null ? avg : null;
      case MIN:
        return state.getMin(index);
      case MAX:
        return state.getMax(index);
      default:
        return null;
    }
  }

  /** Extract group key */
  private GroupKey extractGroupKey(RowData row) {
    List<String> groupByCols = aggregateInfo.getGroupByColumns();
    if (groupByCols.isEmpty()) {
      return GroupKey.EMPTY;
    }

    Object[] keyValues = new Object[groupByCols.size()];
    for (int i = 0; i < groupByCols.size(); i++) {
      int fieldIndex = getFieldIndex(groupByCols.get(i));
      if (fieldIndex >= 0) {
        keyValues[i] = extractValue(row, fieldIndex);
      }
    }
    return new GroupKey(keyValues);
  }

  /** Get field index */
  private int getFieldIndex(String columnName) {
    List<String> fieldNames = sourceRowType.getFieldNames();
    return fieldNames.indexOf(columnName);
  }

  /** Extract field value */
  private Object extractValue(RowData row, int fieldIndex) {
    if (row.isNullAt(fieldIndex)) {
      return null;
    }

    LogicalType fieldType = sourceRowType.getTypeAt(fieldIndex);
    switch (fieldType.getTypeRoot()) {
      case BOOLEAN:
        return row.getBoolean(fieldIndex);
      case TINYINT:
        return row.getByte(fieldIndex);
      case SMALLINT:
        return row.getShort(fieldIndex);
      case INTEGER:
        return row.getInt(fieldIndex);
      case BIGINT:
        return row.getLong(fieldIndex);
      case FLOAT:
        return row.getFloat(fieldIndex);
      case DOUBLE:
        return row.getDouble(fieldIndex);
      case CHAR:
      case VARCHAR:
        // Keep StringData type for group key and result output
        return row.getString(fieldIndex);
      case DECIMAL:
        DecimalType decType = (DecimalType) fieldType;
        DecimalData decData =
            row.getDecimal(fieldIndex, decType.getPrecision(), decType.getScale());
        return decData != null ? decData.toBigDecimal() : null;
      default:
        return null;
    }
  }

  /** Extract numeric type field value */
  private Number extractNumericValue(RowData row, int fieldIndex) {
    if (row.isNullAt(fieldIndex)) {
      return null;
    }

    LogicalType fieldType = sourceRowType.getTypeAt(fieldIndex);
    switch (fieldType.getTypeRoot()) {
      case TINYINT:
        return row.getByte(fieldIndex);
      case SMALLINT:
        return row.getShort(fieldIndex);
      case INTEGER:
        return row.getInt(fieldIndex);
      case BIGINT:
        return row.getLong(fieldIndex);
      case FLOAT:
        return row.getFloat(fieldIndex);
      case DOUBLE:
        return row.getDouble(fieldIndex);
      case DECIMAL:
        DecimalType decType = (DecimalType) fieldType;
        DecimalData decData =
            row.getDecimal(fieldIndex, decType.getPrecision(), decType.getScale());
        return decData != null ? decData.toBigDecimal() : null;
      default:
        return null;
    }
  }

  /** Extract comparable type field value */
  @SuppressWarnings("unchecked")
  private Comparable<?> extractComparableValue(RowData row, int fieldIndex) {
    Object value = extractValue(row, fieldIndex);
    if (value instanceof Comparable) {
      return (Comparable<?>) value;
    }
    return null;
  }

  /** Reset aggregate state */
  public void reset() {
    if (aggregateStates != null) {
      aggregateStates.clear();
    }
  }

  /** Group key */
  private static class GroupKey implements Serializable {
    private static final long serialVersionUID = 1L;

    static final GroupKey EMPTY = new GroupKey(new Object[0]);

    private final Object[] values;
    private final int hashCode;

    GroupKey(Object[] values) {
      this.values = values;
      this.hashCode = Objects.hash((Object[]) values);
    }

    Object[] getValues() {
      return values;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      GroupKey groupKey = (GroupKey) o;
      return java.util.Arrays.equals(values, groupKey.values);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }

  /** Aggregate state */
  private static class AggregateState implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long[] counts;
    private final double[] sums;
    private final long[] avgCounts; // For calculating AVG
    private final Comparable<?>[] mins;
    private final Comparable<?>[] maxs;
    private final Set<Object>[] distinctSets;

    @SuppressWarnings("unchecked")
    AggregateState(int numAggregates) {
      this.counts = new long[numAggregates];
      this.sums = new double[numAggregates];
      this.avgCounts = new long[numAggregates];
      this.mins = new Comparable<?>[numAggregates];
      this.maxs = new Comparable<?>[numAggregates];
      this.distinctSets = new Set[numAggregates];
    }

    void incrementCount(int index) {
      counts[index]++;
    }

    long getCount(int index) {
      return counts[index];
    }

    void addDistinctValue(int index, Object value) {
      if (distinctSets[index] == null) {
        distinctSets[index] = new HashSet<>();
      }
      distinctSets[index].add(value);
    }

    int getDistinctCount(int index) {
      return distinctSets[index] != null ? distinctSets[index].size() : 0;
    }

    void addSum(int index, double value) {
      sums[index] += value;
      counts[index]++; // Mark as has value
    }

    Double getSum(int index) {
      return counts[index] > 0 ? sums[index] : null;
    }

    void addForAvg(int index, double value) {
      sums[index] += value;
      avgCounts[index]++;
    }

    Double getAvg(int index) {
      return avgCounts[index] > 0 ? sums[index] / avgCounts[index] : null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    void updateMin(int index, Comparable<?> value) {
      if (mins[index] == null || ((Comparable) value).compareTo(mins[index]) < 0) {
        mins[index] = value;
      }
    }

    Comparable<?> getMin(int index) {
      return mins[index];
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    void updateMax(int index, Comparable<?> value) {
      if (maxs[index] == null || ((Comparable) value).compareTo(maxs[index]) > 0) {
        maxs[index] = value;
      }
    }

    Comparable<?> getMax(int index) {
      return maxs[index];
    }
  }

  /** Build aggregate result RowType */
  public RowType buildResultRowType() {
    List<String> groupByCols = aggregateInfo.getGroupByColumns();
    List<AggregateInfo.AggregateCall> calls = aggregateInfo.getAggregateCalls();

    List<RowType.RowField> fields = new ArrayList<>();

    // Group columns
    for (String col : groupByCols) {
      int fieldIndex = getFieldIndex(col);
      if (fieldIndex >= 0) {
        LogicalType fieldType = sourceRowType.getTypeAt(fieldIndex);
        fields.add(new RowType.RowField(col, fieldType));
      }
    }

    // Aggregate result columns
    for (AggregateInfo.AggregateCall call : calls) {
      String alias =
          call.getAlias() != null
              ? call.getAlias()
              : call.getFunction().name().toLowerCase()
                  + "_"
                  + (call.getColumn() != null ? call.getColumn() : "star");
      LogicalType resultType = getAggregateResultType(call);
      fields.add(new RowType.RowField(alias, resultType));
    }

    return new RowType(fields);
  }

  /** Get aggregate function result type */
  private LogicalType getAggregateResultType(AggregateInfo.AggregateCall call) {
    switch (call.getFunction()) {
      case COUNT:
      case COUNT_DISTINCT:
        return new BigIntType();
      case SUM:
      case AVG:
        return new DoubleType();
      case MIN:
      case MAX:
        if (call.getColumn() != null) {
          int fieldIndex = getFieldIndex(call.getColumn());
          if (fieldIndex >= 0) {
            return sourceRowType.getTypeAt(fieldIndex);
          }
        }
        return new DoubleType();
      default:
        return new DoubleType();
    }
  }
}
