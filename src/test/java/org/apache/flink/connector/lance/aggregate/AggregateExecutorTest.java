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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** AggregateExecutor unit tests */
@DisplayName("AggregateExecutor Unit Tests")
class AggregateExecutorTest {

  private RowType sourceRowType;

  @BeforeEach
  void setUp() {
    // Create test RowType: (id INT, name VARCHAR, category VARCHAR, amount DOUBLE, quantity INT)
    sourceRowType =
        RowType.of(
            new IntType(),
            new VarCharType(100),
            new VarCharType(50),
            new DoubleType(),
            new IntType());
    sourceRowType =
        new RowType(
            Arrays.asList(
                new RowType.RowField("id", new IntType()),
                new RowType.RowField("name", new VarCharType(100)),
                new RowType.RowField("category", new VarCharType(50)),
                new RowType.RowField("amount", new DoubleType()),
                new RowType.RowField("quantity", new IntType())));
  }

  /** Create test data row */
  private RowData createRow(int id, String name, String category, double amount, int quantity) {
    GenericRowData row = new GenericRowData(5);
    row.setField(0, id);
    row.setField(1, StringData.fromString(name));
    row.setField(2, StringData.fromString(category));
    row.setField(3, amount);
    row.setField(4, quantity);
    return row;
  }

  // ==================== COUNT Aggregate Tests ====================

  @Nested
  @DisplayName("COUNT Aggregate Tests")
  class CountAggregateTests {

    @Test
    @DisplayName("COUNT(*) should correctly count all rows")
    void testCountStar() {
      AggregateInfo aggInfo = AggregateInfo.builder().addCountStar("cnt").build();

      AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
      executor.init();

      // Accumulate 5 rows of data
      executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
      executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
      executor.accumulate(createRow(3, "Charlie", "A", 150.0, 15));
      executor.accumulate(createRow(4, "David", "B", 180.0, 18));
      executor.accumulate(createRow(5, "Eve", "C", 220.0, 22));

      List<RowData> results = executor.getResults();

      assertEquals(1, results.size());
      assertEquals(5L, results.get(0).getLong(0)); // COUNT(*)
    }

    @Test
    @DisplayName("COUNT(column) should correctly count non-null values")
    void testCountColumn() {
      AggregateInfo aggInfo = AggregateInfo.builder().addCount("name", "name_count").build();

      AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
      executor.init();

      executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
      executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
      executor.accumulate(createRow(3, "Charlie", "A", 150.0, 15));

      List<RowData> results = executor.getResults();

      assertEquals(1, results.size());
      assertEquals(3L, results.get(0).getLong(0));
    }

    @Test
    @DisplayName("COUNT(*) on empty dataset should return 0")
    void testCountStarEmpty() {
      AggregateInfo aggInfo = AggregateInfo.builder().addCountStar("cnt").build();

      AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
      executor.init();

      List<RowData> results = executor.getResults();

      assertEquals(1, results.size());
      assertEquals(0L, results.get(0).getLong(0));
    }
  }

  // ==================== SUM Aggregate Tests ====================

  @Nested
  @DisplayName("SUM Aggregate Tests")
  class SumAggregateTests {

    @Test
    @DisplayName("SUM should correctly sum values")
    void testSum() {
      AggregateInfo aggInfo = AggregateInfo.builder().addSum("amount", "total_amount").build();

      AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
      executor.init();

      executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
      executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
      executor.accumulate(createRow(3, "Charlie", "A", 150.0, 15));

      List<RowData> results = executor.getResults();

      assertEquals(1, results.size());
      assertEquals(450.0, results.get(0).getDouble(0), 0.001);
    }

    @Test
    @DisplayName("SUM on empty dataset should return null")
    void testSumEmpty() {
      AggregateInfo aggInfo = AggregateInfo.builder().addSum("amount", "total_amount").build();

      AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
      executor.init();

      List<RowData> results = executor.getResults();

      assertEquals(1, results.size());
      assertTrue(results.get(0).isNullAt(0));
    }
  }

  // ==================== AVG Aggregate Tests ====================

  @Nested
  @DisplayName("AVG Aggregate Tests")
  class AvgAggregateTests {

    @Test
    @DisplayName("AVG should correctly calculate average")
    void testAvg() {
      AggregateInfo aggInfo = AggregateInfo.builder().addAvg("amount", "avg_amount").build();

      AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
      executor.init();

      executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
      executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
      executor.accumulate(createRow(3, "Charlie", "A", 150.0, 15));

      List<RowData> results = executor.getResults();

      assertEquals(1, results.size());
      assertEquals(150.0, results.get(0).getDouble(0), 0.001); // (100+200+150)/3
    }

    @Test
    @DisplayName("AVG on empty dataset should return null")
    void testAvgEmpty() {
      AggregateInfo aggInfo = AggregateInfo.builder().addAvg("amount", "avg_amount").build();

      AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
      executor.init();

      List<RowData> results = executor.getResults();

      assertEquals(1, results.size());
      assertTrue(results.get(0).isNullAt(0));
    }
  }

  // ==================== MIN/MAX Aggregate Tests ====================

  @Nested
  @DisplayName("MIN/MAX Aggregate Tests")
  class MinMaxAggregateTests {

    @Test
    @DisplayName("MIN should return minimum value")
    void testMin() {
      AggregateInfo aggInfo = AggregateInfo.builder().addMin("amount", "min_amount").build();

      AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
      executor.init();

      executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
      executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
      executor.accumulate(createRow(3, "Charlie", "A", 50.0, 15));

      List<RowData> results = executor.getResults();

      assertEquals(1, results.size());
      assertEquals(50.0, results.get(0).getDouble(0), 0.001);
    }

    @Test
    @DisplayName("MAX should return maximum value")
    void testMax() {
      AggregateInfo aggInfo = AggregateInfo.builder().addMax("amount", "max_amount").build();

      AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
      executor.init();

      executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
      executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
      executor.accumulate(createRow(3, "Charlie", "A", 50.0, 15));

      List<RowData> results = executor.getResults();

      assertEquals(1, results.size());
      assertEquals(200.0, results.get(0).getDouble(0), 0.001);
    }

    @Test
    @DisplayName("MIN/MAX on empty dataset should return null")
    void testMinMaxEmpty() {
      AggregateInfo aggInfo =
          AggregateInfo.builder()
              .addMin("amount", "min_amount")
              .addMax("amount", "max_amount")
              .build();

      AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
      executor.init();

      List<RowData> results = executor.getResults();

      assertEquals(1, results.size());
      assertTrue(results.get(0).isNullAt(0)); // MIN
      assertTrue(results.get(0).isNullAt(1)); // MAX
    }
  }

  // ==================== GROUP BY Tests ====================

  @Nested
  @DisplayName("GROUP BY Aggregate Tests")
  class GroupByAggregateTests {

    @Test
    @DisplayName("COUNT with GROUP BY should count by group")
    void testGroupByCount() {
      AggregateInfo aggInfo =
          AggregateInfo.builder().addCountStar("cnt").groupBy("category").build();

      AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
      executor.init();

      executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
      executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
      executor.accumulate(createRow(3, "Charlie", "A", 150.0, 15));
      executor.accumulate(createRow(4, "David", "B", 180.0, 18));
      executor.accumulate(createRow(5, "Eve", "A", 220.0, 22));

      List<RowData> results = executor.getResults();

      assertEquals(2, results.size()); // 2 groups: A and B

      // Verify count for each group
      long countA = 0, countB = 0;
      for (RowData row : results) {
        String category = row.getString(0).toString();
        long count = row.getLong(1);
        if ("A".equals(category)) {
          countA = count;
        } else if ("B".equals(category)) {
          countB = count;
        }
      }
      assertEquals(3, countA); // A has 3 rows
      assertEquals(2, countB); // B has 2 rows
    }

    @Test
    @DisplayName("SUM with GROUP BY should sum by group")
    void testGroupBySum() {
      AggregateInfo aggInfo =
          AggregateInfo.builder().addSum("amount", "total_amount").groupBy("category").build();

      AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
      executor.init();

      executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
      executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
      executor.accumulate(createRow(3, "Charlie", "A", 150.0, 15));

      List<RowData> results = executor.getResults();

      assertEquals(2, results.size());

      // Verify sum for each group
      for (RowData row : results) {
        String category = row.getString(0).toString();
        double sum = row.getDouble(1);
        if ("A".equals(category)) {
          assertEquals(250.0, sum, 0.001); // 100 + 150
        } else if ("B".equals(category)) {
          assertEquals(200.0, sum, 0.001); // 200
        }
      }
    }

    @Test
    @DisplayName("Empty dataset with GROUP BY should return empty result")
    void testGroupByEmpty() {
      AggregateInfo aggInfo =
          AggregateInfo.builder().addCountStar("cnt").groupBy("category").build();

      AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
      executor.init();

      List<RowData> results = executor.getResults();

      assertTrue(results.isEmpty());
    }
  }

  // ==================== Multiple Aggregates Tests ====================

  @Nested
  @DisplayName("Multiple Aggregates Tests")
  class MultipleAggregatesTests {

    @Test
    @DisplayName("Multiple aggregate functions should work together")
    void testMultipleAggregates() {
      AggregateInfo aggInfo =
          AggregateInfo.builder()
              .addCountStar("cnt")
              .addSum("amount", "sum_amount")
              .addAvg("amount", "avg_amount")
              .addMin("amount", "min_amount")
              .addMax("amount", "max_amount")
              .build();

      AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
      executor.init();

      executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
      executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
      executor.accumulate(createRow(3, "Charlie", "A", 150.0, 15));

      List<RowData> results = executor.getResults();

      assertEquals(1, results.size());
      RowData result = results.get(0);

      assertEquals(3L, result.getLong(0)); // COUNT(*)
      assertEquals(450.0, result.getDouble(1), 0.001); // SUM
      assertEquals(150.0, result.getDouble(2), 0.001); // AVG
      assertEquals(100.0, result.getDouble(3), 0.001); // MIN
      assertEquals(200.0, result.getDouble(4), 0.001); // MAX
    }

    @Test
    @DisplayName("Multiple aggregates with GROUP BY should work correctly")
    void testMultipleAggregatesWithGroupBy() {
      AggregateInfo aggInfo =
          AggregateInfo.builder()
              .addCountStar("cnt")
              .addSum("amount", "sum_amount")
              .addAvg("amount", "avg_amount")
              .groupBy("category")
              .build();

      AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
      executor.init();

      executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
      executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
      executor.accumulate(createRow(3, "Charlie", "A", 200.0, 15));

      List<RowData> results = executor.getResults();

      assertEquals(2, results.size());

      for (RowData row : results) {
        String category = row.getString(0).toString();
        long count = row.getLong(1);
        double sum = row.getDouble(2);
        double avg = row.getDouble(3);

        if ("A".equals(category)) {
          assertEquals(2, count);
          assertEquals(300.0, sum, 0.001);
          assertEquals(150.0, avg, 0.001);
        } else if ("B".equals(category)) {
          assertEquals(1, count);
          assertEquals(200.0, sum, 0.001);
          assertEquals(200.0, avg, 0.001);
        }
      }
    }
  }

  // ==================== Reset Tests ====================

  @Nested
  @DisplayName("Reset Tests")
  class ResetTests {

    @Test
    @DisplayName("reset should clear aggregate state")
    void testReset() {
      AggregateInfo aggInfo = AggregateInfo.builder().addCountStar("cnt").build();

      AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
      executor.init();

      executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
      executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));

      // Reset
      executor.reset();

      // Re-initialize and accumulate new data
      executor.init();
      executor.accumulate(createRow(3, "Charlie", "A", 150.0, 15));

      List<RowData> results = executor.getResults();

      assertEquals(1, results.size());
      assertEquals(1L, results.get(0).getLong(0)); // Only 1 row after reset
    }
  }

  // ==================== Result Type Tests ====================

  @Nested
  @DisplayName("Result Type Tests")
  class ResultTypeTests {

    @Test
    @DisplayName("buildResultRowType should return correct result type")
    void testBuildResultRowType() {
      AggregateInfo aggInfo =
          AggregateInfo.builder()
              .addCountStar("cnt")
              .addSum("amount", "sum_amount")
              .groupBy("category")
              .build();

      AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
      executor.init();

      RowType resultType = executor.buildResultRowType();

      assertNotNull(resultType);
      assertEquals(3, resultType.getFieldCount());

      // First field is group column category
      assertEquals("category", resultType.getFieldNames().get(0));

      // Second field is COUNT result
      assertEquals("cnt", resultType.getFieldNames().get(1));
      assertTrue(resultType.getTypeAt(1) instanceof BigIntType);

      // Third field is SUM result
      assertEquals("sum_amount", resultType.getFieldNames().get(2));
      assertTrue(resultType.getTypeAt(2) instanceof DoubleType);
    }
  }
}
