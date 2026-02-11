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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** AggregateInfo unit tests */
@DisplayName("AggregateInfo Unit Tests")
class AggregateInfoTest {

  // ==================== AggregateCall Tests ====================

  @Nested
  @DisplayName("AggregateCall Tests")
  class AggregateCallTests {

    @Test
    @DisplayName("COUNT(*) should be correctly identified")
    void testCountStar() {
      AggregateInfo.AggregateCall call =
          new AggregateInfo.AggregateCall(AggregateInfo.AggregateFunction.COUNT, null, "cnt");

      assertTrue(call.isCountStar());
      assertEquals(AggregateInfo.AggregateFunction.COUNT, call.getFunction());
      assertNull(call.getColumn());
      assertEquals("cnt", call.getAlias());
      assertEquals("COUNT(*)", call.toString());
    }

    @Test
    @DisplayName("COUNT(column) should be correctly identified")
    void testCountColumn() {
      AggregateInfo.AggregateCall call =
          new AggregateInfo.AggregateCall(AggregateInfo.AggregateFunction.COUNT, "id", "id_count");

      assertFalse(call.isCountStar());
      assertEquals(AggregateInfo.AggregateFunction.COUNT, call.getFunction());
      assertEquals("id", call.getColumn());
      assertEquals("id_count", call.getAlias());
      assertEquals("COUNT(id)", call.toString());
    }

    @Test
    @DisplayName("SUM aggregate should be correctly built")
    void testSumAggregate() {
      AggregateInfo.AggregateCall call =
          new AggregateInfo.AggregateCall(
              AggregateInfo.AggregateFunction.SUM, "amount", "total_amount");

      assertFalse(call.isCountStar());
      assertEquals(AggregateInfo.AggregateFunction.SUM, call.getFunction());
      assertEquals("amount", call.getColumn());
      assertEquals("total_amount", call.getAlias());
      assertEquals("SUM(amount)", call.toString());
    }

    @Test
    @DisplayName("AVG aggregate should be correctly built")
    void testAvgAggregate() {
      AggregateInfo.AggregateCall call =
          new AggregateInfo.AggregateCall(
              AggregateInfo.AggregateFunction.AVG, "score", "avg_score");

      assertEquals(AggregateInfo.AggregateFunction.AVG, call.getFunction());
      assertEquals("score", call.getColumn());
      assertEquals("avg_score", call.getAlias());
      assertEquals("AVG(score)", call.toString());
    }

    @Test
    @DisplayName("MIN aggregate should be correctly built")
    void testMinAggregate() {
      AggregateInfo.AggregateCall call =
          new AggregateInfo.AggregateCall(
              AggregateInfo.AggregateFunction.MIN, "price", "min_price");

      assertEquals(AggregateInfo.AggregateFunction.MIN, call.getFunction());
      assertEquals("price", call.getColumn());
      assertEquals("min_price", call.getAlias());
    }

    @Test
    @DisplayName("MAX aggregate should be correctly built")
    void testMaxAggregate() {
      AggregateInfo.AggregateCall call =
          new AggregateInfo.AggregateCall(
              AggregateInfo.AggregateFunction.MAX, "price", "max_price");

      assertEquals(AggregateInfo.AggregateFunction.MAX, call.getFunction());
      assertEquals("price", call.getColumn());
      assertEquals("max_price", call.getAlias());
    }

    @Test
    @DisplayName("AggregateCall equals and hashCode should work correctly")
    void testAggregateCallEqualsAndHashCode() {
      AggregateInfo.AggregateCall call1 =
          new AggregateInfo.AggregateCall(AggregateInfo.AggregateFunction.SUM, "amount", "total");
      AggregateInfo.AggregateCall call2 =
          new AggregateInfo.AggregateCall(AggregateInfo.AggregateFunction.SUM, "amount", "total");
      AggregateInfo.AggregateCall call3 =
          new AggregateInfo.AggregateCall(AggregateInfo.AggregateFunction.SUM, "price", "total");

      assertEquals(call1, call2);
      assertEquals(call1.hashCode(), call2.hashCode());
      assertNotEquals(call1, call3);
    }
  }

  // ==================== AggregateInfo Builder Tests ====================

  @Nested
  @DisplayName("AggregateInfo Builder Tests")
  class AggregateInfoBuilderTests {

    @Test
    @DisplayName("Build simple COUNT(*) query")
    void testBuildSimpleCountStar() {
      AggregateInfo info = AggregateInfo.builder().addCountStar("cnt").build();

      assertNotNull(info);
      assertEquals(1, info.getAggregateCalls().size());
      assertTrue(info.isSimpleCountStar());
      assertFalse(info.hasGroupBy());
    }

    @Test
    @DisplayName("Build aggregate query with GROUP BY")
    void testBuildAggregateWithGroupBy() {
      AggregateInfo info =
          AggregateInfo.builder()
              .addSum("amount", "total_amount")
              .addAvg("score", "avg_score")
              .groupBy("category", "region")
              .build();

      assertNotNull(info);
      assertEquals(2, info.getAggregateCalls().size());
      assertTrue(info.hasGroupBy());
      assertEquals(Arrays.asList("category", "region"), info.getGroupByColumns());
      assertFalse(info.isSimpleCountStar());
    }

    @Test
    @DisplayName("Build multiple aggregates query")
    void testBuildMultipleAggregates() {
      AggregateInfo info =
          AggregateInfo.builder()
              .addCountStar("cnt")
              .addSum("amount", "sum_amount")
              .addAvg("score", "avg_score")
              .addMin("price", "min_price")
              .addMax("price", "max_price")
              .build();

      assertNotNull(info);
      assertEquals(5, info.getAggregateCalls().size());
      assertFalse(info.hasGroupBy());
    }

    @Test
    @DisplayName("Build requires at least one aggregate function")
    void testBuildRequiresAtLeastOneAggregate() {
      assertThrows(
          IllegalArgumentException.class,
          () -> {
            AggregateInfo.builder().build();
          });
    }

    @Test
    @DisplayName("addAggregateCall should work correctly")
    void testAddAggregateCall() {
      AggregateInfo.AggregateCall call =
          new AggregateInfo.AggregateCall(AggregateInfo.AggregateFunction.SUM, "amount", "total");

      AggregateInfo info = AggregateInfo.builder().addAggregateCall(call).build();

      assertEquals(1, info.getAggregateCalls().size());
      assertEquals(call, info.getAggregateCalls().get(0));
    }

    @Test
    @DisplayName("addCount should work correctly")
    void testAddCount() {
      AggregateInfo info = AggregateInfo.builder().addCount("id", "id_count").build();

      AggregateInfo.AggregateCall call = info.getAggregateCalls().get(0);
      assertEquals(AggregateInfo.AggregateFunction.COUNT, call.getFunction());
      assertEquals("id", call.getColumn());
      assertFalse(call.isCountStar());
    }

    @Test
    @DisplayName("groupBy(List) should work correctly")
    void testGroupByWithList() {
      List<String> groupCols = Arrays.asList("col1", "col2", "col3");

      AggregateInfo info = AggregateInfo.builder().addCountStar("cnt").groupBy(groupCols).build();

      assertEquals(groupCols, info.getGroupByColumns());
    }

    @Test
    @DisplayName("groupByFieldIndices should be correctly set")
    void testGroupByFieldIndices() {
      int[] indices = {0, 2, 4};

      AggregateInfo info =
          AggregateInfo.builder()
              .addCountStar("cnt")
              .groupBy("col1", "col3", "col5")
              .groupByFieldIndices(indices)
              .build();

      assertArrayEquals(indices, info.getGroupByFieldIndices());
    }
  }

  // ==================== AggregateInfo Methods Tests ====================

  @Nested
  @DisplayName("AggregateInfo Methods Tests")
  class AggregateInfoMethodTests {

    @Test
    @DisplayName("getRequiredColumns should return all required columns")
    void testGetRequiredColumns() {
      AggregateInfo info =
          AggregateInfo.builder()
              .addSum("amount", "sum_amount")
              .addAvg("score", "avg_score")
              .groupBy("category", "region")
              .build();

      List<String> required = info.getRequiredColumns();

      // Should contain group columns and aggregate columns
      assertTrue(required.contains("category"));
      assertTrue(required.contains("region"));
      assertTrue(required.contains("amount"));
      assertTrue(required.contains("score"));
    }

    @Test
    @DisplayName("getRequiredColumns should deduplicate")
    void testGetRequiredColumnsDedup() {
      AggregateInfo info =
          AggregateInfo.builder()
              .addSum("amount", "sum_amount")
              .addAvg("amount", "avg_amount") // Same column
              .groupBy("category")
              .build();

      List<String> required = info.getRequiredColumns();

      // amount should appear only once
      long amountCount = required.stream().filter(c -> c.equals("amount")).count();
      assertEquals(1, amountCount);
    }

    @Test
    @DisplayName("COUNT(*) does not require column")
    void testCountStarNoColumn() {
      AggregateInfo info = AggregateInfo.builder().addCountStar("cnt").build();

      List<String> required = info.getRequiredColumns();
      assertTrue(required.isEmpty());
    }

    @Test
    @DisplayName("equals and hashCode should work correctly")
    void testEqualsAndHashCode() {
      AggregateInfo info1 =
          AggregateInfo.builder().addSum("amount", "total").groupBy("category").build();

      AggregateInfo info2 =
          AggregateInfo.builder().addSum("amount", "total").groupBy("category").build();

      AggregateInfo info3 =
          AggregateInfo.builder().addAvg("amount", "avg").groupBy("category").build();

      assertEquals(info1, info2);
      assertEquals(info1.hashCode(), info2.hashCode());
      assertNotEquals(info1, info3);
    }

    @Test
    @DisplayName("toString should return meaningful string")
    void testToString() {
      AggregateInfo info =
          AggregateInfo.builder().addSum("amount", "total").groupBy("category").build();

      String str = info.toString();

      assertTrue(str.contains("AggregateInfo"));
      assertTrue(str.contains("SUM(amount)"));
      assertTrue(str.contains("groupBy"));
      assertTrue(str.contains("category"));
    }
  }

  // ==================== Aggregate Function Enum Tests ====================

  @Nested
  @DisplayName("AggregateFunction Enum Tests")
  class AggregateFunctionEnumTests {

    @Test
    @DisplayName("Should contain all supported aggregate functions")
    void testAllAggregateFunctions() {
      AggregateInfo.AggregateFunction[] functions = AggregateInfo.AggregateFunction.values();

      assertEquals(6, functions.length);
      assertTrue(Arrays.asList(functions).contains(AggregateInfo.AggregateFunction.COUNT));
      assertTrue(Arrays.asList(functions).contains(AggregateInfo.AggregateFunction.COUNT_DISTINCT));
      assertTrue(Arrays.asList(functions).contains(AggregateInfo.AggregateFunction.SUM));
      assertTrue(Arrays.asList(functions).contains(AggregateInfo.AggregateFunction.AVG));
      assertTrue(Arrays.asList(functions).contains(AggregateInfo.AggregateFunction.MIN));
      assertTrue(Arrays.asList(functions).contains(AggregateInfo.AggregateFunction.MAX));
    }
  }
}
