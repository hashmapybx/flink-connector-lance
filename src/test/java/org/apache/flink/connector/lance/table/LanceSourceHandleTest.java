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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link LanceSourceHandle}. */
class LanceSourceHandleTest {

  @Test
  @DisplayName("EMPTY handle has no push-down")
  void testEmptyHandle() {
    LanceSourceHandle handle = LanceSourceHandle.EMPTY;

    assertThat(handle.getProjectedFields()).isNull();
    assertThat(handle.getFilters()).isEmpty();
    assertThat(handle.getLimit()).isNull();
    assertThat(handle.getAggregateInfo()).isNull();
    assertThat(handle.hasPushDown()).isFalse();
  }

  @Test
  @DisplayName("Handle with projection only")
  void testProjectionOnly() {
    LanceSourceHandle handle =
        LanceSourceHandle.builder().projectedFields(new int[] {0, 2}).build();

    assertThat(handle.getProjectedFields()).containsExactly(0, 2);
    assertThat(handle.getFilters()).isEmpty();
    assertThat(handle.getLimit()).isNull();
    assertThat(handle.hasPushDown()).isTrue();
  }

  @Test
  @DisplayName("Handle with filters only")
  void testFiltersOnly() {
    LanceSourceHandle handle =
        LanceSourceHandle.builder().filters(Arrays.asList("id > 10", "name = 'x'")).build();

    assertThat(handle.getProjectedFields()).isNull();
    assertThat(handle.getFilters()).containsExactly("id > 10", "name = 'x'");
    assertThat(handle.hasPushDown()).isTrue();
  }

  @Test
  @DisplayName("Handle with limit only")
  void testLimitOnly() {
    LanceSourceHandle handle = LanceSourceHandle.builder().limit(100L).build();

    assertThat(handle.getLimit()).isEqualTo(100L);
    assertThat(handle.hasPushDown()).isTrue();
  }

  @Test
  @DisplayName("Handle with all push-down types")
  void testAllPushDownTypes() {
    AggregateInfo aggInfo =
        AggregateInfo.builder()
            .groupBy(Collections.singletonList("category"))
            .addAggregateCall(
                new AggregateInfo.AggregateCall(AggregateInfo.AggregateFunction.COUNT, null, "cnt"))
            .build();

    LanceSourceHandle handle =
        LanceSourceHandle.builder()
            .projectedFields(new int[] {0, 1})
            .filters(Arrays.asList("id > 5"))
            .limit(50L)
            .aggregateInfo(aggInfo)
            .build();

    assertThat(handle.getProjectedFields()).containsExactly(0, 1);
    assertThat(handle.getFilters()).containsExactly("id > 5");
    assertThat(handle.getLimit()).isEqualTo(50L);
    assertThat(handle.getAggregateInfo()).isEqualTo(aggInfo);
    assertThat(handle.hasPushDown()).isTrue();
  }

  @Test
  @DisplayName("Projected fields defensive copy: external mutation does not affect handle")
  void testProjectedFieldsDefensiveCopy() {
    int[] fields = {0, 1, 2};
    LanceSourceHandle handle = LanceSourceHandle.builder().projectedFields(fields).build();

    // Mutate the original array
    fields[0] = 99;

    // Handle should not be affected
    assertThat(handle.getProjectedFields()).containsExactly(0, 1, 2);
  }

  @Test
  @DisplayName("getProjectedFields returns a copy each time")
  void testGetProjectedFieldsReturnsCopy() {
    LanceSourceHandle handle =
        LanceSourceHandle.builder().projectedFields(new int[] {0, 1}).build();

    int[] first = handle.getProjectedFields();
    int[] second = handle.getProjectedFields();

    assertThat(first).isNotSameAs(second);
    assertThat(first).containsExactly(0, 1);
  }

  @Test
  @DisplayName("Filters list is unmodifiable")
  void testFiltersImmutable() {
    LanceSourceHandle handle = LanceSourceHandle.builder().filters(Arrays.asList("id > 1")).build();

    assertThat(handle.getFilters()).hasSize(1);
    org.junit.jupiter.api.Assertions.assertThrows(
        UnsupportedOperationException.class, () -> handle.getFilters().add("extra"));
  }

  @Test
  @DisplayName("toBuilder creates a modified copy")
  void testToBuilder() {
    LanceSourceHandle original =
        LanceSourceHandle.builder()
            .projectedFields(new int[] {0})
            .filters(Arrays.asList("a > 1"))
            .build();

    LanceSourceHandle modified = original.toBuilder().limit(10L).build();

    // Original is unchanged
    assertThat(original.getLimit()).isNull();
    // Modified has new limit and retains original state
    assertThat(modified.getLimit()).isEqualTo(10L);
    assertThat(modified.getProjectedFields()).containsExactly(0);
    assertThat(modified.getFilters()).containsExactly("a > 1");
  }

  @Test
  @DisplayName("equals and hashCode")
  void testEqualsHashCode() {
    LanceSourceHandle a =
        LanceSourceHandle.builder()
            .projectedFields(new int[] {0, 1})
            .filters(Arrays.asList("id > 5"))
            .limit(50L)
            .build();

    LanceSourceHandle b =
        LanceSourceHandle.builder()
            .projectedFields(new int[] {0, 1})
            .filters(Arrays.asList("id > 5"))
            .limit(50L)
            .build();

    LanceSourceHandle c = LanceSourceHandle.builder().limit(100L).build();

    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
    assertThat(a).isNotEqualTo(c);
  }

  @Test
  @DisplayName("toString contains key info")
  void testToString() {
    LanceSourceHandle handle =
        LanceSourceHandle.builder()
            .projectedFields(new int[] {0})
            .filters(Arrays.asList("id > 5"))
            .limit(50L)
            .build();

    String str = handle.toString();
    assertThat(str).contains("projectedFields");
    assertThat(str).contains("id > 5");
    assertThat(str).contains("50");
  }
}
