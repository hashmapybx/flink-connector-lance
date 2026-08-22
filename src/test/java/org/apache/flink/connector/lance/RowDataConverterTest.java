/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.lance;

import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RowDataConverter} array element handling, covering both
 * Arrow representations the connector maps to Flink arrays: variable-size List
 * (what the converter itself writes) and FixedSizeList (Lance vector columns).
 * Reads are pinned for double and float elements; writes (including
 * realloc-forcing batches) for double.
 */
class RowDataConverterTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    @Test
    @DisplayName("Test ARRAY<DOUBLE> write/read round-trip via List vector")
    void testWriteThenReadArrayOfDoubleRoundTrip() {
        RowType rowType = RowType.of(new IntType(), new ArrayType(new DoubleType()));
        RowDataConverter converter = new RowDataConverter(rowType);

        GenericRowData nullArrayRow = new GenericRowData(2);
        nullArrayRow.setField(0, 4);
        nullArrayRow.setField(1, null);

        List<RowData> rows =
                Arrays.asList(
                        row(1, new Double[] {1.5, 2.5, 3.5}),
                        row(2, new Double[] {4.5, null, 6.5}),
                        row(3, new Double[0]),
                        nullArrayRow);

        try (VectorSchemaRoot root = converter.createVectorSchemaRoot(allocator)) {
            converter.toVectorSchemaRoot(rows, root);

            List<RowData> readBack = converter.toRowDataList(root);

            assertThat(readBack).hasSize(4);

            assertThat(readBack.get(0).getInt(0)).isEqualTo(1);
            ArrayData first = readBack.get(0).getArray(1);
            assertThat(first.size()).isEqualTo(3);
            assertThat(first.getDouble(0)).isEqualTo(1.5);
            assertThat(first.getDouble(1)).isEqualTo(2.5);
            assertThat(first.getDouble(2)).isEqualTo(3.5);

            assertThat(readBack.get(1).getInt(0)).isEqualTo(2);
            ArrayData second = readBack.get(1).getArray(1);
            assertThat(second.size()).isEqualTo(3);
            assertThat(second.getDouble(0)).isEqualTo(4.5);
            assertThat(second.isNullAt(1)).isTrue();
            assertThat(second.getDouble(2)).isEqualTo(6.5);

            assertThat(readBack.get(2).getInt(0)).isEqualTo(3);
            assertThat(readBack.get(2).getArray(1).size()).isZero();

            assertThat(readBack.get(3).getInt(0)).isEqualTo(4);
            assertThat(readBack.get(3).isNullAt(1)).isTrue();
        }
    }

    @Test
    @DisplayName("Test FixedSizeList of double read (Lance float64 vector column)")
    void testReadFixedSizeListOfDouble() {
        Field embeddingField =
                LanceTypeConverter.createFloat64VectorField("embedding", 2, true);
        Schema schema = new Schema(Collections.singletonList(embeddingField));

        ArrayType embeddingType = new ArrayType(new DoubleType());
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new RowType.RowField("embedding", embeddingType)));
        RowDataConverter converter = new RowDataConverter(rowType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            FixedSizeListVector listVector = (FixedSizeListVector) root.getVector("embedding");
            Float8Vector dataVector = (Float8Vector) listVector.getDataVector();
            dataVector.setSafe(0, 0.5);
            dataVector.setSafe(1, 1.5);
            dataVector.setSafe(2, 2.5);
            // Write index 3 before nulling it. On a freshly allocated child the validity
            // bits are already zero, so setNull on its own would leave the slot merely
            // unwritten rather than explicitly nulled.
            dataVector.setSafe(3, 3.5);
            dataVector.setNull(3);
            // Row 2 is nulled at the parent level, but its own two slots carry live
            // values. That is not what pins the parent bit (an all-null array is still a
            // non-null array, so the assertion below reddens either way); it makes the
            // neighbouring assertions stricter, because a read that drifted into slots 4-5
            // now sees 4.5/5.5 instead of a conveniently null unwritten slot.
            dataVector.setSafe(4, 4.5);
            dataVector.setSafe(5, 5.5);
            listVector.setNotNull(0);
            listVector.setNotNull(1);
            listVector.setNull(2);
            root.setRowCount(3);

            List<RowData> readBack = converter.toRowDataList(root);

            assertThat(readBack).hasSize(3);
            ArrayData first = readBack.get(0).getArray(0);
            assertThat(first.size()).isEqualTo(2);
            assertThat(first.getDouble(0)).isEqualTo(0.5);
            assertThat(first.getDouble(1)).isEqualTo(1.5);
            ArrayData second = readBack.get(1).getArray(0);
            assertThat(second.getDouble(0)).isEqualTo(2.5);
            assertThat(second.isNullAt(1)).isTrue();

            assertThat(readBack.get(2).isNullAt(0)).isTrue();
        }
    }

    @Test
    @DisplayName("Test ARRAY<DOUBLE> write beyond the ListVector child's initial capacity")
    void testWriteBeyondInitialListCapacity() {
        RowType rowType = RowType.of(new IntType(), new ArrayType(new DoubleType()));
        RowDataConverter converter = new RowDataConverter(rowType);

        // 200 rows x 3 elements = 600 elements. setInitialCapacity(4) does not literally
        // leave the child at 4: allocateNew rounds the 40-byte request up to 64 and then
        // re-spreads it over data plus validity, landing on 7. Rows 0 and 1 fit in that
        // (indices 0-5), so the first reallocation happens on row 2, and the child then
        // grows 7 -> 15 -> 31 -> 63 -> 126 -> 252 -> 504 -> 1008, seven times across the
        // batch. Correctness therefore depends on setSafe's reallocation copying prior
        // data intact. Without the setInitialCapacity call the default capacity is 4032,
        // which swallows all 600 without a single realloc and would make this test vacuous.
        List<RowData> rows = new ArrayList<>(200);
        for (int i = 0; i < 200; i++) {
            rows.add(row(i, new Double[] {i * 3.0, i * 3.0 + 1.0, i * 3.0 + 2.0}));
        }

        try (VectorSchemaRoot root = converter.createVectorSchemaRoot(allocator)) {
            ListVector listVector = (ListVector) root.getVector("f1");
            listVector.getDataVector().setInitialCapacity(4);

            // Pin the premise rather than trusting the arithmetic above: allocate once and
            // assert the child really does start below 600. If a future Arrow version
            // rounds differently and the whole batch fits, this reddens instead of quietly
            // turning the test into a no-op.
            root.allocateNew();
            assertThat(listVector.getDataVector().getValueCapacity()).isLessThan(600);

            converter.toVectorSchemaRoot(rows, root);

            List<RowData> readBack = converter.toRowDataList(root);
            assertThat(readBack).hasSize(200);
            for (int i : new int[] {0, 1, 99, 100, 198, 199}) {
                ArrayData array = readBack.get(i).getArray(1);
                assertThat(array.size()).isEqualTo(3);
                assertThat(array.getDouble(0)).isEqualTo(i * 3.0);
                assertThat(array.getDouble(1)).isEqualTo(i * 3.0 + 1.0);
                assertThat(array.getDouble(2)).isEqualTo(i * 3.0 + 2.0);
            }
        }
    }

    @Test
    @DisplayName("Test FixedSizeList of float read (Lance f32 vector column)")
    void testReadFixedSizeListOfFloat() {
        Field embeddingField = LanceTypeConverter.createVectorField("embedding", 2, true);
        Schema schema = new Schema(Collections.singletonList(embeddingField));

        ArrayType embeddingType = new ArrayType(new FloatType());
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new RowType.RowField("embedding", embeddingType)));
        RowDataConverter converter = new RowDataConverter(rowType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            FixedSizeListVector listVector = (FixedSizeListVector) root.getVector("embedding");
            Float4Vector dataVector = (Float4Vector) listVector.getDataVector();
            dataVector.setSafe(0, 0.5f);
            dataVector.setSafe(1, 1.5f);
            dataVector.setSafe(2, 2.5f);
            // See the double case: write before nulling, or the slot is only unwritten.
            dataVector.setSafe(3, 3.5f);
            dataVector.setNull(3);
            dataVector.setSafe(4, 4.5f);
            dataVector.setSafe(5, 5.5f);
            listVector.setNotNull(0);
            listVector.setNotNull(1);
            listVector.setNull(2);
            root.setRowCount(3);

            List<RowData> readBack = converter.toRowDataList(root);

            assertThat(readBack).hasSize(3);
            ArrayData first = readBack.get(0).getArray(0);
            assertThat(first.size()).isEqualTo(2);
            assertThat(first.getFloat(0)).isEqualTo(0.5f);
            assertThat(first.getFloat(1)).isEqualTo(1.5f);
            ArrayData second = readBack.get(1).getArray(0);
            assertThat(second.getFloat(0)).isEqualTo(2.5f);
            assertThat(second.isNullAt(1)).isTrue();

            assertThat(readBack.get(2).isNullAt(0)).isTrue();
        }
    }

    private RowData row(int id, Double[] embedding) {
        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, id);
        rowData.setField(1, new GenericArrayData(embedding));
        return rowData;
    }
}
