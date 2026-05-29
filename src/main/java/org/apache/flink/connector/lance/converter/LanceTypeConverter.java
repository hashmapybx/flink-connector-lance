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
package org.apache.flink.connector.lance.converter;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Type converter between Lance/Arrow and Flink types.
 *
 * <p>Supported type mappings:
 *
 * <ul>
 *   <li>Int8 <-> TINYINT
 *   <li>Int16 <-> SMALLINT
 *   <li>Int32 <-> INT
 *   <li>Int64 <-> BIGINT
 *   <li>Float32 <-> FLOAT
 *   <li>Float64 <-> DOUBLE
 *   <li>String/LargeString <-> STRING
 *   <li>Boolean <-> BOOLEAN
 *   <li>Binary/LargeBinary <-> BYTES
 *   <li>Date32 <-> DATE
 *   <li>Timestamp <-> TIMESTAMP
 *   <li>FixedSizeList<Float32> <-> ARRAY<FLOAT>
 *   <li>FixedSizeList<Float64> <-> ARRAY<DOUBLE>
 * </ul>
 */
public class LanceTypeConverter implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(LanceTypeConverter.class);

  /**
   * Convert Arrow Schema to Flink RowType
   *
   * @param schema Arrow Schema
   * @return Flink RowType
   */
  public static RowType toFlinkRowType(Schema schema) {
    List<RowType.RowField> fields = new ArrayList<>();
    for (Field field : schema.getFields()) {
      LogicalType logicalType = arrowTypeToFlinkType(field);
      fields.add(new RowType.RowField(field.getName(), logicalType));
    }
    return new RowType(fields);
  }

  /**
   * Convert Flink RowType to Arrow Schema
   *
   * @param rowType Flink RowType
   * @return Arrow Schema
   */
  public static Schema toArrowSchema(RowType rowType) {
    List<Field> fields = new ArrayList<>();
    for (RowType.RowField rowField : rowType.getFields()) {
      Field arrowField = flinkTypeToArrowField(rowField.getName(), rowField.getType());
      fields.add(arrowField);
    }
    return new Schema(fields);
  }

  /**
   * Convert Arrow Field to Flink LogicalType
   *
   * @param field Arrow Field
   * @return Flink LogicalType
   */
  public static LogicalType arrowTypeToFlinkType(Field field) {
    ArrowType arrowType = field.getType();
    boolean nullable = field.isNullable();

    if (arrowType instanceof ArrowType.Int) {
      ArrowType.Int intType = (ArrowType.Int) arrowType;
      int bitWidth = intType.getBitWidth();
      switch (bitWidth) {
        case 8:
          return new TinyIntType(nullable);
        case 16:
          return new SmallIntType(nullable);
        case 32:
          return new IntType(nullable);
        case 64:
          return new BigIntType(nullable);
        default:
          throw new UnsupportedTypeException("Unsupported Arrow Int bit width: " + bitWidth);
      }
    } else if (arrowType instanceof ArrowType.FloatingPoint) {
      ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
      FloatingPointPrecision precision = fpType.getPrecision();
      switch (precision) {
        case SINGLE:
          return new FloatType(nullable);
        case DOUBLE:
          return new DoubleType(nullable);
        default:
          throw new UnsupportedTypeException(
              "Unsupported Arrow floating point precision: " + precision);
      }
    } else if (arrowType instanceof ArrowType.Utf8 || arrowType instanceof ArrowType.LargeUtf8) {
      return new VarCharType(nullable, VarCharType.MAX_LENGTH);
    } else if (arrowType instanceof ArrowType.Bool) {
      return new BooleanType(nullable);
    } else if (arrowType instanceof ArrowType.Binary) {
      return new VarBinaryType(nullable, VarBinaryType.MAX_LENGTH);
    } else if (arrowType instanceof ArrowType.LargeBinary) {
      return new VarBinaryType(nullable, VarBinaryType.MAX_LENGTH);
    } else if (arrowType instanceof ArrowType.FixedSizeBinary) {
      ArrowType.FixedSizeBinary fixedBinary = (ArrowType.FixedSizeBinary) arrowType;
      return new BinaryType(nullable, fixedBinary.getByteWidth());
    } else if (arrowType instanceof ArrowType.Date) {
      return new DateType(nullable);
    } else if (arrowType instanceof ArrowType.Timestamp) {
      ArrowType.Timestamp tsType = (ArrowType.Timestamp) arrowType;
      // Determine precision based on time unit
      int precision = getTimestampPrecision(tsType.getUnit());
      return new TimestampType(nullable, precision);
    } else if (arrowType instanceof ArrowType.FixedSizeList) {
      // Vector type: FixedSizeList<Float32/Float64>
      ArrowType.FixedSizeList listType = (ArrowType.FixedSizeList) arrowType;
      List<Field> children = field.getChildren();
      if (children != null && !children.isEmpty()) {
        LogicalType elementType = arrowTypeToFlinkType(children.get(0));
        return new ArrayType(nullable, elementType);
      }
      throw new UnsupportedTypeException("FixedSizeList must contain child type");
    } else if (arrowType instanceof ArrowType.List || arrowType instanceof ArrowType.LargeList) {
      // Regular list type
      List<Field> children = field.getChildren();
      if (children != null && !children.isEmpty()) {
        LogicalType elementType = arrowTypeToFlinkType(children.get(0));
        return new ArrayType(nullable, elementType);
      }
      throw new UnsupportedTypeException("List must contain child type");
    } else if (arrowType instanceof ArrowType.Struct) {
      // Struct type
      List<RowType.RowField> structFields = new ArrayList<>();
      for (Field child : field.getChildren()) {
        LogicalType childType = arrowTypeToFlinkType(child);
        structFields.add(new RowType.RowField(child.getName(), childType));
      }
      return new RowType(nullable, structFields);
    } else if (arrowType instanceof ArrowType.Null) {
      // Null type, map to nullable string
      LOG.warn("Arrow Null type mapped to nullable STRING type");
      return new VarCharType(true, VarCharType.MAX_LENGTH);
    }

    throw new UnsupportedTypeException(
        "Unsupported Arrow type: " + arrowType.getClass().getSimpleName());
  }

  /**
   * Convert Flink LogicalType to Arrow Field
   *
   * @param name Field name
   * @param logicalType Flink LogicalType
   * @return Arrow Field
   */
  public static Field flinkTypeToArrowField(String name, LogicalType logicalType) {
    boolean nullable = logicalType.isNullable();
    ArrowType arrowType;
    List<Field> children = null;

    if (logicalType instanceof TinyIntType) {
      arrowType = new ArrowType.Int(8, true);
    } else if (logicalType instanceof SmallIntType) {
      arrowType = new ArrowType.Int(16, true);
    } else if (logicalType instanceof IntType) {
      arrowType = new ArrowType.Int(32, true);
    } else if (logicalType instanceof BigIntType) {
      arrowType = new ArrowType.Int(64, true);
    } else if (logicalType instanceof FloatType) {
      arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    } else if (logicalType instanceof DoubleType) {
      arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    } else if (logicalType instanceof VarCharType) {
      arrowType = ArrowType.Utf8.INSTANCE;
    } else if (logicalType instanceof BooleanType) {
      arrowType = ArrowType.Bool.INSTANCE;
    } else if (logicalType instanceof VarBinaryType) {
      arrowType = ArrowType.Binary.INSTANCE;
    } else if (logicalType instanceof BinaryType) {
      BinaryType binaryType = (BinaryType) logicalType;
      arrowType = new ArrowType.FixedSizeBinary(binaryType.getLength());
    } else if (logicalType instanceof DateType) {
      arrowType = new ArrowType.Date(DateUnit.DAY);
    } else if (logicalType instanceof TimestampType) {
      TimestampType tsType = (TimestampType) logicalType;
      TimeUnit timeUnit = getArrowTimeUnit(tsType.getPrecision());
      arrowType = new ArrowType.Timestamp(timeUnit, null);
    } else if (logicalType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) logicalType;
      LogicalType elementType = arrayType.getElementType();
      Field childField = flinkTypeToArrowField("item", elementType);
      children = new ArrayList<>();
      children.add(childField);
      // For vector types, use List type
      arrowType = ArrowType.List.INSTANCE;
    } else if (logicalType instanceof RowType) {
      RowType rowType = (RowType) logicalType;
      children = new ArrayList<>();
      for (RowType.RowField rowField : rowType.getFields()) {
        Field childField = flinkTypeToArrowField(rowField.getName(), rowField.getType());
        children.add(childField);
      }
      arrowType = ArrowType.Struct.INSTANCE;
    } else {
      throw new UnsupportedTypeException(
          "Unsupported Flink type: " + logicalType.getClass().getSimpleName());
    }

    FieldType fieldType = new FieldType(nullable, arrowType, null);
    return new Field(name, fieldType, children);
  }

  /**
   * Create vector field (FixedSizeList<Float32>)
   *
   * @param name Field name
   * @param dimension Vector dimension
   * @param nullable Whether nullable
   * @return Arrow Field
   */
  public static Field createVectorField(String name, int dimension, boolean nullable) {
    ArrowType elementType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    Field elementField = new Field("item", new FieldType(false, elementType, null), null);

    ArrowType listType = new ArrowType.FixedSizeList(dimension);
    List<Field> children = new ArrayList<>();
    children.add(elementField);

    return new Field(name, new FieldType(nullable, listType, null), children);
  }

  /**
   * Create Float64 vector field (FixedSizeList<Float64>)
   *
   * @param name Field name
   * @param dimension Vector dimension
   * @param nullable Whether nullable
   * @return Arrow Field
   */
  public static Field createFloat64VectorField(String name, int dimension, boolean nullable) {
    ArrowType elementType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    Field elementField = new Field("item", new FieldType(false, elementType, null), null);

    ArrowType listType = new ArrowType.FixedSizeList(dimension);
    List<Field> children = new ArrayList<>();
    children.add(elementField);

    return new Field(name, new FieldType(nullable, listType, null), children);
  }

  /**
   * Check if field is vector type (FixedSizeList<Float32/Float64>)
   *
   * @param field Arrow Field
   * @return Whether vector type
   */
  public static boolean isVectorField(Field field) {
    ArrowType arrowType = field.getType();
    if (!(arrowType instanceof ArrowType.FixedSizeList)) {
      return false;
    }

    List<Field> children = field.getChildren();
    if (children == null || children.isEmpty()) {
      return false;
    }

    ArrowType childType = children.get(0).getType();
    if (childType instanceof ArrowType.FloatingPoint) {
      FloatingPointPrecision precision = ((ArrowType.FloatingPoint) childType).getPrecision();
      return precision == FloatingPointPrecision.SINGLE
          || precision == FloatingPointPrecision.DOUBLE;
    }

    return false;
  }

  /**
   * Get vector field dimension
   *
   * @param field Arrow Field
   * @return Vector dimension, returns -1 if not vector field
   */
  public static int getVectorDimension(Field field) {
    ArrowType arrowType = field.getType();
    if (arrowType instanceof ArrowType.FixedSizeList) {
      return ((ArrowType.FixedSizeList) arrowType).getListSize();
    }
    return -1;
  }

  /**
   * Convert Flink DataType to LogicalType
   *
   * @param dataType Flink DataType
   * @return LogicalType
   */
  public static LogicalType toLogicalType(DataType dataType) {
    return dataType.getLogicalType();
  }

  /**
   * Convert LogicalType to Flink DataType
   *
   * @param logicalType Flink LogicalType
   * @return Flink DataType
   */
  public static DataType toDataType(LogicalType logicalType) {
    if (logicalType instanceof TinyIntType) {
      return DataTypes.TINYINT();
    } else if (logicalType instanceof SmallIntType) {
      return DataTypes.SMALLINT();
    } else if (logicalType instanceof IntType) {
      return DataTypes.INT();
    } else if (logicalType instanceof BigIntType) {
      return DataTypes.BIGINT();
    } else if (logicalType instanceof FloatType) {
      return DataTypes.FLOAT();
    } else if (logicalType instanceof DoubleType) {
      return DataTypes.DOUBLE();
    } else if (logicalType instanceof VarCharType) {
      return DataTypes.STRING();
    } else if (logicalType instanceof BooleanType) {
      return DataTypes.BOOLEAN();
    } else if (logicalType instanceof VarBinaryType) {
      return DataTypes.BYTES();
    } else if (logicalType instanceof BinaryType) {
      BinaryType binaryType = (BinaryType) logicalType;
      return DataTypes.BINARY(binaryType.getLength());
    } else if (logicalType instanceof DateType) {
      return DataTypes.DATE();
    } else if (logicalType instanceof TimestampType) {
      TimestampType tsType = (TimestampType) logicalType;
      return DataTypes.TIMESTAMP(tsType.getPrecision());
    } else if (logicalType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) logicalType;
      DataType elementDataType = toDataType(arrayType.getElementType());
      return DataTypes.ARRAY(elementDataType);
    } else if (logicalType instanceof RowType) {
      RowType rowType = (RowType) logicalType;
      DataTypes.Field[] fields =
          rowType.getFields().stream()
              .map(f -> DataTypes.FIELD(f.getName(), toDataType(f.getType())))
              .toArray(DataTypes.Field[]::new);
      return DataTypes.ROW(fields);
    }

    throw new UnsupportedTypeException(
        "Unsupported LogicalType: " + logicalType.getClass().getSimpleName());
  }

  /** Get Flink Timestamp precision based on Arrow TimeUnit */
  private static int getTimestampPrecision(TimeUnit timeUnit) {
    switch (timeUnit) {
      case SECOND:
        return 0;
      case MILLISECOND:
        return 3;
      case MICROSECOND:
        return 6;
      case NANOSECOND:
        return 9;
      default:
        return 6; // Default microsecond precision
    }
  }

  /** Get Arrow TimeUnit based on Flink Timestamp precision */
  private static TimeUnit getArrowTimeUnit(int precision) {
    if (precision <= 0) {
      return TimeUnit.SECOND;
    } else if (precision <= 3) {
      return TimeUnit.MILLISECOND;
    } else if (precision <= 6) {
      return TimeUnit.MICROSECOND;
    } else {
      return TimeUnit.NANOSECOND;
    }
  }

  /** Unsupported type exception */
  public static class UnsupportedTypeException extends RuntimeException {
    public UnsupportedTypeException(String message) {
      super(message);
    }

    public UnsupportedTypeException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
