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

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Lance SQL integration tests. */
class LanceSqlITCase {

  @TempDir Path tempDir;

  private String datasetPath;
  private String warehousePath;

  @BeforeEach
  void setUp() {
    datasetPath = tempDir.resolve("test_sql_dataset").toString();
    warehousePath = tempDir.resolve("test_warehouse").toString();
  }

  @Test
  @DisplayName("Test LanceDynamicTableFactory identifier")
  void testFactoryIdentifier() {
    LanceDynamicTableFactory factory = new LanceDynamicTableFactory();
    assertThat(factory.factoryIdentifier()).isEqualTo("lance");
  }

  @Test
  @DisplayName("Test LanceDynamicTableFactory required options - path is now optional")
  void testRequiredOptions() {
    LanceDynamicTableFactory factory = new LanceDynamicTableFactory();
    Set<String> requiredOptionKeys = new HashSet<>();
    factory.requiredOptions().forEach(opt -> requiredOptionKeys.add(opt.key()));

    // path is no longer required (Catalog mode injects it automatically)
    assertThat(requiredOptionKeys).isEmpty();
  }

  @Test
  @DisplayName("Test LanceDynamicTableFactory optional options")
  void testOptionalOptions() {
    LanceDynamicTableFactory factory = new LanceDynamicTableFactory();
    Set<String> optionalOptionKeys = new HashSet<>();
    factory.optionalOptions().forEach(opt -> optionalOptionKeys.add(opt.key()));

    assertThat(optionalOptionKeys)
        .contains(
            "path",
            "read.batch-size",
            "read.columns",
            "read.filter",
            "write.batch-size",
            "write.mode",
            "write.max-rows-per-file",
            "index.type",
            "index.column",
            "vector.column",
            "vector.metric",
            "s3-access-key",
            "s3-secret-key",
            "s3-region",
            "s3-endpoint");
  }

  @Test
  @DisplayName("Test LanceDynamicTableSource creation")
  void testDynamicTableSourceCreation() {
    LanceOptions options = LanceOptions.builder().path(datasetPath).readBatchSize(512).build();

    List<RowType.RowField> fields = new ArrayList<>();
    fields.add(new RowType.RowField("id", new BigIntType()));
    fields.add(new RowType.RowField("content", new VarCharType()));
    fields.add(new RowType.RowField("embedding", new ArrayType(new FloatType())));
    RowType rowType = new RowType(fields);

    DataType dataType =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("content", DataTypes.STRING()),
            DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())));

    LanceDynamicTableSource source = new LanceDynamicTableSource(options, dataType);

    assertThat(source.getOptions()).isEqualTo(options);
    assertThat(source.getPhysicalDataType()).isEqualTo(dataType);
    assertThat(source.asSummaryString()).isEqualTo("Lance Table Source");
  }

  @Test
  @DisplayName("Test LanceDynamicTableSink creation")
  void testDynamicTableSinkCreation() {
    LanceOptions options =
        LanceOptions.builder()
            .path(datasetPath)
            .writeBatchSize(256)
            .writeMode(LanceOptions.WriteMode.APPEND)
            .build();

    DataType dataType =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("content", DataTypes.STRING()),
            DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())));

    LanceDynamicTableSink sink = new LanceDynamicTableSink(options, dataType);

    assertThat(sink.getOptions()).isEqualTo(options);
    assertThat(sink.getPhysicalDataType()).isEqualTo(dataType);
    assertThat(sink.asSummaryString()).isEqualTo("Lance Table Sink");
  }

  @Test
  @DisplayName("Test LanceDynamicTableSource copy")
  void testDynamicTableSourceCopy() {
    LanceOptions options = LanceOptions.builder().path(datasetPath).build();

    DataType dataType = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT()));

    LanceDynamicTableSource source = new LanceDynamicTableSource(options, dataType);
    LanceDynamicTableSource copiedSource = (LanceDynamicTableSource) source.copy();

    assertThat(copiedSource).isNotSameAs(source);
    assertThat(copiedSource.getOptions()).isEqualTo(source.getOptions());
  }

  @Test
  @DisplayName("Test LanceDynamicTableSink copy")
  void testDynamicTableSinkCopy() {
    LanceOptions options = LanceOptions.builder().path(datasetPath).build();

    DataType dataType = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT()));

    LanceDynamicTableSink sink = new LanceDynamicTableSink(options, dataType);
    LanceDynamicTableSink copiedSink = (LanceDynamicTableSink) sink.copy();

    assertThat(copiedSink).isNotSameAs(sink);
    assertThat(copiedSink.getOptions()).isEqualTo(sink.getOptions());
  }

  @Test
  @DisplayName("Test LanceCatalogFactory identifier")
  void testCatalogFactoryIdentifier() {
    LanceCatalogFactory factory = new LanceCatalogFactory();
    assertThat(factory.factoryIdentifier()).isEqualTo("lance");
  }

  @Test
  @DisplayName("Test LanceCatalogFactory required options")
  void testCatalogRequiredOptions() {
    LanceCatalogFactory factory = new LanceCatalogFactory();
    Set<String> requiredOptionKeys = new HashSet<>();
    factory.requiredOptions().forEach(opt -> requiredOptionKeys.add(opt.key()));

    assertThat(requiredOptionKeys).contains("warehouse");
  }

  @Test
  @DisplayName("Test LanceCatalogFactory optional options")
  void testCatalogOptionalOptions() {
    LanceCatalogFactory factory = new LanceCatalogFactory();
    Set<String> optionalOptionKeys = new HashSet<>();
    factory.optionalOptions().forEach(opt -> optionalOptionKeys.add(opt.key()));

    assertThat(optionalOptionKeys).contains("default-database");
  }

  @Test
  @DisplayName("Test LanceCatalog creation and basic operations")
  void testLanceCatalogBasicOperations() throws Exception {
    LanceCatalog catalog = new LanceCatalog("test_catalog", "default", warehousePath);

    try {
      catalog.open();

      // Verify default database exists
      assertThat(catalog.databaseExists("default")).isTrue();

      // List databases
      List<String> databases = catalog.listDatabases();
      assertThat(databases).contains("default");

      // Create new database
      catalog.createDatabase("test_db", null, false);
      assertThat(catalog.databaseExists("test_db")).isTrue();

      // List tables (empty)
      List<String> tables = catalog.listTables("test_db");
      assertThat(tables).isEmpty();

      // Drop database
      catalog.dropDatabase("test_db", false, true);
      assertThat(catalog.databaseExists("test_db")).isFalse();

    } finally {
      catalog.close();
    }
  }

  @Test
  @DisplayName("Test LanceCatalog warehouse path")
  void testLanceCatalogWarehouse() throws Exception {
    LanceCatalog catalog = new LanceCatalog("test", "default", warehousePath);

    try {
      catalog.open();
      assertThat(catalog.getWarehouse()).isEqualTo(warehousePath);
    } finally {
      catalog.close();
    }
  }

  @Test
  @DisplayName("Test configuration options definition")
  void testConfigOptions() {
    assertThat(LanceDynamicTableFactory.PATH.key()).isEqualTo("path");
    assertThat(LanceDynamicTableFactory.READ_BATCH_SIZE.key()).isEqualTo("read.batch-size");
    assertThat(LanceDynamicTableFactory.READ_BATCH_SIZE.defaultValue()).isEqualTo(1024);
    assertThat(LanceDynamicTableFactory.WRITE_BATCH_SIZE.key()).isEqualTo("write.batch-size");
    assertThat(LanceDynamicTableFactory.WRITE_MODE.key()).isEqualTo("write.mode");
    assertThat(LanceDynamicTableFactory.WRITE_MODE.defaultValue()).isEqualTo("append");
    assertThat(LanceDynamicTableFactory.INDEX_TYPE.key()).isEqualTo("index.type");
    assertThat(LanceDynamicTableFactory.INDEX_TYPE.defaultValue()).isEqualTo("IVF_PQ");
    assertThat(LanceDynamicTableFactory.VECTOR_METRIC.key()).isEqualTo("vector.metric");
    assertThat(LanceDynamicTableFactory.VECTOR_METRIC.defaultValue()).isEqualTo("L2");
  }

  @Test
  @DisplayName("Test Catalog configuration options definition")
  void testCatalogConfigOptions() {
    assertThat(LanceCatalogFactory.WAREHOUSE.key()).isEqualTo("warehouse");
    assertThat(LanceCatalogFactory.DEFAULT_DATABASE.key()).isEqualTo("default-database");
    assertThat(LanceCatalogFactory.DEFAULT_DATABASE.defaultValue()).isEqualTo("default");
  }

  @Test
  @DisplayName("Test S3 Catalog configuration options definition")
  void testS3CatalogConfigOptions() {
    // S3 configuration options
    assertThat(LanceCatalogFactory.S3_ACCESS_KEY.key()).isEqualTo("s3-access-key");
    assertThat(LanceCatalogFactory.S3_SECRET_KEY.key()).isEqualTo("s3-secret-key");
    assertThat(LanceCatalogFactory.S3_REGION.key()).isEqualTo("s3-region");
    assertThat(LanceCatalogFactory.S3_ENDPOINT.key()).isEqualTo("s3-endpoint");
    assertThat(LanceCatalogFactory.S3_VIRTUAL_HOSTED_STYLE.key())
        .isEqualTo("s3-virtual-hosted-style");
    assertThat(LanceCatalogFactory.S3_ALLOW_HTTP.key()).isEqualTo("s3-allow-http");

    // Default values
    assertThat(LanceCatalogFactory.S3_VIRTUAL_HOSTED_STYLE.defaultValue()).isTrue();
    assertThat(LanceCatalogFactory.S3_ALLOW_HTTP.defaultValue()).isFalse();
  }

  @Test
  @DisplayName("Test LanceCatalog S3 remote storage detection")
  void testLanceCatalogRemoteStorageDetection() {
    // S3 path should be identified as remote storage
    LanceCatalog s3Catalog = new LanceCatalog("test", "default", "s3://bucket/path");
    assertThat(s3Catalog.isRemoteStorage()).isTrue();

    // S3A path
    LanceCatalog s3aCatalog = new LanceCatalog("test", "default", "s3a://bucket/path");
    assertThat(s3aCatalog.isRemoteStorage()).isTrue();

    // GCS path
    LanceCatalog gcsCatalog = new LanceCatalog("test", "default", "gs://bucket/path");
    assertThat(gcsCatalog.isRemoteStorage()).isTrue();

    // Azure path
    LanceCatalog azCatalog = new LanceCatalog("test", "default", "az://container/path");
    assertThat(azCatalog.isRemoteStorage()).isTrue();

    // Local path should be identified as local storage
    LanceCatalog localCatalog = new LanceCatalog("test", "default", warehousePath);
    assertThat(localCatalog.isRemoteStorage()).isFalse();
  }

  @Test
  @DisplayName("Test LanceCatalog construction with storage options")
  void testLanceCatalogWithStorageOptions() {
    Map<String, String> storageOptions = new HashMap<>();
    storageOptions.put("aws_access_key_id", "test-key");
    storageOptions.put("aws_secret_access_key", "test-secret");
    storageOptions.put("aws_region", "us-east-1");

    LanceCatalog catalog =
        new LanceCatalog("test_catalog", "default", "s3://bucket/warehouse", storageOptions);

    assertThat(catalog.getStorageOptions()).containsEntry("aws_access_key_id", "test-key");
    assertThat(catalog.getStorageOptions()).containsEntry("aws_secret_access_key", "test-secret");
    assertThat(catalog.getStorageOptions()).containsEntry("aws_region", "us-east-1");
  }

  @Test
  @DisplayName("Test vector search UDF configuration")
  void testVectorSearchFunctionConfiguration() {
    LanceVectorSearchFunction function = new LanceVectorSearchFunction();
    assertThat(function).isNotNull();
  }

  // ==================== CREATE TABLE with Namespace Integration Tests ====================

  @Test
  @DisplayName("Test CREATE TABLE via Catalog creates empty Lance Dataset")
  void testCreateTableViaCatalogCreatesDataset() throws Exception {
    LanceCatalog catalog = new LanceCatalog("test_catalog", "default", warehousePath);

    try {
      catalog.open();

      // Build a CatalogTable with schema
      Schema schema =
          Schema.newBuilder()
              .column("id", DataTypes.BIGINT())
              .column("name", DataTypes.STRING())
              .column("score", DataTypes.DOUBLE())
              .build();
      CatalogTable catalogTable =
          CatalogTable.of(schema, "test table", Collections.emptyList(), Collections.emptyMap());

      ObjectPath tablePath = new ObjectPath("default", "users");

      // Create table
      catalog.createTable(tablePath, catalogTable, false);

      // Table should now exist
      assertThat(catalog.tableExists(tablePath)).isTrue();

      // Should appear in listTables
      List<String> tables = catalog.listTables("default");
      assertThat(tables).contains("users");

      // getTable should return valid CatalogTable with correct schema
      CatalogBaseTable retrievedTable = catalog.getTable(tablePath);
      assertThat(retrievedTable).isInstanceOf(CatalogTable.class);

      // Verify the returned table has correct options
      Map<String, String> options = retrievedTable.getOptions();
      assertThat(options).containsKey("connector");
      assertThat(options.get("connector")).isEqualTo("lance");
      assertThat(options).containsKey("path");

    } finally {
      catalog.close();
    }
  }

  @Test
  @DisplayName("Test CREATE TABLE via Catalog preserves user options")
  void testCreateTablePreservesUserOptions() throws Exception {
    LanceCatalog catalog = new LanceCatalog("test_catalog", "default", warehousePath);

    try {
      catalog.open();

      Schema schema =
          Schema.newBuilder()
              .column("id", DataTypes.BIGINT())
              .column("value", DataTypes.STRING())
              .build();

      Map<String, String> userOptions = new HashMap<>();
      userOptions.put("write.batch-size", "256");
      userOptions.put("write.mode", "overwrite");

      CatalogTable catalogTable =
          CatalogTable.of(schema, "test table", Collections.emptyList(), userOptions);

      ObjectPath tablePath = new ObjectPath("default", "my_table");
      catalog.createTable(tablePath, catalogTable, false);

      // getTable should return merged options
      CatalogBaseTable retrievedTable = catalog.getTable(tablePath);
      Map<String, String> options = retrievedTable.getOptions();
      assertThat(options.get("write.batch-size")).isEqualTo("256");
      assertThat(options.get("write.mode")).isEqualTo("overwrite");
      // Connector and path should be set by catalog
      assertThat(options.get("connector")).isEqualTo("lance");
      assertThat(options).containsKey("path");

    } finally {
      catalog.close();
    }
  }

  @Test
  @DisplayName("Test CREATE TABLE twice throws TableAlreadyExistException")
  void testCreateTableDuplicate() throws Exception {
    LanceCatalog catalog = new LanceCatalog("test_catalog", "default", warehousePath);

    try {
      catalog.open();

      Schema schema = Schema.newBuilder().column("id", DataTypes.BIGINT()).build();
      CatalogTable catalogTable =
          CatalogTable.of(schema, "test", Collections.emptyList(), Collections.emptyMap());

      ObjectPath tablePath = new ObjectPath("default", "dup_table");
      catalog.createTable(tablePath, catalogTable, false);

      // Second create should throw
      assertThatThrownBy(() -> catalog.createTable(tablePath, catalogTable, false))
          .isInstanceOf(TableAlreadyExistException.class);

      // With ignoreIfExists=true, should not throw
      catalog.createTable(tablePath, catalogTable, true);

    } finally {
      catalog.close();
    }
  }

  @Test
  @DisplayName("Test DROP TABLE after CREATE TABLE")
  void testDropTableAfterCreate() throws Exception {
    LanceCatalog catalog = new LanceCatalog("test_catalog", "default", warehousePath);

    try {
      catalog.open();

      Schema schema =
          Schema.newBuilder()
              .column("id", DataTypes.BIGINT())
              .column("name", DataTypes.STRING())
              .build();
      CatalogTable catalogTable =
          CatalogTable.of(schema, "drop test", Collections.emptyList(), Collections.emptyMap());

      ObjectPath tablePath = new ObjectPath("default", "to_drop");
      catalog.createTable(tablePath, catalogTable, false);
      assertThat(catalog.tableExists(tablePath)).isTrue();

      // Drop table
      catalog.dropTable(tablePath, false);
      assertThat(catalog.tableExists(tablePath)).isFalse();

      // Drop again should throw
      assertThatThrownBy(() -> catalog.dropTable(tablePath, false))
          .isInstanceOf(TableNotExistException.class);

    } finally {
      catalog.close();
    }
  }

  @Test
  @DisplayName("Test CREATE TABLE with vector column")
  void testCreateTableWithVectorColumn() throws Exception {
    LanceCatalog catalog = new LanceCatalog("test_catalog", "default", warehousePath);

    try {
      catalog.open();

      Schema schema =
          Schema.newBuilder()
              .column("id", DataTypes.BIGINT())
              .column("content", DataTypes.STRING())
              .column("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
              .build();
      CatalogTable catalogTable =
          CatalogTable.of(schema, "vector table", Collections.emptyList(), Collections.emptyMap());

      ObjectPath tablePath = new ObjectPath("default", "vector_table");
      catalog.createTable(tablePath, catalogTable, false);

      assertThat(catalog.tableExists(tablePath)).isTrue();

      // Verify schema round-trip
      CatalogBaseTable retrieved = catalog.getTable(tablePath);
      assertThat(retrieved).isInstanceOf(CatalogTable.class);

    } finally {
      catalog.close();
    }
  }

  @Test
  @DisplayName("Test CREATE TABLE in non-default database")
  void testCreateTableInCustomDatabase() throws Exception {
    LanceCatalog catalog = new LanceCatalog("test_catalog", "default", warehousePath);

    try {
      catalog.open();

      // Create a custom database first
      catalog.createDatabase("mydb", null, false);

      Schema schema =
          Schema.newBuilder()
              .column("id", DataTypes.BIGINT())
              .column("data", DataTypes.STRING())
              .build();
      CatalogTable catalogTable =
          CatalogTable.of(
              schema, "custom db table", Collections.emptyList(), Collections.emptyMap());

      ObjectPath tablePath = new ObjectPath("mydb", "my_table");
      catalog.createTable(tablePath, catalogTable, false);

      assertThat(catalog.tableExists(tablePath)).isTrue();
      assertThat(catalog.listTables("mydb")).contains("my_table");

      // Verify path contains database name
      CatalogBaseTable retrieved = catalog.getTable(tablePath);
      String path = retrieved.getOptions().get("path");
      assertThat(path).contains("mydb");
      assertThat(path).contains("my_table");

    } finally {
      catalog.close();
    }
  }

  @Test
  @DisplayName("Test DynamicTableFactory S3 options are declared")
  void testDynamicTableFactoryS3Options() {
    LanceDynamicTableFactory factory = new LanceDynamicTableFactory();
    Set<String> optionalOptionKeys = new HashSet<>();
    factory.optionalOptions().forEach(opt -> optionalOptionKeys.add(opt.key()));

    assertThat(optionalOptionKeys)
        .contains("s3-access-key", "s3-secret-key", "s3-region", "s3-endpoint");
  }
}
