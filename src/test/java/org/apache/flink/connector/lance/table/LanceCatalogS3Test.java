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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Lance Catalog S3 integration tests.
 *
 * <p>This test class is divided into two parts:
 *
 * <ul>
 *   <li>Unit tests that don't require MinIO connection (always run)
 *   <li>Integration tests that require MinIO connection (require external MinIO service
 *       configuration)
 * </ul>
 *
 * <p>To run tests that require MinIO, set the following environment variables:
 *
 * <ul>
 *   <li>MINIO_ENDPOINT - MinIO service address, e.g., http://localhost:9000
 *   <li>MINIO_ACCESS_KEY - MinIO access key (default: minioadmin)
 *   <li>MINIO_SECRET_KEY - MinIO secret key (default: minioadmin)
 *   <li>MINIO_BUCKET - Test bucket name (default: lance-test-bucket)
 * </ul>
 *
 * <p>Quick way to start MinIO (using Docker):
 *
 * <pre>
 * docker run -p 9000:9000 -p 9001:9001 \
 *   -e "MINIO_ROOT_USER=minioadmin" \
 *   -e "MINIO_ROOT_PASSWORD=minioadmin" \
 *   minio/minio server /data --console-address ":9001"
 * </pre>
 *
 * <p>Or use a locally installed MinIO service.
 */
class LanceCatalogS3Test {

  private static final Logger LOG = LoggerFactory.getLogger(LanceCatalogS3Test.class);

  // MinIO configuration - read from environment variables or system properties
  private static String minioEndpoint;
  private static String minioAccessKey;
  private static String minioSecretKey;
  private static String testBucket;
  private static boolean minioAvailable = false;

  /** Check if MinIO is available */
  static boolean isMinioAvailable() {
    return minioAvailable;
  }

  @BeforeAll
  static void initMinioConfig() {
    // Read configuration from environment variables
    minioEndpoint = getConfigValue("MINIO_ENDPOINT", "minio.endpoint", null);
    minioAccessKey = getConfigValue("MINIO_ACCESS_KEY", "minio.access.key", "minioadmin");
    minioSecretKey = getConfigValue("MINIO_SECRET_KEY", "minio.secret.key", "minioadmin");
    testBucket = getConfigValue("MINIO_BUCKET", "minio.bucket", "lance-test-bucket");

    if (minioEndpoint != null && !minioEndpoint.isEmpty()) {
      LOG.info("MinIO configuration detected:");
      LOG.info("  Endpoint: {}", minioEndpoint);
      LOG.info("  Bucket: {}", testBucket);

      // Try to connect to MinIO to verify availability
      try {
        minioAvailable = checkMinioConnection();
        if (minioAvailable) {
          LOG.info("MinIO connection verification successful, integration tests will be enabled");
        } else {
          LOG.warn("MinIO connection verification failed, integration tests will be skipped");
        }
      } catch (Exception e) {
        LOG.warn(
            "MinIO connection check failed: {}, integration tests will be skipped", e.getMessage());
        minioAvailable = false;
      }
    } else {
      LOG.info(
          "No MinIO configuration detected (MINIO_ENDPOINT environment variable not set), integration tests will be skipped");
      LOG.info("To enable MinIO integration tests, set the following environment variables:");
      LOG.info("  export MINIO_ENDPOINT=http://localhost:9000");
      LOG.info("  export MINIO_ACCESS_KEY=minioadmin");
      LOG.info("  export MINIO_SECRET_KEY=minioadmin");
      LOG.info("  export MINIO_BUCKET=lance-test-bucket");
    }
  }

  /** Get configuration value from environment variable or system property */
  private static String getConfigValue(String envKey, String propKey, String defaultValue) {
    String value = System.getenv(envKey);
    if (value == null || value.isEmpty()) {
      value = System.getProperty(propKey, defaultValue);
    }
    return value;
  }

  /** Check if MinIO connection is available */
  private static boolean checkMinioConnection() {
    try {
      // Try to create a simple HTTP connection to check if MinIO service is available
      java.net.URL url = new java.net.URL(minioEndpoint + "/minio/health/live");
      java.net.HttpURLConnection connection = (java.net.HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.setConnectTimeout(5000);
      connection.setReadTimeout(5000);
      int responseCode = connection.getResponseCode();
      connection.disconnect();
      return responseCode == 200;
    } catch (Exception e) {
      LOG.debug("MinIO health check failed: {}", e.getMessage());
      return false;
    }
  }

  // ==================== Unit Tests That Don't Require MinIO (Always Run) ====================

  /** Unit tests that don't require MinIO connection */
  @Nested
  @DisplayName("Unit Tests - No MinIO Required")
  class UnitTests {

    // ==================== Remote Path Detection Tests ====================

    @Test
    @DisplayName("Test remote path detection - S3 protocol")
    void testRemotePathDetectionS3() {
      LanceCatalog catalog = new LanceCatalog("test", "default", "s3://bucket/path");
      assertThat(catalog.isRemoteStorage()).isTrue();
    }

    @Test
    @DisplayName("Test remote path detection - S3A protocol")
    void testRemotePathDetectionS3A() {
      LanceCatalog catalog = new LanceCatalog("test", "default", "s3a://bucket/path");
      assertThat(catalog.isRemoteStorage()).isTrue();
    }

    @Test
    @DisplayName("Test remote path detection - GCS protocol")
    void testRemotePathDetectionGCS() {
      LanceCatalog catalog = new LanceCatalog("test", "default", "gs://bucket/path");
      assertThat(catalog.isRemoteStorage()).isTrue();
    }

    @Test
    @DisplayName("Test remote path detection - Azure protocol")
    void testRemotePathDetectionAzure() {
      LanceCatalog catalog = new LanceCatalog("test", "default", "az://container/path");
      assertThat(catalog.isRemoteStorage()).isTrue();
    }

    @Test
    @DisplayName("Test local path detection")
    void testLocalPathDetection() {
      LanceCatalog catalog = new LanceCatalog("test", "default", "/tmp/local/path");
      assertThat(catalog.isRemoteStorage()).isFalse();
    }

    // ==================== Factory Tests ====================

    @Test
    @DisplayName("Test LanceCatalogFactory S3 configuration options")
    void testCatalogFactoryS3Options() {
      LanceCatalogFactory factory = new LanceCatalogFactory();

      Set<String> optionalOptionKeys = new HashSet<>();
      factory.optionalOptions().forEach(opt -> optionalOptionKeys.add(opt.key()));

      // Verify S3 related options exist
      assertThat(optionalOptionKeys)
          .contains(
              "s3-access-key",
              "s3-secret-key",
              "s3-region",
              "s3-endpoint",
              "s3-virtual-hosted-style",
              "s3-allow-http");
    }

    @Test
    @DisplayName("Test S3 configuration options default values")
    void testS3ConfigOptionsDefaults() {
      assertThat(LanceCatalogFactory.S3_VIRTUAL_HOSTED_STYLE.defaultValue()).isTrue();
      assertThat(LanceCatalogFactory.S3_ALLOW_HTTP.defaultValue()).isFalse();
    }

    @Test
    @DisplayName("Test S3 configuration options descriptions")
    void testS3ConfigOptionsDescriptions() {
      // Verify configuration options exist and have descriptions
      assertThat(LanceCatalogFactory.S3_ACCESS_KEY.key()).isEqualTo("s3-access-key");
      assertThat(LanceCatalogFactory.S3_SECRET_KEY.key()).isEqualTo("s3-secret-key");
      assertThat(LanceCatalogFactory.S3_REGION.key()).isEqualTo("s3-region");
      assertThat(LanceCatalogFactory.S3_ENDPOINT.key()).isEqualTo("s3-endpoint");

      // Verify descriptions are not null
      assertThat(LanceCatalogFactory.S3_ACCESS_KEY.description()).isNotNull();
      assertThat(LanceCatalogFactory.S3_SECRET_KEY.description()).isNotNull();
      assertThat(LanceCatalogFactory.S3_REGION.description()).isNotNull();
      assertThat(LanceCatalogFactory.S3_ENDPOINT.description()).isNotNull();
    }

    // ==================== Path Normalization Tests ====================

    @Test
    @DisplayName("Test warehouse path normalization - remove trailing slashes")
    void testWarehousePathNormalization() {
      LanceCatalog catalog1 = new LanceCatalog("test", "default", "s3://bucket/path/");
      assertThat(catalog1.getWarehouse()).isEqualTo("s3://bucket/path");

      LanceCatalog catalog2 = new LanceCatalog("test", "default", "s3://bucket/path///");
      assertThat(catalog2.getWarehouse()).isEqualTo("s3://bucket/path");
    }

    @Test
    @DisplayName("Test warehouse path normalization - preserve root path")
    void testWarehousePathNormalizationRoot() {
      LanceCatalog catalog = new LanceCatalog("test", "default", "s3://bucket");
      assertThat(catalog.getWarehouse()).isEqualTo("s3://bucket");
    }

    // ==================== Edge Case Tests ====================

    @Test
    @DisplayName("Test S3 path with empty storage options")
    void testS3PathWithEmptyOptions() {
      LanceCatalog catalog =
          new LanceCatalog("test", "default", "s3://bucket/path", Collections.emptyMap());

      assertThat(catalog.isRemoteStorage()).isTrue();
      assertThat(catalog.getStorageOptions()).isEmpty();
    }

    @Test
    @DisplayName("Test null storage options")
    void testNullStorageOptions() {
      LanceCatalog catalog = new LanceCatalog("test", "default", "s3://bucket/path", null);

      assertThat(catalog.isRemoteStorage()).isTrue();
      assertThat(catalog.getStorageOptions()).isEmpty();
    }

    @Test
    @DisplayName("Test storage options immutability")
    void testStorageOptionsImmutability() {
      Map<String, String> originalOptions = new HashMap<>();
      originalOptions.put("key", "value");

      LanceCatalog catalog =
          new LanceCatalog("test", "default", "s3://bucket/path", originalOptions);

      // Modifying original map should not affect catalog internal options
      originalOptions.put("new_key", "new_value");

      assertThat(catalog.getStorageOptions()).doesNotContainKey("new_key");
    }

    @Test
    @DisplayName("Test getStorageOptions returns unmodifiable Map")
    void testGetStorageOptionsReturnsUnmodifiable() {
      Map<String, String> storageOptions = new HashMap<>();
      storageOptions.put("key", "value");

      LanceCatalog catalog =
          new LanceCatalog("test", "default", "s3://bucket/path", storageOptions);

      Map<String, String> returnedOptions = catalog.getStorageOptions();

      // Attempting to modify returned map should throw exception
      assertThatThrownBy(() -> returnedOptions.put("new_key", "new_value"))
          .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName("Test S3 Catalog basic properties (no connection required)")
    void testS3CatalogBasicProperties() {
      Map<String, String> storageOptions = new HashMap<>();
      storageOptions.put("aws_access_key_id", "test_key");
      storageOptions.put("aws_secret_access_key", "test_secret");
      storageOptions.put("aws_region", "us-east-1");

      LanceCatalog catalog =
          new LanceCatalog("test_catalog", "default", "s3://test-bucket/warehouse", storageOptions);

      assertThat(catalog.getName()).isEqualTo("test_catalog");
      assertThat(catalog.getDefaultDatabase()).isEqualTo("default");
      assertThat(catalog.getWarehouse()).isEqualTo("s3://test-bucket/warehouse");
      assertThat(catalog.isRemoteStorage()).isTrue();
      assertThat(catalog.getStorageOptions()).containsEntry("aws_access_key_id", "test_key");
    }
  }

  // ==================== Integration Tests That Require MinIO ====================

  /**
   * Integration tests that require MinIO connection. Only run when MINIO_ENDPOINT environment
   * variable is set and MinIO service is available.
   */
  @Nested
  @DisplayName("Integration Tests - MinIO Required")
  @EnabledIf("org.apache.flink.connector.lance.table.LanceCatalogS3Test#isMinioAvailable")
  class MinioIntegrationTests {

    private LanceCatalog s3Catalog;
    private String warehousePath;
    private String testId;

    @BeforeEach
    void setUp() throws Exception {
      // Generate unique path for each test to avoid interference between tests
      testId = UUID.randomUUID().toString().substring(0, 8);
      warehousePath = String.format("s3://%s/lance-warehouse-%s", testBucket, testId);

      // Create Catalog with S3 configuration
      Map<String, String> storageOptions = new HashMap<>();
      storageOptions.put("aws_access_key_id", minioAccessKey);
      storageOptions.put("aws_secret_access_key", minioSecretKey);
      storageOptions.put("aws_region", "us-east-1");
      storageOptions.put("aws_endpoint", minioEndpoint);
      storageOptions.put("aws_virtual_hosted_style_request", "false");
      storageOptions.put("allow_http", "true");

      s3Catalog = new LanceCatalog("lance_s3_catalog", "default", warehousePath, storageOptions);
      s3Catalog.open();

      LOG.info("Test Catalog created, warehouse: {}", warehousePath);
    }

    @AfterEach
    void tearDown() throws Exception {
      if (s3Catalog != null) {
        s3Catalog.close();
      }
    }

    // ==================== Basic Properties Tests ====================

    @Test
    @DisplayName("Test S3 Catalog basic properties")
    void testS3CatalogProperties() {
      assertThat(s3Catalog.getName()).isEqualTo("lance_s3_catalog");
      assertThat(s3Catalog.getDefaultDatabase()).isEqualTo("default");
      assertThat(s3Catalog.getWarehouse()).isEqualTo(warehousePath);
      assertThat(s3Catalog.isRemoteStorage()).isTrue();
    }

    @Test
    @DisplayName("Test S3 storage options configuration")
    void testS3StorageOptions() {
      Map<String, String> options = s3Catalog.getStorageOptions();

      assertThat(options).containsEntry("aws_access_key_id", minioAccessKey);
      assertThat(options).containsEntry("aws_secret_access_key", minioSecretKey);
      assertThat(options).containsEntry("aws_region", "us-east-1");
      assertThat(options).containsEntry("aws_endpoint", minioEndpoint);
      assertThat(options).containsEntry("allow_http", "true");
    }

    // ==================== Database Operation Tests ====================

    @Test
    @DisplayName("Test S3 Catalog default database exists")
    void testDefaultDatabaseExists() throws Exception {
      assertThat(s3Catalog.databaseExists("default")).isTrue();
    }

    @Test
    @DisplayName("Test S3 Catalog list databases")
    void testListDatabases() throws Exception {
      List<String> databases = s3Catalog.listDatabases();
      assertThat(databases).contains("default");
    }

    @Test
    @DisplayName("Test S3 Catalog create database")
    void testCreateDatabase() throws Exception {
      String dbName = "test_s3_db_" + testId;

      // Create database
      s3Catalog.createDatabase(dbName, null, false);

      // Verify database exists
      assertThat(s3Catalog.databaseExists(dbName)).isTrue();

      // Verify database in list
      List<String> databases = s3Catalog.listDatabases();
      assertThat(databases).contains(dbName);
    }

    @Test
    @DisplayName("Test S3 Catalog create existing database (ignoreIfExists=false)")
    void testCreateExistingDatabaseWithoutIgnore() throws Exception {
      String dbName = "existing_db_" + testId;
      s3Catalog.createDatabase(dbName, null, false);

      // Creating again should throw exception
      assertThatThrownBy(() -> s3Catalog.createDatabase(dbName, null, false))
          .isInstanceOf(DatabaseAlreadyExistException.class);
    }

    @Test
    @DisplayName("Test S3 Catalog create existing database (ignoreIfExists=true)")
    void testCreateExistingDatabaseWithIgnore() throws Exception {
      String dbName = "existing_db_2_" + testId;
      s3Catalog.createDatabase(dbName, null, false);

      // Creating again should not throw exception
      s3Catalog.createDatabase(dbName, null, true);

      assertThat(s3Catalog.databaseExists(dbName)).isTrue();
    }

    @Test
    @DisplayName("Test S3 Catalog get database")
    void testGetDatabase() throws Exception {
      String dbName = "get_db_test_" + testId;
      s3Catalog.createDatabase(dbName, null, false);

      CatalogDatabase database = s3Catalog.getDatabase(dbName);
      assertThat(database).isNotNull();
      assertThat(database.getComment()).contains("Lance Database");
    }

    @Test
    @DisplayName("Test S3 Catalog get non-existing database")
    void testGetNonExistingDatabase() {
      assertThatThrownBy(() -> s3Catalog.getDatabase("non_existing_db_" + testId))
          .isInstanceOf(DatabaseNotExistException.class);
    }

    @Test
    @DisplayName("Test S3 Catalog drop database")
    void testDropDatabase() throws Exception {
      String dbName = "drop_db_test_" + testId;
      s3Catalog.createDatabase(dbName, null, false);
      assertThat(s3Catalog.databaseExists(dbName)).isTrue();

      // Drop database
      s3Catalog.dropDatabase(dbName, false, false);

      // Verify database not in list
      List<String> databases = s3Catalog.listDatabases();
      assertThat(databases).doesNotContain(dbName);
    }

    @Test
    @DisplayName("Test S3 Catalog drop non-existing database (ignoreIfNotExists=false)")
    void testDropNonExistingDatabaseWithoutIgnore() {
      assertThatThrownBy(
              () -> s3Catalog.dropDatabase("non_existing_drop_db_" + testId, false, false))
          .isInstanceOf(DatabaseNotExistException.class);
    }

    @Test
    @DisplayName("Test S3 Catalog drop non-existing database (ignoreIfNotExists=true)")
    void testDropNonExistingDatabaseWithIgnore() throws Exception {
      // Should not throw exception
      s3Catalog.dropDatabase("non_existing_drop_db_2_" + testId, true, false);
    }

    // ==================== Table Operation Tests ====================

    @Test
    @DisplayName("Test S3 Catalog list tables (empty database)")
    void testListTablesEmpty() throws Exception {
      String dbName = "empty_tables_db_" + testId;
      s3Catalog.createDatabase(dbName, null, false);

      List<String> tables = s3Catalog.listTables(dbName);
      assertThat(tables).isEmpty();
    }

    @Test
    @DisplayName("Test S3 Catalog table not exists")
    void testTableNotExists() throws Exception {
      String dbName = "table_check_db_" + testId;
      s3Catalog.createDatabase(dbName, null, false);

      assertThat(
              s3Catalog.tableExists(
                  new org.apache.flink.table.catalog.ObjectPath(dbName, "non_existing_table")))
          .isFalse();
    }

    // ==================== SQL DDL Create Catalog Tests ====================

    @Test
    @DisplayName("Test creating S3 Catalog via SQL DDL")
    void testCreateS3CatalogViaSql() throws Exception {
      EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
      TableEnvironment tableEnv = TableEnvironment.create(settings);

      String catalogName = "lance_s3_sql_" + testId;

      // Create S3 Catalog using SQL
      String createCatalogSql =
          String.format(
              "CREATE CATALOG %s WITH ("
                  + "'type' = 'lance', "
                  + "'warehouse' = '%s', "
                  + "'default-database' = 'default', "
                  + "'s3-access-key' = '%s', "
                  + "'s3-secret-key' = '%s', "
                  + "'s3-region' = 'us-east-1', "
                  + "'s3-endpoint' = '%s', "
                  + "'s3-allow-http' = 'true', "
                  + "'s3-virtual-hosted-style' = 'false'"
                  + ")",
              catalogName, warehousePath, minioAccessKey, minioSecretKey, minioEndpoint);

      tableEnv.executeSql(createCatalogSql);

      // Verify Catalog was created
      String[] catalogs = tableEnv.listCatalogs();
      assertThat(catalogs).contains(catalogName);

      // Use Catalog
      tableEnv.useCatalog(catalogName);
      assertThat(tableEnv.getCurrentCatalog()).isEqualTo(catalogName);

      // Verify default database
      assertThat(tableEnv.getCurrentDatabase()).isEqualTo("default");
    }

    @Test
    @DisplayName("Test creating database in S3 Catalog via SQL DDL")
    void testCreateDatabaseViaSql() throws Exception {
      EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
      TableEnvironment tableEnv = TableEnvironment.create(settings);

      String catalogName = "lance_s3_db_sql_" + testId;

      // Create S3 Catalog
      String createCatalogSql =
          String.format(
              "CREATE CATALOG %s WITH ("
                  + "'type' = 'lance', "
                  + "'warehouse' = '%s', "
                  + "'s3-access-key' = '%s', "
                  + "'s3-secret-key' = '%s', "
                  + "'s3-region' = 'us-east-1', "
                  + "'s3-endpoint' = '%s', "
                  + "'s3-allow-http' = 'true', "
                  + "'s3-virtual-hosted-style' = 'false'"
                  + ")",
              catalogName, warehousePath, minioAccessKey, minioSecretKey, minioEndpoint);

      tableEnv.executeSql(createCatalogSql);
      tableEnv.useCatalog(catalogName);

      // Create database
      String dbName = "test_database_" + testId;
      tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + dbName);

      // Verify database was created
      String[] databases = tableEnv.listDatabases();
      assertThat(databases).contains(dbName);
    }

    // ==================== Multiple Catalog Tests ====================

    @Test
    @DisplayName("Test multiple S3 Catalog instances")
    void testMultipleS3Catalogs() throws Exception {
      Map<String, String> storageOptions = new HashMap<>();
      storageOptions.put("aws_access_key_id", minioAccessKey);
      storageOptions.put("aws_secret_access_key", minioSecretKey);
      storageOptions.put("aws_region", "us-east-1");
      storageOptions.put("aws_endpoint", minioEndpoint);
      storageOptions.put("allow_http", "true");

      // Create first Catalog
      LanceCatalog catalog1 =
          new LanceCatalog(
              "catalog1",
              "default",
              "s3://" + testBucket + "/warehouse1_" + testId,
              storageOptions);
      catalog1.open();

      // Create second Catalog
      LanceCatalog catalog2 =
          new LanceCatalog(
              "catalog2",
              "default",
              "s3://" + testBucket + "/warehouse2_" + testId,
              storageOptions);
      catalog2.open();

      try {
        // Verify two Catalogs work independently
        assertThat(catalog1.getWarehouse()).isNotEqualTo(catalog2.getWarehouse());

        String db1 = "db1_" + testId;
        String db2 = "db2_" + testId;

        catalog1.createDatabase(db1, null, false);
        catalog2.createDatabase(db2, null, false);

        assertThat(catalog1.listDatabases()).contains(db1);
        assertThat(catalog2.listDatabases()).contains(db2);
        assertThat(catalog1.listDatabases()).doesNotContain(db2);
        assertThat(catalog2.listDatabases()).doesNotContain(db1);
      } finally {
        catalog1.close();
        catalog2.close();
      }
    }
  }
}
