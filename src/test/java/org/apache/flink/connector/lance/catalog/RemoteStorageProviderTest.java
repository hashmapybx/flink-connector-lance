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
package org.apache.flink.connector.lance.catalog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link RemoteStorageProvider}. */
class RemoteStorageProviderTest {

  private LanceCatalogPathResolver pathResolver;
  private RemoteStorageProvider provider;

  @BeforeEach
  void setup() {
    pathResolver = new LanceCatalogPathResolver("s3://test-bucket/warehouse");
    Map<String, String> opts = new HashMap<>();
    opts.put("aws_access_key_id", "AKID_TEST");
    opts.put("aws_secret_access_key", "SECRET_TEST");
    provider = new RemoteStorageProvider(pathResolver, opts);
  }

  @Nested
  @DisplayName("initializeWarehouse")
  class InitializeTests {

    @Test
    @DisplayName("Registers default database")
    void testInitialize() {
      provider.initializeWarehouse("default");
      assertThat(provider.getKnownDatabases()).contains("default");
    }
  }

  @Nested
  @DisplayName("Database Operations")
  class DatabaseTests {

    @BeforeEach
    void init() {
      provider.initializeWarehouse("default");
    }

    @Test
    @DisplayName("listDatabases returns registered databases")
    void testListDatabases() throws IOException {
      provider.createDatabase("db1");
      provider.createDatabase("db2");

      List<String> databases = provider.listDatabases();
      assertThat(databases).contains("default", "db1", "db2");
    }

    @Test
    @DisplayName("databaseExists returns true for any database (remote assumption)")
    void testDatabaseExists() {
      // Remote storage always returns true for databaseExists
      assertThat(provider.databaseExists("anything")).isTrue();
    }

    @Test
    @DisplayName("createDatabase registers new database")
    void testCreateDatabase() throws IOException {
      provider.createDatabase("newdb");
      assertThat(provider.getKnownDatabases()).contains("newdb");
    }

    @Test
    @DisplayName("dropDatabase removes database and its tables when cascade")
    void testDropDatabaseCascade() throws IOException {
      provider.createDatabase("dropme");
      provider.registerTable("dropme", "t1");
      provider.registerTable("dropme", "t2");

      provider.dropDatabase("dropme", true);

      assertThat(provider.getKnownDatabases()).doesNotContain("dropme");
      assertThat(provider.getKnownTables()).doesNotContain("dropme/t1", "dropme/t2");
    }

    @Test
    @DisplayName("dropDatabase without cascade keeps tables untouched")
    void testDropDatabaseNoCascade() throws IOException {
      provider.createDatabase("dropme");
      provider.registerTable("dropme", "t1");

      provider.dropDatabase("dropme", false);

      assertThat(provider.getKnownDatabases()).doesNotContain("dropme");
      // Tables are still in the set (orphaned records)
      assertThat(provider.getKnownTables()).contains("dropme/t1");
    }
  }

  @Nested
  @DisplayName("Table Operations")
  class TableTests {

    @BeforeEach
    void init() {
      provider.initializeWarehouse("default");
    }

    @Test
    @DisplayName("registerTable and listTables")
    void testRegisterAndListTables() throws IOException {
      provider.registerTable("default", "t1");
      provider.registerTable("default", "t2");

      List<String> tables = provider.listTables("default");
      assertThat(tables).containsExactlyInAnyOrder("t1", "t2");
    }

    @Test
    @DisplayName("tableExists returns true for registered table")
    void testTableExistsRegistered() {
      provider.registerTable("default", "known");
      assertThat(provider.tableExists("default", "known")).isTrue();
    }

    @Test
    @DisplayName("tableExists returns false for unregistered table (no real dataset)")
    void testTableExistsUnregistered() {
      // Without a real S3 dataset, probing will fail → returns false
      assertThat(provider.tableExists("default", "unknown")).isFalse();
    }

    @Test
    @DisplayName("dropTable removes table record")
    void testDropTable() {
      provider.registerTable("default", "toDrop");
      assertThat(provider.getKnownTables()).contains("default/toDrop");

      provider.dropTable("default", "toDrop");
      assertThat(provider.getKnownTables()).doesNotContain("default/toDrop");
    }

    @Test
    @DisplayName("renameTable throws UnsupportedOperationException")
    void testRenameTableUnsupported() {
      assertThatThrownBy(() -> provider.renameTable("default", "old", "new"))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("does not support renaming");
    }

    @Test
    @DisplayName("listTables only returns tables for the given database")
    void testListTablesScoped() throws IOException {
      provider.registerTable("default", "t1");
      provider.registerTable("other", "t2");

      assertThat(provider.listTables("default")).containsExactly("t1");
      assertThat(provider.listTables("other")).containsExactly("t2");
    }
  }

  @Nested
  @DisplayName("Environment Configuration")
  class EnvironmentTests {

    @Test
    @DisplayName("configureEnvironment sets system properties")
    void testConfigureEnvironment() {
      provider.configureEnvironment();

      assertThat(System.getProperty("AWS_ACCESS_KEY_ID")).isEqualTo("AKID_TEST");
      assertThat(System.getProperty("AWS_SECRET_ACCESS_KEY")).isEqualTo("SECRET_TEST");

      // Cleanup
      System.clearProperty("AWS_ACCESS_KEY_ID");
      System.clearProperty("AWS_SECRET_ACCESS_KEY");
    }
  }

  @Nested
  @DisplayName("getStorageOptions")
  class StorageOptionsTests {

    @Test
    @DisplayName("Returns the provided storage options")
    void testGetStorageOptions() {
      assertThat(provider.getStorageOptions()).containsEntry("aws_access_key_id", "AKID_TEST");
    }
  }

  @Nested
  @DisplayName("clear")
  class ClearTests {

    @Test
    @DisplayName("Clear removes all registries")
    void testClear() {
      provider.initializeWarehouse("default");
      provider.registerTable("default", "t1");

      provider.clear();

      assertThat(provider.getKnownDatabases()).isEmpty();
      assertThat(provider.getKnownTables()).isEmpty();
    }
  }
}
