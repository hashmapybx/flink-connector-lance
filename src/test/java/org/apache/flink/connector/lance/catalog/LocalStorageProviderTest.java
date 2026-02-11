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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link LocalStorageProvider}. */
class LocalStorageProviderTest {

  @TempDir Path tempDir;

  private LanceCatalogPathResolver pathResolver;
  private LocalStorageProvider provider;

  @BeforeEach
  void setup() {
    String warehouse = tempDir.resolve("warehouse").toString();
    pathResolver = new LanceCatalogPathResolver(warehouse);
    provider = new LocalStorageProvider(pathResolver);
  }

  @Nested
  @DisplayName("initializeWarehouse")
  class InitializeTests {

    @Test
    @DisplayName("Creates warehouse and default database directories")
    void testInitialize() throws IOException {
      provider.initializeWarehouse("default");

      Path warehousePath = Paths.get(pathResolver.getWarehouse());
      assertThat(warehousePath).exists().isDirectory();
      assertThat(warehousePath.resolve("default")).exists().isDirectory();
    }

    @Test
    @DisplayName("Idempotent: calling twice does not fail")
    void testInitializeIdempotent() throws IOException {
      provider.initializeWarehouse("default");
      provider.initializeWarehouse("default");

      assertThat(Paths.get(pathResolver.getWarehouse(), "default")).exists();
    }
  }

  @Nested
  @DisplayName("Database Operations")
  class DatabaseTests {

    @BeforeEach
    void init() throws IOException {
      provider.initializeWarehouse("default");
    }

    @Test
    @DisplayName("listDatabases returns existing databases")
    void testListDatabases() throws IOException {
      provider.createDatabase("db1");
      provider.createDatabase("db2");

      List<String> databases = provider.listDatabases();
      assertThat(databases).contains("default", "db1", "db2");
    }

    @Test
    @DisplayName("databaseExists returns true for existing database")
    void testDatabaseExists() throws IOException {
      provider.createDatabase("testdb");
      assertThat(provider.databaseExists("testdb")).isTrue();
    }

    @Test
    @DisplayName("databaseExists returns false for non-existing database")
    void testDatabaseNotExists() {
      assertThat(provider.databaseExists("nonexistent")).isFalse();
    }

    @Test
    @DisplayName("dropDatabase removes database directory")
    void testDropDatabase() throws IOException {
      provider.createDatabase("toDrop");
      assertThat(provider.databaseExists("toDrop")).isTrue();

      provider.dropDatabase("toDrop", false);
      assertThat(provider.databaseExists("toDrop")).isFalse();
    }
  }

  @Nested
  @DisplayName("Table Operations")
  class TableTests {

    @BeforeEach
    void init() throws IOException {
      provider.initializeWarehouse("default");
    }

    @Test
    @DisplayName("tableExists returns false for non-existing table")
    void testTableNotExists() {
      assertThat(provider.tableExists("default", "nonexistent")).isFalse();
    }

    @Test
    @DisplayName("tableExists returns true for a valid Lance dataset directory")
    void testTableExists() throws IOException {
      // Create a fake Lance dataset directory
      Path tablePath = Paths.get(pathResolver.resolveTablePath("default", "mytable"));
      Files.createDirectories(tablePath.resolve("_versions"));

      assertThat(provider.tableExists("default", "mytable")).isTrue();
    }

    @Test
    @DisplayName("listTables returns valid datasets")
    void testListTables() throws IOException {
      // Create valid dataset
      Path table1 = Paths.get(pathResolver.resolveTablePath("default", "t1"));
      Files.createDirectories(table1.resolve("_versions"));

      // Create non-dataset directory (no _versions)
      Path notATable = Paths.get(pathResolver.resolveTablePath("default", "notatable"));
      Files.createDirectories(notATable);

      List<String> tables = provider.listTables("default");
      assertThat(tables).containsExactly("t1");
    }

    @Test
    @DisplayName("dropTable removes table directory")
    void testDropTable() throws IOException {
      Path tablePath = Paths.get(pathResolver.resolveTablePath("default", "toDelete"));
      Files.createDirectories(tablePath.resolve("_versions"));

      assertThat(provider.tableExists("default", "toDelete")).isTrue();
      provider.dropTable("default", "toDelete");
      assertThat(provider.tableExists("default", "toDelete")).isFalse();
    }

    @Test
    @DisplayName("renameTable moves table directory")
    void testRenameTable() throws IOException {
      Path oldPath = Paths.get(pathResolver.resolveTablePath("default", "oldName"));
      Files.createDirectories(oldPath.resolve("_versions"));

      provider.renameTable("default", "oldName", "newName");

      assertThat(provider.tableExists("default", "oldName")).isFalse();
      assertThat(provider.tableExists("default", "newName")).isTrue();
    }
  }

  @Nested
  @DisplayName("No-op Methods")
  class NoOpTests {

    @Test
    @DisplayName("registerTable is a no-op for local storage")
    void testRegisterTable() {
      provider.registerTable("default", "table");
      // No exception, no side effects
    }

    @Test
    @DisplayName("configureEnvironment is a no-op for local storage")
    void testConfigureEnvironment() {
      provider.configureEnvironment();
      // No exception
    }
  }
}
