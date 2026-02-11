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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Local filesystem implementation of {@link LanceStorageProvider}.
 *
 * <p>Manages databases as directories and tables as subdirectories containing Lance dataset files.
 * A valid Lance dataset directory contains a {@code _versions} subdirectory.
 */
public final class LocalStorageProvider implements LanceStorageProvider {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(LocalStorageProvider.class);

  private final LanceCatalogPathResolver pathResolver;

  public LocalStorageProvider(LanceCatalogPathResolver pathResolver) {
    this.pathResolver = pathResolver;
  }

  @Override
  public void initializeWarehouse(String defaultDatabase) throws IOException {
    Path warehousePath = Paths.get(pathResolver.getWarehouse());
    if (!Files.exists(warehousePath)) {
      Files.createDirectories(warehousePath);
    }

    Path defaultDbPath = warehousePath.resolve(defaultDatabase);
    if (!Files.exists(defaultDbPath)) {
      Files.createDirectories(defaultDbPath);
    }
  }

  @Override
  public List<String> listDatabases() throws IOException {
    Path warehousePath = Paths.get(pathResolver.getWarehouse());
    if (!Files.exists(warehousePath)) {
      return Collections.emptyList();
    }

    return Files.list(warehousePath)
        .filter(Files::isDirectory)
        .map(path -> path.getFileName().toString())
        .collect(Collectors.toList());
  }

  @Override
  public boolean databaseExists(String databaseName) {
    Path dbPath = Paths.get(pathResolver.resolveDatabasePath(databaseName));
    return Files.exists(dbPath) && Files.isDirectory(dbPath);
  }

  @Override
  public void createDatabase(String databaseName) throws IOException {
    Path dbPath = Paths.get(pathResolver.resolveDatabasePath(databaseName));
    Files.createDirectories(dbPath);
    LOG.info("Created database directory: {}", dbPath);
  }

  @Override
  public void dropDatabase(String databaseName, boolean cascade) throws IOException {
    Path dbPath = Paths.get(pathResolver.resolveDatabasePath(databaseName));
    deleteDirectory(dbPath);
    LOG.info("Deleted database directory: {}", dbPath);
  }

  @Override
  public List<String> listTables(String databaseName) throws IOException {
    Path dbPath = Paths.get(pathResolver.resolveDatabasePath(databaseName));
    return Files.list(dbPath)
        .filter(Files::isDirectory)
        .filter(path -> Files.exists(path.resolve("_versions")))
        .map(path -> path.getFileName().toString())
        .collect(Collectors.toList());
  }

  @Override
  public boolean tableExists(String databaseName, String tableName) {
    Path datasetPath = Paths.get(pathResolver.resolveTablePath(databaseName, tableName));
    return Files.exists(datasetPath)
        && Files.isDirectory(datasetPath)
        && Files.exists(datasetPath.resolve("_versions"));
  }

  @Override
  public void dropTable(String databaseName, String tableName) throws IOException {
    Path datasetPath = Paths.get(pathResolver.resolveTablePath(databaseName, tableName));
    deleteDirectory(datasetPath);
    LOG.info("Deleted table directory: {}", datasetPath);
  }

  @Override
  public void renameTable(String databaseName, String oldTableName, String newTableName)
      throws IOException {
    Path oldPath = Paths.get(pathResolver.resolveTablePath(databaseName, oldTableName));
    Path newPath = Paths.get(pathResolver.resolveTablePath(databaseName, newTableName));
    Files.move(oldPath, newPath);
    LOG.info("Renamed table: {} -> {}", oldPath, newPath);
  }

  @Override
  public void registerTable(String databaseName, String tableName) {
    // No-op for local storage — tables are registered by their presence on disk
  }

  @Override
  public void configureEnvironment() {
    // No-op for local storage
  }

  /** Recursively delete a directory tree. */
  private void deleteDirectory(Path path) throws IOException {
    if (Files.isDirectory(path)) {
      Files.list(path)
          .forEach(
              child -> {
                try {
                  deleteDirectory(child);
                } catch (IOException e) {
                  LOG.warn("Failed to delete: {}", child, e);
                }
              });
    }
    Files.deleteIfExists(path);
  }
}
