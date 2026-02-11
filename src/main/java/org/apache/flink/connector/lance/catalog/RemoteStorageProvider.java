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

import com.lancedb.lance.Dataset;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.connector.lance.config.LanceDatasetFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Remote object-store (S3/GCS/Azure) implementation of {@link LanceStorageProvider}.
 *
 * <p>Because remote object stores do not have a true directory hierarchy, this provider maintains
 * in-memory registries of known databases and tables. Actual existence is verified lazily by
 * attempting to open the Lance dataset.
 *
 * <p>Remote storage credentials are configured via {@link StorageEnvironmentManager}.
 */
public final class RemoteStorageProvider implements LanceStorageProvider {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(RemoteStorageProvider.class);

  private final LanceCatalogPathResolver pathResolver;
  private final Map<String, String> storageOptions;

  // In-memory registries for known databases and tables
  private final Set<String> knownDatabases = ConcurrentHashMap.newKeySet();
  private final Set<String> knownTables = ConcurrentHashMap.newKeySet();

  // Transient allocator for probing dataset existence
  private transient BufferAllocator allocator;

  public RemoteStorageProvider(
      LanceCatalogPathResolver pathResolver, Map<String, String> storageOptions) {
    this.pathResolver = pathResolver;
    this.storageOptions = storageOptions;
  }

  /** Set the allocator (called from LanceCatalog.open). */
  public void setAllocator(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  @Override
  public void initializeWarehouse(String defaultDatabase) {
    knownDatabases.add(defaultDatabase);
    LOG.info(
        "Remote storage mode enabled, registered default database: {}, storage config count: {}",
        defaultDatabase,
        storageOptions.size());
  }

  @Override
  public List<String> listDatabases() {
    return new ArrayList<>(knownDatabases);
  }

  @Override
  public boolean databaseExists(String databaseName) {
    // For remote storage, assume database always exists if known or by default
    return knownDatabases.contains(databaseName) || true;
  }

  @Override
  public void createDatabase(String databaseName) {
    knownDatabases.add(databaseName);
    LOG.info("Registered remote database: {}", databaseName);
  }

  @Override
  public void dropDatabase(String databaseName, boolean cascade) throws IOException {
    if (cascade) {
      String prefix = databaseName + "/";
      List<String> tablesToRemove =
          knownTables.stream().filter(t -> t.startsWith(prefix)).collect(Collectors.toList());
      knownTables.removeAll(tablesToRemove);
    }
    knownDatabases.remove(databaseName);
    LOG.info("Removed remote database record: {}", databaseName);
  }

  @Override
  public List<String> listTables(String databaseName) {
    String prefix = databaseName + "/";
    return knownTables.stream()
        .filter(t -> t.startsWith(prefix))
        .map(t -> t.substring(prefix.length()))
        .collect(Collectors.toList());
  }

  @Override
  public boolean tableExists(String databaseName, String tableName) {
    String tableKey = databaseName + "/" + tableName;
    if (knownTables.contains(tableKey)) {
      return true;
    }

    // Try to open dataset to verify existence
    try {
      configureEnvironment();
      String datasetPath = pathResolver.resolveTablePath(databaseName, tableName);
      Dataset dataset = LanceDatasetFactory.open(datasetPath, allocator);
      dataset.close();
      knownTables.add(tableKey);
      return true;
    } catch (Exception e) {
      LOG.debug("Table does not exist or cannot be accessed: {}/{}", databaseName, tableName, e);
      return false;
    }
  }

  @Override
  public void dropTable(String databaseName, String tableName) {
    String tableKey = databaseName + "/" + tableName;
    knownTables.remove(tableKey);
    String datasetPath = pathResolver.resolveTablePath(databaseName, tableName);
    LOG.warn(
        "Remote storage mode: table record removed, but actual data needs manual deletion"
            + " from storage: {}",
        datasetPath);
  }

  @Override
  public void renameTable(String databaseName, String oldTableName, String newTableName) {
    throw new UnsupportedOperationException("Remote storage mode does not support renaming tables");
  }

  @Override
  public void registerTable(String databaseName, String tableName) {
    String tableKey = databaseName + "/" + tableName;
    knownTables.add(tableKey);
  }

  @Override
  public void configureEnvironment() {
    StorageEnvironmentManager.configure(storageOptions);
  }

  /** Get storage options (for building table connector options). */
  public Map<String, String> getStorageOptions() {
    return storageOptions;
  }

  /** Get known databases set (for testing). */
  Set<String> getKnownDatabases() {
    return knownDatabases;
  }

  /** Get known tables set (for testing). */
  Set<String> getKnownTables() {
    return knownTables;
  }

  /** Clear all in-memory registries. */
  public void clear() {
    knownDatabases.clear();
    knownTables.clear();
  }
}
