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

import com.lancedb.lance.Dataset;
import com.lancedb.lance.WriteParams;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.connector.lance.catalog.LanceCatalogPathResolver;
import org.apache.flink.connector.lance.catalog.LanceStorageProvider;
import org.apache.flink.connector.lance.catalog.LocalStorageProvider;
import org.apache.flink.connector.lance.catalog.RemoteStorageProvider;
import org.apache.flink.connector.lance.catalog.StorageEnvironmentManager;
import org.apache.flink.connector.lance.config.LanceDatasetFactory;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lance Catalog implementation.
 *
 * <p>Implements Flink Catalog interface, supports managing Lance datasets as Flink tables. Supports
 * local file system and S3 protocol object storage.
 *
 * <p>Storage-specific logic is delegated to {@link LanceStorageProvider} implementations:
 *
 * <ul>
 *   <li>{@link LocalStorageProvider} — Local filesystem
 *   <li>{@link RemoteStorageProvider} — S3/GCS/Azure object storage
 * </ul>
 *
 * <p>Usage example (local path):
 *
 * <pre>{@code
 * CREATE CATALOG lance_catalog WITH (
 *     'type' = 'lance',
 *     'warehouse' = '/path/to/warehouse',
 *     'default-database' = 'default'
 * );
 * }</pre>
 *
 * <p>Usage example (S3 path):
 *
 * <pre>{@code
 * CREATE CATALOG lance_s3_catalog WITH (
 *     'type' = 'lance',
 *     'warehouse' = 's3://bucket-name/warehouse',
 *     'default-database' = 'default',
 *     's3-access-key' = 'your-access-key',
 *     's3-secret-key' = 'your-secret-key',
 *     's3-region' = 'us-east-1'
 * );
 * }</pre>
 */
public class LanceCatalog extends AbstractCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(LanceCatalog.class);

  public static final String DEFAULT_DATABASE = "default";

  private final LanceCatalogPathResolver pathResolver;
  private final Map<String, String> storageOptions;
  private final LanceStorageProvider storageProvider;
  private transient BufferAllocator allocator;

  /**
   * In-memory cache of user-provided table options from CREATE TABLE. Key: "database/table", Value:
   * user options map.
   */
  private final Map<String, Map<String, String>> tableOptionsCache = new ConcurrentHashMap<>();

  /**
   * Create LanceCatalog (local storage)
   *
   * @param name Catalog name
   * @param defaultDatabase Default database name
   * @param warehouse Warehouse path
   */
  public LanceCatalog(String name, String defaultDatabase, String warehouse) {
    this(name, defaultDatabase, warehouse, Collections.emptyMap());
  }

  /**
   * Create LanceCatalog (supports remote storage)
   *
   * @param name Catalog name
   * @param defaultDatabase Default database name
   * @param warehouse Warehouse path (local path or S3 URI)
   * @param storageOptions Storage configuration options (e.g., S3 credentials)
   */
  public LanceCatalog(
      String name, String defaultDatabase, String warehouse, Map<String, String> storageOptions) {
    super(name, defaultDatabase);
    this.pathResolver = new LanceCatalogPathResolver(warehouse);
    this.storageOptions =
        storageOptions != null ? new HashMap<>(storageOptions) : Collections.emptyMap();
    this.storageProvider = createStorageProvider();
  }

  /** Create the appropriate storage provider based on the warehouse path. */
  private LanceStorageProvider createStorageProvider() {
    if (pathResolver.isRemote()) {
      return new RemoteStorageProvider(pathResolver, storageOptions);
    } else {
      return new LocalStorageProvider(pathResolver);
    }
  }

  @Override
  public void open() throws CatalogException {
    LOG.info(
        "Opening Lance Catalog: {}, warehouse path: {}, remote storage: {}",
        getName(),
        pathResolver.getWarehouse(),
        pathResolver.isRemote());

    this.allocator = LanceDatasetFactory.createAllocator();

    // For remote provider, set the allocator so it can probe dataset existence
    if (storageProvider instanceof RemoteStorageProvider) {
      ((RemoteStorageProvider) storageProvider).setAllocator(allocator);
    }

    try {
      storageProvider.initializeWarehouse(getDefaultDatabase());
    } catch (IOException e) {
      throw new CatalogException("Failed to initialize warehouse", e);
    }
  }

  @Override
  public void close() throws CatalogException {
    LOG.info("Closing Lance Catalog: {}", getName());

    LanceDatasetFactory.closeQuietly(allocator);
    allocator = null;

    if (storageProvider instanceof RemoteStorageProvider) {
      ((RemoteStorageProvider) storageProvider).clear();
    }
  }

  // ==================== Database Operations ====================

  @Override
  public List<String> listDatabases() throws CatalogException {
    try {
      return storageProvider.listDatabases();
    } catch (IOException e) {
      throw new CatalogException("Failed to list databases", e);
    }
  }

  @Override
  public CatalogDatabase getDatabase(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    if (!databaseExists(databaseName)) {
      throw new DatabaseNotExistException(getName(), databaseName);
    }

    return new CatalogDatabaseImpl(Collections.emptyMap(), "Lance Database: " + databaseName);
  }

  @Override
  public boolean databaseExists(String databaseName) throws CatalogException {
    return storageProvider.databaseExists(databaseName);
  }

  @Override
  public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
      throws DatabaseAlreadyExistException, CatalogException {
    if (databaseExists(name)) {
      if (!ignoreIfExists) {
        throw new DatabaseAlreadyExistException(getName(), name);
      }
      return;
    }

    try {
      storageProvider.createDatabase(name);
      LOG.info("Created database: {}", name);
    } catch (IOException e) {
      throw new CatalogException("Failed to create database: " + name, e);
    }
  }

  @Override
  public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
      throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
    if (!databaseExists(name)) {
      if (!ignoreIfNotExists) {
        throw new DatabaseNotExistException(getName(), name);
      }
      return;
    }

    try {
      List<String> tables = listTables(name);
      if (!tables.isEmpty() && !cascade) {
        throw new DatabaseNotEmptyException(getName(), name);
      }

      // If cascade, delete all tables first
      if (cascade) {
        for (String table : tables) {
          try {
            dropTable(new ObjectPath(name, table), true);
          } catch (TableNotExistException e) {
            // Ignore
          }
        }
      }

      storageProvider.dropDatabase(name, cascade);
      LOG.info("Dropped database: {}", name);
    } catch (DatabaseNotEmptyException e) {
      throw e;
    } catch (IOException e) {
      throw new CatalogException("Failed to drop database: " + name, e);
    }
  }

  @Override
  public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
      throws DatabaseNotExistException, CatalogException {
    if (!databaseExists(name)) {
      if (!ignoreIfNotExists) {
        throw new DatabaseNotExistException(getName(), name);
      }
      return;
    }
    // Lance database does not support modifying properties
    LOG.warn("Lance Catalog does not support modifying database properties");
  }

  // ==================== Table Operations ====================

  @Override
  public List<String> listTables(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    if (!databaseExists(databaseName)) {
      throw new DatabaseNotExistException(getName(), databaseName);
    }

    try {
      return storageProvider.listTables(databaseName);
    } catch (IOException e) {
      throw new CatalogException("Failed to list tables", e);
    }
  }

  @Override
  public List<String> listViews(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    // Lance does not support views
    return Collections.emptyList();
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    if (!tableExists(tablePath)) {
      throw new TableNotExistException(getName(), tablePath);
    }

    String datasetPath =
        pathResolver.resolveTablePath(tablePath.getDatabaseName(), tablePath.getObjectName());

    try {
      storageProvider.configureEnvironment();
      Dataset dataset = LanceDatasetFactory.open(datasetPath, allocator);

      try {
        // Infer Flink Schema from Lance Schema
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = dataset.getSchema();
        RowType rowType = LanceTypeConverter.toFlinkRowType(arrowSchema);

        // Build CatalogTable
        Schema.Builder schemaBuilder = Schema.newBuilder();
        for (RowType.RowField field : rowType.getFields()) {
          DataType dataType = LanceTypeConverter.toDataType(field.getType());
          schemaBuilder.column(field.getName(), dataType);
        }

        Map<String, String> options = new HashMap<>();
        options.put("connector", LanceDynamicTableFactory.IDENTIFIER);
        options.put("path", datasetPath);

        // If remote storage, add storage config to table options
        if (pathResolver.isRemote()) {
          options.putAll(StorageEnvironmentManager.toTableOptions(storageOptions));
        }

        // Merge user-provided table options from CREATE TABLE
        String tableKey = tablePath.getDatabaseName() + "/" + tablePath.getObjectName();
        Map<String, String> cachedOptions = tableOptionsCache.get(tableKey);
        if (cachedOptions != null) {
          for (Map.Entry<String, String> entry : cachedOptions.entrySet()) {
            // Do not override connector and path
            if (!"connector".equals(entry.getKey()) && !"path".equals(entry.getKey())) {
              options.put(entry.getKey(), entry.getValue());
            }
          }
        }

        return CatalogTable.of(
            schemaBuilder.build(),
            "Lance Table: " + tablePath.getFullName(),
            Collections.emptyList(),
            options);
      } finally {
        dataset.close();
      }
    } catch (Exception e) {
      throw new CatalogException("Failed to get table info: " + tablePath, e);
    }
  }

  @Override
  public boolean tableExists(ObjectPath tablePath) throws CatalogException {
    if (!databaseExists(tablePath.getDatabaseName())) {
      return false;
    }

    return storageProvider.tableExists(tablePath.getDatabaseName(), tablePath.getObjectName());
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    if (!tableExists(tablePath)) {
      if (!ignoreIfNotExists) {
        throw new TableNotExistException(getName(), tablePath);
      }
      return;
    }

    try {
      storageProvider.dropTable(tablePath.getDatabaseName(), tablePath.getObjectName());
      LOG.info("Dropped table: {}", tablePath);
    } catch (IOException e) {
      throw new CatalogException("Failed to drop table: " + tablePath, e);
    }
  }

  @Override
  public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
      throws TableNotExistException, TableAlreadyExistException, CatalogException {
    if (!tableExists(tablePath)) {
      if (!ignoreIfNotExists) {
        throw new TableNotExistException(getName(), tablePath);
      }
      return;
    }

    ObjectPath newTablePath = new ObjectPath(tablePath.getDatabaseName(), newTableName);
    if (tableExists(newTablePath)) {
      throw new TableAlreadyExistException(getName(), newTablePath);
    }

    try {
      storageProvider.renameTable(
          tablePath.getDatabaseName(), tablePath.getObjectName(), newTableName);
      LOG.info("Renamed table: {} -> {}", tablePath, newTablePath);
    } catch (UnsupportedOperationException e) {
      throw new CatalogException(e.getMessage());
    } catch (IOException e) {
      throw new CatalogException("Failed to rename table: " + tablePath, e);
    }
  }

  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
      throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    if (!databaseExists(tablePath.getDatabaseName())) {
      throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
    }

    if (tableExists(tablePath)) {
      if (!ignoreIfExists) {
        throw new TableAlreadyExistException(getName(), tablePath);
      }
      return;
    }

    String datasetPath =
        pathResolver.resolveTablePath(tablePath.getDatabaseName(), tablePath.getObjectName());

    try {
      storageProvider.configureEnvironment();

      // Extract physical columns from the table schema and build Arrow Schema
      Schema tableSchema = table.getUnresolvedSchema();
      List<Schema.UnresolvedColumn> columns = tableSchema.getColumns();
      List<RowType.RowField> rowFields = new ArrayList<>();
      for (Schema.UnresolvedColumn column : columns) {
        if (column instanceof Schema.UnresolvedPhysicalColumn) {
          Schema.UnresolvedPhysicalColumn physCol = (Schema.UnresolvedPhysicalColumn) column;
          DataType dataType = (DataType) physCol.getDataType();
          rowFields.add(new RowType.RowField(physCol.getName(), dataType.getLogicalType()));
        }
      }

      if (!rowFields.isEmpty()) {
        RowType rowType = new RowType(rowFields);
        org.apache.arrow.vector.types.pojo.Schema arrowSchema =
            LanceTypeConverter.toArrowSchema(rowType);

        // Create an empty dataset with just the schema using Dataset.create()
        WriteParams writeParams = new WriteParams.Builder().build();
        Dataset dataset = Dataset.create(allocator, datasetPath, arrowSchema, writeParams);
        dataset.close();
      }

      // Cache user-provided table options
      if (table.getOptions() != null && !table.getOptions().isEmpty()) {
        String tableKey = tablePath.getDatabaseName() + "/" + tablePath.getObjectName();
        tableOptionsCache.put(tableKey, new HashMap<>(table.getOptions()));
      }

      storageProvider.registerTable(tablePath.getDatabaseName(), tablePath.getObjectName());
      LOG.info("Created table with empty dataset: {}", tablePath);

    } catch (Exception e) {
      throw new CatalogException("Failed to create table: " + tablePath, e);
    }
  }

  @Override
  public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    if (!tableExists(tablePath)) {
      if (!ignoreIfNotExists) {
        throw new TableNotExistException(getName(), tablePath);
      }
      return;
    }

    // Lance does not support modifying table structure
    throw new CatalogException("Lance Catalog does not support altering table structure");
  }

  // ==================== Partition Operations (Lance does not support partitions)
  // ====================

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    return Collections.emptyList();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws TableNotExistException,
          TableNotPartitionedException,
          PartitionSpecInvalidException,
          CatalogException {
    return Collections.emptyList();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitionsByFilter(
      ObjectPath tablePath, List<Expression> filters)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    return Collections.emptyList();
  }

  @Override
  public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
  }

  @Override
  public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws CatalogException {
    return false;
  }

  @Override
  public void createPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition partition,
      boolean ignoreIfExists)
      throws TableNotExistException,
          TableNotPartitionedException,
          PartitionSpecInvalidException,
          PartitionAlreadyExistsException,
          CatalogException {
    throw new CatalogException("Lance Catalog does not support partition operations");
  }

  @Override
  public void dropPartition(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new CatalogException("Lance Catalog does not support partition operations");
  }

  @Override
  public void alterPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition newPartition,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new CatalogException("Lance Catalog does not support partition operations");
  }

  // ==================== Function Operations (Lance does not support UDFs) ====================

  @Override
  public List<String> listFunctions(String dbName)
      throws DatabaseNotExistException, CatalogException {
    return Collections.emptyList();
  }

  @Override
  public CatalogFunction getFunction(ObjectPath functionPath)
      throws FunctionNotExistException, CatalogException {
    throw new FunctionNotExistException(getName(), functionPath);
  }

  @Override
  public boolean functionExists(ObjectPath functionPath) throws CatalogException {
    return false;
  }

  @Override
  public void createFunction(
      ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
      throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
    throw new CatalogException("Lance Catalog does not support user-defined functions");
  }

  @Override
  public void alterFunction(
      ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
      throws FunctionNotExistException, CatalogException {
    throw new CatalogException("Lance Catalog does not support user-defined functions");
  }

  @Override
  public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
      throws FunctionNotExistException, CatalogException {
    throw new CatalogException("Lance Catalog does not support user-defined functions");
  }

  // ==================== Statistics Operations ====================

  @Override
  public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    return CatalogTableStatistics.UNKNOWN;
  }

  @Override
  public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    return CatalogColumnStatistics.UNKNOWN;
  }

  @Override
  public CatalogTableStatistics getPartitionStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    return CatalogTableStatistics.UNKNOWN;
  }

  @Override
  public CatalogColumnStatistics getPartitionColumnStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    return CatalogColumnStatistics.UNKNOWN;
  }

  @Override
  public void alterTableStatistics(
      ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    // Not supported
  }

  @Override
  public void alterTableColumnStatistics(
      ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    // Not supported
  }

  @Override
  public void alterPartitionStatistics(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogTableStatistics partitionStatistics,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    // Not supported
  }

  @Override
  public void alterPartitionColumnStatistics(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogColumnStatistics columnStatistics,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    // Not supported
  }

  // ==================== Accessor Methods ====================

  /** Get warehouse path */
  public String getWarehouse() {
    return pathResolver.getWarehouse();
  }

  /** Get storage configuration options */
  public Map<String, String> getStorageOptions() {
    return Collections.unmodifiableMap(storageOptions);
  }

  /** Whether is remote storage */
  public boolean isRemoteStorage() {
    return pathResolver.isRemote();
  }

  /** Get the path resolver (for testing). */
  public LanceCatalogPathResolver getPathResolver() {
    return pathResolver;
  }

  /** Get the storage provider (for testing). */
  public LanceStorageProvider getStorageProvider() {
    return storageProvider;
  }
}
