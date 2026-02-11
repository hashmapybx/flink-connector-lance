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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Storage abstraction for Lance Catalog operations.
 *
 * <p>Encapsulates all filesystem / object-store interactions so that {@code LanceCatalog} can
 * delegate storage-specific logic to concrete implementations.
 *
 * <p>Two built-in implementations:
 *
 * <ul>
 *   <li>{@link LocalStorageProvider} — Local filesystem (default)
 *   <li>{@link RemoteStorageProvider} — S3/GCS/Azure object storage
 * </ul>
 */
public interface LanceStorageProvider extends Serializable {

  /**
   * Ensure the warehouse root and the default database directory exist.
   *
   * @param defaultDatabase Default database name
   * @throws IOException if directories cannot be created
   */
  void initializeWarehouse(String defaultDatabase) throws IOException;

  /**
   * List all databases under the warehouse.
   *
   * @return List of database names
   * @throws IOException on I/O error
   */
  List<String> listDatabases() throws IOException;

  /**
   * Check whether a database exists.
   *
   * @param databaseName Database name
   * @return true if the database exists
   */
  boolean databaseExists(String databaseName);

  /**
   * Create a database directory.
   *
   * @param databaseName Database name
   * @throws IOException if the directory cannot be created
   */
  void createDatabase(String databaseName) throws IOException;

  /**
   * Drop a database directory.
   *
   * @param databaseName Database name
   * @param cascade if true, also delete contents
   * @throws IOException on I/O error
   */
  void dropDatabase(String databaseName, boolean cascade) throws IOException;

  /**
   * List all Lance datasets (tables) in a database.
   *
   * @param databaseName Database name
   * @return List of table names
   * @throws IOException on I/O error
   */
  List<String> listTables(String databaseName) throws IOException;

  /**
   * Check whether a table (Lance dataset) exists.
   *
   * @param databaseName Database name
   * @param tableName Table name
   * @return true if the table exists
   */
  boolean tableExists(String databaseName, String tableName);

  /**
   * Delete a table (Lance dataset) from storage.
   *
   * @param databaseName Database name
   * @param tableName Table name
   * @throws IOException on I/O error
   */
  void dropTable(String databaseName, String tableName) throws IOException;

  /**
   * Rename a table (Lance dataset).
   *
   * @param databaseName Database name
   * @param oldTableName Current table name
   * @param newTableName New table name
   * @throws IOException on I/O error
   */
  void renameTable(String databaseName, String oldTableName, String newTableName)
      throws IOException;

  /**
   * Register a table in the metadata store (for remote storage that tracks tables in-memory).
   *
   * @param databaseName Database name
   * @param tableName Table name
   */
  void registerTable(String databaseName, String tableName);

  /** Configure storage environment (e.g., S3 credentials). No-op for local storage. */
  void configureEnvironment();
}
