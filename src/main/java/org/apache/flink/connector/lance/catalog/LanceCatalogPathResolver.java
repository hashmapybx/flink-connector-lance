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

import java.io.Serializable;

/**
 * Resolves and manages warehouse paths for Lance Catalog.
 *
 * <p>Handles path normalization, database/table path construction, and remote storage detection.
 * Supports local filesystem paths and S3/GCS/Azure remote storage URIs.
 *
 * <p>This class is immutable and thread-safe.
 */
public final class LanceCatalogPathResolver implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String warehouse;
  private final boolean remote;

  /**
   * Create a path resolver for the given warehouse.
   *
   * @param warehouse Warehouse root path (local or remote URI)
   */
  public LanceCatalogPathResolver(String warehouse) {
    this.warehouse = normalize(warehouse);
    this.remote = detectRemote(warehouse);
  }

  /** Get the normalized warehouse path. */
  public String getWarehouse() {
    return warehouse;
  }

  /** Whether the warehouse uses remote storage (S3, GCS, Azure, etc.). */
  public boolean isRemote() {
    return remote;
  }

  /**
   * Resolve database path under the warehouse.
   *
   * @param databaseName Database name
   * @return Full path to the database directory
   */
  public String resolveDatabasePath(String databaseName) {
    return warehouse + "/" + databaseName;
  }

  /**
   * Resolve dataset (table) path under the warehouse.
   *
   * @param databaseName Database name
   * @param tableName Table name
   * @return Full path to the dataset
   */
  public String resolveTablePath(String databaseName, String tableName) {
    return warehouse + "/" + databaseName + "/" + tableName;
  }

  /**
   * Normalize a warehouse path: remove trailing slashes.
   *
   * @param path Raw path
   * @return Normalized path
   */
  static String normalize(String path) {
    if (path == null) {
      return null;
    }
    while (path.endsWith("/") && path.length() > 1) {
      path = path.substring(0, path.length() - 1);
    }
    return path;
  }

  /**
   * Detect whether a path is a remote storage URI.
   *
   * @param path Path to check
   * @return true if path starts with a known remote protocol prefix
   */
  static boolean detectRemote(String path) {
    if (path == null) {
      return false;
    }
    String lower = path.toLowerCase();
    return lower.startsWith("s3://")
        || lower.startsWith("s3a://")
        || lower.startsWith("gs://")
        || lower.startsWith("az://")
        || lower.startsWith("https://")
        || lower.startsWith("http://");
  }
}
