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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages S3/remote storage environment configuration for Lance SDK.
 *
 * <p>Lance's Rust backend reads AWS credentials from environment variables / system properties.
 * This class encapsulates the mapping between user-provided storage options and the system
 * properties that Lance expects.
 *
 * <p>Supported configuration keys (user-facing → system property):
 *
 * <ul>
 *   <li>{@code aws_access_key_id} → {@code AWS_ACCESS_KEY_ID}
 *   <li>{@code aws_secret_access_key} → {@code AWS_SECRET_ACCESS_KEY}
 *   <li>{@code aws_region} → {@code AWS_DEFAULT_REGION}
 *   <li>{@code aws_endpoint} → {@code AWS_ENDPOINT}
 *   <li>{@code aws_virtual_hosted_style_request} → {@code AWS_VIRTUAL_HOSTED_STYLE_REQUEST}
 *   <li>{@code allow_http} → {@code AWS_ALLOW_HTTP}
 * </ul>
 *
 * <p>This class also converts storage options into table-level connector options (e.g., {@code
 * s3-access-key}).
 */
public final class StorageEnvironmentManager {

  private static final Logger LOG = LoggerFactory.getLogger(StorageEnvironmentManager.class);

  /** Mapping from internal storage option keys to system property names. */
  private static final Map<String, String> KEY_TO_SYS_PROP;

  static {
    Map<String, String> m = new HashMap<>();
    m.put("aws_access_key_id", "AWS_ACCESS_KEY_ID");
    m.put("aws_secret_access_key", "AWS_SECRET_ACCESS_KEY");
    m.put("aws_region", "AWS_DEFAULT_REGION");
    m.put("aws_endpoint", "AWS_ENDPOINT");
    m.put("aws_virtual_hosted_style_request", "AWS_VIRTUAL_HOSTED_STYLE_REQUEST");
    m.put("allow_http", "AWS_ALLOW_HTTP");
    KEY_TO_SYS_PROP = Collections.unmodifiableMap(m);
  }

  /** Mapping from internal storage option keys to table-level connector option keys. */
  private static final Map<String, String> KEY_TO_TABLE_OPT;

  static {
    Map<String, String> m = new HashMap<>();
    m.put("aws_access_key_id", "s3-access-key");
    m.put("aws_secret_access_key", "s3-secret-key");
    m.put("aws_region", "s3-region");
    m.put("aws_endpoint", "s3-endpoint");
    KEY_TO_TABLE_OPT = Collections.unmodifiableMap(m);
  }

  private StorageEnvironmentManager() {
    // Utility class
  }

  /**
   * Configure system properties for remote storage access.
   *
   * <p>This sets JVM system properties that Lance's Rust backend reads to authenticate with S3 or
   * other cloud storage.
   *
   * @param storageOptions Storage configuration options from the catalog
   */
  public static void configure(Map<String, String> storageOptions) {
    if (storageOptions == null || storageOptions.isEmpty()) {
      return;
    }

    for (Map.Entry<String, String> entry : KEY_TO_SYS_PROP.entrySet()) {
      String value = storageOptions.get(entry.getKey());
      if (value != null) {
        System.setProperty(entry.getValue(), value);
      }
    }

    LOG.debug("Configured remote storage environment variables");
  }

  /**
   * Convert internal storage options into table-level connector options.
   *
   * <p>Used when building {@code CatalogTable} options so downstream connectors can also access
   * storage credentials.
   *
   * @param storageOptions Internal storage options
   * @return Table-level connector options (e.g., s3-access-key, s3-secret-key)
   */
  public static Map<String, String> toTableOptions(Map<String, String> storageOptions) {
    if (storageOptions == null || storageOptions.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, String> tableOpts = new HashMap<>();
    for (Map.Entry<String, String> entry : KEY_TO_TABLE_OPT.entrySet()) {
      String value = storageOptions.get(entry.getKey());
      if (value != null) {
        tableOpts.put(entry.getValue(), value);
      }
    }
    return tableOpts;
  }
}
