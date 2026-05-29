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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Lance Catalog factory.
 *
 * <p>Used to create LanceCatalog via SQL DDL.
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
 *     's3-region' = 'us-east-1',
 *     's3-endpoint' = 'https://s3.amazonaws.com'
 * );
 * }</pre>
 */
public class LanceCatalogFactory implements CatalogFactory {

  public static final String IDENTIFIER = "lance";

  public static final ConfigOption<String> WAREHOUSE =
      ConfigOptions.key("warehouse")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Lance data warehouse path, supports local path or S3 path (e.g., s3://bucket/path)");

  public static final ConfigOption<String> DEFAULT_DATABASE =
      ConfigOptions.key("default-database")
          .stringType()
          .defaultValue(LanceCatalog.DEFAULT_DATABASE)
          .withDescription("Default database name");

  // ==================== S3 Configuration Options ====================

  public static final ConfigOption<String> S3_ACCESS_KEY =
      ConfigOptions.key("s3-access-key")
          .stringType()
          .noDefaultValue()
          .withDescription("S3 Access Key ID");

  public static final ConfigOption<String> S3_SECRET_KEY =
      ConfigOptions.key("s3-secret-key")
          .stringType()
          .noDefaultValue()
          .withDescription("S3 Secret Access Key");

  public static final ConfigOption<String> S3_REGION =
      ConfigOptions.key("s3-region")
          .stringType()
          .noDefaultValue()
          .withDescription("S3 Region (e.g., us-east-1)");

  public static final ConfigOption<String> S3_ENDPOINT =
      ConfigOptions.key("s3-endpoint")
          .stringType()
          .noDefaultValue()
          .withDescription("S3 Endpoint URL (for S3-compatible object storage like MinIO)");

  public static final ConfigOption<Boolean> S3_VIRTUAL_HOSTED_STYLE =
      ConfigOptions.key("s3-virtual-hosted-style")
          .booleanType()
          .defaultValue(true)
          .withDescription("Whether to use virtual hosted style URL (default true)");

  public static final ConfigOption<Boolean> S3_ALLOW_HTTP =
      ConfigOptions.key("s3-allow-http")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether to allow HTTP connections (default false, HTTPS only)");

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(WAREHOUSE);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(DEFAULT_DATABASE);
    // S3 related options
    options.add(S3_ACCESS_KEY);
    options.add(S3_SECRET_KEY);
    options.add(S3_REGION);
    options.add(S3_ENDPOINT);
    options.add(S3_VIRTUAL_HOSTED_STYLE);
    options.add(S3_ALLOW_HTTP);
    return options;
  }

  @Override
  public Catalog createCatalog(Context context) {
    FactoryUtil.CatalogFactoryHelper helper = FactoryUtil.createCatalogFactoryHelper(this, context);
    helper.validate();

    String catalogName = context.getName();
    String warehouse = helper.getOptions().get(WAREHOUSE);
    String defaultDatabase = helper.getOptions().get(DEFAULT_DATABASE);

    // Collect storage configuration
    Map<String, String> storageOptions = new HashMap<>();

    // S3 configuration
    String accessKey = helper.getOptions().get(S3_ACCESS_KEY);
    String secretKey = helper.getOptions().get(S3_SECRET_KEY);
    String region = helper.getOptions().get(S3_REGION);
    String endpoint = helper.getOptions().get(S3_ENDPOINT);
    Boolean virtualHostedStyle = helper.getOptions().get(S3_VIRTUAL_HOSTED_STYLE);
    Boolean allowHttp = helper.getOptions().get(S3_ALLOW_HTTP);

    if (accessKey != null) {
      storageOptions.put("aws_access_key_id", accessKey);
    }
    if (secretKey != null) {
      storageOptions.put("aws_secret_access_key", secretKey);
    }
    if (region != null) {
      storageOptions.put("aws_region", region);
    }
    if (endpoint != null) {
      storageOptions.put("aws_endpoint", endpoint);
    }
    if (virtualHostedStyle != null) {
      storageOptions.put("aws_virtual_hosted_style_request", virtualHostedStyle.toString());
    }
    if (allowHttp != null) {
      storageOptions.put("allow_http", allowHttp.toString());
    }

    return new LanceCatalog(catalogName, defaultDatabase, warehouse, storageOptions);
  }
}
