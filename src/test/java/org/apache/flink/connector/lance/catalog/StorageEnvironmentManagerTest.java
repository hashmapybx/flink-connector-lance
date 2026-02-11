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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link StorageEnvironmentManager}. */
class StorageEnvironmentManagerTest {

  @Nested
  @DisplayName("configure")
  class ConfigureTests {

    @Test
    @DisplayName("Setting S3 credentials sets system properties")
    void testConfigureSetsSystemProperties() {
      Map<String, String> opts = new HashMap<>();
      opts.put("aws_access_key_id", "AKID_TEST");
      opts.put("aws_secret_access_key", "SECRET_TEST");
      opts.put("aws_region", "us-west-2");
      opts.put("aws_endpoint", "http://localhost:9000");
      opts.put("aws_virtual_hosted_style_request", "false");
      opts.put("allow_http", "true");

      StorageEnvironmentManager.configure(opts);

      assertThat(System.getProperty("AWS_ACCESS_KEY_ID")).isEqualTo("AKID_TEST");
      assertThat(System.getProperty("AWS_SECRET_ACCESS_KEY")).isEqualTo("SECRET_TEST");
      assertThat(System.getProperty("AWS_DEFAULT_REGION")).isEqualTo("us-west-2");
      assertThat(System.getProperty("AWS_ENDPOINT")).isEqualTo("http://localhost:9000");
      assertThat(System.getProperty("AWS_VIRTUAL_HOSTED_STYLE_REQUEST")).isEqualTo("false");
      assertThat(System.getProperty("AWS_ALLOW_HTTP")).isEqualTo("true");

      // Cleanup
      System.clearProperty("AWS_ACCESS_KEY_ID");
      System.clearProperty("AWS_SECRET_ACCESS_KEY");
      System.clearProperty("AWS_DEFAULT_REGION");
      System.clearProperty("AWS_ENDPOINT");
      System.clearProperty("AWS_VIRTUAL_HOSTED_STYLE_REQUEST");
      System.clearProperty("AWS_ALLOW_HTTP");
    }

    @Test
    @DisplayName("Null options map is a no-op")
    void testConfigureNullOptions() {
      StorageEnvironmentManager.configure(null);
      // Should not throw
    }

    @Test
    @DisplayName("Empty options map is a no-op")
    void testConfigureEmptyOptions() {
      StorageEnvironmentManager.configure(Collections.emptyMap());
      // Should not throw
    }

    @Test
    @DisplayName("Unknown keys are ignored")
    void testConfigureUnknownKeys() {
      Map<String, String> opts = new HashMap<>();
      opts.put("unknown_key", "value");

      StorageEnvironmentManager.configure(opts);
      // Should not set any system property for unknown keys
      assertThat(System.getProperty("unknown_key")).isNull();
    }
  }

  @Nested
  @DisplayName("toTableOptions")
  class ToTableOptionsTests {

    @Test
    @DisplayName("Convert storage options to table options")
    void testToTableOptions() {
      Map<String, String> storageOpts = new HashMap<>();
      storageOpts.put("aws_access_key_id", "AKID");
      storageOpts.put("aws_secret_access_key", "SECRET");
      storageOpts.put("aws_region", "us-east-1");
      storageOpts.put("aws_endpoint", "http://s3.example.com");

      Map<String, String> tableOpts = StorageEnvironmentManager.toTableOptions(storageOpts);

      assertThat(tableOpts).hasSize(4);
      assertThat(tableOpts.get("s3-access-key")).isEqualTo("AKID");
      assertThat(tableOpts.get("s3-secret-key")).isEqualTo("SECRET");
      assertThat(tableOpts.get("s3-region")).isEqualTo("us-east-1");
      assertThat(tableOpts.get("s3-endpoint")).isEqualTo("http://s3.example.com");
    }

    @Test
    @DisplayName("Null options returns empty map")
    void testToTableOptionsNull() {
      assertThat(StorageEnvironmentManager.toTableOptions(null)).isEmpty();
    }

    @Test
    @DisplayName("Empty options returns empty map")
    void testToTableOptionsEmpty() {
      assertThat(StorageEnvironmentManager.toTableOptions(Collections.emptyMap())).isEmpty();
    }

    @Test
    @DisplayName("Partial options only map known keys")
    void testToTableOptionsPartial() {
      Map<String, String> storageOpts = new HashMap<>();
      storageOpts.put("aws_access_key_id", "AKID");
      // Only access key, no secret, region, endpoint

      Map<String, String> tableOpts = StorageEnvironmentManager.toTableOptions(storageOpts);

      assertThat(tableOpts).hasSize(1);
      assertThat(tableOpts.get("s3-access-key")).isEqualTo("AKID");
    }
  }
}
