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

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link LanceCatalogPathResolver}. */
class LanceCatalogPathResolverTest {

  // ==================== Path Normalization ====================

  @Nested
  @DisplayName("Path Normalization")
  class NormalizationTests {

    @Test
    @DisplayName("Remove trailing slashes")
    void testNormalize() {
      assertThat(LanceCatalogPathResolver.normalize("/data/warehouse/"))
          .isEqualTo("/data/warehouse");
    }

    @Test
    @DisplayName("Remove multiple trailing slashes")
    void testNormalizeMultipleSlashes() {
      assertThat(LanceCatalogPathResolver.normalize("/data///")).isEqualTo("/data");
    }

    @Test
    @DisplayName("Null input returns null")
    void testNormalizeNull() {
      assertThat(LanceCatalogPathResolver.normalize(null)).isNull();
    }

    @Test
    @DisplayName("Root path preserved")
    void testNormalizeRootPath() {
      assertThat(LanceCatalogPathResolver.normalize("/")).isEqualTo("/");
    }

    @Test
    @DisplayName("S3 path normalized correctly")
    void testNormalizeS3Path() {
      assertThat(LanceCatalogPathResolver.normalize("s3://bucket/path/"))
          .isEqualTo("s3://bucket/path");
    }
  }

  // ==================== Remote Detection ====================

  @Nested
  @DisplayName("Remote Detection")
  class RemoteDetectionTests {

    @Test
    @DisplayName("S3 path is remote")
    void testS3() {
      assertThat(LanceCatalogPathResolver.detectRemote("s3://bucket/path")).isTrue();
    }

    @Test
    @DisplayName("S3A path is remote")
    void testS3A() {
      assertThat(LanceCatalogPathResolver.detectRemote("s3a://bucket/path")).isTrue();
    }

    @Test
    @DisplayName("GCS path is remote")
    void testGcs() {
      assertThat(LanceCatalogPathResolver.detectRemote("gs://bucket/path")).isTrue();
    }

    @Test
    @DisplayName("Azure path is remote")
    void testAzure() {
      assertThat(LanceCatalogPathResolver.detectRemote("az://container/path")).isTrue();
    }

    @Test
    @DisplayName("HTTPS path is remote")
    void testHttps() {
      assertThat(LanceCatalogPathResolver.detectRemote("https://example.com/path")).isTrue();
    }

    @Test
    @DisplayName("HTTP path is remote")
    void testHttp() {
      assertThat(LanceCatalogPathResolver.detectRemote("http://example.com/path")).isTrue();
    }

    @Test
    @DisplayName("Local path is not remote")
    void testLocalPath() {
      assertThat(LanceCatalogPathResolver.detectRemote("/tmp/local/path")).isFalse();
    }

    @Test
    @DisplayName("Null path is not remote")
    void testNullPath() {
      assertThat(LanceCatalogPathResolver.detectRemote(null)).isFalse();
    }

    @Test
    @DisplayName("Case insensitive detection")
    void testCaseInsensitive() {
      assertThat(LanceCatalogPathResolver.detectRemote("S3://Bucket/Path")).isTrue();
    }
  }

  // ==================== Path Resolution ====================

  @Nested
  @DisplayName("Path Resolution")
  class PathResolutionTests {

    @Test
    @DisplayName("Resolve database path for local warehouse")
    void testResolveDatabasePathLocal() {
      LanceCatalogPathResolver resolver = new LanceCatalogPathResolver("/data/warehouse");
      assertThat(resolver.resolveDatabasePath("mydb")).isEqualTo("/data/warehouse/mydb");
    }

    @Test
    @DisplayName("Resolve table path for local warehouse")
    void testResolveTablePathLocal() {
      LanceCatalogPathResolver resolver = new LanceCatalogPathResolver("/data/warehouse");
      assertThat(resolver.resolveTablePath("mydb", "mytable"))
          .isEqualTo("/data/warehouse/mydb/mytable");
    }

    @Test
    @DisplayName("Resolve database path for S3 warehouse")
    void testResolveDatabasePathS3() {
      LanceCatalogPathResolver resolver = new LanceCatalogPathResolver("s3://bucket/warehouse");
      assertThat(resolver.resolveDatabasePath("mydb")).isEqualTo("s3://bucket/warehouse/mydb");
    }

    @Test
    @DisplayName("Resolve table path for S3 warehouse")
    void testResolveTablePathS3() {
      LanceCatalogPathResolver resolver = new LanceCatalogPathResolver("s3://bucket/warehouse");
      assertThat(resolver.resolveTablePath("mydb", "mytable"))
          .isEqualTo("s3://bucket/warehouse/mydb/mytable");
    }

    @Test
    @DisplayName("Trailing slashes removed before resolving")
    void testTrailingSlashHandling() {
      LanceCatalogPathResolver resolver = new LanceCatalogPathResolver("/data/warehouse///");
      assertThat(resolver.getWarehouse()).isEqualTo("/data/warehouse");
      assertThat(resolver.resolveDatabasePath("db")).isEqualTo("/data/warehouse/db");
    }
  }

  // ==================== isRemote Property ====================

  @Nested
  @DisplayName("isRemote property")
  class IsRemoteTests {

    @Test
    @DisplayName("Local path is not remote")
    void testLocalIsNotRemote() {
      LanceCatalogPathResolver resolver = new LanceCatalogPathResolver("/tmp/warehouse");
      assertThat(resolver.isRemote()).isFalse();
    }

    @Test
    @DisplayName("S3 path is remote")
    void testS3IsRemote() {
      LanceCatalogPathResolver resolver = new LanceCatalogPathResolver("s3://bucket/warehouse");
      assertThat(resolver.isRemote()).isTrue();
    }
  }
}
