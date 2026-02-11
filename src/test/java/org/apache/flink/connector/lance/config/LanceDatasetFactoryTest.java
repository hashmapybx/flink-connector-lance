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
package org.apache.flink.connector.lance.config;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link LanceDatasetFactory}. */
class LanceDatasetFactoryTest {

  @TempDir Path tempDir;

  // ==================== Path Validation Tests ====================

  @Nested
  @DisplayName("Path Validation")
  class PathValidationTests {

    @Test
    @DisplayName("validatePath rejects null path")
    void testNullPath() {
      assertThatThrownBy(() -> LanceDatasetFactory.validatePath(null))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("must not be null or empty");
    }

    @Test
    @DisplayName("validatePath rejects empty path")
    void testEmptyPath() {
      assertThatThrownBy(() -> LanceDatasetFactory.validatePath(""))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("must not be null or empty");
    }

    @Test
    @DisplayName("validatePath accepts valid path")
    void testValidPath() {
      // Should not throw
      LanceDatasetFactory.validatePath("/some/path");
    }
  }

  // ==================== Allocator Tests ====================

  @Nested
  @DisplayName("Allocator Management")
  class AllocatorTests {

    @Test
    @DisplayName("createAllocator returns a usable allocator")
    void testCreateAllocator() {
      BufferAllocator allocator = LanceDatasetFactory.createAllocator();
      assertThat(allocator).isNotNull();
      allocator.close();
    }

    @Test
    @DisplayName("closeQuietly handles null allocator")
    void testCloseQuietlyNullAllocator() {
      // Should not throw
      LanceDatasetFactory.closeQuietly((BufferAllocator) null);
    }
  }

  // ==================== Dataset Open Tests ====================

  @Nested
  @DisplayName("Dataset Open")
  class DatasetOpenTests {

    @Test
    @DisplayName("open with null path throws IllegalArgumentException")
    void testOpenNullPath() {
      BufferAllocator allocator = LanceDatasetFactory.createAllocator();
      try {
        assertThatThrownBy(() -> LanceDatasetFactory.open(null, allocator))
            .isInstanceOf(IllegalArgumentException.class);
      } finally {
        allocator.close();
      }
    }

    @Test
    @DisplayName("open with empty path throws IllegalArgumentException")
    void testOpenEmptyPath() {
      BufferAllocator allocator = LanceDatasetFactory.createAllocator();
      try {
        assertThatThrownBy(() -> LanceDatasetFactory.open("", allocator))
            .isInstanceOf(IllegalArgumentException.class);
      } finally {
        allocator.close();
      }
    }

    @Test
    @DisplayName("open non-existent dataset throws IOException")
    void testOpenNonExistentDataset() {
      BufferAllocator allocator = LanceDatasetFactory.createAllocator();
      String fakePath = tempDir.resolve("non_existent_dataset").toString();
      try {
        assertThatThrownBy(() -> LanceDatasetFactory.open(fakePath, allocator))
            .isInstanceOf(IOException.class)
            .hasMessageContaining("Failed to open Lance dataset");
      } finally {
        allocator.close();
      }
    }

    @Test
    @DisplayName("openManaged with null path throws IllegalArgumentException")
    void testOpenManagedNullPath() {
      assertThatThrownBy(() -> LanceDatasetFactory.openManaged(null))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("openManaged with empty path throws IllegalArgumentException")
    void testOpenManagedEmptyPath() {
      assertThatThrownBy(() -> LanceDatasetFactory.openManaged(""))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("openManaged non-existent dataset throws IOException and cleans up")
    void testOpenManagedNonExistentDataset() {
      String fakePath = tempDir.resolve("non_existent_dataset").toString();
      assertThatThrownBy(() -> LanceDatasetFactory.openManaged(fakePath))
          .isInstanceOf(IOException.class)
          .hasMessageContaining("Failed to open Lance dataset");
      // Allocator should be cleaned up (no leak)
    }
  }

  // ==================== closeQuietly Tests ====================

  @Nested
  @DisplayName("closeQuietly")
  class CloseQuietlyTests {

    @Test
    @DisplayName("closeQuietly handles null dataset without exception")
    void testCloseQuietlyNullDataset() {
      LanceDatasetFactory.closeQuietly((com.lancedb.lance.Dataset) null);
      // Should not throw
    }
  }
}
