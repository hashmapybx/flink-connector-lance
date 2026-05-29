/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.lance;

import org.apache.flink.connector.lance.config.LanceOptions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Lance time travel functionality.
 *
 * <p>Validates configuration options for version-based and timestamp-based time travel.
 */
public class LanceTimeTravelTest {

    @TempDir
    Path tempDir;

    @Test
    public void testTimeTravelByVersion() {
        // Test that read.version option is correctly set
        LanceOptions options = LanceOptions.builder()
                .path(tempDir.resolve("test_dataset").toString())
                .readVersion(3)
                .build();

        assertEquals(3, options.getReadVersion());
        assertNull(options.getReadTimestamp());
    }

    @Test
    public void testTimeTravelByTimestamp() {
        // Test that read.timestamp option is correctly set
        long timestamp = System.currentTimeMillis();
        LanceOptions options = LanceOptions.builder()
                .path(tempDir.resolve("test_dataset").toString())
                .readTimestamp(timestamp)
                .build();

        assertEquals(timestamp, options.getReadTimestamp());
        assertNull(options.getReadVersion());
    }

    @Test
    public void testCannotSetBothVersionAndTimestamp() {
        // Test that setting both version and timestamp throws an exception
        assertThrows(IllegalArgumentException.class, () -> {
            LanceOptions.builder()
                    .path(tempDir.resolve("test_dataset").toString())
                    .readVersion(3)
                    .readTimestamp(System.currentTimeMillis())
                    .build();
        });
    }

    @Test
    public void testInvalidVersion() {
        // Test that negative version throws an exception
        assertThrows(IllegalArgumentException.class, () -> {
            LanceOptions.builder()
                    .path(tempDir.resolve("test_dataset").toString())
                    .readVersion(-1)
                    .build();
        });

        // Test that zero version throws an exception
        assertThrows(IllegalArgumentException.class, () -> {
            LanceOptions.builder()
                    .path(tempDir.resolve("test_dataset").toString())
                    .readVersion(0)
                    .build();
        });
    }

    @Test
    public void testInvalidTimestamp() {
        // Test that negative timestamp throws an exception
        assertThrows(IllegalArgumentException.class, () -> {
            LanceOptions.builder()
                    .path(tempDir.resolve("test_dataset").toString())
                    .readTimestamp(-1L)
                    .build();
        });
    }

    @Test
    public void testNoTimeTravelByDefault() {
        // Test that by default, no time travel is configured
        LanceOptions options = LanceOptions.builder()
                .path(tempDir.resolve("test_dataset").toString())
                .build();

        assertNull(options.getReadVersion());
        assertNull(options.getReadTimestamp());
    }

    @Test
    public void testTimeTravelWithOtherOptions() {
        // Test that time travel works with other read options
        LanceOptions options = LanceOptions.builder()
                .path(tempDir.resolve("test_dataset").toString())
                .readVersion(5)
                .readBatchSize(2048)
                .readLimit(100L)
                .readFilter("id > 10")
                .build();

        assertEquals(5, options.getReadVersion());
        assertEquals(2048, options.getReadBatchSize());
        assertEquals(100L, options.getReadLimit());
        assertEquals("id > 10", options.getReadFilter());
    }

    @Test
    public void testLanceSourceBuilderWithVersion() {
        // Test LanceSource builder with version
        LanceSource source = LanceSource.builder()
                .path(tempDir.resolve("test_dataset").toString())
                .version(3)
                .build();

        assertEquals(3, source.getOptions().getReadVersion());
    }

    @Test
    public void testLanceSourceBuilderWithTimestamp() {
        // Test LanceSource builder with timestamp
        long ts = System.currentTimeMillis();
        LanceSource source = LanceSource.builder()
                .path(tempDir.resolve("test_dataset").toString())
                .timestamp(ts)
                .build();

        assertEquals(ts, source.getOptions().getReadTimestamp());
    }

    @Test
    public void testOptionsToString() {
        // Test that toString includes version and timestamp
        LanceOptions options = LanceOptions.builder()
                .path(tempDir.resolve("test_dataset").toString())
                .readVersion(3)
                .build();

        String str = options.toString();
        assertTrue(str.contains("readVersion=3"));
        assertTrue(str.contains("readTimestamp=null"));
    }

    @Test
    public void testOptionsEquality() {
        // Test that two options with same version are equal
        LanceOptions options1 = LanceOptions.builder()
                .path("/test/path")
                .readVersion(3)
                .build();

        LanceOptions options2 = LanceOptions.builder()
                .path("/test/path")
                .readVersion(3)
                .build();

        assertEquals(options1, options2);
        assertEquals(options1.hashCode(), options2.hashCode());
    }

    @Test
    public void testOptionsInequality() {
        // Test that two options with different versions are not equal
        LanceOptions options1 = LanceOptions.builder()
                .path("/test/path")
                .readVersion(3)
                .build();

        LanceOptions options2 = LanceOptions.builder()
                .path("/test/path")
                .readVersion(5)
                .build();

        assertNotEquals(options1, options2);
    }
}
