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

package org.apache.flink.connector.lance.catalog.namespace;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.lance.namespace.LanceNamespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Lance Namespace connection manager.
 *
 * <p>Responsible for creating and managing the lifecycle of {@link LanceNamespace} instances
 * (connection initialization and resource cleanup). This class does not wrap any business methods
 * of LanceNamespace; callers should use {@link #getNamespace()} to obtain the instance and invoke
 * the LanceNamespace native API directly.</p>
 */
public class LanceNamespaceAdapter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LanceNamespaceAdapter.class);

    private final BufferAllocator allocator;
    private final LanceNamespaceConfig config;
    private LanceNamespace namespace;

    public LanceNamespaceAdapter(BufferAllocator allocator, LanceNamespaceConfig config) {
        this.allocator = Objects.requireNonNull(allocator, "Allocator cannot be null");
        this.config = Objects.requireNonNull(config, "Config cannot be null");
    }

    /**
     * Creates an Adapter instance from a properties map.
     */
    public static LanceNamespaceAdapter create(Map<String, String> properties) {
        LanceNamespaceConfig config = LanceNamespaceConfig.from(properties);
        BufferAllocator allocator = new RootAllocator();
        return new LanceNamespaceAdapter(allocator, config);
    }

    /**
     * Initializes the LanceNamespace connection.
     */
    public void init() {
        try {
            if (namespace != null) {
                return;
            }

            Map<String, String> properties = new HashMap<>();
            properties.put(LanceNamespaceConfig.KEY_IMPL, config.getImpl());
            config.getRoot().ifPresent(root -> properties.put(LanceNamespaceConfig.KEY_ROOT, root));
            config.getUri().ifPresent(uri -> properties.put(LanceNamespaceConfig.KEY_URI, uri));

            namespace = LanceNamespace.connect(config.getImpl(), properties, allocator);
            LOG.info("LanceNamespace initialized successfully with impl: {}", config.getImpl());
        } catch (Exception e) {
            LOG.error("Failed to initialize LanceNamespace", e);
            throw new RuntimeException("Failed to initialize LanceNamespace", e);
        }
    }

    /**
     * Returns the underlying LanceNamespace instance, initializing it automatically if needed.
     */
    public LanceNamespace getNamespace() {
        if (namespace == null) {
            init();
        }
        return namespace;
    }

    /**
     * Returns the BufferAllocator.
     */
    public BufferAllocator getAllocator() {
        return allocator;
    }

    /**
     * Returns the configuration.
     */
    public LanceNamespaceConfig getConfig() {
        return config;
    }

    /**
     * Closes the connection and releases resources.
     */
    @Override
    public void close() {
        try {
            if (namespace instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) namespace).close();
                } catch (Exception e) {
                    LOG.debug("Error invoking close() on namespace", e);
                }
            }
        } catch (Exception e) {
            LOG.warn("Error during namespace cleanup", e);
        }

        if (allocator != null) {
            allocator.close();
        }
    }

    /**
     * Holder class for table metadata.
     */
    public static class TableMetadata {
        private final String location;
        private final Map<String, String> storageOptions;

        public TableMetadata(String location, Map<String, String> storageOptions) {
            this.location = location;
            this.storageOptions = storageOptions;
        }

        public String getLocation() {
            return location;
        }

        public Map<String, String> getStorageOptions() {
            return storageOptions;
        }
    }
}