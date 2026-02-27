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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Factory for creating and managing Lance Namespace Adapters and Catalogs.
 * 
 * This factory follows the factory pattern used in lance-spark, providing
 * a unified way to create LanceNamespaceAdapter instances with different
 * implementations (DirectoryNamespace, RestNamespace, etc.).
 * 
 * Example usage:
 * <pre>
 * LanceCatalogFactory factory = new LanceCatalogFactory();
 * LanceNamespaceConfig config = LanceNamespaceConfig.builder()
 *     .impl("dir")
 *     .root("/data/lance")
 *     .build();
 * LanceNamespaceAdapter adapter = factory.createAdapter(config);
 * </pre>
 */
public class LanceCatalogFactory {
    
    private static final Logger LOG = LoggerFactory.getLogger(LanceCatalogFactory.class);
    
    private volatile BufferAllocator sharedAllocator;
    
    /**
     * Create a new factory with a shared buffer allocator.
     */
    public LanceCatalogFactory() {
        this.sharedAllocator = new RootAllocator();
        LOG.info("Created LanceCatalogFactory with shared RootAllocator");
    }
    
    /**
     * Create a new factory with a custom buffer allocator.
     */
    public LanceCatalogFactory(BufferAllocator allocator) {
        this.sharedAllocator = Objects.requireNonNull(allocator, "Allocator cannot be null");
        LOG.info("Created LanceCatalogFactory with custom allocator");
    }
    
    /**
     * Create a LanceNamespaceAdapter from configuration.
     * 
     * @param config The namespace configuration
     * @return A configured LanceNamespaceAdapter
     */
    public LanceNamespaceAdapter createAdapter(LanceNamespaceConfig config) {
        LOG.info("Creating LanceNamespaceAdapter with config");
        
        Objects.requireNonNull(config, "Configuration cannot be null");
        
        try {
            Map<String, String> properties = new HashMap<>();
            properties.put(LanceNamespaceConfig.KEY_IMPL, config.getImpl());
            if (config.getRoot().isPresent()) {
                properties.put(LanceNamespaceConfig.KEY_ROOT, config.getRoot().get());
            }
            if (config.getUri().isPresent()) {
                properties.put(LanceNamespaceConfig.KEY_URI, config.getUri().get());
            }
            if (config.getExtraLevel().isPresent()) {
                properties.put(LanceNamespaceConfig.KEY_EXTRA_LEVEL, config.getExtraLevel().get());
            }
            if (config.getParent().isPresent()) {
                properties.put(LanceNamespaceConfig.KEY_PARENT, config.getParent().get());
            }
            
            return LanceNamespaceAdapter.create(properties);
        } catch (Exception e) {
            LOG.error("Failed to create adapter", e);
            throw new RuntimeException("Failed to create LanceNamespaceAdapter", e);
        }
    }
    
    /**
     * Create a LanceNamespaceAdapter from properties map.
     * 
     * @param properties The properties map
     * @return A configured LanceNamespaceAdapter
     */
    public LanceNamespaceAdapter createAdapter(Map<String, String> properties) {
        LOG.info("Creating LanceNamespaceAdapter from properties");
        
        Objects.requireNonNull(properties, "Properties cannot be null");
        
        LanceNamespaceConfig config = LanceNamespaceConfig.from(properties);
        return createAdapter(config);
    }
    
    /**
     * Create a LanceNamespaceAdapter with directory namespace implementation.
     * 
     * @param rootPath The root directory path
     * @return A configured LanceNamespaceAdapter
     */
    public LanceNamespaceAdapter createDirectoryAdapter(String rootPath) {
        LOG.info("Creating directory namespace adapter with root: {}", rootPath);
        
        Objects.requireNonNull(rootPath, "Root path cannot be null");
        
        LanceNamespaceConfig config = LanceNamespaceConfig.builder()
            .impl("dir")
            .root(rootPath)
            .build();
        
        return createAdapter(config);
    }
    
    /**
     * Create a LanceNamespaceAdapter with REST namespace implementation.
     * 
     * @param uri The REST service URI
     * @return A configured LanceNamespaceAdapter
     */
    public LanceNamespaceAdapter createRestAdapter(String uri) {
        LOG.info("Creating REST namespace adapter with URI: {}", uri);
        
        Objects.requireNonNull(uri, "URI cannot be null");
        
        LanceNamespaceConfig config = LanceNamespaceConfig.builder()
            .impl("rest")
            .uri(uri)
            .build();
        
        return createAdapter(config);
    }
    
    /**
     * Get the shared buffer allocator.
     */
    public BufferAllocator getSharedAllocator() {
        return sharedAllocator;
    }
    
    /**
     * Close the factory and cleanup resources.
     */
    public void close() {
        LOG.info("Closing LanceCatalogFactory");
        try {
            if (sharedAllocator != null) {
                sharedAllocator.close();
            }
        } catch (Exception e) {
            LOG.warn("Error closing shared allocator", e);
        }
    }
}
