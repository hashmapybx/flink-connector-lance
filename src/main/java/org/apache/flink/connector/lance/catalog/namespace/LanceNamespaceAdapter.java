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
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateEmptyTableRequest;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.namespace.model.TableExistsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Adapter for Lance Namespace API.
 * 
 * Provides unified interface for interacting with Lance Namespace,
 * supporting both directory-based and REST-based implementations.
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
     * Create adapter from properties.
     */
    public static LanceNamespaceAdapter create(Map<String, String> properties) {
        LanceNamespaceConfig config = LanceNamespaceConfig.from(properties);
        BufferAllocator allocator = new RootAllocator();
        return new LanceNamespaceAdapter(allocator, config);
    }
    
    /**
     * Initialize the namespace connection.
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
     * List all namespaces.
     */
    public List<String> listNamespaces() {
        LOG.debug("Listing root level namespaces");
        return listNamespacesRecursive(new ArrayList<>());
    }
    
    /**
     * List namespaces under parent.
     */
    public List<String> listNamespaces(String... parentNamespace) {
        LOG.debug("Listing namespaces under: {}", Arrays.toString(parentNamespace));
        return listNamespacesRecursive(Arrays.asList(parentNamespace));
    }
    
    /**
     * Internal recursive method for listing namespaces.
     */
    private List<String> listNamespacesRecursive(List<String> parent) {
        try {
            if (namespace == null) {
                init();
            }
            
            ListNamespacesRequest request = new ListNamespacesRequest();
            if (!parent.isEmpty()) {
                request.setId(parent);
            }
            
            ListNamespacesResponse response = namespace.listNamespaces(request);
            if (response.getNamespaces() != null) {
                Set<String> namespaceSet = response.getNamespaces();
                return new ArrayList<>(namespaceSet);
            }
            return new ArrayList<>();
        } catch (Exception e) {
            LOG.warn("Failed to list namespaces under: {}", parent, e);
            return new ArrayList<>();
        }
    }
    
    /**
     * Check if namespace exists.
     */
    public boolean namespaceExists(String... namespaceId) {
        LOG.debug("Checking if namespace exists: {}", Arrays.toString(namespaceId));
        
        try {
            if (namespace == null) {
                init();
            }
            
            NamespaceExistsRequest request = new NamespaceExistsRequest();
            request.setId(Arrays.asList(namespaceId));
            
            namespace.namespaceExists(request);
            return true;
        } catch (Exception e) {
            LOG.debug("Namespace does not exist: {}", Arrays.toString(namespaceId));
            return false;
        }
    }
    
    /**
     * Create a namespace.
     */
    public void createNamespace(Map<String, String> properties, String... namespaceId) {
        LOG.info("Creating namespace: {}", Arrays.toString(namespaceId));
        
        try {
            if (namespace == null) {
                init();
            }
            
            CreateNamespaceRequest request = new CreateNamespaceRequest();
            request.setId(Arrays.asList(namespaceId));
            if (properties != null) {
                request.setProperties(properties);
            }
            
            namespace.createNamespace(request);
            LOG.info("Namespace created successfully: {}", Arrays.toString(namespaceId));
        } catch (Exception e) {
            LOG.error("Failed to create namespace: {}", Arrays.toString(namespaceId), e);
            throw new RuntimeException("Failed to create namespace", e);
        }
    }
    
    /**
     * Drop a namespace.
     */
    public void dropNamespace(boolean cascade, String... namespaceId) {
        LOG.info("Dropping namespace: {} (cascade={})", Arrays.toString(namespaceId), cascade);
        
        try {
            if (namespace == null) {
                init();
            }
            
            DropNamespaceRequest request = new DropNamespaceRequest();
            request.setId(Arrays.asList(namespaceId));
            request.setCascade(cascade);
            
            namespace.dropNamespace(request);
            LOG.info("Namespace dropped successfully: {}", Arrays.toString(namespaceId));
        } catch (Exception e) {
            LOG.error("Failed to drop namespace: {}", Arrays.toString(namespaceId), e);
            throw new RuntimeException("Failed to drop namespace", e);
        }
    }
    
    /**
     * Get namespace metadata.
     */
    public Map<String, String> getNamespaceMetadata(String... namespaceId) {
        LOG.debug("Getting namespace metadata: {}", Arrays.toString(namespaceId));
        
        try {
            if (namespace == null) {
                init();
            }
            
            DescribeNamespaceRequest request = new DescribeNamespaceRequest();
            request.setId(Arrays.asList(namespaceId));
            
            DescribeNamespaceResponse response = namespace.describeNamespace(request);
            return response.getProperties() != null ? response.getProperties() : new HashMap<>();
        } catch (Exception e) {
            LOG.warn("Failed to get namespace metadata: {}", Arrays.toString(namespaceId), e);
            return new HashMap<>();
        }
    }
    
    /**
     * List tables in a namespace.
     */
    public List<String> listTables(String... namespaceId) {
        LOG.debug("Listing tables in namespace: {}", Arrays.toString(namespaceId));
        
        try {
            if (namespace == null) {
                init();
            }
            
            ListTablesRequest request = new ListTablesRequest();
            request.setId(Arrays.asList(namespaceId));
            
            ListTablesResponse response = namespace.listTables(request);
            if (response.getTables() != null) {
                Set<String> tableSet = response.getTables();
                return new ArrayList<>(tableSet);
            }
            return new ArrayList<>();
        } catch (Exception e) {
            LOG.warn("Failed to list tables in namespace: {}", Arrays.toString(namespaceId), e);
            return new ArrayList<>();
        }
    }
    
    /**
     * Check if table exists.
     */
    public boolean tableExists(String... tableId) {
        LOG.debug("Checking if table exists: {}", Arrays.toString(tableId));
        
        try {
            if (namespace == null) {
                init();
            }
            
            TableExistsRequest request = new TableExistsRequest();
            request.setId(Arrays.asList(tableId));
            
            namespace.tableExists(request);
            return true;
        } catch (Exception e) {
            LOG.debug("Table does not exist: {}", Arrays.toString(tableId));
            return false;
        }
    }
    
    /**
     * Create an empty table.
     */
    public void createEmptyTable(String location, Map<String, String> properties, String... tableId) {
        LOG.info("Creating empty table: {} at {}", Arrays.toString(tableId), location);
        
        try {
            if (namespace == null) {
                init();
            }
            
            CreateEmptyTableRequest request = new CreateEmptyTableRequest();
            request.setId(Arrays.asList(tableId));
            request.setLocation(location);
            if (properties != null) {
                request.setProperties(properties);
            }
            
            namespace.createEmptyTable(request);
            LOG.info("Table created successfully: {}", Arrays.toString(tableId));
        } catch (Exception e) {
            LOG.error("Failed to create table: {}", Arrays.toString(tableId), e);
            throw new RuntimeException("Failed to create table", e);
        }
    }
    
    /**
     * Drop a table.
     */
    public void dropTable(String... tableId) {
        LOG.info("Dropping table: {}", Arrays.toString(tableId));
        
        try {
            if (namespace == null) {
                init();
            }
            
            DropTableRequest request = new DropTableRequest();
            request.setId(Arrays.asList(tableId));
            
            namespace.dropTable(request);
            LOG.info("Table dropped successfully: {}", Arrays.toString(tableId));
        } catch (Exception e) {
            LOG.error("Failed to drop table: {}", Arrays.toString(tableId), e);
            throw new RuntimeException("Failed to drop table", e);
        }
    }
    
    /**
     * Get table metadata.
     */
    public TableMetadata getTableMetadata(String... tableId) {
        LOG.debug("Getting table metadata: {}", Arrays.toString(tableId));
        
        try {
            if (namespace == null) {
                init();
            }
            
            DescribeTableRequest request = new DescribeTableRequest();
            request.setId(Arrays.asList(tableId));
            
            DescribeTableResponse response = namespace.describeTable(request);
            
            String location = response.getLocation();
            Map<String, String> options = response.getProperties() != null
                    ? response.getProperties()
                    : new HashMap<>();
            
            return new TableMetadata(location, options);
        } catch (Exception e) {
            LOG.warn("Failed to get table metadata: {}", Arrays.toString(tableId), e);
            return new TableMetadata("/path/to/table", new HashMap<>());
        }
    }
    
    /**
     * Get the underlying Lance Namespace instance.
     */
    public LanceNamespace getNamespace() {
        if (namespace == null) {
            init();
        }
        return namespace;
    }
    
    /**
     * Get the Buffer Allocator.
     */
    public BufferAllocator getAllocator() {
        return allocator;
    }
    
    /**
     * Close the adapter and release resources.
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
     * Table metadata holder.
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