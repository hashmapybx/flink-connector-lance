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

import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
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
import java.util.Optional;
import java.util.Set;

/**
 * Base Flink Catalog built on top of Lance Namespace.
 *
 * <p>Delegates namespace/table operations directly to the {@link LanceNamespace} API,
 * while using {@link LanceNamespaceAdapter} to manage connection lifecycle.</p>
 */
public abstract class BaseLanceNamespaceCatalog extends AbstractCatalog {
    
    private static final Logger LOG = LoggerFactory.getLogger(BaseLanceNamespaceCatalog.class);
    
    protected final LanceNamespaceAdapter namespaceAdapter;
    protected final LanceNamespaceConfig config;
    protected final Optional<String> extraLevel;
    protected final Optional<String[]> parentPrefix;
    
    public BaseLanceNamespaceCatalog(String catalogName, LanceNamespaceAdapter adapter, LanceNamespaceConfig config) {
        super(catalogName, "default");
        
        this.namespaceAdapter = Objects.requireNonNull(adapter, "Namespace adapter cannot be null");
        this.config = Objects.requireNonNull(config, "Configuration cannot be null");
        
        LOG.info("Initializing BaseLanceNamespaceCatalog: {}", catalogName);
        
        // Configure extra level
        if (config.getExtraLevel().isPresent()) {
            this.extraLevel = config.getExtraLevel();
        } else if (config.isDirectoryNamespace()) {
            this.extraLevel = Optional.of("default");
        } else {
            this.extraLevel = Optional.empty();
        }
        
        // Configure parent prefix
        this.parentPrefix = config.getParentArray();
        
        LOG.info("Catalog configuration - impl: {}, extraLevel: {}, parentPrefix: {}",
                config.getImpl(), extraLevel, parentPrefix);
    }
    
    /**
     * Returns the underlying LanceNamespace instance.
     */
    protected LanceNamespace namespace() {
        return namespaceAdapter.getNamespace();
    }
    
    // ========== Database Operations ==========
    
    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        
        LOG.info("Creating database: {} (ignoreIfExists={})", name, ignoreIfExists);
        
        try {
            if (databaseExists(name)) {
                if (ignoreIfExists) {
                    LOG.info("Database already exists, skipping creation: {}", name);
                    return;
                } else {
                    throw new DatabaseAlreadyExistException(getName(), name);
                }
            }
            
            String[] namespacePath = transformDatabaseNameToNamespace(name);
            CreateNamespaceRequest request = new CreateNamespaceRequest();
            request.setId(Arrays.asList(namespacePath));
            Map<String, String> properties = database.getProperties();
            if (properties != null) {
                request.setProperties(properties);
            }
            namespace().createNamespace(request);
            
            LOG.info("Database created successfully: {}", name);
        } catch (DatabaseAlreadyExistException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to create database: {}", name, e);
            throw new CatalogException("Failed to create database: " + name, e);
        }
    }
    
    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, CatalogException {
        
        LOG.info("Dropping database: {} (cascade={})", name, cascade);
        
        try {
            if (!databaseExists(name)) {
                if (ignoreIfNotExists) {
                    LOG.info("Database does not exist, skipping drop: {}", name);
                    return;
                } else {
                    throw new DatabaseNotExistException(getName(), name);
                }
            }
            
            String[] namespacePath = transformDatabaseNameToNamespace(name);
            DropNamespaceRequest request = new DropNamespaceRequest();
            request.setId(Arrays.asList(namespacePath));
            request.setBehavior(cascade ? "CASCADE" : "RESTRICT");
            namespace().dropNamespace(request);
            
            LOG.info("Database dropped successfully: {}", name);
        } catch (DatabaseNotExistException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to drop database: {}", name, e);
            throw new CatalogException("Failed to drop database: " + name, e);
        }
    }
    
    @Override
    public List<String> listDatabases() throws CatalogException {
        LOG.debug("Listing databases");
        
        try {
            ListNamespacesRequest request = new ListNamespacesRequest();
            ListNamespacesResponse response = namespace().listNamespaces(request);
            if (response.getNamespaces() != null) {
                Set<String> namespaceSet = response.getNamespaces();
                return new ArrayList<>(namespaceSet);
            }
            return new ArrayList<>();
        } catch (Exception e) {
            LOG.error("Failed to list databases", e);
            throw new CatalogException("Failed to list databases", e);
        }
    }
    
    @Override
    public CatalogDatabase getDatabase(String name)
            throws DatabaseNotExistException, CatalogException {
        
        LOG.debug("Getting database: {}", name);
        
        try {
            if (!databaseExists(name)) {
                throw new DatabaseNotExistException(getName(), name);
            }
            
            String[] namespacePath = transformDatabaseNameToNamespace(name);
            DescribeNamespaceRequest request = new DescribeNamespaceRequest();
            request.setId(Arrays.asList(namespacePath));
            DescribeNamespaceResponse response = namespace().describeNamespace(request);
            Map<String, String> metadata = response.getProperties() != null
                    ? response.getProperties() : new HashMap<>();
            
            return new org.apache.flink.table.catalog.CatalogDatabaseImpl(metadata, "");
        } catch (DatabaseNotExistException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to get database: {}", name, e);
            throw new CatalogException("Failed to get database: " + name, e);
        }
    }
    
    @Override
    public boolean databaseExists(String name) {
        LOG.debug("Checking if database exists: {}", name);
        
        try {
            String[] namespacePath = transformDatabaseNameToNamespace(name);
            NamespaceExistsRequest request = new NamespaceExistsRequest();
            request.setId(Arrays.asList(namespacePath));
            namespace().namespaceExists(request);
            return true;
        } catch (Exception e) {
            LOG.debug("Database does not exist: {}", name);
            return false;
        }
    }
    
    // ========== Table Operations ==========
    
    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        
        LOG.info("Creating table: {} (ignoreIfExists={})", tablePath, ignoreIfExists);
        
        try {
            String dbName = tablePath.getDatabaseName();
            String tblName = tablePath.getObjectName();
            
            if (!databaseExists(dbName)) {
                throw new DatabaseNotExistException(getName(), dbName);
            }
            
            if (tableExists(tablePath)) {
                if (ignoreIfExists) {
                    LOG.info("Table already exists, skipping creation: {}", tablePath);
                    return;
                } else {
                    throw new TableAlreadyExistException(getName(), tablePath);
                }
            }
            
            String[] tableId = transformTableNameToId(dbName, tblName);
            CreateEmptyTableRequest request = new CreateEmptyTableRequest();
            request.setId(Arrays.asList(tableId));
            Map<String, String> properties = table.getOptions();
            if (properties != null) {
                request.setProperties(properties);
            }
            namespace().createEmptyTable(request);
            
            LOG.info("Table created successfully: {}", tablePath);
        } catch (TableAlreadyExistException | DatabaseNotExistException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to create table: {}", tablePath, e);
            throw new CatalogException("Failed to create table: " + tablePath, e);
        }
    }
    
    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        
        LOG.info("Dropping table: {}", tablePath);
        
        try {
            if (!tableExists(tablePath)) {
                if (ignoreIfNotExists) {
                    LOG.info("Table does not exist, skipping drop: {}", tablePath);
                    return;
                } else {
                    throw new TableNotExistException(getName(), tablePath);
                }
            }
            
            String[] tableId = transformTableNameToId(tablePath.getDatabaseName(), tablePath.getObjectName());
            DropTableRequest request = new DropTableRequest();
            request.setId(Arrays.asList(tableId));
            namespace().dropTable(request);
            
            LOG.info("Table dropped successfully: {}", tablePath);
        } catch (TableNotExistException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to drop table: {}", tablePath, e);
            throw new CatalogException("Failed to drop table: " + tablePath, e);
        }
    }
    
    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        
        LOG.debug("Listing tables in database: {}", databaseName);
        
        try {
            if (!databaseExists(databaseName)) {
                throw new DatabaseNotExistException(getName(), databaseName);
            }
            
            String[] namespacePath = transformDatabaseNameToNamespace(databaseName);
            ListTablesRequest request = new ListTablesRequest();
            request.setId(Arrays.asList(namespacePath));
            ListTablesResponse response = namespace().listTables(request);
            if (response.getTables() != null) {
                Set<String> tableSet = response.getTables();
                return new ArrayList<>(tableSet);
            }
            return new ArrayList<>();
        } catch (DatabaseNotExistException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to list tables in database: {}", databaseName, e);
            throw new CatalogException("Failed to list tables in database: " + databaseName, e);
        }
    }
    
    @Override
    public CatalogTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        
        LOG.debug("Getting table: {}", tablePath);
        
        try {
            if (!tableExists(tablePath)) {
                throw new TableNotExistException(getName(), tablePath);
            }
            
            String[] tableId = transformTableNameToId(tablePath.getDatabaseName(), tablePath.getObjectName());
            DescribeTableRequest request = new DescribeTableRequest();
            request.setId(Arrays.asList(tableId));
            DescribeTableResponse response = namespace().describeTable(request);

            String location = response.getLocation();
            Map<String, String> options = response.getProperties() != null
                    ? response.getProperties() : new HashMap<>();
            LanceNamespaceAdapter.TableMetadata metadata = new LanceNamespaceAdapter.TableMetadata(location, options);
            
            return createCatalogTable(tablePath.getDatabaseName(), tablePath.getObjectName(), metadata);
        } catch (TableNotExistException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to get table: {}", tablePath, e);
            throw new CatalogException("Failed to get table: " + tablePath, e);
        }
    }
    
    @Override
    public boolean tableExists(ObjectPath tablePath) {
        LOG.debug("Checking if table exists: {}", tablePath);
        
        try {
            String[] tableId = transformTableNameToId(tablePath.getDatabaseName(), tablePath.getObjectName());
            TableExistsRequest request = new TableExistsRequest();
            request.setId(Arrays.asList(tableId));
            namespace().tableExists(request);
            return true;
        } catch (Exception e) {
            LOG.debug("Table does not exist: {}", tablePath);
            return false;
        }
    }
    
    // ========== Abstract method to be implemented by subclasses ==========
    
    protected abstract CatalogTable createCatalogTable(
            String databaseName,
            String tableName,
            LanceNamespaceAdapter.TableMetadata metadata) throws CatalogException;
    
    // ========== Helper methods ==========
    
    protected String[] transformDatabaseNameToNamespace(String databaseName) {
        String[] baseNamespace = new String[] {databaseName};
        
        if (parentPrefix.isPresent()) {
            String[] parent = parentPrefix.get();
            String[] result = new String[parent.length + baseNamespace.length];
            System.arraycopy(parent, 0, result, 0, parent.length);
            System.arraycopy(baseNamespace, 0, result, parent.length, baseNamespace.length);
            return result;
        } else if (extraLevel.isPresent()) {
            String[] result = new String[baseNamespace.length + 1];
            result[0] = extraLevel.get();
            System.arraycopy(baseNamespace, 0, result, 1, baseNamespace.length);
            return result;
        } else {
            return baseNamespace;
        }
    }
    
    protected String[] transformTableNameToId(String databaseName, String tableName) {
        String[] dbPath = transformDatabaseNameToNamespace(databaseName);
        String[] result = new String[dbPath.length + 1];
        System.arraycopy(dbPath, 0, result, 0, dbPath.length);
        result[dbPath.length] = tableName;
        return result;
    }
    
    // ========== Not implemented - Partition operations not supported ==========
    
    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
            CatalogPartition partition, boolean ignoreIfExists)
            throws TableNotExistException, PartitionAlreadyExistsException, CatalogException {
        throw new CatalogException("Partition operations are not supported");
    }
    
    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new CatalogException("Partition operations are not supported");
    }
    
    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        throw new CatalogException("Partition operations are not supported");
    }
    
    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new CatalogException("Partition operations are not supported");
    }
    
    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        throw new CatalogException("Partition operations are not supported");
    }
    
    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new CatalogException("Partition operations are not supported");
    }
    
    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<org.apache.flink.table.expressions.Expression> filters)
            throws CatalogException {
        throw new CatalogException("Partition operations are not supported");
    }
    
    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        LOG.debug("Alter table statistics not supported: {}", tablePath);
    }
    
    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new CatalogException("Partition operations are not supported");
    }
    
    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        LOG.debug("Alter table column statistics not supported: {}", tablePath);
    }
    
    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new CatalogException("Partition operations are not supported");
    }
    
    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        LOG.debug("Get table statistics not supported: {}", tablePath);
        return null;
    }
    
    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        LOG.debug("Get table column statistics not supported: {}", tablePath);
        return null;
    }
    
    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new CatalogException("Partition operations are not supported");
    }
    
    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new CatalogException("Partition operations are not supported");
    }
    
    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        LOG.debug("Alter table not supported: {}", tablePath);
    }
    
    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws CatalogException {
        LOG.debug("Rename table not supported: {} -> {}", tablePath, newTableName);
    }
    
    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        LOG.debug("Alter database not supported: {}", name);
    }
    
    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new CatalogException("Function operations are not supported");
    }
    
    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws CatalogException {
        throw new CatalogException("Function operations are not supported");
    }
    
    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new CatalogException("Function operations are not supported");
    }
    
    @Override
    public List<String> listFunctions(String databaseName)
            throws CatalogException {
        throw new CatalogException("Function operations are not supported");
    }
    
    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws CatalogException {
        throw new CatalogException("Function operations are not supported");
    }
    
    @Override
    public boolean functionExists(ObjectPath functionPath)
            throws CatalogException {
        throw new CatalogException("Function operations are not supported");
    }
}
