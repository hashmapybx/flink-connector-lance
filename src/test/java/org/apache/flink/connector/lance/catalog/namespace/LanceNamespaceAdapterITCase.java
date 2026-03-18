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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Lance Namespace 集成测试。
 * 
 * <p>本测试直接使用 Lance Namespace API（通过 LanceNamespaceAdapter 管理连接），
 * 使用真实的 Lance Namespace 后端（DirectoryNamespace 和 RestNamespace），无 mock。
 */
@DisplayName("Lance Namespace Integration Test")
class LanceNamespaceAdapterITCase {
    
    @TempDir
    Path tempDir;
    
    private LanceNamespaceAdapter adapter;
    private LanceNamespace ns;
    private String warehousePath;
    
    @BeforeEach
    void setUp() {
        warehousePath = tempDir.resolve("warehouse").toString();
        
        Map<String, String> properties = new HashMap<>();
        properties.put(LanceNamespaceConfig.KEY_IMPL, "dir");
        properties.put(LanceNamespaceConfig.KEY_ROOT, warehousePath);
        
        adapter = LanceNamespaceAdapter.create(properties);
        adapter.init();
        ns = adapter.getNamespace();
    }
    
    @AfterEach
    void tearDown() throws Exception {
        if (adapter != null) {
            adapter.close();
        }
    }
    
    // ========== 辅助方法 ==========
    
    private void createNamespace(Map<String, String> props, String... id) {
        CreateNamespaceRequest req = new CreateNamespaceRequest();
        req.setId(Arrays.asList(id));
        if (props != null) {
            req.setProperties(props);
        }
        ns.createNamespace(req);
    }
    
    private boolean namespaceExists(String... id) {
        try {
            NamespaceExistsRequest req = new NamespaceExistsRequest();
            req.setId(Arrays.asList(id));
            ns.namespaceExists(req);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    private List<String> listNamespaces(String... parent) {
        ListNamespacesRequest req = new ListNamespacesRequest();
        if (parent.length > 0) {
            req.setId(Arrays.asList(parent));
        }
        ListNamespacesResponse resp = ns.listNamespaces(req);
        if (resp.getNamespaces() != null) {
            return new ArrayList<>(resp.getNamespaces());
        }
        return new ArrayList<>();
    }
    
    private Map<String, String> getNamespaceMetadata(String... id) {
        DescribeNamespaceRequest req = new DescribeNamespaceRequest();
        req.setId(Arrays.asList(id));
        DescribeNamespaceResponse resp = ns.describeNamespace(req);
        return resp.getProperties() != null ? resp.getProperties() : new HashMap<>();
    }
    
    private void dropNamespace(boolean cascade, String... id) {
        DropNamespaceRequest req = new DropNamespaceRequest();
        req.setId(Arrays.asList(id));
        req.setBehavior(cascade ? "CASCADE" : "RESTRICT");
        ns.dropNamespace(req);
    }
    
    private void createEmptyTable(String location, Map<String, String> props, String... id) {
        CreateEmptyTableRequest req = new CreateEmptyTableRequest();
        req.setId(Arrays.asList(id));
        req.setLocation(location);
        if (props != null) {
            req.setProperties(props);
        }
        ns.createEmptyTable(req);
    }
    
    private boolean tableExists(String... id) {
        try {
            TableExistsRequest req = new TableExistsRequest();
            req.setId(Arrays.asList(id));
            ns.tableExists(req);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    private List<String> listTables(String... namespaceId) {
        ListTablesRequest req = new ListTablesRequest();
        req.setId(Arrays.asList(namespaceId));
        ListTablesResponse resp = ns.listTables(req);
        if (resp.getTables() != null) {
            return new ArrayList<>(resp.getTables());
        }
        return new ArrayList<>();
    }
    
    private LanceNamespaceAdapter.TableMetadata getTableMetadata(String... id) {
        DescribeTableRequest req = new DescribeTableRequest();
        req.setId(Arrays.asList(id));
        DescribeTableResponse resp = ns.describeTable(req);
        String location = resp.getLocation();
        Map<String, String> options = resp.getProperties() != null ? resp.getProperties() : new HashMap<>();
        return new LanceNamespaceAdapter.TableMetadata(location, options);
    }
    
    private void dropTable(String... id) {
        DropTableRequest req = new DropTableRequest();
        req.setId(Arrays.asList(id));
        ns.dropTable(req);
    }
    
    // ==================== Namespace 管理测试 ====================
    
    @Test
    @DisplayName("Test creating namespace")
    void testCreateNamespace() {
        String namespaceName = "test_db";
        Map<String, String> properties = new HashMap<>();
        properties.put("description", "Test database");
        
        createNamespace(properties, namespaceName);
        
        assertThat(namespaceExists(namespaceName)).isTrue();
        Map<String, String> metadata = getNamespaceMetadata(namespaceName);
        assertThat(metadata).isNotNull();
    }
    
    @Test
    @DisplayName("Test creating nested namespace")
    void testCreateNestedNamespace() {
        String parentNamespace = "parent_db";
        String childNamespace = "child_db";
        
        createNamespace(new HashMap<>(), parentNamespace);
        createNamespace(new HashMap<>(), parentNamespace, childNamespace);
        
        assertThat(namespaceExists(parentNamespace)).isTrue();
        assertThat(namespaceExists(parentNamespace, childNamespace)).isTrue();
    }
    
    @Test
    @DisplayName("Test listing all top-level namespaces")
    void testListNamespaces() {
        createNamespace(new HashMap<>(), "db1");
        createNamespace(new HashMap<>(), "db2");
        createNamespace(new HashMap<>(), "db3");
        
        List<String> namespaces = listNamespaces();
        
        assertThat(namespaces).isNotNull();
        assertThat(namespaces).contains("db1", "db2", "db3");
        assertThat(namespaces.size()).isGreaterThanOrEqualTo(3);
    }
    
    @Test
    @DisplayName("Test listing child namespaces")
    void testListChildNamespaces() {
        String parent = "my_warehouse";
        createNamespace(new HashMap<>(), parent);
        createNamespace(new HashMap<>(), parent, "schema1");
        createNamespace(new HashMap<>(), parent, "schema2");
        
        List<String> childNamespaces = listNamespaces(parent);
        
        assertThat(childNamespaces).isNotNull();
        assertThat(childNamespaces).contains("schema1", "schema2");
    }
    
    @Test
    @DisplayName("Test checking namespace existence")
    void testNamespaceExists() {
        String namespaceName = "existing_db";
        createNamespace(new HashMap<>(), namespaceName);
        
        assertThat(namespaceExists(namespaceName)).isTrue();
        assertThat(namespaceExists("non_existing_db")).isFalse();
    }
    
    @Test
    @DisplayName("Test dropping namespace")
    void testDropNamespace() {
        String namespaceName = "temp_db";
        createNamespace(new HashMap<>(), namespaceName);
        assertThat(namespaceExists(namespaceName)).isTrue();
        
        dropNamespace(false, namespaceName);
        
        assertThat(namespaceExists(namespaceName)).isFalse();
    }
    
    @Test
    @DisplayName("Test cascade dropping namespace and its contents")
    void testDropNamespaceCascade() {
        String namespaceName = "cascade_db";
        createNamespace(new HashMap<>(), namespaceName);
        
        dropNamespace(true, namespaceName);
        
        assertThat(namespaceExists(namespaceName)).isFalse();
    }
    
    @Test
    @DisplayName("Test getting namespace metadata")
    void testGetNamespaceMetadata() {
        String namespaceName = "metadata_db";
        Map<String, String> properties = new HashMap<>();
        properties.put("owner", "admin");
        properties.put("environment", "test");
        
        createNamespace(properties, namespaceName);
        
        Map<String, String> metadata = getNamespaceMetadata(namespaceName);
        
        assertThat(metadata).isNotNull();
        assertThat(metadata).containsKeys("owner", "environment");
    }
    
    // ==================== Table 管理测试 ====================
    
    @Test
    @DisplayName("Test creating table in namespace")
    void testCreateTable() {
        String namespaceName = "my_db";
        String tableName = "my_table";
        createNamespace(new HashMap<>(), namespaceName);
        
        String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("format", "lance");
        
        createEmptyTable(tableLocation, tableProperties, namespaceName, tableName);
        
        assertThat(tableExists(namespaceName, tableName)).isTrue();
    }
    
    @Test
    @DisplayName("Test creating multiple tables in same namespace")
    void testCreateMultipleTables() {
        String namespaceName = "test_db";
        createNamespace(new HashMap<>(), namespaceName);
        
        String[] tableNames = {"users", "products", "orders", "analytics"};
        
        for (String tableName : tableNames) {
            String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
            createEmptyTable(tableLocation, new HashMap<>(), namespaceName, tableName);
        }
        
        List<String> tables = listTables(namespaceName);
        assertThat(tables).isNotNull();
        assertThat(tables).contains(tableNames);
    }
    
    @Test
    @DisplayName("Test listing all tables in namespace")
    void testListTables() {
        String namespaceName = "query_db";
        createNamespace(new HashMap<>(), namespaceName);
        
        createEmptyTable(
            warehousePath + "/" + namespaceName + "/table1",
            new HashMap<>(),
            namespaceName, "table1"
        );
        createEmptyTable(
            warehousePath + "/" + namespaceName + "/table2",
            new HashMap<>(),
            namespaceName, "table2"
        );
        
        List<String> tables = listTables(namespaceName);
        
        assertThat(tables).isNotNull();
        assertThat(tables).contains("table1", "table2");
        assertThat(tables.size()).isGreaterThanOrEqualTo(2);
    }
    
    @Test
    @DisplayName("Test checking table existence")
    void testTableExists() {
        String namespaceName = "check_db";
        String tableName = "check_table";
        createNamespace(new HashMap<>(), namespaceName);
        
        String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
        createEmptyTable(tableLocation, new HashMap<>(), namespaceName, tableName);
        
        assertThat(tableExists(namespaceName, tableName)).isTrue();
        assertThat(tableExists(namespaceName, "non_existing_table")).isFalse();
    }
    
    @Test
    @DisplayName("Test getting table metadata")
    void testGetTableMetadata() {
        String namespaceName = "metadata_db";
        String tableName = "metadata_table";
        createNamespace(new HashMap<>(), namespaceName);
        
        String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("format", "lance");
        tableProperties.put("index", "ivf");
        
        createEmptyTable(tableLocation, tableProperties, namespaceName, tableName);
        
        LanceNamespaceAdapter.TableMetadata metadata = 
            getTableMetadata(namespaceName, tableName);
        
        assertThat(metadata).isNotNull();
        assertThat(metadata.getLocation()).isNotNull();
        assertThat(metadata.getStorageOptions()).isNotNull();
    }
    
    @Test
    @DisplayName("Test dropping table")
    void testDropTable() {
        String namespaceName = "drop_db";
        String tableName = "drop_table";
        createNamespace(new HashMap<>(), namespaceName);
        
        String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
        createEmptyTable(tableLocation, new HashMap<>(), namespaceName, tableName);
        assertThat(tableExists(namespaceName, tableName)).isTrue();
        
        dropTable(namespaceName, tableName);
        
        assertThat(tableExists(namespaceName, tableName)).isFalse();
    }
    
    @Test
    @DisplayName("Test dropping multiple tables in namespace")
    void testDropMultipleTables() {
        String namespaceName = "cleanup_db";
        createNamespace(new HashMap<>(), namespaceName);
        
        String[] tableNames = {"temp1", "temp2", "temp3"};
        for (String tableName : tableNames) {
            String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
            createEmptyTable(tableLocation, new HashMap<>(), namespaceName, tableName);
        }
        
        for (String tableName : tableNames) {
            assertThat(tableExists(namespaceName, tableName)).isTrue();
        }
        
        for (String tableName : tableNames) {
            dropTable(namespaceName, tableName);
        }
        
        for (String tableName : tableNames) {
            assertThat(tableExists(namespaceName, tableName)).isFalse();
        }
    }
    
    // ==================== 综合场景测试 ====================
    
    @Test
    @DisplayName("Test complete table CRUD lifecycle")
    void testCompleteTableCrudLifecycle() {
        // 1. 创建 namespace
        String namespaceName = "complete_db";
        Map<String, String> dbProps = new HashMap<>();
        dbProps.put("owner", "admin");
        createNamespace(dbProps, namespaceName);
        assertThat(namespaceExists(namespaceName)).isTrue();
        
        // 2. 创建 table
        String tableName = "complete_table";
        String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("format", "lance");
        createEmptyTable(tableLocation, tableProps, namespaceName, tableName);
        assertThat(tableExists(namespaceName, tableName)).isTrue();
        
        // 3. 列出 tables
        List<String> tables = listTables(namespaceName);
        assertThat(tables).contains(tableName);
        
        // 4. 获取 table metadata
        LanceNamespaceAdapter.TableMetadata tableMetadata = 
            getTableMetadata(namespaceName, tableName);
        assertThat(tableMetadata.getLocation()).contains(tableName);
        
        // 5. 删除 table
        dropTable(namespaceName, tableName);
        assertThat(tableExists(namespaceName, tableName)).isFalse();
        
        // 6. 删除 namespace
        dropNamespace(true, namespaceName);
        assertThat(namespaceExists(namespaceName)).isFalse();
    }
    
    @Test
    @DisplayName("Test independence of multiple namespaces")
    void testMultipleNamespaceIndependence() {
        String db1 = "database1";
        String db2 = "database2";
        String tableName = "test_table";
        
        createNamespace(new HashMap<>(), db1);
        createNamespace(new HashMap<>(), db2);
        
        createEmptyTable(
            warehousePath + "/" + db1 + "/" + tableName,
            new HashMap<>(),
            db1, tableName
        );
        createEmptyTable(
            warehousePath + "/" + db2 + "/" + tableName,
            new HashMap<>(),
            db2, tableName
        );
        
        assertThat(tableExists(db1, tableName)).isTrue();
        assertThat(tableExists(db2, tableName)).isTrue();
        
        dropTable(db1, tableName);
        assertThat(tableExists(db1, tableName)).isFalse();
        assertThat(tableExists(db2, tableName)).isTrue();
    }
    
    @Test
    @DisplayName("Test naming with underscores and numbers")
    void testSpecialCharacterNaming() {
        String namespaceName = "test_db_123";
        String tableName = "data_table_v2_001";
        
        createNamespace(new HashMap<>(), namespaceName);
        
        String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
        createEmptyTable(tableLocation, new HashMap<>(), namespaceName, tableName);
        
        assertThat(namespaceExists(namespaceName)).isTrue();
        assertThat(tableExists(namespaceName, tableName)).isTrue();
    }
    
    @Test
    @DisplayName("Test creating many tables in single namespace")
    void testCreateManyTables() {
        String namespaceName = "scale_db";
        createNamespace(new HashMap<>(), namespaceName);
        
        int tableCount = 50;
        
        for (int i = 0; i < tableCount; i++) {
            String tableName = "table_" + String.format("%03d", i);
            String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
            createEmptyTable(tableLocation, new HashMap<>(), namespaceName, tableName);
        }
        
        List<String> tables = listTables(namespaceName);
        assertThat(tables).isNotNull();
        assertThat(tables.size()).isGreaterThanOrEqualTo(tableCount);
    }
    
    @Test
    @DisplayName("Test creating existing namespace throws exception")
    void testCreateExistingNamespaceThrowsException() {
        String namespaceName = "existing_db";
        createNamespace(new HashMap<>(), namespaceName);
        
        assertThatThrownBy(() -> 
            createNamespace(new HashMap<>(), namespaceName)
        ).isNotNull();
    }
    
    @Test
    @DisplayName("Test dropping non-existing namespace throws exception")
    void testDropNonExistingNamespaceThrowsException() {
        assertThatThrownBy(() -> 
            dropNamespace(false, "non_existing_db")
        ).isNotNull();
    }
    
    @Test
    @DisplayName("Test dropping non-existing table throws exception")
    void testDropNonExistingTableThrowsException() {
        String namespaceName = "error_db";
        createNamespace(new HashMap<>(), namespaceName);
        
        assertThatThrownBy(() -> 
            dropTable(namespaceName, "non_existing_table")
        ).isNotNull();
    }
    
    @Test
    @DisplayName("Test adapter closes correctly and releases resources")
    void testAdapterCloseAndResourceCleanup() throws Exception {
        String namespaceName = "cleanup_db";
        createNamespace(new HashMap<>(), namespaceName);
        
        assertThat(namespaceExists(namespaceName)).isTrue();
        
        adapter.close();
        
        // 创建新的 adapter 验证数据持久化
        Map<String, String> properties = new HashMap<>();
        properties.put(LanceNamespaceConfig.KEY_IMPL, "dir");
        properties.put(LanceNamespaceConfig.KEY_ROOT, warehousePath);
        
        LanceNamespaceAdapter newAdapter = LanceNamespaceAdapter.create(properties);
        newAdapter.init();
        LanceNamespace newNs = newAdapter.getNamespace();
        
        try {
            NamespaceExistsRequest req = new NamespaceExistsRequest();
            req.setId(Arrays.asList(namespaceName));
            newNs.namespaceExists(req);
            // 如果没有抛异常，说明 namespace 存在
        } finally {
            newAdapter.close();
        }
    }
    
    // ==================== REST Namespace 测试 ====================
    
    @Nested
    @DisplayName("REST Namespace Backend Tests")
    @EnabledIfEnvironmentVariable(named = "LANCE_REST_URI", matches = ".+")
    class RestNamespaceTests {
        
        private LanceNamespaceAdapter restAdapter;
        private LanceNamespace restNs;
        
        @BeforeEach
        void setUp() {
            String restUri = System.getenv("LANCE_REST_URI");
            
            Map<String, String> properties = new HashMap<>();
            properties.put(LanceNamespaceConfig.KEY_IMPL, "rest");
            properties.put(LanceNamespaceConfig.KEY_URI, restUri);
            
            restAdapter = LanceNamespaceAdapter.create(properties);
            restAdapter.init();
            restNs = restAdapter.getNamespace();
        }
        
        @AfterEach
        void tearDown() throws Exception {
            if (restAdapter != null) {
                restAdapter.close();
            }
        }
        
        @Test
        @DisplayName("Test REST namespace creation and listing")
        void testRestNamespaceOperations() {
            String testNamespace = "rest_test_db_" + System.currentTimeMillis();
            
            try {
                CreateNamespaceRequest createReq = new CreateNamespaceRequest();
                createReq.setId(Arrays.asList(testNamespace));
                restNs.createNamespace(createReq);
                
                NamespaceExistsRequest existsReq = new NamespaceExistsRequest();
                existsReq.setId(Arrays.asList(testNamespace));
                restNs.namespaceExists(existsReq);
                
                ListNamespacesRequest listReq = new ListNamespacesRequest();
                ListNamespacesResponse listResp = restNs.listNamespaces(listReq);
                assertThat(listResp.getNamespaces()).contains(testNamespace);
            } finally {
                try {
                    DropNamespaceRequest dropReq = new DropNamespaceRequest();
                    dropReq.setId(Arrays.asList(testNamespace));
                    dropReq.setBehavior("CASCADE");
                    restNs.dropNamespace(dropReq);
                } catch (Exception ignored) {
                }
            }
        }
        
        @Test
        @DisplayName("Test REST table operations")
        void testRestTableOperations() {
            String testNamespace = "rest_table_db_" + System.currentTimeMillis();
            String testTable = "rest_test_table";
            
            try {
                CreateNamespaceRequest createNsReq = new CreateNamespaceRequest();
                createNsReq.setId(Arrays.asList(testNamespace));
                restNs.createNamespace(createNsReq);
                
                CreateEmptyTableRequest createTblReq = new CreateEmptyTableRequest();
                createTblReq.setId(Arrays.asList(testNamespace, testTable));
                restNs.createEmptyTable(createTblReq);
                
                TableExistsRequest existsReq = new TableExistsRequest();
                existsReq.setId(Arrays.asList(testNamespace, testTable));
                restNs.tableExists(existsReq);
                
                ListTablesRequest listReq = new ListTablesRequest();
                listReq.setId(Arrays.asList(testNamespace));
                ListTablesResponse listResp = restNs.listTables(listReq);
                assertThat(listResp.getTables()).contains(testTable);
                
                DescribeTableRequest descReq = new DescribeTableRequest();
                descReq.setId(Arrays.asList(testNamespace, testTable));
                DescribeTableResponse descResp = restNs.describeTable(descReq);
                assertThat(descResp).isNotNull();
                
                DropTableRequest dropTblReq = new DropTableRequest();
                dropTblReq.setId(Arrays.asList(testNamespace, testTable));
                restNs.dropTable(dropTblReq);
                
                try {
                    restNs.tableExists(existsReq);
                    // 如果没抛异常说明还存在，这不应该发生
                    assertThat(false).isTrue();
                } catch (Exception e) {
                    // 预期: table 已不存在
                }
            } finally {
                try {
                    DropNamespaceRequest dropReq = new DropNamespaceRequest();
                    dropReq.setId(Arrays.asList(testNamespace));
                    dropReq.setBehavior("CASCADE");
                    restNs.dropNamespace(dropReq);
                } catch (Exception ignored) {
                }
            }
        }
    }
}
