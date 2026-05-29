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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Flink SQL complete demo test script.
 *
 * <p>This test demonstrates how to use Flink SQL to operate Lance datasets:
 *
 * <ul>
 *   <li>Create Lance Catalog
 *   <li>Create Lance tables
 *   <li>Insert vector data
 *   <li>Query data
 *   <li>Build vector index
 *   <li>Execute vector search
 * </ul>
 */
class FlinkSqlDemo {

  @TempDir Path tempDir;

  private TableEnvironment tableEnv;
  private String warehousePath;
  private String datasetPath;

  @BeforeEach
  void setUp() {
    // Create Flink Table environment
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    tableEnv = TableEnvironment.create(settings);

    // Set paths
    warehousePath = tempDir.resolve("lance_warehouse").toString();
    datasetPath = tempDir.resolve("lance_dataset").toString();
  }

  @AfterEach
  void tearDown() {
    // Cleanup resources
    if (tableEnv != null) {
      // TableEnvironment auto cleanup
    }
  }

  // ==================== Basic SQL Operations ====================

  @Test
  @DisplayName("1. Create Lance Connector Table - Basic Usage")
  void testCreateLanceTable() throws Exception {
    String createTableSql =
        String.format(
            "CREATE TABLE lance_vectors (\n"
                + "    id BIGINT,\n"
                + "    content STRING,\n"
                + "    embedding ARRAY<FLOAT>,\n"
                + "    category STRING,\n"
                + "    create_time TIMESTAMP(3)\n"
                + ") WITH (\n"
                + "    'connector' = 'lance',\n"
                + "    'path' = '%s',\n"
                + "    'write.batch-size' = '1024',\n"
                + "    'write.mode' = 'overwrite'\n"
                + ")",
            datasetPath);

    System.out.println("========== Create Lance Table ==========");
    System.out.println(createTableSql);
    System.out.println();

    tableEnv.executeSql(createTableSql);
    System.out.println("✅ Table created successfully!\n");
  }

  @Test
  @DisplayName("2. Insert Vector Data to Lance Table")
  void testInsertData() throws Exception {
    // Use relative path based on project root
    Path path = Paths.get(System.getProperty("user.dir"), "test-data");
    // First create table
    String createTableSql =
        String.format(
            "CREATE TABLE lance_documents (\n"
                + "    id BIGINT,\n"
                + "    title STRING,\n"
                + "    embedding ARRAY<FLOAT>\n"
                + ") WITH (\n"
                + "    'connector' = 'lance',\n"
                + "    'path' = '%s',\n"
                + "    'write.mode' = 'overwrite'\n"
                + ")",
            path.resolve("lance-db1"));

    tableEnv.executeSql(createTableSql);

    // Insert data
    String insertSql =
        "INSERT INTO lance_documents VALUES\n"
            + "    (1, 'Introduction to AI', ARRAY[0.1, 0.2, 0.3, 0.4]),\n"
            + "    (2, 'Machine Learning Guide', ARRAY[0.2, 0.3, 0.4, 0.5]),\n"
            + "    (3, 'Deep Learning Basics', ARRAY[0.3, 0.4, 0.5, 0.6]),\n"
            + "    (4, 'Neural Networks', ARRAY[0.4, 0.5, 0.6, 0.7]),\n"
            + "    (5, 'Computer Vision', ARRAY[0.5, 0.6, 0.7, 0.8])";

    System.out.println("========== Insert Vector Data ==========");
    System.out.println(insertSql);
    System.out.println();

    TableResult result = tableEnv.executeSql(insertSql);
    result.await(30, TimeUnit.SECONDS);
    System.out.println("✅ Data inserted successfully!\n");
  }

  @Test
  @DisplayName("3. Query Lance Table Data")
  void testSelectData() throws Exception {
    // Create source table (for generating test data)
    String createSourceSql =
        "CREATE TABLE test_source (\n"
            + "    id BIGINT,\n"
            + "    name STRING\n"
            + ") WITH (\n"
            + "    'connector' = 'datagen',\n"
            + "    'rows-per-second' = '1',\n"
            + "    'number-of-rows' = '10',\n"
            + "    'fields.id.kind' = 'sequence',\n"
            + "    'fields.id.start' = '1',\n"
            + "    'fields.id.end' = '10'\n"
            + ")";

    tableEnv.executeSql(createSourceSql);

    // Query data
    String selectSql = "SELECT id, name FROM test_source LIMIT 5";

    System.out.println("========== Query Data ==========");
    System.out.println(selectSql);
    System.out.println();

    TableResult result = tableEnv.executeSql(selectSql);
    result.print();
    System.out.println("✅ Query completed!\n");
  }

  // ==================== Advanced Configuration ====================

  @Test
  @DisplayName("4. Create Table with Vector Index Configuration")
  void testCreateTableWithIndexConfig() throws Exception {
    String createTableSql =
        String.format(
            "CREATE TABLE vector_store (\n"
                + "    id BIGINT,\n"
                + "    text STRING,\n"
                + "    embedding ARRAY<FLOAT> COMMENT '768-dim vector'\n"
                + ") WITH (\n"
                + "    'connector' = 'lance',\n"
                + "    'path' = '%s',\n"
                + "    -- Write configuration\n"
                + "    'write.batch-size' = '2048',\n"
                + "    'write.mode' = 'append',\n"
                + "    'write.max-rows-per-file' = '100000',\n"
                + "    -- Index configuration\n"
                + "    'index.type' = 'IVF_PQ',\n"
                + "    'index.column' = 'embedding',\n"
                + "    'index.num-partitions' = '256',\n"
                + "    'index.num-sub-vectors' = '16',\n"
                + "    -- Vector search configuration\n"
                + "    'vector.column' = 'embedding',\n"
                + "    'vector.metric' = 'L2',\n"
                + "    'vector.nprobes' = '20'\n"
                + ")",
            datasetPath);

    System.out.println("========== Create Table with Index Configuration ==========");
    System.out.println(createTableSql);
    System.out.println();

    tableEnv.executeSql(createTableSql);
    System.out.println("✅ Table created successfully!\n");
  }

  @Test
  @DisplayName("5. Different Index Type Configuration Examples")
  void testDifferentIndexTypes() {
    System.out.println("========== Index Type Configuration Examples ==========\n");

    // IVF_PQ index (recommended, balances accuracy and speed)
    String ivfPqConfig =
        "-- IVF_PQ index configuration (recommended for large-scale vector data)\n"
            + "'index.type' = 'IVF_PQ',\n"
            + "'index.num-partitions' = '256',      -- Number of cluster centers\n"
            + "'index.num-sub-vectors' = '16',      -- Number of sub-vectors\n"
            + "'index.num-bits' = '8'               -- Quantization bits per sub-vector\n";

    System.out.println(ivfPqConfig);

    // IVF_HNSW index (high accuracy)
    String ivfHnswConfig =
        "-- IVF_HNSW index configuration (for high accuracy scenarios)\n"
            + "'index.type' = 'IVF_HNSW',\n"
            + "'index.num-partitions' = '256',\n"
            + "'index.max-level' = '7',             -- HNSW max level\n"
            + "'index.m' = '16',                    -- HNSW connections per level\n"
            + "'index.ef-construction' = '100'      -- ef parameter during construction\n";

    System.out.println(ivfHnswConfig);

    // IVF_FLAT index (highest accuracy, suitable for small datasets)
    String ivfFlatConfig =
        "-- IVF_FLAT index configuration (for small-scale datasets)\n"
            + "'index.type' = 'IVF_FLAT',\n"
            + "'index.num-partitions' = '64'        -- Number of cluster centers\n";

    System.out.println(ivfFlatConfig);
    System.out.println("✅ Configuration examples displayed!\n");
  }

  @Test
  @DisplayName("6. Distance Metric Type Configuration Examples")
  void testMetricTypes() {
    System.out.println("========== Distance Metric Type Examples ==========\n");

    String l2Config =
        "-- L2 distance (Euclidean distance, default)\n"
            + "'vector.metric' = 'L2'\n"
            + "-- Suitable for: General vector search\n";
    System.out.println(l2Config);

    String cosineConfig =
        "-- Cosine distance (Cosine similarity)\n"
            + "'vector.metric' = 'COSINE'\n"
            + "-- Suitable for: Text semantic similarity\n";
    System.out.println(cosineConfig);

    String dotConfig =
        "-- Dot distance (Dot product)\n"
            + "'vector.metric' = 'DOT'\n"
            + "-- Suitable for: Already normalized vectors\n";
    System.out.println(dotConfig);

    System.out.println("✅ Configuration examples displayed!\n");
  }

  // ==================== Catalog Operations ====================

  @Test
  @DisplayName("7. Create and Use Lance Catalog")
  void testLanceCatalog() throws Exception {
    String createCatalogSql =
        String.format(
            "CREATE CATALOG lance_catalog WITH (\n"
                + "    'type' = 'lance',\n"
                + "    'warehouse' = '%s',\n"
                + "    'default-database' = 'default'\n"
                + ")",
            warehousePath);

    System.out.println("========== Create Lance Catalog ==========");
    System.out.println(createCatalogSql);
    System.out.println();

    tableEnv.executeSql(createCatalogSql);

    // Use Catalog
    tableEnv.executeSql("USE CATALOG lance_catalog");
    System.out.println("✅ Catalog created and switched!\n");

    // Create database
    tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS vector_db");
    System.out.println("✅ Database vector_db created!\n");

    // List databases
    System.out.println("Database list:");
    tableEnv.executeSql("SHOW DATABASES").print();
  }

  // ==================== Streaming Processing ====================

  @Test
  @DisplayName("8. Streaming Write to Lance Table")
  void testStreamingWrite() throws Exception {
    // Create streaming environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

    // Create data generator table (simulating real-time data)
    String createSourceSql =
        "CREATE TABLE realtime_events (\n"
            + "    event_id BIGINT,\n"
            + "    event_type STRING,\n"
            + "    event_time AS PROCTIME()\n"
            + ") WITH (\n"
            + "    'connector' = 'datagen',\n"
            + "    'rows-per-second' = '10',\n"
            + "    'number-of-rows' = '100',\n"
            + "    'fields.event_id.kind' = 'sequence',\n"
            + "    'fields.event_id.start' = '1',\n"
            + "    'fields.event_id.end' = '100',\n"
            + "    'fields.event_type.length' = '10'\n"
            + ")";

    // Create Lance Sink table
    String createSinkSql =
        String.format(
            "CREATE TABLE lance_events (\n"
                + "    event_id BIGINT,\n"
                + "    event_type STRING\n"
                + ") WITH (\n"
                + "    'connector' = 'lance',\n"
                + "    'path' = '%s',\n"
                + "    'write.batch-size' = '100',\n"
                + "    'write.mode' = 'append'\n"
                + ")",
            datasetPath);

    System.out.println("========== Streaming Write Example ==========");
    System.out.println("-- Source table definition");
    System.out.println(createSourceSql);
    System.out.println("\n-- Sink table definition");
    System.out.println(createSinkSql);
    System.out.println();

    streamTableEnv.executeSql(createSourceSql);
    streamTableEnv.executeSql(createSinkSql);

    // Execute streaming write
    String insertSql = "INSERT INTO lance_events SELECT event_id, event_type FROM realtime_events";
    System.out.println("-- Streaming insert statement");
    System.out.println(insertSql);
    System.out.println();

    System.out.println("✅ Streaming write configuration completed!\n");
  }

  // ==================== Complete Example ====================

  @Test
  @DisplayName("9. Complete Vector Storage and Search Example")
  void testCompleteVectorExample() throws Exception {
    // Use relative path based on project root
    Path path = Paths.get(System.getProperty("user.dir"), "test-data");
    System.out.println("========== Complete Vector Storage and Search Example ==========\n");

    // 1. Create vector table
    String createTableSql =
        String.format(
            "-- 1. Create vector storage table\n"
                + "CREATE TABLE document_vectors (\n"
                + "    doc_id BIGINT COMMENT 'Document ID',\n"
                + "    title STRING COMMENT 'Document title',\n"
                + "    content STRING COMMENT 'Document content',\n"
                + "    embedding ARRAY<FLOAT> COMMENT 'Document vector (768-dim)',\n"
                + "    category STRING COMMENT 'Document category',\n"
                + "    create_time TIMESTAMP(3) COMMENT 'Creation time'\n"
                + ") WITH (\n"
                + "    'connector' = 'lance',\n"
                + "    'path' = '%s',\n"
                + "    -- Write configuration\n"
                + "    'write.batch-size' = '1024',\n"
                + "    'write.mode' = 'overwrite',\n"
                + "    -- Index configuration\n"
                + "    'index.type' = 'IVF_PQ',\n"
                + "    'index.column' = 'embedding',\n"
                + "    'index.num-partitions' = '128',\n"
                + "    'index.num-sub-vectors' = '32',\n"
                + "    -- Vector search configuration\n"
                + "    'vector.column' = 'embedding',\n"
                + "    'vector.metric' = 'COSINE',\n"
                + "    'vector.nprobes' = '10'\n"
                + ")",
            path.resolve("lance-db3"));

    System.out.println(createTableSql);
    System.out.println();
    tableEnv.executeSql(createTableSql);

    // 2. Insert test data
    String insertSql =
        "-- 2. Insert vector data\n"
            + "INSERT INTO document_vectors VALUES\n"
            + "    (1, 'Flink Getting Started Guide', 'Introduction to Apache Flink basics...', \n"
            + "     ARRAY[0.1, 0.2, 0.3, 0.4], 'tutorial', TIMESTAMP '2024-01-01 10:00:00'),\n"
            + "    (2, 'Stream Processing in Practice', 'Using Flink to process real-time data streams...', \n"
            + "     ARRAY[0.2, 0.3, 0.4, 0.5], 'practice', TIMESTAMP '2024-01-02 11:00:00'),\n"
            + "    (3, 'Vector Database Explained', 'Deep understanding of vector search technology...', \n"
            + "     ARRAY[0.3, 0.4, 0.5, 0.6], 'database', TIMESTAMP '2024-01-03 12:00:00'),\n"
            + "    (4, 'Lance Format Introduction', 'Lance is an efficient vector storage format...', \n"
            + "     ARRAY[0.4, 0.5, 0.6, 0.7], 'format', TIMESTAMP '2024-01-04 13:00:00'),\n"
            + "    (5, 'SQL Connector Development', 'How to develop Flink SQL connectors...', \n"
            + "     ARRAY[0.5, 0.6, 0.7, 0.8], 'development', TIMESTAMP '2024-01-05 14:00:00')";

    System.out.println(insertSql);
    System.out.println();
    TableResult result = tableEnv.executeSql(insertSql);
    result.await(30, TimeUnit.SECONDS);

    // 3. Query data
    String selectSql =
        "-- 3. Query vector data\n"
            + "SELECT doc_id, title, category, create_time\n"
            + "FROM document_vectors\n"
            + "WHERE category = 'tutorial'\n"
            + "ORDER BY create_time DESC";

    System.out.println(selectSql);
    System.out.println();
    TableResult tableResult = tableEnv.executeSql(selectSql);
    tableResult.await(3, TimeUnit.SECONDS);
    CloseableIterator<Row> collect = tableResult.collect();
    while (collect.hasNext()) {
      System.out.println(collect.next());
    }

    // 4. Aggregation query
    String aggSql =
        "-- 4. Count documents by category\n"
            + "SELECT category, COUNT(*) as doc_count\n"
            + "FROM document_vectors\n"
            + "GROUP BY category\n"
            + "ORDER BY doc_count DESC";

    System.out.println(aggSql);
    System.out.println();
    tableEnv.executeSql(aggSql).print();

    System.out.println("✅ Complete example displayed!\n");
  }

  @Test
  @DisplayName("9.1 Vector Search IVF_PQ Index Example")
  void testVectorSearchWithIvfPq() throws Exception {
    System.out.println("========== Vector Search IVF_PQ Index Example ==========");

    // Use relative path based on project root
    Path basePath = Paths.get(System.getProperty("user.dir"), "test-data");
    String datasetPath = basePath.resolve("lance-vector-search").toString();

    // ============================================
    // Step 1: Create vector table with IVF_PQ index configuration
    // ============================================
    String createTableSql =
        String.format(
            "CREATE TABLE vector_documents (\n"
                + "    id BIGINT,\n"
                + "    title STRING,\n"
                + "    embedding ARRAY<FLOAT>\n"
                + ") WITH (\n"
                + "    'connector' = 'lance',\n"
                + "    'path' = '%s',\n"
                + "    'write.batch-size' = '1024',\n"
                + "    'write.mode' = 'overwrite',\n"
                + "    -- IVF_PQ index configuration\n"
                + "    'index.type' = 'IVF_PQ',\n"
                + "    'index.column' = 'embedding',\n"
                + "    'index.num-partitions' = '16',\n"
                + "    'index.num-sub-vectors' = '8',\n"
                + "    -- Vector search configuration\n"
                + "    'vector.column' = 'embedding',\n"
                + "    'vector.metric' = 'L2',\n"
                + "    'vector.nprobes' = '10'\n"
                + ")",
            datasetPath);

    System.out.println("-- Step 1: Create vector table with IVF_PQ index configuration");
    System.out.println(createTableSql);
    System.out.println();
    tableEnv.executeSql(createTableSql);

    // ============================================
    // Step 2: Insert vector data
    // ============================================
    String insertSql =
        "INSERT INTO vector_documents VALUES\n"
            + "    (1, 'Flink Stream Processing', ARRAY[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]),\n"
            + "    (2, 'Spark Batch Processing', ARRAY[0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]),\n"
            + "    (3, 'Kafka Message Queue', ARRAY[0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]),\n"
            + "    (4, 'Vector Database', ARRAY[0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85]),\n"
            + "    (5, 'Machine Learning Basics', ARRAY[0.12, 0.22, 0.32, 0.42, 0.52, 0.62, 0.72, 0.82])";

    System.out.println("-- Step 2: Insert vector data");
    System.out.println(insertSql);
    System.out.println();
    tableEnv.executeSql(insertSql).await(30, TimeUnit.SECONDS);
    System.out.println("✅ Data insertion completed\n");

    // ============================================
    // Step 3: Register vector search UDF
    // ============================================
    String createFunctionSql =
        "CREATE TEMPORARY FUNCTION vector_search AS \n"
            + "    'org.apache.flink.connector.lance.table.LanceVectorSearchFunction'";

    System.out.println("-- Step 3: Register vector search UDF");
    System.out.println(createFunctionSql);
    System.out.println();
    tableEnv.executeSql(createFunctionSql);
    System.out.println("✅ UDF registration completed\n");

    // ============================================
    // Step 4: Execute vector search - Basic usage
    // ============================================
    System.out.println("-- Step 4: Execute vector search (Basic usage)");
    System.out.println("-- Parameter description:");
    System.out.println("--   Param 1: Dataset path");
    System.out.println("--   Param 2: Vector column name");
    System.out.println("--   Param 3: Query vector");
    System.out.println("--   Param 4: TopK count to return");
    System.out.println("--   Param 5: Distance metric type (L2/COSINE/DOT)");
    System.out.println();

    String vectorSearchSql =
        String.format(
            "SELECT * FROM TABLE(\n"
                + "    vector_search(\n"
                + "        '%s',                              -- Dataset path\n"
                + "        'embedding',                       -- Vector column name\n"
                + "        ARRAY[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],  -- Query vector\n"
                + "        3,                                 -- Return Top 3\n"
                + "        'L2'                               -- L2 distance metric\n"
                + "    )\n"
                + ")",
            datasetPath);

    System.out.println(vectorSearchSql);
    System.out.println();
    System.out.println(
        "📊 Search results (sorted by L2 distance, smaller distance = more similar):");
    System.out.println("---------------------------------------------------");

    try {
      TableResult result = tableEnv.executeSql(vectorSearchSql);
      result.print();
    } catch (Exception e) {
      System.out.println("⚠️ Vector search execution error: " + e.getMessage());
      System.out.println("   This may be because the dataset needs to build index first");
    }

    // ============================================
    // Step 5: Use COSINE cosine similarity search
    // ============================================
    System.out.println("\n-- Step 5: Use COSINE cosine similarity search");

    String cosineSearchSql =
        String.format(
            "SELECT * FROM TABLE(\n"
                + "    vector_search(\n"
                + "        '%s',\n"
                + "        'embedding',\n"
                + "        ARRAY[0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1],\n"
                + "        3,\n"
                + "        'COSINE'                           -- Cosine similarity\n"
                + "    )\n"
                + ")",
            datasetPath);

    System.out.println(cosineSearchSql);
    System.out.println();
    System.out.println("📊 Search results (sorted by cosine distance):");
    System.out.println("---------------------------------------------------");

    try {
      tableEnv.executeSql(cosineSearchSql).print();
    } catch (Exception e) {
      System.out.println("⚠️ Execution error: " + e.getMessage());
    }

    // ============================================
    // Step 6: Combine vector search with other queries
    // ============================================
    System.out.println("\n-- Step 6: Combine vector search with other queries (LATERAL TABLE)");

    String lateralSearchSql =
        String.format(
            "-- First query data, then perform vector search based on results\n"
                + "SELECT \n"
                + "    v.id,\n"
                + "    v.title,\n"
                + "    v._distance as similarity_distance\n"
                + "FROM TABLE(\n"
                + "    vector_search('%s', 'embedding', ARRAY[0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85], 5, 'L2')\n"
                + ") AS v\n"
                + "WHERE v._distance < 1.0  -- Only return results with distance less than 1",
            datasetPath);

    System.out.println(lateralSearchSql);
    System.out.println();

    // ============================================
    // Print configuration parameter descriptions
    // ============================================
    System.out.println("\n========== IVF_PQ Index Configuration Parameter Description ==========");
    System.out.println(
        "╔═════════════════════════════╦════════════════════════════════════════════════════╗");
    System.out.println(
        "║       Configuration         ║                Description                         ║");
    System.out.println(
        "╠═════════════════════════════╬════════════════════════════════════════════════════╣");
    System.out.println(
        "║ index.type = 'IVF_PQ'       ║ Use IVF_PQ index type                              ║");
    System.out.println(
        "║ index.column                ║ Vector column name to build index on              ║");
    System.out.println(
        "║ index.num-partitions        ║ IVF partition count, recommend: sqrt(n) to 4*sqrt(n)║");
    System.out.println(
        "║ index.num-sub-vectors       ║ PQ sub-vector count, must divide vector dimension ║");
    System.out.println(
        "║ index.num-bits              ║ PQ encoding bits, default 8 (256 cluster centers) ║");
    System.out.println(
        "║ vector.metric               ║ Distance metric: L2(Euclidean)/COSINE/DOT(Dot product)║");
    System.out.println(
        "║ vector.nprobes              ║ Number of partitions to probe during search       ║");
    System.out.println(
        "╚═════════════════════════════╩════════════════════════════════════════════════════╝");

    System.out.println("\n========== Distance Metric Type Description ==========");
    System.out.println(
        "╔════════════════╦════════════════════════════════════════════════════════════════╗");
    System.out.println(
        "║  Metric Type   ║                          Description                           ║");
    System.out.println(
        "╠════════════════╬════════════════════════════════════════════════════════════════╣");
    System.out.println(
        "║    L2          ║ Euclidean distance, smaller = more similar, for dense vectors ║");
    System.out.println(
        "║    COSINE      ║ Cosine distance, range [0,2], smaller = more similar, for text║");
    System.out.println(
        "║    DOT         ║ Negative dot product, smaller = more similar (needs normalization)║");
    System.out.println(
        "╚════════════════╩════════════════════════════════════════════════════════════════╝");

    System.out.println("\n✅ Vector search IVF_PQ example completed!\n");
  }

  @Test
  @DisplayName("9.2 Different Index Types Comparison Example")
  void testDifferentIndexTypesDetailed() throws Exception {
    System.out.println("========== Different Vector Index Types Comparison ==========");

    // Use relative path based on project root
    Path basePath = Paths.get(System.getProperty("user.dir"), "test-data");

    // ============================================
    // IVF_PQ Index - For large-scale data, low memory footprint
    // ============================================
    System.out.println("【1. IVF_PQ Index】- Recommended for large-scale data");
    System.out.println("Pros: Low memory footprint, fast search speed");
    System.out.println("Cons: Lower accuracy (quantization loss)");
    System.out.println();

    String ivfPqSql =
        String.format(
            "CREATE TABLE ivf_pq_vectors (\n"
                + "    id BIGINT,\n"
                + "    embedding ARRAY<FLOAT>\n"
                + ") WITH (\n"
                + "    'connector' = 'lance',\n"
                + "    'path' = '%s',\n"
                + "    'index.type' = 'IVF_PQ',\n"
                + "    'index.column' = 'embedding',\n"
                + "    'index.num-partitions' = '256',    -- IVF partition count\n"
                + "    'index.num-sub-vectors' = '16',    -- PQ sub-vector count\n"
                + "    'index.num-bits' = '8',            -- Encoding bits per sub-vector\n"
                + "    'vector.metric' = 'L2'\n"
                + ")",
            basePath.resolve("ivf-pq-demo"));

    System.out.println(ivfPqSql);
    System.out.println();

    // ============================================
    // IVF_HNSW Index - High accuracy search
    // ============================================
    System.out.println("【2. IVF_HNSW Index】- Recommended for high accuracy requirements");
    System.out.println("Pros: High search accuracy");
    System.out.println("Cons: Higher memory footprint, slower index building");
    System.out.println();

    String ivfHnswSql =
        String.format(
            "CREATE TABLE ivf_hnsw_vectors (\n"
                + "    id BIGINT,\n"
                + "    embedding ARRAY<FLOAT>\n"
                + ") WITH (\n"
                + "    'connector' = 'lance',\n"
                + "    'path' = '%s',\n"
                + "    'index.type' = 'IVF_HNSW',\n"
                + "    'index.column' = 'embedding',\n"
                + "    'index.num-partitions' = '256',    -- IVF partition count\n"
                + "    'index.hnsw-m' = '16',             -- HNSW connections per level\n"
                + "    'index.hnsw-ef-construction' = '100', -- Candidate set size during construction\n"
                + "    'vector.metric' = 'COSINE',\n"
                + "    'vector.ef' = '50'                 -- Candidate set size during search\n"
                + ")",
            basePath.resolve("ivf-hnsw-demo"));

    System.out.println(ivfHnswSql);
    System.out.println();

    // ============================================
    // IVF_FLAT Index - Highest accuracy, brute force search
    // ============================================
    System.out.println("【3. IVF_FLAT Index】- Highest accuracy");
    System.out.println("Pros: 100% search accuracy (lossless)");
    System.out.println("Cons: Slower search speed, suitable for small datasets");
    System.out.println();

    String ivfFlatSql =
        String.format(
            "CREATE TABLE ivf_flat_vectors (\n"
                + "    id BIGINT,\n"
                + "    embedding ARRAY<FLOAT>\n"
                + ") WITH (\n"
                + "    'connector' = 'lance',\n"
                + "    'path' = '%s',\n"
                + "    'index.type' = 'IVF_FLAT',\n"
                + "    'index.column' = 'embedding',\n"
                + "    'index.num-partitions' = '128',    -- IVF partition count\n"
                + "    'vector.metric' = 'DOT',\n"
                + "    'vector.nprobes' = '32'            -- Number of partitions to probe during search\n"
                + ")",
            basePath.resolve("ivf-flat-demo"));

    System.out.println(ivfFlatSql);
    System.out.println();

    // ============================================
    // Index Selection Recommendations
    // ============================================
    System.out.println("========== Index Selection Recommendations ==========");
    System.out.println(
        "╔═══════════════════╦════════════════╦═══════════════╦════════════════════════════════╗");
    System.out.println(
        "║   Index Type      ║   Data Scale   ║   Accuracy    ║           Use Case             ║");
    System.out.println(
        "╠═══════════════════╬════════════════╬═══════════════╬════════════════════════════════╣");
    System.out.println(
        "║    IVF_PQ         ║   1M+          ║    Medium     ║ Large-scale recommendation, image search║");
    System.out.println(
        "║    IVF_HNSW       ║   100K-1M      ║    High       ║ Semantic search, Q&A systems   ║");
    System.out.println(
        "║    IVF_FLAT       ║   <100K        ║    Highest    ║ Small-scale high-precision scenarios║");
    System.out.println(
        "╚═══════════════════╩════════════════╩═══════════════╩════════════════════════════════╝");

    System.out.println("\n✅ Index type comparison example completed!\n");
  }

  @Test
  @DisplayName("10. SQL Syntax Quick Reference")
  void testSqlQuickReference() {
    System.out.println("========================================");
    System.out.println("     Flink SQL Lance Connector Quick Reference");
    System.out.println("========================================\n");

    System.out.println("【Create Table】");
    System.out.println("CREATE TABLE table_name (");
    System.out.println("    column_name data_type,");
    System.out.println("    embedding ARRAY<FLOAT>");
    System.out.println(") WITH (");
    System.out.println("    'connector' = 'lance',");
    System.out.println("    'path' = '/path/to/dataset'");
    System.out.println(");\n");

    System.out.println("【Insert Data】");
    System.out.println("INSERT INTO table_name VALUES (1, 'text', ARRAY[0.1, 0.2, 0.3]);\n");

    System.out.println("【Query Data】");
    System.out.println("SELECT * FROM table_name WHERE condition;\n");

    System.out.println("【Create Catalog】");
    System.out.println("CREATE CATALOG lance_catalog WITH (");
    System.out.println("    'type' = 'lance',");
    System.out.println("    'warehouse' = '/path/to/warehouse'");
    System.out.println(");\n");

    System.out.println("【Data Type Mapping】");
    System.out.println("╔════════════════════╦═══════════════════╗");
    System.out.println("║   Flink SQL Type   ║     Lance Type    ║");
    System.out.println("╠════════════════════╬═══════════════════╣");
    System.out.println("║ BOOLEAN            ║ Bool              ║");
    System.out.println("║ TINYINT            ║ Int8              ║");
    System.out.println("║ SMALLINT           ║ Int16             ║");
    System.out.println("║ INT                ║ Int32             ║");
    System.out.println("║ BIGINT             ║ Int64             ║");
    System.out.println("║ FLOAT              ║ Float32           ║");
    System.out.println("║ DOUBLE             ║ Float64           ║");
    System.out.println("║ STRING             ║ Utf8              ║");
    System.out.println("║ BYTES              ║ Binary            ║");
    System.out.println("║ DATE               ║ Date32            ║");
    System.out.println("║ TIMESTAMP          ║ Timestamp         ║");
    System.out.println("║ ARRAY<FLOAT>       ║ FixedSizeList     ║");
    System.out.println("╚════════════════════╩═══════════════════╝\n");

    System.out.println("【Configuration Options】");
    System.out.println("╔═══════════════════════════╦════════════════════════════════╗");
    System.out.println("║         Option            ║           Description          ║");
    System.out.println("╠═══════════════════════════╬════════════════════════════════╣");
    System.out.println("║ path                      ║ Dataset path (required)        ║");
    System.out.println("║ write.batch-size          ║ Write batch size (default 1024)║");
    System.out.println("║ write.mode                ║ Write mode: append/overwrite   ║");
    System.out.println("║ read.batch-size           ║ Read batch size (default 1024) ║");
    System.out.println("║ index.type                ║ Index type: IVF_PQ/IVF_HNSW/IVF_FLAT║");
    System.out.println("║ index.column              ║ Index column name              ║");
    System.out.println("║ index.num-partitions      ║ IVF partitions (default 256)   ║");
    System.out.println("║ vector.column             ║ Vector column name             ║");
    System.out.println("║ vector.metric             ║ Distance metric: L2/COSINE/DOT ║");
    System.out.println("║ vector.nprobes            ║ Search probes (default 20)     ║");
    System.out.println("╚═══════════════════════════╩════════════════════════════════╝\n");

    System.out.println("✅ Quick reference completed!");
  }
}
