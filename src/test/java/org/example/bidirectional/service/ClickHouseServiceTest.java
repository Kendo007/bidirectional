package org.example.bidirectional.service;

import org.example.bidirectional.config.ClickHouseProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class ClickHouseServiceTest {
    private static ClickHouseService clickHouseService;
    private static final String tableName = "users_test";

    @BeforeAll
    static void setUp() throws ExecutionException, InterruptedException {
        // Create dummy properties
        ClickHouseProperties props = new ClickHouseProperties();
        props.setHost("localhost");
        props.setPort(8123);
        props.setUsername("default");
        props.setPassword("");
        props.setDatabase("test_db");
        props.setDelimiter(",");

        // Build service with mock client
        clickHouseService = new ClickHouseService(props);

        // Create test table
        clickHouseService.getClient().query("DROP TABLE IF EXISTS " + tableName).get();
        clickHouseService.getClient().query("CREATE TABLE " + tableName + " (name String, age Int32) ENGINE = MergeTree() ORDER BY name").get();

        // Insert some test data
        clickHouseService.getClient().query("INSERT INTO " + tableName + " VALUES ('Alice', 30), ('Bob', 25)").get();
    }

    @AfterAll
    static void tearDown() throws ExecutionException, InterruptedException {
        // Optionally clean up
        clickHouseService.getClient().query("DROP TABLE IF EXISTS " + tableName).get();
    }

    @Test
    void testListTables() throws Exception {
        List<String> tables = clickHouseService.listTables();
        assertNotNull(tables);
        assertTrue(tables.contains(tableName), "Table " + tableName + " should exist");
    }

    @Test
    void testGetColumns() throws Exception {
        List<String> columns = clickHouseService.getColumns(tableName);
        assertNotNull(columns);
        assertTrue(columns.containsAll(Arrays.asList("name", "age")),
                "Expected columns should be present");
    }

    @Test
    void testFetchDataAndProcessWithRealClickHouse() throws Exception {
        clickHouseService.fetchDataAndProcess(tableName, List.of("name", "age"), row -> System.out.println("Processed: " + String.join(" | ", row)));
    }
}