package org.example.bidirectional.service;

import com.clickhouse.client.api.Client;
import org.example.bidirectional.config.ConnectionConfig;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ClickHouseServiceIntegrationTest {

    private static ClickHouseService service;
    private static Client client;

    @BeforeEach
    public void setUp() throws Exception {
        // Set up the connection to the actual ClickHouse instance
        ConnectionConfig props = new ConnectionConfig();
        props.setHost("localhost");
        props.setPort(8123);
        props.setUsername("default");
        props.setPassword("");
        props.setDatabase("test_db");

        // Initialize the service with real database connection
        service = new ClickHouseService(props);
        client = service.getClient();

        // Set up the test database and table
        client.query("CREATE DATABASE IF NOT EXISTS test_db").get();
        client.query("CREATE TABLE IF NOT EXISTS test_db.users (name String, age Int32) ORDER BY age").get();
    }

    @Test
    public void testListTables() {
        // Act: Fetch the list of tables from the database
        List<String> tables = service.listTables();

        // Assert: Check if 'users' table is present
        assertTrue(tables.contains("users"));
    }


    @AfterEach
    public void tearDown() throws Exception {
        // Clean up: Drop the test table and database
        client.query("DROP TABLE IF EXISTS test_db.users").get();
    }
}
