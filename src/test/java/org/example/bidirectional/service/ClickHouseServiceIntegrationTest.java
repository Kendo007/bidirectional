package org.example.bidirectional.service;

import com.clickhouse.client.api.Client;
import org.example.bidirectional.config.ClickHouseProperties;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ClickHouseServiceIntegrationTest {

    private static ClickHouseService service;
    private static Client client;

    @BeforeEach
    public void setUp() throws Exception {
        // Set up the connection to the actual ClickHouse instance
        ClickHouseProperties props = new ClickHouseProperties();
        props.setHost("localhost");
        props.setPort(8123);
        props.setUsername("default");
        props.setPassword("");
        props.setDatabase("test_db");  // Use a dedicated test database
        props.setDelimiter("|");

        // Initialize the service with real database connection
        service = new ClickHouseService(props);
        client = service.getClient();

        // Set up the test database and table
        client.query("CREATE DATABASE IF NOT EXISTS test_db").get();
        client.query("CREATE TABLE IF NOT EXISTS test_db.users (name String, age Int32) ORDER BY age").get();
    }

    @Test
    public void testFetchDataAndProcess() throws Exception {
        // Insert some test data into the table
        client.query("INSERT INTO test_db.users VALUES ('Alice', 30), ('Bob', 25)").get();

        // Act: Fetch data from the database and process it
        List<String> columns = List.of("name", "age");
        service.fetchDataAndProcess("users", columns, row -> {
            assertNotNull(row);
            assertTrue(row.length == 2);
            assertNotNull(row[0]); // Check if name is not null
            assertNotNull(row[1]); // Check if age is not null
            System.out.println("Processed row: " + String.join(", ", row));
        });
    }

    @Test
    public void testListTables() throws Exception {
        // Act: Fetch the list of tables from the database
        List<String> tables = service.listTables();

        // Assert: Check if 'users' table is present
        assertTrue(tables.contains("users"));
    }

    @Test
    public void testGetColumns() throws Exception {
        // Act: Get columns for the 'users' table
        List<String> columns = service.getColumns("users");

        // Assert: Check if 'name' and 'age' are in the columns
        assertTrue(columns.contains("name"));
        assertTrue(columns.contains("age"));
    }

    @AfterEach
    public void tearDown() throws Exception {
        // Clean up: Drop the test table and database
        client.query("DROP TABLE IF EXISTS test_db.users").get();
    }
}
