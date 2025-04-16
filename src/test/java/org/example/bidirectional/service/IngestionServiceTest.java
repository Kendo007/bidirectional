package org.example.bidirectional.service;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import com.opencsv.CSVWriter;
import org.example.bidirectional.config.ClickHouseProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.ResourceUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

@SpringJUnitConfig
public class IngestionServiceTest {
    private Client mockClient;

    private ClickHouseService clickHouseService;

    private FileService fileService;

    private IngestionService ingestionService;

    private String tableName = "test_table";

    @BeforeEach
    public void setUp() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setHost("localhost");
        props.setPort(8123);
        props.setUsername("default");
        props.setPassword("");
        props.setDatabase("uk");
        props.setDelimiter(",");

        // Build service with mock client
        clickHouseService = new ClickHouseService(props);
        fileService = new FileService();
        ingestionService = new IngestionService(clickHouseService);

        // Create a test table in ClickHouse
        String createTableQuery = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                "id UInt32, " +
                "name String" +
                ") ENGINE = MergeTree() ORDER BY id";

        // Use the ClickHouseService to create the table (assuming client.query() exists and works properly)
        clickHouseService.getClient().query(createTableQuery); // Create table in ClickHouse
    }

    @AfterEach
    public void tearDown() throws Exception {
        // Clean up the test table after each test
        String deleteDataQuery = "TRUNCATE TABLE " + tableName;
        clickHouseService.getClient().query(deleteDataQuery); // Clean up data
    }

    @Test
    public void testIngestDataFromFile() throws Exception {
        // Prepare the CSV data to be written to a file
        String filePath = "test-data.csv";
        try (CSVWriter writer = new CSVWriter(new FileWriter(filePath))) {
            writer.writeNext(new String[]{"id", "name"});
            writer.writeNext(new String[]{"1", "Alice"});
            writer.writeNext(new String[]{"2", "Bob"});
        }

        // Ingest the data from CSV into ClickHouse using the FileService and ClickHouseService
        fileService.readCsv(filePath, ','); // Read the CSV
        // Assuming IngestionService has a method to handle ingestion from file to ClickHouse.
        ingestionService.ingestDataFromFile(tableName, Paths.get(filePath), ','); // Assuming ingestionService exists

        // Verify the data is ingested into ClickHouse
        // Fetch rows from ClickHouse using the ClickHouseService (assumes client.query returns data)
        List<String[]> rows = clickHouseService.fetchData(tableName, clickHouseService.getColumns(tableName));

        assertEquals(2, rows.size(), "Data ingestion failed. Expected 2 rows.");

        // Clean up the file after test
        File file = new File(filePath);
        file.delete();
    }

    @Test
    public void testIngestDataToFile() throws Exception {
        // First insert some data into the database
        String insertQuery1 = "INSERT INTO " + tableName + " VALUES (1, 'Alice')";
        String insertQuery2 = "INSERT INTO " + tableName + " VALUES (2, 'Bob')";
        clickHouseService.getClient().query(insertQuery1);
        clickHouseService.getClient().query(insertQuery2);

        // Set the output CSV path
        String outputPath = "output-test.csv";
        // Ingest data from ClickHouse to a CSV file
        ingestionService.ingestDataToFile(tableName, List.of("id", "name"), Paths.get(outputPath), ',');

        // Verify that the output file contains the ingested data
        List<String[]> csvData = fileService.readCsv(outputPath, ',');
        assertEquals(3, csvData.size(), "CSV file ingestion failed. Expected 3 rows (header + 2 data).");

        // Clean up the output file after test
        File file = new File(outputPath);
        file.delete();
    }

}
