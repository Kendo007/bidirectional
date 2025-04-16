package org.example.bidirectional.service;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.example.bidirectional.config.ClickHouseProperties;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Service
public class ClickHouseService {
    private final Client client;
    protected final char delimiter;

    public ClickHouseService(ClickHouseProperties props) {
        this.delimiter = props.getDelimiter().charAt(0);
        this.client = new Client.Builder()
                .addEndpoint("http://" + props.getHost() + ":" + props.getPort())
                .setUsername(props.getUsername())
                .setPassword(props.getPassword())
                .compressServerResponse(true)
                .setDefaultDatabase(props.getDatabase())
                .build();
    }

    public List<String> listTables() throws Exception {
        // Use QuerySettings to set the format (TabSeparated) for the SHOW TABLES query
        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.TabSeparated);

        // Execute the query to get the list of tables
        Future<QueryResponse> response = client.query("SHOW TABLES", settings);

        try (QueryResponse qr = response.get();
             BufferedReader reader = new BufferedReader(new InputStreamReader(qr.getInputStream()));
             CSVReader csvReader = new CSVReaderBuilder(reader)
                     .withCSVParser(new CSVParserBuilder().withSeparator('\t').build())
                     .build()) {

            // Process the result and collect table names
            return csvReader.readAll()
                    .stream()
                    .map(row -> row[0])  // The first column contains the table name
                    .collect(Collectors.toList());
        }
    }

    public List<String> getColumns(String tableName) throws Exception {
        String sql = "DESCRIBE TABLE " + tableName;
        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.TabSeparated);

        Future<QueryResponse> response = client.query(sql, settings);

        try (QueryResponse qr = response.get();
             BufferedReader reader = new BufferedReader(new InputStreamReader(qr.getInputStream()));
             CSVReader csvReader = new CSVReaderBuilder(reader)
                     .withCSVParser(new CSVParserBuilder().withSeparator('\t').build())
                     .build()) {

            return csvReader.readAll().stream()
                    .map(row -> row.length > 0 ? row[0].trim() : "")
                    .filter(name -> !name.isEmpty())
                    .collect(Collectors.toList());
        }
    }

    public void createTable(String[] headers, String tableName) throws Exception {
        // Construct a CREATE TABLE query based on the headers
        StringBuilder createTableQuery = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (");
        for (int i = 0; i < headers.length; i++) {
            createTableQuery.append(headers[i]).append(" String");
            if (i < headers.length - 1) {
                createTableQuery.append(", ");
            }
        }
        createTableQuery.append(") ENGINE = MergeTree() ORDER BY tuple();");

        // Execute the CREATE TABLE query
        client.query(createTableQuery.toString()).get();
    }

    /**
     * Fetches data from the given table and columns, and returns the rows as a list of String arrays.
     * The response is expected in CSVWithNames format, and the header row is skipped.
     *
     * @param tableName the name of the table
     * @param columns   list of columns to fetch
     * @return a list of rows (each row as String[]), excluding the header row
     * @throws Exception if an error occurs during the query or reading the data
     */
    public List<String[]> fetchData(String tableName, List<String> columns) throws Exception {
        String sql = "SELECT " + String.join(",", columns) + " FROM " + tableName;
        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.CSVWithNames);

        Future<QueryResponse> response = client.query(sql, settings);

        try (QueryResponse qr = response.get();
             BufferedReader reader = new BufferedReader(new InputStreamReader(qr.getInputStream()));
             CSVReader csvReader = new CSVReaderBuilder(reader)
                     .withCSVParser(new CSVParserBuilder().withSeparator(delimiter).build())
                     .build()) {

            // Skip the header row
            csvReader.readNext();

            // Return all subsequent rows as a List<String[]>
            return csvReader.readAll();
        }
    }

    /**
     * Fetch data from ClickHouse and process each row.
     *
     * @param tableName  The table name.
     * @param columns    The columns to fetch.
     * @param processor  The custom data processor.
     * @throws Exception if an error occurs during the process.
     */
    public void fetchDataAndProcess(String tableName, List<String> columns, DataProcessor processor) throws Exception {
        String sql = "SELECT " + String.join(",", columns) + " FROM " + tableName;
        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.CSVWithNames);

        Future<QueryResponse> response = client.query(sql, settings);

        try (QueryResponse qr = response.get();
             BufferedReader reader = new BufferedReader(new InputStreamReader(qr.getInputStream()));
             CSVReader csvReader = new CSVReader(reader)) {

            String[] row;
            while ((row = csvReader.readNext()) != null) {
                processor.process(row);  // Process each row using the custom processor
            }
        }
    }

    public Client getClient() {
        return client;
    }

    /**
     * Interface to define custom row processing logic.
     */
    public interface DataProcessor {
        void process(String[] row) throws IOException;
    }
}
