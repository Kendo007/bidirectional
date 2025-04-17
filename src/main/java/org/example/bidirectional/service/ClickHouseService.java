package org.example.bidirectional.service;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import com.opencsv.CSVReader;
import org.example.bidirectional.config.ClickHouseProperties;
import org.example.bidirectional.exception.AuthenticationException;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

@Service
public class ClickHouseService {
    private final Client client;
    private final char delimiter;
    private final String database;

    private static char convertStringToChar(String input) {
        if (input == null || input.isEmpty()) {
            throw new IllegalArgumentException("Input must be a non-empty string");
        }

        return switch (input) {
            case "\\n" -> '\n';
            case "\\t" -> '\t';
            case "\\r" -> '\r';
            case "\\b" -> '\b';
            case "\\f" -> '\f';
            case "\\'" -> '\'';
            case "\\\"" -> '\"';
            case "\\\\" -> '\\';
            default -> input.charAt(0);
        };
    }

    public ClickHouseService(ClickHouseProperties props) {
        this.delimiter = convertStringToChar(props.getDelimiter());
        this.database = props.getDatabase().trim();

//         ✅ Prefer JWT token if provided
        try {
            if (props.getJwtToken() != null && !props.getJwtToken().isEmpty()) {
                this.client = new Client.Builder()
                        .addEndpoint("http://" + props.getHost() + ":" + props.getPort())
                        .setUsername(props.getUsername()) // ✅ Always required
                        .setAccessToken(props.getJwtToken())
                        .compressServerResponse(true)
                        .setDefaultDatabase(props.getDatabase())
                        .setConnectTimeout(60_000)     // ⏱️ 30 seconds
                        .setSocketTimeout(60_000)        // ⏱️ 60 seconds
                        .build();
            } else if (props.getPassword() != null) {
                this.client = new Client.Builder()
                        .addEndpoint("http://" + props.getHost() + ":" + props.getPort())
                        .setUsername(props.getUsername()) // ✅ Always required
                        .setPassword(props.getPassword())
                        .compressServerResponse(true)
                        .setDefaultDatabase(props.getDatabase())
                        .setConnectTimeout(60_000)     // ⏱️ 30 seconds
                        .setSocketTimeout(60_000)        // ⏱️ 60 seconds
                        .build();
            } else {
                throw new AuthenticationException("Invalid credentials or token.");
            }
        } catch (Exception e) {
            if (e instanceof AuthenticationException) {
                throw (AuthenticationException) e;
            } else if (e.getMessage().toLowerCase().contains("authentication")) {
                throw new AuthenticationException("Invalid credentials or token.");
            }
            throw new RuntimeException("Failed to connect to ClickHouse: " + e.getMessage(), e);
        }
    }

    /**
     * Fetches a list of strings from ClickHouse based on the provided SQL query.
     * The first column of each row is returned as a list of strings.
     *
     * @param sqlQuery the SQL query to execute
     * @return a list of strings from the first column of the result set
     */
    private List<String> getListFromResponse(String sqlQuery) {
        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.CSV);
        Future<QueryResponse> response = client.query(sqlQuery, settings);

        try (QueryResponse qr = response.get();
             BufferedReader reader = new BufferedReader(new InputStreamReader(qr.getInputStream()));
             CSVReader csvReader = new CSVReader(reader)) {
            List<String> names = new ArrayList<>();

            String[] row;
            while ((row = csvReader.readNext()) != null)
                names.add(row[0]);

            return names;
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch data from ClickHouse: " + e.getMessage(), e);
        }
    }

    public List<String> listTables() {
        return getListFromResponse("SHOW TABLES FROM " + database);
    }

    public List<String> getColumns(String tableName) {
        String sql = String.format("SELECT name FROM system.columns WHERE database = '%s' AND table = '%s';",
                database, tableName);

        return getListFromResponse(sql);
    }

    public void createTable(String[] headers, String tableName) throws Exception {
        // Construct a CREATE TABLE query based on the headers
        StringBuilder createTableQuery = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (");
        for (int i = 0; i < headers.length; i++) {
            createTableQuery.append('`').append(headers[i]).append("` String");
            if (i < headers.length - 1) {
                createTableQuery.append(", ");
            }
        }
        createTableQuery.append(") ENGINE = MergeTree() ORDER BY tuple();");

        // Execute the CREATE TABLE query
        client.query(createTableQuery.toString()).get();
    }

    /**
     * Fetches data from ClickHouse using the provided SQL query and
     * returns the result as a list of String arrays.
     */
    private List<String[]> fetchDataHelper(String sql) throws Exception {
        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.CSV);

        Future<QueryResponse> response = client.query(sql, settings);

        try (QueryResponse qr = response.get();
             BufferedReader reader = new BufferedReader(new InputStreamReader(qr.getInputStream()));
             CSVReader csvReader = new CSVReader(reader)) {
            return csvReader.readAll();
        }
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
        return fetchDataHelper(sql);
    }

    public List<String[]> fetchDataWithLimit(String tableName, List<String> columns, int limit) throws Exception {
        String sql = "SELECT " + String.join(",", columns) + " FROM " + tableName + " LIMIT " + limit;
        return fetchDataHelper(sql);
    }

    public Client getClient() {
        return client;
    }

    public char getDelimiter() {
        return delimiter;
    }

    public String getDatabase() {
        return database;
    }
}
