package org.example.bidirectional.service;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import com.opencsv.CSVReader;
import org.example.bidirectional.config.ConnectionConfig;
import org.example.bidirectional.exception.AuthenticationException;
import org.example.bidirectional.model.ColumnInfo;
import org.example.bidirectional.model.JoinTable;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.Future;

public class ClickHouseService {
    private final Client client;
    private final String database;
    private static ArrayList<String> types = null;

    public static char convertStringToChar(String input) {
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

    public ClickHouseService(ConnectionConfig props) {
        this.database = props.getDatabase().trim();

        try {
            var cb = new Client.Builder()
                    .setUsername(props.getUsername())
                    .compressServerResponse(true)
                    .setDefaultDatabase(props.getDatabase())
                    .setConnectTimeout(60_000)     // ⏱️ 30 seconds
                    .setSocketTimeout(60_000);        // ⏱️ 60 seconds

            String host = props.getHost();

            if (props.getProtocol().equalsIgnoreCase("http")) {
                if (!host.startsWith("http://"))
                    host = "http://" + host;

                cb.addEndpoint(host + ":" + props.getPort());
            } else if (props.getProtocol().equalsIgnoreCase("https")) {
                if (!host.startsWith("https://"))
                    host = "https://" + host;

                cb.addEndpoint(host + ":" + props.getPort());
            }

            if (props.getAuthType().equalsIgnoreCase("jwt")) {
                this.client = cb.setAccessToken(props.getJwt()).build();
            } else if (props.getAuthType().equalsIgnoreCase("password")) {
                this.client = cb.setPassword(props.getPassword()).build();
            } else {
                throw new AuthenticationException("Invalid credentials or token.");
            }

            if (!client.ping())
                throw new AuthenticationException("Invalid credentials or token.");
        } catch (Exception e) {
            if (e instanceof AuthenticationException) {
                throw (AuthenticationException) e;
            } else if (e.getMessage().toLowerCase().contains("authentication")) {
                throw new AuthenticationException("Invalid credentials or token.");
            }
            throw new RuntimeException("Failed to connect to ClickHouse: " + e.getMessage(), e);
        }
    }

    public boolean testConnection() {
        return client.ping();
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

    public long getTotalRows(String tableName) {
        String query = "SELECT COUNT(*) FROM " + quote(tableName);
        return Long.parseLong(getListFromResponse(query).getFirst());
    }

    public List<String> listTables() {
        return getListFromResponse("SHOW TABLES FROM " + database);
    }

    public List<ColumnInfo> getColumns(String tableName) {
        String sql = String.format("SELECT name, type FROM system.columns WHERE database = '%s' AND table = '%s';",
                database, tableName);

        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.CSV);
        Future<QueryResponse> response = client.query(sql, settings);

        try (QueryResponse qr = response.get();
             BufferedReader reader = new BufferedReader(new InputStreamReader(qr.getInputStream()));
             CSVReader csvReader = new CSVReader(reader)) {
            List<ColumnInfo> names = new ArrayList<>();

            String[] row;
            while ((row = csvReader.readNext()) != null)
                names.add(new ColumnInfo(row[0], row[1]));

            return names;
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch data from ClickHouse: " + e.getMessage(), e);
        }
    }

    public ArrayList<String> getTypes() {
        if (types != null) {
            return types;
        }

        String sql = "SELECT name FROM system.data_type_families " +
                     "UNION DISTINCT " +
                     "SELECT alias_to AS name FROM system.data_type_families WHERE name != '';";

        types = (ArrayList<String>) getListFromResponse(sql);

        types.remove("String");
        Collections.sort(types);
        types.addFirst("String");

        return types;
    }

    public void createTable(String tableName, Map<String, String> types) throws Exception {
        // Construct a CREATE TABLE query based on the headers
        int i = types.size() - 1;

        StringBuilder createTableQuery = new StringBuilder("CREATE TABLE IF NOT EXISTS " + quote(tableName) + " (");
        for (Map.Entry<String, String> e : types.entrySet()) {
            createTableQuery.append(quote(e.getKey())).append(' ').append(e.getValue());

            if (i > 0)
                createTableQuery.append(", ");

            --i;
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
        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.CSVWithNames);

        Future<QueryResponse> response = client.query(sql, settings);

        try (QueryResponse qr = response.get();
             BufferedReader reader = new BufferedReader(new InputStreamReader(qr.getInputStream()));
             CSVReader csvReader = new CSVReader(reader)) {
            return csvReader.readAll();
        }
    }

    protected static String quote(String name) {
        return "`" + name.replace("`", "``") + "`";
    }

    /**
     * Builds the query with proper joins. THe query is joined as a complete default should add
     * condition to the string (if you want) before executing
     */
    public String getJoinedQuery(String tableName, List<String> columns, List<JoinTable> joins) {
        // Build the SQL query string
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("SELECT ");
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).contains(".")) {
                String[] column = columns.get(i).split("\\.");
                queryBuilder.append(quote(column[0]));
                queryBuilder.append(".");
                queryBuilder.append(quote(column[1]));
            } else {
                queryBuilder.append(quote(columns.get(i)));
            }

            if (i < columns.size() - 1) queryBuilder.append(',');
        }
        queryBuilder.append(" FROM `").append(tableName).append('`');

        if (joins != null && !joins.isEmpty()) {
            for (JoinTable jt : joins) {
                queryBuilder.append(" ")
                        .append(jt.getJoinType()).append(" ")
                        .append(quote(jt.getTableName())).append(" ON ")
                        .append(jt.getJoinCondition());
            }
        }

        return queryBuilder.toString();
    }

    public List<String[]> querySelectedColumns(
            String tableName,
            List<String> columns,
            List<JoinTable> joins,
            String delimiter
    ) throws Exception {
        String sql = getJoinedQuery(tableName, columns, joins);

        return fetchDataHelper(sql + " LIMIT 100");
    }

    public Client getClient() {
        return client;
    }
}
