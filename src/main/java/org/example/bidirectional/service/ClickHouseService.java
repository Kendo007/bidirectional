package org.example.bidirectional.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Service
public class ClickHouseService {

    @Value("${clickhouse.host}")
    private String host;

    @Value("${clickhouse.port}")
    private String port;

    @Value("${clickhouse.database}")
    private String database;

    @Value("${clickhouse.username}")
    private String username;

    @Value("${clickhouse.jwtToken}")
    private String jwtToken;

    // Create a connection to ClickHouse
    public Connection getConnection() throws SQLException {
        String url = "jdbc:clickhouse://" + host + ":" + port + "/" + database;
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", "");  // JWT token used as password/credential
        return DriverManager.getConnection(url, props);
    }

    // Fetch list of tables from ClickHouse
    public List<String> fetchTables() throws SQLException {
        List<String> tables = new ArrayList<>();
        try (Connection conn = getConnection();
             ResultSet rs = conn.getMetaData().getTables(null, null, "%", new String[]{"TABLE"})) {
            while(rs.next()){
                tables.add(rs.getString("TABLE_NAME"));
            }
        }
        return tables;
    }

    // Fetch list of columns for a given table
    public List<String> fetchColumns(String tableName) throws SQLException {
        List<String> columns = new ArrayList<>();
        try (Connection conn = getConnection();
             ResultSet rs = conn.getMetaData().getColumns(null, null, tableName, "%")) {
            while(rs.next()){
                columns.add(rs.getString("COLUMN_NAME"));
            }
        }
        return columns;
    }
}
