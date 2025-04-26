package org.example.bidirectional.config;

import java.util.Map;

public class UploadConfig {
    private Integer totalCols;
    private ConnectionConfig connection;
    private String tableName;
    private boolean createNewTable = false;
    private String delimiter;
    private Map<String, String> columnTypes;

    // Getters and Setters
    public Integer getTotalCols() {
        return totalCols;
    }

    public void setTotalCols(Integer totalCols) {
        this.totalCols = totalCols;
    }

    public ConnectionConfig getConnection() {
        return connection;
    }

    public void setConnection(ConnectionConfig connection) {
        this.connection = connection;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isCreateNewTable() {
        return createNewTable;
    }

    public void setCreateNewTable(boolean createNewTable) {
        this.createNewTable = createNewTable;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public Map<String, String> getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(Map<String, String> columnTypes) {
        this.columnTypes = columnTypes;
    }
}
