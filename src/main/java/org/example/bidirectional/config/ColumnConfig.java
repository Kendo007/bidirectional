package org.example.bidirectional.config;

public class ColumnConfig {
    private ConnectionConfig connection;
    private String tableName;

    // Getters and setters
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
}
