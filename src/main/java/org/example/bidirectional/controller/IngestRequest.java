package org.example.bidirectional.controller;

import org.example.bidirectional.config.ClickHouseProperties;

import java.util.List;

public class IngestRequest {
    private String tableName;
    private List<String> columns;
    private ClickHouseProperties properties;

    // Getters & Setters
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public ClickHouseProperties getProperties() {
        return properties;
    }

    public void setProperties(ClickHouseProperties properties) {
        this.properties = properties;
    }
}
