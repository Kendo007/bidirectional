package org.example.bidirectional.model;

public class ColumnInfo {
    private String name;
    private String type;

    // Constructors
    public ColumnInfo() {}

    public ColumnInfo(String name, String type) {
        this.name = name;
        this.type = type;
    }

    // Getters and Setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
