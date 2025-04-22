package org.example.bidirectional.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "clickhouse")
public class ClickHouseProperties {

    private String protocol;
    private String host;
    private int port;
    private String database;
    private String username;
    private String jwtToken;  // For JWT or token
    private String password;   // For password
    private String delimiter = ",";  // Default delimiter

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    // Getter and Setter for host
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    // Getter and Setter for port
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    // Getter and Setter for database
    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    // Getter and Setter for username
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    // Getter and Setter for jwtToken (used as a token or JWT)
    public String getJwtToken() {
        return jwtToken;
    }

    public void setJwtToken(String jwtToken) {
        this.jwtToken = jwtToken;
    }

    // Getter and Setter for password
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    // Getter and Setter for delimiter
    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }
}
