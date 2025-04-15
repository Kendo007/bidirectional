package org.example.bidirectional.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class ClickHouseServiceTest {

    @Autowired
    private ClickHouseService clickHouseService;

    @Test
    public void testConnection() throws Exception {
        try (Connection conn = clickHouseService.getConnection()) {
            assertNotNull(conn);
            assertFalse(conn.isClosed());
        }
    }

    @Test
    public void testFetchTables() throws Exception {
        List<String> tables = clickHouseService.fetchTables();
        assertTrue(tables.contains("uk_price_paid"), "Expected 'uk_price_paid' table");
    }

    @Test
    public void testFetchColumns() throws Exception {
        List<String> columns = clickHouseService.fetchColumns("uk_price_paid");

        assertTrue(columns.contains("price"));
        assertTrue(columns.contains("date"));
        assertTrue(columns.contains("postcode1"));
        assertTrue(columns.contains("postcode2"));
        assertTrue(columns.contains("type"));
        assertTrue(columns.contains("is_new"));
        assertTrue(columns.contains("duration"));
        assertTrue(columns.contains("addr1"));
        assertTrue(columns.contains("addr2"));
        assertTrue(columns.contains("street"));
        assertTrue(columns.contains("locality"));
        assertTrue(columns.contains("town"));
        assertTrue(columns.contains("district"));
        assertTrue(columns.contains("county"));
    }
}
