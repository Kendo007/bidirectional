package org.example.bidirectional.service;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import org.example.bidirectional.config.ClickHouseProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SpringBootTest
public class ClickHouseServiceTest {
    private ClickHouseService clickHouseService;

    private Client mockClient;

    @BeforeEach
    void setUp() {
        // Create dummy properties
        ClickHouseProperties props = new ClickHouseProperties();
        props.setHost("localhost");
        props.setPort(8123);
        props.setUsername("default");
        props.setPassword("");
        props.setDatabase("default");
        props.setDelimiter(",");

        // Build service with mock client
        clickHouseService = new ClickHouseService(props);

        // Use reflection to inject a mock client
        mockClient = mock(Client.class);
        try {
            var clientField = ClickHouseService.class.getDeclaredField("client");
            clientField.setAccessible(true);
            clientField.set(clickHouseService, mockClient);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testListTables() throws Exception {
        List<String> tables = clickHouseService.listTables();
        assertNotNull(tables);
        assertTrue(tables.contains("uk_price_paid"), "Table 'uk_price_paid' should exist");
    }

    @Test
    void testGetColumns() throws Exception {
        List<String> columns = clickHouseService.getColumns("uk_price_paid");
        assertNotNull(columns);
        assertTrue(columns.containsAll(Arrays.asList("price", "county")),
                "Expected columns should be present");
    }

    @Test
    void testFetchDataAndProcess() throws Exception {
        // Arrange
        String csvData = "name,age\nAlice,30\nBob,25\n";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(csvData.getBytes());

        QueryResponse mockResponse = mock(QueryResponse.class);
        when(mockResponse.getInputStream()).thenReturn(inputStream);

        CompletableFuture<QueryResponse> future = CompletableFuture.completedFuture(mockResponse);
        when(mockClient.query(anyString(), any(QuerySettings.class))).thenReturn(future);

        // Act
        clickHouseService.fetchDataAndProcess("users", List.of("name", "age"), row -> {
            System.out.println("Processed: " + String.join(" | ", row));
        });

        // Assert: You can add verifications if you store processed results in a list or use a spy
    }
}
