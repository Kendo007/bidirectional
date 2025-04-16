//package org.example.bidirectional.service;
//
//import org.junit.jupiter.api.*;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.test.context.junit.jupiter.SpringExtension;
//
//import java.io.File;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.sql.*;
//import java.util.List;
//import java.util.UUID;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//@SpringBootTest
//@ExtendWith(SpringExtension.class)
//public class IngestionServiceIntegrationTests {
//
//    @Autowired
//    private IngestionService ingestionService;
//
//    @Autowired
//    private ClickHouseService clickHouseService;
//
//    @Autowired
//    private FileService fileService;
//
//    private static String testTableName;
//
//    @BeforeEach
//    void setup() throws SQLException {
//        testTableName = "uk_price_paid_test_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
//        createTestTable(testTableName);
//    }
//
//    @AfterEach
//    void cleanup() throws SQLException {
//        dropTestTable(testTableName);
//    }
//
//    private void createTestTable(String tableName) throws SQLException {
//        String createTableSQL = """
//            CREATE TABLE IF NOT EXISTS uk.""" + tableName + """
//            (
//                price UInt32,
//                date Date,
//                postcode1 LowCardinality(String),
//                postcode2 LowCardinality(String),
//                type Enum8('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
//                is_new UInt8,
//                duration Enum8('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
//                addr1 String,
//                addr2 String,
//                street LowCardinality(String),
//                locality LowCardinality(String),
//                town LowCardinality(String),
//                district LowCardinality(String),
//                county LowCardinality(String)
//            )
//            ENGINE = MergeTree
//            ORDER BY (postcode1, postcode2, addr1, addr2)
//        """;
//
//        try (Connection conn = clickHouseService.getConnection();
//             Statement stmt = conn.createStatement()) {
//            stmt.execute(createTableSQL);
//        }
//    }
//
//    private void dropTestTable(String tableName) throws SQLException {
//        try (Connection conn = clickHouseService.getConnection();
//             Statement stmt = conn.createStatement()) {
//            stmt.execute("DROP TABLE IF EXISTS uk." + tableName);
//        }
//    }
//
//    @Test
//    void testIngestFileToClickHouse() throws Exception {
//        String csvFilePath = "src/test/resources/test_data.csv";
//        File csvFile = new File(csvFilePath);
//        if (!csvFile.exists()) {
//            csvFile.createNewFile();
//            Files.write(Paths.get(csvFilePath), """
//                price,date,postcode1,postcode2,type,is_new,duration,addr1,addr2,street,locality,town,district,county
//                100000,2025-04-01,AB12,34,terraced,1,freehold,123 Main St,Apt 4,Elm St,Downtown,Smalltown,Urban,State
//                200000,2025-04-02,CD34,56,semi-detached,0,leasehold,456 Oak Rd,Suite 3,Maple Ave,Uptown,Bigcity,Suburban,Province
//            """.getBytes());
//        }
//
//        List<String> columns = List.of("price", "date", "postcode1", "postcode2", "type", "is_new", "duration", "addr1", "addr2", "street", "locality", "town", "district", "county");
//        int recordCount = ingestionService.ingestFileToClickHouse(csvFilePath, testTableName, columns, ',');
//
//        assertEquals(2, recordCount);
//
//        try (Connection conn = clickHouseService.getConnection();
//             Statement stmt = conn.createStatement();
//             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM uk." + testTableName)) {
//
//            rs.next();
//            int count = rs.getInt(1);
//            assertEquals(2, count, "The record count should match the inserted data");
//        }
//    }
//
//    @Test
//    void testIngestEmptyFile() throws Exception {
//        // Test with an empty CSV file
//        String emptyCsvPath = "src/test/resources/empty_test_data.csv";
//        File emptyFile = new File(emptyCsvPath);
//        if (!emptyFile.exists()) {
//            emptyFile.createNewFile();
//            Files.write(Paths.get(emptyCsvPath), "".getBytes());
//        }
//
//        List<String> columns = List.of("price", "date", "postcode1", "postcode2", "type", "is_new", "duration", "addr1", "addr2", "street", "locality", "town", "district", "county");
//        int recordCount = ingestionService.ingestFileToClickHouse(emptyCsvPath, testTableName, columns, ',');
//
//        assertEquals(0, recordCount, "The record count should be 0 for an empty file");
//
//        // Verify that the ClickHouse table is still empty
//        try (Connection conn = clickHouseService.getConnection();
//             Statement stmt = conn.createStatement();
//             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM uk." + testTableName)) {
//
//            rs.next();
//            int count = rs.getInt(1);
//            assertEquals(0, count, "The table should remain empty when an empty CSV is ingested");
//        }
//    }
//}
