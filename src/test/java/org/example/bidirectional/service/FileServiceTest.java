package org.example.bidirectional.service;

import com.opencsv.exceptions.CsvException;
import org.junit.jupiter.api.*;
import java.io.*;
import java.nio.file.Files;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FileServiceTest {

    private FileService fileService;
    private final String testFilePath = "test_output.csv";

    @BeforeAll
    void setUp() {
        fileService = new FileService();
    }

    @AfterEach
    void cleanUp() throws IOException {
        Files.deleteIfExists(new File(testFilePath).toPath());
    }

    @Test
    void testWriteAndReadCsvWithCommas() throws IOException, CsvException {
        List<String[]> data = Arrays.asList(
                new String[]{"ID", "Name", "Comment"},
                new String[]{"1", "Alice", "Likes apples, oranges"},
                new String[]{"2", "Bob", "Said, \"Hello!\""}
        );

        fileService.writeCsv(testFilePath, data, ',');
        List<String[]> result = fileService.readCsv(testFilePath, ',');

        assertEquals(3, result.size());
        assertArrayEquals(data.get(1), result.get(1));
        assertEquals("Said, \"Hello!\"", result.get(2)[2]);
    }

    @Test
    void testWriteAndReadCsvWithTabDelimiter() throws IOException, CsvException {
        List<String[]> data = List.of(
                new String[]{"ID", "Name"},
                new String[]{"1", "Eve"},
                new String[]{"2", "Frank"}
        );

        fileService.writeCsv(testFilePath, data, '\t');
        List<String[]> result = fileService.readCsv(testFilePath, '\t');

        assertEquals(3, result.size());
        assertArrayEquals(new String[]{"2", "Frank"}, result.get(2));
    }

    @Test
    void testQuotedFieldsAndNewlines() throws IOException, CsvException {
        List<String[]> data = List.of(
                new String[]{"ID", "Bio"},
                new String[]{"1", "Loves reading\nand hiking"},
                new String[]{"2", "Line1\nLine2\nLine3"}
        );

        fileService.writeCsv(testFilePath, data, ',');
        List<String[]> result = fileService.readCsv(testFilePath, ',');

        assertEquals(3, result.size());
        assertTrue(result.get(1)[1].contains("\n"));
        assertEquals("Line1\nLine2\nLine3", result.get(2)[1]);
    }

    @Test
    void testEmptyCsvRead() throws IOException, CsvException {
        fileService.writeCsv(testFilePath, List.of(), ',');
        List<String[]> result = fileService.readCsv(testFilePath, ',');
        assertTrue(result.isEmpty());
    }

    @Test
    void testPipeDelimiter() throws IOException, CsvException {
        List<String[]> data = List.of(
                new String[]{"User", "Role"},
                new String[]{"Alice", "Admin"},
                new String[]{"Bob", "User"}
        );

        fileService.writeCsv(testFilePath, data, '|');
        List<String[]> result = fileService.readCsv(testFilePath, '|');

        assertEquals(3, result.size());
        assertArrayEquals(data.get(2), result.get(2));
    }

    @Test
    void testInvalidFilePath() {
        assertThrows(IOException.class, () -> fileService.readCsv("non_existent.csv", ','));
    }
}
