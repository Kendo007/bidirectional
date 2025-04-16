package org.example.bidirectional.service;

import com.opencsv.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class IngestionService {

    private final ClickHouseService clickHouseService;

    @Autowired
    public IngestionService(ClickHouseService clickHouseService) {
        this.clickHouseService = clickHouseService;
    }

    /**
     * Ingest data from ClickHouse into a CSV file.
     *
     * @param tableName  The table name.
     * @param columns    The columns to fetch.
     * @param outputPath The output file path.
     */
    public void ingestDataToFile(String tableName, List<String> columns, Path outputPath) {
        // Create a CSVWriter instance directly
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {

            CSVWriter csvWriter = new CSVWriter(writer,
                    clickHouseService.delimiter,
                    CSVWriter.NO_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END);

            clickHouseService.fetchDataAndProcess(tableName, columns, row -> {
                    // Write the current row to CSV
                    csvWriter.writeNext(row);
                    csvWriter.flush();
            }
            );

            csvWriter.close();
        } catch (Exception e) {
            throw new RuntimeException("Error writing row to CSV file", e);
        }
    }

    /**
     * Ingests data from a CSV file into a ClickHouse table.
     * Generates a single INSERT statement that batches multiple rows.
     *
     * @param tableName the target table name
     * @param inputPath the CSV file path
     * @throws Exception if an error occurs during reading or data ingestion
     */
    public void ingestDataFromFile(String tableName, Path inputPath) throws Exception {
        List<String> tables = clickHouseService.listTables();

        // SQL template to be used for a batched insert query.
        String sqlTemplate = "INSERT INTO " + tableName + " VALUES %s";

        try (CSVReader csvReader = new CSVReaderBuilder(new FileReader(inputPath.toFile()))
                .withCSVParser(new CSVParserBuilder().withSeparator(clickHouseService.delimiter).build())
                .build()) {

            List<String> batch = new ArrayList<>();

            String[] headers = csvReader.readNext();
            if (headers == null) {
                throw new RuntimeException("Headers are missing in the CSV file.");
            }

            if (tables.isEmpty() || !tables.contains(tableName)) {
                // Construct a CREATE TABLE query based on the headers
                StringBuilder createTableQuery = new StringBuilder("CREATE TABLE " + tableName + " (");
                for (int i = 0; i < headers.length; i++) {
                    createTableQuery.append(headers[i]).append(" String");
                    if (i < headers.length - 1) {
                        createTableQuery.append(", ");
                    }
                }
                createTableQuery.append(") ENGINE = MergeTree() ORDER BY tuple();");

                // Execute the CREATE TABLE query
                clickHouseService.getClient().query(createTableQuery.toString()).get();
            }

            String[] row;
            while ((row = csvReader.readNext()) != null) {
                // Create an individual row as a tuple: ('value1', 'value2', ...)
                String rowValues = "(" +
                        String.join(",", Arrays.stream(row)
                                .map(value -> "'" + value.replace("'", "''") + "'")
                                .toArray(String[]::new))
                        + ")";
                batch.add(rowValues);

                // Execute batch every 1000 rows
                if (batch.size() >= 1000) {
                    executeBatch(sqlTemplate, batch);
                    batch.clear();
                }
            }

            // Execute any remaining rows
            if (!batch.isEmpty()) {
                executeBatch(sqlTemplate, batch);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading CSV file", e);
        }
    }

    /**
     * Executes a batched INSERT query that combines multiple row values in one statement.
     *
     * @param sqlTemplate the SQL template containing a placeholder for row values
     * @param batch       the list of row value strings, each formatted as a tuple (e.g., ('a', 'b'))
     * @throws Exception if an error occurs during query execution
     */
    private void executeBatch(String sqlTemplate, List<String> batch) throws Exception {
        // Combine the batch rows into a comma-separated string
        String rowsCombined = String.join(", ", batch);
        // Format the full INSERT query: "INSERT INTO tableName VALUES (row1), (row2), ... ;"
        String fullQuery = String.format(sqlTemplate, rowsCombined) + ";";
        // Execute the query synchronously
        clickHouseService.getClient().query(fullQuery).get();
    }

}
