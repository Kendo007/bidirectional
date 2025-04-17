package org.example.bidirectional.service;

import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;
import com.opencsv.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
        // SQL template to be used for a batched insert query.
        String sqlTemplate = "INSERT INTO " + tableName + " VALUES ";
        int count = 0;

        try (CSVReader csvReader = new CSVReaderBuilder(new FileReader(inputPath.toFile()))
                .withCSVParser(new CSVParserBuilder().withSeparator(clickHouseService.delimiter).build())
                .build()) {

            StringBuilder sb = new StringBuilder(sqlTemplate);

            String[] headers = csvReader.readNext();
            if (headers == null) {
                throw new RuntimeException("Headers are missing in the CSV file.");
            }
            clickHouseService.createTable(headers, tableName);

            String[] row;
            while ((row = csvReader.readNext()) != null) {
                // Create an individual row as a tuple: ('value1', 'value2', ...)
                String rowValues = "(" +
                        String.join(",", Arrays.stream(row)
                                .map(value -> "'" + value.replace("'", "''") + "'")
                                .toArray(String[]::new))
                        + ")";
                sb.append(rowValues).append(',');

                // Execute batch every 1000 rows
                if (count >= 1000) {
                    executeBatch(sb.deleteCharAt(sb.length() - 1).append(';').toString());
                    sb.setLength(0);
                    sb.append(sqlTemplate);
                    count = 0;
                }

                ++count;
            }

            // Execute any remaining rows
            if (count > 0) {
                executeBatch(sb.deleteCharAt(sb.length() - 1).append(';').toString());
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading CSV file", e);
        }
    }

    /**
     * Executes a batched INSERT query that combines multiple row values in one statement.
     * @throws Exception if an error occurs during query execution
     */
    private void executeBatch(String fullQuery) throws Exception {
        // Execute the query synchronously
        clickHouseService.getClient().query(fullQuery).get();
    }

    public void ingestDataFromStream(String tableName, InputStream inputStream, boolean hasHeader) throws Exception {
        InsertSettings settings = new InsertSettings()
                .serverSetting("format_csv_delimiter", Character.toString(clickHouseService.delimiter));

        clickHouseService.getClient()
                .insert(tableName, inputStream, hasHeader ? ClickHouseFormat.CSVWithNames : ClickHouseFormat.CSV, settings)
                .get();
    }
}
