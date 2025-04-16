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
     * @param delimiter  The custom delimiter.
     * @throws Exception if an error occurs during the process.
     */
    public void ingestDataToFile(String tableName, List<String> columns, Path outputPath, char delimiter) throws Exception {
        clickHouseService.fetchDataAndProcess(tableName, columns, row -> {
            try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                // Create a CSVWriter instance directly
                CSVWriter csvWriter = new CSVWriter(writer,
                        delimiter,
                        CSVWriter.NO_QUOTE_CHARACTER,
                        CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                        CSVWriter.DEFAULT_LINE_END);

                // Write header if the file is empty
                if (Files.size(outputPath) == 0) {
                    csvWriter.writeNext(columns.toArray(new String[0]));
                }

                // Write the current row to CSV
                csvWriter.writeNext(row);
                csvWriter.flush();
                csvWriter.close();
            } catch (IOException e) {
                throw new RuntimeException("Error writing row to CSV file", e);
            }
        });
    }


    /**
     * Ingest data from a CSV file into ClickHouse.
     *
     * @param tableName  The table name.
     * @param inputPath  The CSV input file path.
     * @param delimiter  The custom delimiter.
     * @throws Exception if an error occurs during the process.
     */
    public void ingestDataFromFile(String tableName, Path inputPath, char delimiter) throws Exception {
        // Prepare a SQL insert query template (use placeholders for parameters)
        String sqlTemplate = "INSERT INTO " + tableName + " VALUES (%s)";

        try (CSVReader csvReader = new CSVReaderBuilder(new FileReader(inputPath.toFile()))
                .withCSVParser(new CSVParserBuilder().withSeparator(delimiter).build())
                .build()) {
            List<String> batch = new ArrayList<>(); // To hold the batch of insert statements

            csvReader.readNext();   // skip the first row

            String[] row;
            while ((row = csvReader.readNext()) != null) {
                // Create the insert statement by joining the row values as a string
                String insertValues = String.join(",", Arrays.stream(row)
                        .map(value -> "'" + value.replace("'", "''") + "'")  // Escape single quotes
                        .toArray(String[]::new));

                // Construct the full insert query
                String query = String.format(sqlTemplate, insertValues);

                // Add to batch (for batching insert)
                batch.add(query);

                // Execute the batch after a certain number of rows (for performance)
                if (batch.size() >= 1000) { // Assuming you want to batch after 1000 rows
                    executeBatch(batch);
                    batch.clear(); // Clear batch for next set of rows
                }
            }

            // If there are remaining rows in the batch, execute them
            if (!batch.isEmpty()) {
                executeBatch(batch);
            }

        } catch (IOException e) {
            throw new RuntimeException("Error reading CSV file", e);
        }
    }

    private void executeBatch(List<String> batch) throws Exception {
        // Combine all insert queries in the batch and execute them in a single call
        String fullQuery = String.join(";", batch);
        clickHouseService.getClient().query(fullQuery).get(); // Execute the batch query
    }

}
