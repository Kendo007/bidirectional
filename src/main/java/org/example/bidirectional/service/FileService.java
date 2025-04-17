package org.example.bidirectional.service;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvValidationException;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.List;

@Service
public class FileService {

    // Read CSV file and return data as a list of String arrays
    public List<String[]> readCsv(String filePath, char delimiter) throws IOException, CsvException {
        try (CSVReader reader = new CSVReaderBuilder(new FileReader(filePath))
                .withCSVParser(new CSVParserBuilder().withSeparator(delimiter).build())
                .build()) {
            return reader.readAll();
        }
    }

    // Write data to CSV file
    // Write data to CSV file
    public void writeCsv(String filePath, List<String[]> data, char delimiter) throws IOException {
        try (CSVWriter writer = new CSVWriter(new FileWriter(filePath), delimiter,
                CSVWriter.DEFAULT_QUOTE_CHARACTER,  // <--- enable proper quoting
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END)) {
            writer.writeAll(data);
        }
    }

    public static String[] readCsvHeader(InputStream inputStream, char delimiter) throws IOException {
        // Read header with the first stream
        try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(inputStream))
                .withCSVParser(new CSVParserBuilder().withSeparator(delimiter).build())
                .build()) {
            return reader.readNext(); // Read only the header
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
    }
}
