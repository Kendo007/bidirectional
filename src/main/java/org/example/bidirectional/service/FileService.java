package org.example.bidirectional.service;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import org.springframework.stereotype.Service;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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

}
