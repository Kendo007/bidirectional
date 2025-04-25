package org.example.bidirectional.service;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.ArrayList;
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

    public static List<String[]> readCsvRows(InputStream inputStream, String delimiter) {
        List<String[]> data = new ArrayList<>();

        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.setHeaderExtractionEnabled(true);
        parserSettings.getFormat().setDelimiter(ClickHouseService.convertStringToChar(delimiter));

        CsvParser parser = new CsvParser(parserSettings);
        parser.beginParsing(new BufferedReader(new InputStreamReader(inputStream)));

        try {
            data.add(parser.getContext().headers());

            String[] row;
            while ((row = parser.parseNext()) != null) {
                data.add(row);
            }
        } finally {
            parser.stopParsing();
        }

        return data;
    }
}
