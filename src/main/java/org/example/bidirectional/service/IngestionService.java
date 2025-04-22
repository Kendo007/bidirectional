package org.example.bidirectional.service;

import com.clickhouse.client.api.DataStreamWriter;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.List;

@Service
public class IngestionService {

    private final ClickHouseService clickHouseService;

    @Autowired
    public IngestionService(ClickHouseService clickHouseService) {
        this.clickHouseService = clickHouseService;
    }

    public void ingestDataFromStream(String tableName, InputStream inputStream, List<String> headers) throws Exception {
        DataStreamWriter writer = outputStream -> {
            int bufferSize = 32768; // 32 KB buffer size
            CsvParserSettings parserSettings = new CsvParserSettings();
            parserSettings.setHeaderExtractionEnabled(true);
            parserSettings.getFormat().setDelimiter(clickHouseService.getDelimiter());
            parserSettings.selectFields(headers.toArray(new String[0]));
            parserSettings.setInputBufferSize(bufferSize);

            CsvParser parser = new CsvParser(parserSettings);
            parser.beginParsing(new BufferedReader(new InputStreamReader(inputStream), bufferSize));

            CsvWriterSettings writerSettings = new CsvWriterSettings();
            writerSettings.getFormat().setDelimiter(clickHouseService.getDelimiter());
            CsvWriter csvWriter = new CsvWriter(new BufferedWriter(new OutputStreamWriter(outputStream), bufferSize), writerSettings);

            try {
                csvWriter.writeHeaders(parser.getContext().selectedHeaders());

                String[] row;
                while ((row = parser.parseNext()) != null) {
                    csvWriter.writeRow((Object[]) row);
                }
            } finally {
                parser.stopParsing();
                csvWriter.close(); // Proper manual close
            }
        };

        InsertSettings settings = new InsertSettings()
                .serverSetting("input_format_with_names_use_header", "1")
                .serverSetting("input_format_skip_unknown_fields", "1")
                .serverSetting("format_csv_delimiter", String.valueOf(clickHouseService.getDelimiter()));

        tableName = "`" + tableName + "`";
        clickHouseService.getClient()
                .insert(tableName, writer, ClickHouseFormat.CSVWithNames, settings)
                .get();
    }

    public void streamDataToOutputStream(String tableName, List<String> columns, OutputStream outputStream) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            sb.append('`').append(columns.get(i)).append('`');
            if (i < columns.size() - 1) sb.append(',');
        }

        String sql = String.format("SELECT %s FROM `%s`", sb, tableName)
                + " FORMAT CSVWithNames SETTINGS format_csv_delimiter = '" + clickHouseService.getDelimiter() + "';";

        var response = clickHouseService.getClient()
                .query(sql)
                .get();

        try (InputStream csvStream = response.getInputStream()) {
            csvStream.transferTo(outputStream);
            outputStream.flush();
        }
    }
}
