package org.example.bidirectional.service;

import com.clickhouse.client.api.DataStreamWriter;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import org.example.bidirectional.config.SelectedColumnsQueryConfig;

import java.io.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.example.bidirectional.service.ClickHouseService.quote;

public class IngestionService {

    private final ClickHouseService clickHouseService;

    public IngestionService(ClickHouseService clickHouseService) {
        this.clickHouseService = clickHouseService;
    }

    public long ingestDataFromStream(
            String tableName,
            List<String> headers,
            String delimiter,
            InputStream inputStream
    ) throws Exception {
        AtomicLong lines = new AtomicLong();

        DataStreamWriter writer = outputStream -> {
            int bufferSize = 131072; // 128 KB buffer size

            CsvParserSettings parserSettings = new CsvParserSettings();
            parserSettings.setHeaderExtractionEnabled(true);
            parserSettings.getFormat().setDelimiter(ClickHouseService.convertStringToChar(delimiter));
            parserSettings.selectFields(headers.toArray(new String[0]));
            parserSettings.setInputBufferSize(bufferSize);

            CsvParser parser = new CsvParser(parserSettings);
            parser.beginParsing(new BufferedReader(new InputStreamReader(inputStream), bufferSize));

            CsvWriterSettings writerSettings = new CsvWriterSettings();
            CsvWriter csvWriter = new CsvWriter(new BufferedWriter(new OutputStreamWriter(outputStream), bufferSize), writerSettings);

            try {
                csvWriter.writeHeaders(parser.getContext().selectedHeaders());

                String[] row;
                while ((row = parser.parseNext()) != null) {
                    lines.incrementAndGet();
                    csvWriter.writeRow((Object[]) row);
                }
            } finally {
                parser.stopParsing();
                csvWriter.close(); // Proper manual close
            }
        };

        InsertSettings settings = new InsertSettings()
                .serverSetting("input_format_with_names_use_header", "1")
                .serverSetting("input_format_skip_unknown_fields", "1");

        clickHouseService.getClient()
                .insert(quote(tableName), writer, ClickHouseFormat.CSVWithNames, settings)
                .get();

        return lines.get();
    }

    public void streamDataToOutputStream(
            SelectedColumnsQueryConfig config,
            OutputStream outputStream) throws Exception {

        String sql =
                clickHouseService.getJoinedQuery(config.getTableName(), config.getColumns(), config.getJoinTables())   // Getting joined query
                + " FORMAT CSVWithNames SETTINGS format_csv_delimiter = '"
                + ClickHouseService.convertStringToChar(config.getDelimiter()) + "';";

        var response = clickHouseService.getClient()
                .query(sql)
                .get();

        try (InputStream csvStream = response.getInputStream()) {
            csvStream.transferTo(outputStream);
            outputStream.flush();
        }
    }
}
