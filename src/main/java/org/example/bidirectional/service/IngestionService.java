package org.example.bidirectional.service;

import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;
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

    public void ingestDataFromStream(String tableName, InputStream inputStream, boolean hasHeader) throws Exception {
        InsertSettings settings = new InsertSettings()
                .serverSetting("format_csv_delimiter", Character.toString(clickHouseService.getDelimiter()));

        clickHouseService.getClient()
                .insert(tableName, inputStream, hasHeader ? ClickHouseFormat.CSVWithNames : ClickHouseFormat.CSV, settings)
                .get();
    }

    public void streamDataToOutputStream(String tableName, List<String> columns, OutputStream outputStream) throws Exception {
        String sql =
                String.format("SELECT %s FROM %s FORMAT CSVWithNames SETTINGS format_csv_delimiter = '%s';",
                        String.join(",", columns),
                        tableName,
                        clickHouseService.getDelimiter());

        var response = clickHouseService.getClient()
                .query(sql)
                .get();

        try (InputStream csvStream = response.getInputStream()) {
            csvStream.transferTo(outputStream);
            outputStream.flush();
        }
    }
}
