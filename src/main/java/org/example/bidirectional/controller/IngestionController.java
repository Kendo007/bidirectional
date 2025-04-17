package org.example.bidirectional.controller;

import org.example.bidirectional.config.ClickHouseProperties;
import org.example.bidirectional.service.ClickHouseService;
import org.example.bidirectional.service.FileService;
import org.example.bidirectional.service.IngestionService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
@RequestMapping("/api/ingestion")
@CrossOrigin(origins = "*")
public class IngestionController {
    private static final ExecutorService EXPORT_EXECUTOR = Executors.newCachedThreadPool();

    @PostMapping("/tables")
    public ResponseEntity<List<String>> listTables(@RequestBody ClickHouseProperties props) throws Exception {
        ClickHouseService clickHouseService = new ClickHouseService(props);
        return ResponseEntity.ok(clickHouseService.listTables());
    }

    @PostMapping("/columns")
    public ResponseEntity<List<String>> getColumns(@RequestParam String tableName,
                                                   @RequestBody ClickHouseProperties props) throws Exception {
        ClickHouseService clickHouseService = new ClickHouseService(props);
        return ResponseEntity.ok(clickHouseService.getColumns(tableName));
    }

    @PostMapping("/to-file")
    public ResponseEntity<StreamingResponseBody> ingestToFile(@RequestBody IngestRequest request) {
        try {
            ClickHouseService clickHouseService = new ClickHouseService(request.getProperties());
            IngestionService ingestionService = new IngestionService(clickHouseService);

            PipedOutputStream pos = new PipedOutputStream();
            PipedInputStream pis = new PipedInputStream(pos);

            // ✅ Start streaming ClickHouse data on a background thread
            EXPORT_EXECUTOR.submit(() -> {
                try {
                    ingestionService.streamDataToOutputStream(request.getTableName(), request.getColumns(), pos);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        pos.close();
                    } catch (IOException ignored) {}
                }
            });

            StreamingResponseBody responseBody = outputStream -> {
                byte[] buffer = new byte[8192];
                int bytesRead;
                try (pis) {
                    while ((bytesRead = pis.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, bytesRead);
                    }
                }
            };

            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + request.getTableName() + "_export.csv")
                    .contentType(MediaType.parseMediaType("text/csv"))
                    .body(responseBody);

        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().build();
        }
    }



    @PostMapping(value = "/from-file", consumes = {"multipart/form-data"})
    public ResponseEntity<String> ingestFromFile(
            @RequestPart("file") MultipartFile file,
            @RequestParam("tableName") String tableName,
            @RequestPart("config") ClickHouseProperties props
    ) {
        try {
            // ✅ Step 1: Save file to temp path
            Path tempPath = Files.createTempFile("upload_", ".csv");
            try (InputStream inputStream = file.getInputStream();
                 OutputStream outStream = Files.newOutputStream(tempPath, StandardOpenOption.WRITE)) {
                inputStream.transferTo(outStream);
            }

            // ✅ Step 2: Setup services
            ClickHouseService clickHouseService = new ClickHouseService(props);
            IngestionService ingestionService = new IngestionService(clickHouseService);

            // ✅ Step 3: Open stream #1 to read header (if needed)
            try (InputStream headerStream = Files.newInputStream(tempPath)) {
                    String[] headers = FileService.readCsvHeader(headerStream, clickHouseService.delimiter);
                    clickHouseService.createTable(headers, tableName);
            }

            // ✅ Step 4: Open stream #2 to ingest all rows from start
            try (InputStream ingestionStream = Files.newInputStream(tempPath)) {
                ingestionService.ingestDataFromStream(tableName, ingestionStream, true);
            }

            // ✅ Step 5: Delete temp file
            Files.deleteIfExists(tempPath);

            return ResponseEntity.ok("✅ File uploaded and ingested into ClickHouse table: " + tableName);

        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError()
                    .body("❌ Error uploading file: " + e.getMessage());
        }
    }



    @PostMapping("/preview")
    public ResponseEntity<List<String[]>> previewData(@RequestBody IngestRequest request) {
        try {
            ClickHouseService clickHouseService = new ClickHouseService(request.getProperties());
            List<String[]> rows = clickHouseService.fetchDataWithLimit(request.getTableName(), request.getColumns(), 100);

            return ResponseEntity.ok(rows);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}
