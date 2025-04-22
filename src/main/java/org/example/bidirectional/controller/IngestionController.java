package org.example.bidirectional.controller;

import org.example.bidirectional.config.ClickHouseProperties;
import org.example.bidirectional.service.ClickHouseService;
import org.example.bidirectional.service.FileService;
import org.example.bidirectional.service.IngestionService;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/ingestion")
@CrossOrigin(origins = "*")
public class IngestionController {
    @PostMapping("/tables")
    public ResponseEntity<List<String>> listTables(@RequestBody ClickHouseProperties props) {
        ClickHouseService clickHouseService = new ClickHouseService(props);
        return ResponseEntity.ok(clickHouseService.listTables());
    }

    @PostMapping("/columns")
    public ResponseEntity<List<String>> getColumns(@RequestParam String tableName,
                                                   @RequestBody ClickHouseProperties props) {
        ClickHouseService clickHouseService = new ClickHouseService(props);
        return ResponseEntity.ok(clickHouseService.getColumns(tableName));
    }

    @PostMapping("/to-file")
    public ResponseEntity<FileSystemResource> ingestToFile(@RequestBody IngestRequest request) {
        Path path;
        try {
            ClickHouseService clickHouseService = new ClickHouseService(request.getProperties());
            IngestionService ingestionService = new IngestionService(clickHouseService);

            // Creating temp file and writing to it
            path = Files.createTempFile(request.getTableName() + "_export", Math.random() + ".csv");
            try (OutputStream outStream = Files.newOutputStream(path)) {
                ingestionService.streamDataToOutputStream(request.getTableName(), request.getColumns(), outStream);
            }

            // Creating fileSystemResource for downloading
            FileSystemResource resource = getFileSystemResource(path);

            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + path.getFileName())
                    .contentType(MediaType.parseMediaType("text/csv"))
                    .contentLength(resource.contentLength())
                    .body(resource);

        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().build();
        }
    }

    private static FileSystemResource getFileSystemResource(Path path) {
        File file = path.toFile();
        return new FileSystemResource(file) {
            @Override
            public InputStream getInputStream() throws IOException {
                InputStream original = super.getInputStream();
                return new FilterInputStream(original) {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        try { Files.deleteIfExists(file.toPath()); }
                        catch (IOException _) {}
                    }
                };
            }
        };
    }

    @PostMapping(value = "/from-file", consumes = {"multipart/form-data"})
    public ResponseEntity<String> ingestFromFile(
            @RequestPart("file") MultipartFile file,
            @RequestParam("tableName") String tableName,
            @RequestPart("config") ClickHouseProperties props,
            @RequestPart("headers") List<String> selectedHeaders
    ) {
        Path tempPath;

        try {
            // ✅ Step 1: Save uploaded file to temp path
            tempPath = Files.createTempFile("upload_", ".csv");
            try (InputStream inputStream = file.getInputStream();
                 OutputStream outStream = Files.newOutputStream(tempPath, StandardOpenOption.WRITE)) {
                inputStream.transferTo(outStream);
            }

            // ✅ Step 2: Set up services
            ClickHouseService clickHouseService = new ClickHouseService(props);
            IngestionService ingestionService = new IngestionService(clickHouseService);

            // ✅ Step 3: Create table using selected headers
            clickHouseService.createTable(selectedHeaders.toArray(new String[0]), tableName);

            // ✅ Step 4: Ingest only selected columns from CSV stream
            try (InputStream ingestionStream = Files.newInputStream(tempPath)) {
                ingestionService.ingestDataFromStream(tableName, ingestionStream, selectedHeaders);
            }

            // ✅ Step 5: Clean up temp file
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

    @PostMapping(value = "/preview-csv", consumes = {"multipart/form-data"})
    public ResponseEntity<Map<String, Object>> previewCSV(
            @RequestPart("file") MultipartFile file,
            @RequestPart("config") ClickHouseProperties props
    ) {
        try {
            // Read headers and preview rows
            ClickHouseService clickHouseService = new ClickHouseService(props);

            List<String[]> rows = FileService.readCsvRows(file.getInputStream(), clickHouseService.getDelimiter(), 101);
            if (rows.isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of("error", "No rows found"));
            }

            String[] headers = rows.getFirst();
            List<String[]> data = rows.subList(1, rows.size());

            return ResponseEntity.ok(Map.of(
                    "headers", headers,
                    "data", data
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to preview: " + e.getMessage()));
        }
    }

}
