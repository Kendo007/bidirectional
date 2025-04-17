package org.example.bidirectional.controller;

import org.example.bidirectional.config.ClickHouseProperties;
import org.example.bidirectional.service.ClickHouseService;
import org.example.bidirectional.service.FileService;
import org.example.bidirectional.service.IngestionService;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

@RestController
@RequestMapping("/api/ingestion")
@CrossOrigin(origins = "*")
public class IngestionController {

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
    public ResponseEntity<InputStreamResource> ingestToFile(@RequestBody IngestRequest request) {
        try {
            // Prepare file
            ClickHouseService clickHouseService = new ClickHouseService(request.getProperties());
            IngestionService ingestionService = new IngestionService(clickHouseService);

            Path path = Files.createTempFile(request.getTableName() + "_export", ".csv");
            ingestionService.ingestDataToFile(request.getTableName(), request.getColumns(), path);

            // Stream the file content
            InputStream inputStream = new FileInputStream(path.toFile());
            InputStreamResource resource = new InputStreamResource(inputStream);

            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + path.getFileName())
                    .contentType(MediaType.parseMediaType("text/csv"))
                    .contentLength(Files.size(path))
                    .body(resource);

        } catch (Exception e) {
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
