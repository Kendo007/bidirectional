package org.example.bidirectional.controller;

import org.example.bidirectional.config.ClickHouseProperties;
import org.example.bidirectional.service.ClickHouseService;
import org.example.bidirectional.service.FileService;
import org.example.bidirectional.service.IngestionService;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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
            // 1. Write uploaded CSV to temp path
            Path path = Files.createTempFile("upload_", ".csv");
            Files.write(path, file.getBytes());

            // 2. Create ClickHouse client and ingestion service
            ClickHouseService clickHouseService = new ClickHouseService(props);
            IngestionService ingestionService = new IngestionService(clickHouseService);

            // 3. Ingest file into ClickHouse
            ingestionService.ingestDataFromFile(tableName, path);

            return ResponseEntity.ok("✅ File data ingested into ClickHouse table: " + tableName);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("❌ Error ingesting file: " + e.getMessage());
        }
    }
}
