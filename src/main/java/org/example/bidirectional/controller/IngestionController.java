package org.example.bidirectional.controller;

import org.example.bidirectional.config.ClickHouseProperties;
import org.example.bidirectional.service.ClickHouseService;
import org.example.bidirectional.service.FileService;
import org.example.bidirectional.service.IngestionService;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

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
    public ResponseEntity<String> ingestToFile(@RequestBody IngestRequest request) {
        try {
            ClickHouseService clickHouseService = new ClickHouseService(request.getProperties());
            IngestionService ingestionService = new IngestionService(clickHouseService);

            Path path = Files.createTempFile(request.getTableName() + "_output", ".csv");
            ingestionService.ingestDataToFile(request.getTableName(), request.getColumns(), path);
            return ResponseEntity.ok("Data written to: " + path.toAbsolutePath());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Error: " + e.getMessage());
        }
    }

    @PostMapping("/from-file")
    public ResponseEntity<String> ingestFromFile(@RequestParam("file") MultipartFile file,
                                                 @RequestParam String tableName,
                                                 @RequestBody ClickHouseProperties props) {
        try {
            ClickHouseService clickHouseService = new ClickHouseService(props);
            IngestionService ingestionService = new IngestionService(clickHouseService);

            Path path = Files.createTempFile("upload_", ".csv");
            Files.write(path, file.getBytes());
            ingestionService.ingestDataFromFile(tableName, path);
            return ResponseEntity.ok("File data ingested into table: " + tableName);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Error: " + e.getMessage());
        }
    }
}
