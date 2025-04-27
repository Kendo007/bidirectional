package org.example.bidirectional.controller;

import jakarta.annotation.PostConstruct;
import org.example.bidirectional.config.*;
import org.example.bidirectional.exception.AuthenticationException;
import org.example.bidirectional.model.ColumnInfo;
import org.example.bidirectional.service.ClickHouseService;
import org.example.bidirectional.service.FileService;
import org.example.bidirectional.service.IngestionService;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.beans.factory.annotation.Value;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;

@RestController
@RequestMapping("/api/clickhouse")
public class IngestionController {
    @Value("${config.frontend}")
    private String frontendUrl;

    @PostConstruct
    public void init() {
        System.out.println("Ensure FrontEnd is running on: " + frontendUrl);
    }

    @PostMapping("/test-connection")
    public ResponseEntity<?> testConnection(@RequestBody ConnectionConfig props) {
        try {
            ClickHouseService clickHouseService = new ClickHouseService(props);

            return ResponseEntity.ok(Collections.singletonMap("success", clickHouseService.testConnection()));
        } catch (AuthenticationException e) {
            return ResponseEntity.status(401).body(false);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(false);
        }
    }

    @PostMapping("/tables")
    public ResponseEntity<Map<String, List<String>>> listTables(@RequestBody TablesConfig tablesConfig) {
        ClickHouseService clickHouseService = new ClickHouseService(tablesConfig.getConnection());
        return ResponseEntity.ok(Collections.singletonMap("tables", clickHouseService.listTables()));
    }

    @PostMapping("/columns")
    public ResponseEntity<Map<String, List<ColumnInfo>>> getColumns(@RequestBody ColumnConfig props) {
        ClickHouseService clickHouseService = new ClickHouseService(props.getConnection());
        return ResponseEntity.ok(Collections.singletonMap("columns", clickHouseService.getColumns(props.getTableName())));
    }

    /**
     * Extracts the header and data from the given list of rows
     */
    private ResponseEntity<Map<String, Object>> getHeadAndData(List<String[]> rows) {
        if (rows.isEmpty()) {
            return ResponseEntity.ok(Map.of("headers", List.of(), "rows", List.of()));
        }

        List<String> headers = List.of(rows.getFirst());
        List<String[]> data = rows.subList(1, rows.size());

        return ResponseEntity.ok(Map.of(
                "headers", headers,
                "rows", data
        ));
    }

    @PostMapping("/query-selected-columns")
    public ResponseEntity<Map<String, Object>> querySelectedColumns(@RequestBody SelectedColumnsQueryConfig config) throws Exception {
        try {
            ClickHouseService clickHouseService = new ClickHouseService(config.getConnection());

            List<String[]> rows = clickHouseService.querySelectedColumns(
                    config.getTableName(),
                    config.getColumns(),
                    config.getJoinTables(),
                    config.getDelimiter()
            );

            // Build response: first row is headers, remaining rows are data
            return getHeadAndData(rows);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to query selected columns.");
        }
    }

    @PostMapping(value = "/preview-csv", consumes = {"multipart/form-data"})
    public ResponseEntity<Map<String, Object>> previewCSV(
            @RequestPart("file") MultipartFile file,
            @RequestParam(value = "delimiter", defaultValue = ",") String delimiter
    ) throws Exception {
        try {
            List<String[]> rows = FileService.readCsvRows(file.getInputStream(), delimiter);
            return getHeadAndData(rows);
        } catch (Exception e) {
            throw new Exception("Failed to preview CSV. Please check if file exists.");
        }
    }

    @PostMapping("/download")
    public ResponseEntity<FileSystemResource> ingestToFile(@RequestBody SelectedColumnsQueryConfig request) throws IOException {
        long lineCount;
        ClickHouseService clickHouseService = new ClickHouseService(request.getConnection());
        IngestionService ingestionService = new IngestionService(clickHouseService);

        // Creating temp file and writing to it
        Path path = Files.createTempFile(request.getTableName() + "_export", Math.random() + ".csv");
        try (OutputStream outStream = Files.newOutputStream(path)) {
            lineCount = ingestionService.streamDataToOutputStream(request, outStream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Creating fileSystemResource for downloading
        FileSystemResource resource = getFileSystemResource(path);
        String cleanFilename = request.getTableName().replaceAll("[^a-zA-Z0-9-_]", "_") + ".csv";

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + cleanFilename)
                .header(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, "Content-Disposition, Content-Length", "X-Line-Count")
                .header("X-Line-Count", String.valueOf(lineCount))
                .contentType(MediaType.parseMediaType("text/csv"))
                .contentLength(resource.contentLength())
                .body(resource);
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

    @PostMapping(value = "/upload", consumes = {"multipart/form-data"})
    public ResponseEntity<Map<String, Object>> ingestFromFile(
            @RequestPart("file") MultipartFile file,
            @RequestPart("config") String configJson
    ) {
        Map<String, Object> response = new HashMap<>();
        try {
            // Parse the config JSON string to UploadConfig POJO
            ObjectMapper mapper = new ObjectMapper();
            UploadConfig request = mapper.readValue(configJson, UploadConfig.class);

            // Setting up services
            ClickHouseService clickHouseService = new ClickHouseService(request.getConnection());
            IngestionService ingestionService = new IngestionService(clickHouseService);

            if (request.isCreateNewTable())
                clickHouseService.createTable(request.getTableName(), request.getColumnTypes());

            long lines;
            // Ingest only selected columns from CSV stream
            try (InputStream ingestionStream = file.getInputStream()) {
                lines = ingestionService.ingestDataFromStream(
                        request.getTotalCols(),
                        request.getTableName(),
                        new ArrayList<>(request.getColumnTypes().keySet()),
                        request.getDelimiter(),
                        ingestionStream);
            }

            response.put("lines", lines);
            response.put("success", true);
            response.put("message", "Upload successful");

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("lines", 0);
            response.put("success", false);
            response.put("message", "Error: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    @PostMapping("/types")
    public ResponseEntity<Map<String, ArrayList<String>>>  getTypes(@RequestBody TypesConfig typesConfig) {
        ClickHouseService clickHouseService = new ClickHouseService(typesConfig.getConnection());
        return ResponseEntity.ok(Collections.singletonMap("types", clickHouseService.getTypes()));
    }
}
