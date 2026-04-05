package com.example.cvgateway.controller;

import com.example.cvgateway.service.CvParseService;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.slf4j.MDC;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@Validated
@RestController
@RequestMapping("/api/v1/cv")
public class CvParseController {

    private static final long MAX_BYTES = 10L * 1024L * 1024L;
    private final CvParseService cvParseService;

    public CvParseController(CvParseService cvParseService) {
        this.cvParseService = cvParseService;
    }

    @PostMapping(value = "/parse", consumes = MediaType.MULTIPART_FORM_DATA_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<JsonNode> parseCv(@RequestParam("file") @NotNull MultipartFile file) {
        long started = System.currentTimeMillis();
        String correlationId = cvParseService.newCorrelationId();
        MDC.put("correlationId", correlationId);
        if (file.isEmpty()) {
            throw new IllegalArgumentException("Uploaded file is empty");
        }
        if (file.getSize() > MAX_BYTES) {
            throw new org.springframework.web.server.ResponseStatusException(
                    org.springframework.http.HttpStatus.PAYLOAD_TOO_LARGE,
                    "Uploaded file exceeds 10MB");
        }
        if (!MediaType.APPLICATION_PDF_VALUE.equalsIgnoreCase(file.getContentType())
                && (file.getOriginalFilename() == null || !file.getOriginalFilename().toLowerCase().endsWith(".pdf"))) {
            throw new IllegalArgumentException("Only PDF files are supported");
        }
        validatePdfMagic(file);

        try {
            JsonNode result = cvParseService.processCv(file, correlationId);
            return ResponseEntity.ok()
                    .header("X-Correlation-Id", correlationId)
                    .header("X-Processing-Time-Ms", String.valueOf(System.currentTimeMillis() - started))
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(result);
        } finally {
            MDC.remove("correlationId");
        }
    }

    private void validatePdfMagic(MultipartFile file) {
        try {
            byte[] bytes = file.getBytes();
            if (bytes.length < 4) {
                throw new org.springframework.web.server.ResponseStatusException(
                        org.springframework.http.HttpStatus.UNSUPPORTED_MEDIA_TYPE,
                        "Invalid PDF content");
            }
            String magic = new String(bytes, 0, 4, StandardCharsets.US_ASCII);
            if (!"%PDF".equals(magic)) {
                throw new org.springframework.web.server.ResponseStatusException(
                        org.springframework.http.HttpStatus.UNSUPPORTED_MEDIA_TYPE,
                        "File magic number is not a PDF");
            }
        } catch (IOException ex) {
            throw new org.springframework.web.server.ResponseStatusException(
                    org.springframework.http.HttpStatus.BAD_REQUEST,
                    "Failed to read uploaded file");
        }
    }
}
