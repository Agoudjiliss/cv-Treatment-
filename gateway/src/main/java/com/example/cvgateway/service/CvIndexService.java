package com.example.cvgateway.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.LocalDate;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class CvIndexService {

    private final EmbeddingCacheService embeddingCacheService;
    private final ElasticsearchClient elasticsearchClient;
    private static final DateTimeFormatter DMY = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    @Value("${cv.index.index-name}")
    private String indexName;

    @Value("${cv.index.enabled:true}")
    private boolean enabled;

    public void indexAsync(String correlationId, JsonNode cvJson) {
        if (!enabled || cvJson == null || cvJson.isNull()) {
            return;
        }
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            executor.submit(() -> indexNow(correlationId, cvJson));
        } catch (Exception ex) {
            log.error("Failed scheduling index task correlationId={}", correlationId, ex);
        }
    }

    private void indexNow(String correlationId, JsonNode cvJson) {
        try {
            String cvString = cvJson.toString();
            float[] emb = embeddingCacheService.embedText(cvString);
            List<Float> vector = new ArrayList<>(emb.length);
            for (float v : emb) {
                vector.add(v);
            }
            Map<String, Object> doc = new HashMap<>();
            String cvId = UUID.randomUUID().toString();
            doc.put("cvId", cvId);
            doc.put("skillsSet", extractSkills(cvJson));
            doc.put("languages", extractLanguages(cvJson));
            doc.put("seniorityYears", estimateSeniority(cvJson));
            doc.put("locationCountry", extractCountry(cvJson));
            doc.put("embeddingVector", vector);
            doc.put("confidence", cvJson.path("confidence").asDouble(0.0));
            doc.put("cvJson", cvJson);
            doc.put("indexedAt", Instant.now().toString());
            elasticsearchClient.index(IndexRequest.of(r -> r.index(indexName).id(cvId).document(doc)));
        } catch (Exception ex) {
            log.error("Failed to index CV correlationId={}", correlationId, ex);
        }
    }

    private List<String> extractSkills(JsonNode cvJson) {
        List<String> out = new ArrayList<>();
        JsonNode t = cvJson.path("skills").path("technical");
        if (t.isArray()) {
            t.forEach(n -> out.add(n.asText("")));
        }
        return out;
    }

    private List<String> extractLanguages(JsonNode cvJson) {
        List<String> out = new ArrayList<>();
        JsonNode t = cvJson.path("languages");
        if (t.isArray()) {
            t.forEach(n -> {
                String lang = n.path("language").asText("");
                if (!lang.isBlank()) {
                    out.add(lang);
                }
            });
        }
        return out;
    }

    private int estimateSeniority(JsonNode cvJson) {
        long totalMonths = 0;
        JsonNode exp = cvJson.path("experience");
        if (exp.isArray()) {
            for (JsonNode e : exp) {
                LocalDate start = parseDate(e.path("startDate").asText(""));
                LocalDate end = parseDate(e.path("endDate").asText(""));
                if (start == null) {
                    continue;
                }
                if (end == null) {
                    end = LocalDate.now(ZoneOffset.UTC);
                }
                if (end.isBefore(start)) {
                    continue;
                }
                long months = ChronoUnit.MONTHS.between(start.withDayOfMonth(1), end.withDayOfMonth(1));
                if (months >= 0 && months < 12 * 70) {
                    totalMonths += months;
                }
            }
        }
        return (int) Math.max(0, Math.min(70, totalMonths / 12));
    }

    private String extractCountry(JsonNode cvJson) {
        String loc = cvJson.path("contact").path("location").asText("");
        String u = loc == null ? "" : loc.toUpperCase();
        if (u.contains("ALGERIA") || u.contains("ALGÉRIE") || u.contains("ALGERIE") || u.contains("DZ") || u.contains("ALGIERS") || u.contains("ALGER")) {
            return "DZ";
        }
        if (u.contains("FRANCE") || u.contains("PARIS") || u.contains("LYON") || u.contains("MARSEILLE") || u.contains("FR")) {
            return "FR";
        }
        return "";
    }

    private LocalDate parseDate(String raw) {
        if (raw == null) {
            return null;
        }
        String v = raw.trim();
        if (v.isBlank()) {
            return null;
        }
        try {
            if (v.matches("^\\d{2}/\\d{2}/\\d{4}$")) {
                return LocalDate.parse(v, DMY);
            }
            if (v.matches("^\\d{2}/\\d{4}$")) {
                return LocalDate.parse("01/" + v, DMY);
            }
            if (v.matches("^\\d{4}$")) {
                return LocalDate.of(Integer.parseInt(v), 1, 1);
            }
        } catch (DateTimeParseException | NumberFormatException ignored) {
            return null;
        }
        return null;
    }
}
