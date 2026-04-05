package com.example.cvgateway.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
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
        JsonNode t = cvJson.path("skills").path("languages");
        if (t.isArray()) {
            t.forEach(n -> out.add(n.asText("")));
        }
        return out;
    }

    private int estimateSeniority(JsonNode cvJson) {
        int years = 0;
        JsonNode exp = cvJson.path("experience");
        if (exp.isArray()) {
            for (JsonNode e : exp) {
                String duration = e.path("duration").asText("");
                if (duration.contains("20")) {
                    years += 1;
                }
            }
        }
        return years;
    }

    private String extractCountry(JsonNode cvJson) {
        String loc = cvJson.path("contact").path("location").asText("");
        if (loc.toUpperCase().contains("ALGERIA")) {
            return "DZ";
        }
        if (loc.toUpperCase().contains("FRANCE")) {
            return "FR";
        }
        return "";
    }
}
