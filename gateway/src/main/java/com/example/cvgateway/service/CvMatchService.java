package com.example.cvgateway.service;

import com.example.cvgateway.dto.CvMatchResult;
import com.example.cvgateway.dto.JobMatchRequest;
import com.example.cvgateway.dto.JobMatchResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CvMatchService {

    private final CvFilterService cvFilterService;
    private final EmbeddingCacheService embeddingCacheService;
    private final ObjectMapper objectMapper;

    @Value("${PYTHON_ENGINE_BASE_URL:http://engine:8000}")
    private String engineBaseUrl;

    public JobMatchResponse match(JobMatchRequest request) {
        long started = System.currentTimeMillis();
        List<Map<String, Object>> filtered = cvFilterService.filterCandidates(request);
        float[] jobEmbedding = embeddingCacheService.embedText(request.getJobDescription());
        List<Map<String, Object>> top200 = filtered.stream()
                .sorted((a, b) -> Double.compare(score(jobEmbedding, vector(b)), score(jobEmbedding, vector(a))))
                .limit(200)
                .toList();

        List<Map<String, Object>> topK = top200.stream()
                .sorted((a, b) -> Double.compare(score(jobEmbedding, vector(b)), score(jobEmbedding, vector(a))))
                .limit(request.getTopK())
                .toList();

        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<CvMatchResult>> futures = new ArrayList<>();
            int rank = 1;
            for (Map<String, Object> doc : topK) {
                int currentRank = rank++;
                futures.add(CompletableFuture.supplyAsync(
                        () -> buildResult(request, doc, jobEmbedding, currentRank), executor));
            }
            List<CvMatchResult> results = futures.stream().map(CompletableFuture::join)
                    .sorted(Comparator.comparingInt(CvMatchResult::getRank))
                    .toList();
            return JobMatchResponse.builder()
                    .jobId(UUID.randomUUID().toString())
                    .processingMs(System.currentTimeMillis() - started)
                    .totalFiltered(filtered.size())
                    .results(results)
                    .build();
        }
    }

    private CvMatchResult buildResult(JobMatchRequest req, Map<String, Object> doc, float[] jobEmbedding, int rank) {
        float[] cvVector = vector(doc);
        double s = score(jobEmbedding, cvVector);
        JsonNode cvJson = objectMapper.valueToTree(doc.getOrDefault("cvJson", Map.of()));
        List<String> skills = extractSkills(cvJson);
        List<String> matched = req.getRequiredSkills().stream().filter(r -> skills.stream().anyMatch(sv -> sv.equalsIgnoreCase(r))).toList();
        List<String> missing = req.getRequiredSkills().stream().filter(r -> matched.stream().noneMatch(m -> m.equalsIgnoreCase(r))).toList();
        String explanation = explain(req.getJobDescription(), cvJson, s);
        return CvMatchResult.builder()
                .rank(rank)
                .cvId(String.valueOf(doc.getOrDefault("cvId", UUID.randomUUID().toString())))
                .vectorScore(s)
                .confidence(((Number) doc.getOrDefault("confidence", 0.0)).doubleValue())
                .matchedSkills(matched)
                .missingSkills(missing)
                .explanation(explanation)
                .contact(cvJson.path("contact"))
                .build();
    }

    private String explain(String jobDescription, JsonNode cvJson, double score) {
        try {
            Map<String, Object> payload = Map.of(
                    "jobDescription", jobDescription,
                    "cvJson", cvJson,
                    "vectorScore", score);
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(engineBaseUrl + "/api/v1/cv/explain"))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload)))
                    .build();
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                JsonNode n = objectMapper.readTree(resp.body());
                return n.path("explanation").asText(null);
            }
        } catch (Exception ignored) {
            return null;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private float[] vector(Map<String, Object> doc) {
        Object o = doc.get("embeddingVector");
        if (o instanceof List<?> l && !l.isEmpty()) {
            float[] out = new float[l.size()];
            for (int i = 0; i < l.size(); i++) {
                out[i] = ((Number) l.get(i)).floatValue();
            }
            return out;
        }
        return new float[384];
    }

    private double score(float[] a, float[] b) {
        if (a.length != b.length) {
            return 0.0;
        }
        double dot = 0, na = 0, nb = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }
        if (na == 0 || nb == 0) {
            return 0.0;
        }
        return dot / (Math.sqrt(na) * Math.sqrt(nb));
    }

    private List<String> extractSkills(JsonNode cvJson) {
        List<String> out = new ArrayList<>();
        JsonNode n = cvJson.path("skills").path("technical");
        if (n.isArray()) {
            n.forEach(x -> out.add(x.asText("")));
        }
        return out;
    }
}
