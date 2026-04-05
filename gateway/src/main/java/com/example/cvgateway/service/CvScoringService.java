package com.example.cvgateway.service;

import com.example.cvgateway.dto.ScoredCvDto;
import com.example.cvgateway.exception.CvScoringException;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class CvScoringService {

    private final EmbeddingCacheService embeddingCacheService;

    @Value("${cv.score.top-k}")
    private int topK;

    public List<ScoredCvDto> scoreTopMatches(String jobDescription, List<JsonNode> parsedCvs) {
        Optional<String> jdOpt = Optional.ofNullable(jobDescription).map(String::trim).filter(s -> !s.isEmpty());
        if (jdOpt.isEmpty()) {
            throw new CvScoringException("Job description must not be blank");
        }
        if (parsedCvs == null || parsedCvs.isEmpty()) {
            throw new CvScoringException("parsedCvs must not be empty");
        }

        try {
            float[] jdVector = embeddingCacheService.embedText(jdOpt.get());
            List<ScoredCvDto> scored = new ArrayList<>();
            for (int i = 0; i < parsedCvs.size(); i++) {
                JsonNode cv = parsedCvs.get(i);
                String flat = flattenCv(cv);
                float[] cvVector = embeddingCacheService.embedText(flat);
                double score = cosineSimilarity(jdVector, cvVector);
                scored.add(ScoredCvDto.builder().index(i).score(score).parsedCv(cv).build());
            }
            scored.sort(Comparator.comparingDouble(ScoredCvDto::getScore).reversed());
            int limit = Math.min(topK, scored.size());
            return scored.subList(0, limit);
        } catch (CvScoringException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new CvScoringException("Embedding scoring failed", ex);
        }
    }

    private static double cosineSimilarity(float[] a, float[] b) {
        if (a.length != b.length) {
            return 0.0;
        }
        double dot = 0.0;
        double na = 0.0;
        double nb = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }
        if (na == 0.0 || nb == 0.0) {
            return 0.0;
        }
        return dot / (Math.sqrt(na) * Math.sqrt(nb));
    }

    private static String flattenCv(JsonNode node) {
        if (node == null || node.isNull()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        node.fields().forEachRemaining(entry -> sb.append(entry.getKey())
                .append(": ")
                .append(entry.getValue().toString())
                .append('\n'));
        return sb.toString();
    }
}
