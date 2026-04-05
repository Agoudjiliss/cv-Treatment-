package com.example.cvgateway.service;

import com.github.benmanes.caffeine.cache.Cache;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class EmbeddingCacheService {

    private final EmbeddingModel embeddingModel;
    private final Cache<String, float[]> embeddingCache;
    private final MeterRegistry meterRegistry;

    public float[] embedText(String text) {
        String key = sha256(text);
        float[] cached = embeddingCache.getIfPresent(key);
        if (cached != null) {
            counter("cv.embedding.cache.hit").increment();
            return cached;
        }
        counter("cv.embedding.cache.miss").increment();
        Response<Embedding> resp = embeddingModel.embed(TextSegment.from(text));
        float[] vector = resp.content().vector();
        embeddingCache.put(key, vector);
        return vector;
    }

    private Counter counter(String name) {
        return meterRegistry.counter(name);
    }

    private String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
