package com.example.cvgateway.config;

import com.example.cvgateway.dto.CvResultMessage;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CacheConfig {

    @Bean
    public Cache<String, CompletableFuture<CvResultMessage>> correlationPendingCache(
            @Value("${cv.parse.timeout-seconds}") long timeoutSeconds) {
        return Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(timeoutSeconds + 10))
                .maximumSize(100_000)
                .build();
    }

    @Bean
    public Cache<String, float[]> embeddingCache() {
        return Caffeine.newBuilder()
                .maximumSize(50_000)
                .expireAfterAccess(Duration.ofHours(12))
                .build();
    }
}
