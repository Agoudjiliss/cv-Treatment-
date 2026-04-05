package com.example.cvgateway.service;

import com.example.cvgateway.dto.CvResultMessage;
import com.github.benmanes.caffeine.cache.Cache;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Component
public class CvParsePendingRegistry {

    private final Cache<String, CompletableFuture<CvResultMessage>> pending;

    public CvParsePendingRegistry(Cache<String, CompletableFuture<CvResultMessage>> correlationPendingCache) {
        this.pending = correlationPendingCache;
    }

    public void register(String correlationId, CompletableFuture<CvResultMessage> future) {
        pending.put(correlationId, future);
    }

    public void complete(CvResultMessage message) {
        Optional.ofNullable(message.getCorrelationId())
                .map(id -> pending.asMap().remove(id))
                .ifPresent(future -> {
                    if (!future.isDone()) {
                        future.complete(message);
                    }
                });
    }

    public void discard(String correlationId) {
        CompletableFuture<CvResultMessage> future = pending.asMap().remove(correlationId);
        if (future != null && !future.isDone()) {
            future.cancel(true);
        }
    }
}
