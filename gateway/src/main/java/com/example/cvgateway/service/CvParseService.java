package com.example.cvgateway.service;

import com.example.cvgateway.dto.ApiErrorResponse;
import com.example.cvgateway.dto.CvJobMessage;
import com.example.cvgateway.dto.CvResultMessage;
import com.example.cvgateway.exception.DownstreamServiceException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
public class CvParseService {

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;
    private final CvParsePendingRegistry pendingRegistry;
    private final MeterRegistry meterRegistry;
    private final String parseExchange;
    private final String parseQueue;
    private final long timeoutSeconds;

    public CvParseService(
            RabbitTemplate rabbitTemplate,
            ObjectMapper objectMapper,
            CvParsePendingRegistry pendingRegistry,
            MeterRegistry meterRegistry,
            @Value("${cv.rabbit.exchange}") String parseExchange,
            @Value("${cv.rabbit.parse-queue}") String parseQueue,
            @Value("${cv.parse.timeout-seconds}") long timeoutSeconds) {
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
        this.pendingRegistry = pendingRegistry;
        this.meterRegistry = meterRegistry;
        this.parseExchange = parseExchange;
        this.parseQueue = parseQueue;
        this.timeoutSeconds = timeoutSeconds;
    }

    public JsonNode processCv(MultipartFile file, String correlationId) {
        long started = System.currentTimeMillis();
        CompletableFuture<CvResultMessage> future = new CompletableFuture<>();
        pendingRegistry.register(correlationId, future);
        try {
            byte[] bytes = file.getBytes();
            String filename = file.getOriginalFilename() != null ? file.getOriginalFilename() : "cv.pdf";
            CvJobMessage job = CvJobMessage.builder()
                    .correlationId(correlationId)
                    .filename(filename)
                    .pdfBase64(Base64.getEncoder().encodeToString(bytes))
                    .build();
            String payload = objectMapper.writeValueAsString(job);
            publishWithRetry(payload);
            meterRegistry.counter("cv.parse.enqueued").increment();

            CvResultMessage result = future.get(timeoutSeconds, TimeUnit.SECONDS);
            if (!"ok".equalsIgnoreCase(result.getStatus())) {
                meterRegistry.counter("cv.parse.failed").increment();
                String err = result.getError() != null ? result.getError() : "Unknown worker error";
                throw new DownstreamServiceException("CV parsing failed: " + err);
            }
            if (result.getResult() == null || result.getResult().isNull()) {
                meterRegistry.counter("cv.parse.empty_result").increment();
                throw new DownstreamServiceException("CV parsing returned empty result");
            }
            meterRegistry.counter("cv.parse.succeeded").increment();
            return result.getResult();
        } catch (TimeoutException ex) {
            meterRegistry.counter("cv.parse.timeout").increment();
            ApiErrorResponse err = ApiErrorResponse.builder()
                    .correlationId(correlationId)
                    .error("CV parsing timed out")
                    .timestamp(Instant.now())
                    .build();
            throw new DownstreamServiceException("CV parsing timed out: " + err.getCorrelationId(), ex);
        } catch (DownstreamServiceException ex) {
            throw ex;
        } catch (Exception ex) {
            meterRegistry.counter("cv.parse.enqueue_error").increment();
            throw new DownstreamServiceException("Failed to enqueue CV parse job", ex);
        } finally {
            meterRegistry.timer("cv.parse.request.ms").record(System.currentTimeMillis() - started, java.util.concurrent.TimeUnit.MILLISECONDS);
            pendingRegistry.discard(correlationId);
        }
    }

    public String newCorrelationId() {
        return UUID.randomUUID().toString();
    }

    @Retryable(
            retryFor = Exception.class,
            maxAttemptsExpression = "${cv.retry.max-attempts}",
            backoff = @Backoff(
                    delayExpression = "${cv.retry.initial-interval-ms}",
                    multiplierExpression = "${cv.retry.multiplier}",
                    maxDelayExpression = "${cv.retry.max-interval-ms}"))
    public void publishWithRetry(String payload) {
        rabbitTemplate.convertAndSend(parseExchange, parseQueue, payload);
    }

    @Recover
    public void recoverPublish(Exception ex, String payload) {
        throw new DownstreamServiceException("Failed to publish parse job after retries", ex);
    }
}
