package com.example.cvgateway.service;

import com.example.cvgateway.dto.CvJobMessage;
import com.example.cvgateway.dto.CvResultMessage;
import com.example.cvgateway.exception.DownstreamServiceException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class CvRabbitParseService {

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;
    private final CvParsePendingRegistry pendingRegistry;

    private final String parseQueue;
    private final long timeoutSeconds;

    public CvRabbitParseService(
            RabbitTemplate rabbitTemplate,
            ObjectMapper objectMapper,
            CvParsePendingRegistry pendingRegistry,
            @Value("${cv.rabbit.parse-queue}") String parseQueue,
            @Value("${cv.parse.timeout-seconds}") long timeoutSeconds) {
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
        this.pendingRegistry = pendingRegistry;
        this.parseQueue = parseQueue;
        this.timeoutSeconds = timeoutSeconds;
    }

    public JsonNode processCv(MultipartFile file) {
        String correlationId = UUID.randomUUID().toString();
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
            rabbitTemplate.convertAndSend("", parseQueue, payload);

            CvResultMessage result = future.orTimeout(timeoutSeconds, TimeUnit.SECONDS).join();
            if (!"ok".equalsIgnoreCase(result.getStatus())) {
                String err = result.getError() != null ? result.getError() : "Unknown worker error";
                throw new DownstreamServiceException("CV parsing failed: " + err);
            }
            if (result.getResult() == null || result.getResult().isNull()) {
                throw new DownstreamServiceException("CV parsing returned empty result");
            }
            return result.getResult();
        } catch (CompletionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof TimeoutException) {
                throw new DownstreamServiceException("CV parsing timed out", cause);
            }
            throw new DownstreamServiceException("CV parsing failed", cause != null ? cause : ex);
        } catch (DownstreamServiceException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DownstreamServiceException("Failed to enqueue CV parse job", ex);
        } finally {
            pendingRegistry.discard(correlationId);
        }
    }
}
