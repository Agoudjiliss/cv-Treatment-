package com.example.cvgateway.listener;

import com.example.cvgateway.dto.CvResultMessage;
import com.example.cvgateway.service.CvParsePendingRegistry;
import com.example.cvgateway.service.CvIndexService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class CvResultListener {

    private final ObjectMapper objectMapper;
    private final CvParsePendingRegistry pendingRegistry;
    private final CvIndexService cvIndexService;
    private final MeterRegistry meterRegistry;

    @RabbitListener(queues = "${cv.rabbit.result-queue}", concurrency = "1")
    public void onResult(String body) {
        try {
            CvResultMessage message = objectMapper.readValue(body, CvResultMessage.class);
            pendingRegistry.complete(message);
            if ("ok".equalsIgnoreCase(message.getStatus())) {
                meterRegistry.counter("cv.parse.result.ok").increment();
            } else {
                meterRegistry.counter("cv.parse.result.error").increment();
            }
            if ("ok".equalsIgnoreCase(message.getStatus()) && message.getResult() != null) {
                java.util.concurrent.CompletableFuture.runAsync(
                        () -> cvIndexService.indexAsync(message.getCorrelationId(), message.getResult()));
            }
        } catch (Exception ex) {
            meterRegistry.counter("cv.parse.result.decode_error").increment();
            log.error("Failed to process CV result message", ex);
        }
    }
}
