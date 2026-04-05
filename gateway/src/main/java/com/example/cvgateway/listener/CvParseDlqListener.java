package com.example.cvgateway.listener;

import com.example.cvgateway.dto.CvResultMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class CvParseDlqListener {

    private final ObjectMapper objectMapper;
    private final RabbitTemplate rabbitTemplate;

    @Value("${cv.rabbit.result-queue}")
    private String resultQueue;

    @RabbitListener(queues = "${cv.rabbit.dlq-queue}", concurrency = "1")
    public void onDlq(String body) {
        try {
            JsonNode n = objectMapper.readTree(body);
            String correlationId = n.path("correlationId").asText("");
            log.error("Message moved to DLQ, correlationId={}", correlationId);
            CvResultMessage error = CvResultMessage.builder()
                    .correlationId(correlationId)
                    .status("error")
                    .error("Message failed after retries and reached DLQ")
                    .build();
            rabbitTemplate.convertAndSend("", resultQueue, objectMapper.writeValueAsString(error));
        } catch (Exception ex) {
            log.error("Failed handling DLQ message", ex);
        }
    }
}
