package com.example.cvgateway.config;

import java.util.HashMap;
import java.util.Map;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.support.RetryTemplate;

@Configuration
public class RabbitMqConfig {

    @Bean
    public DirectExchange cvExchange(@Value("${cv.rabbit.exchange}") String exchangeName) {
        return new DirectExchange(exchangeName, true, false);
    }

    @Bean
    public Queue cvParseQueue(
            @Value("${cv.rabbit.parse-queue}") String parseQueue,
            @Value("${cv.rabbit.exchange}") String exchangeName,
            @Value("${cv.rabbit.dlq-queue}") String dlqQueue) {
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", exchangeName);
        args.put("x-dead-letter-routing-key", dlqQueue);
        return new Queue(parseQueue, true, false, false, args);
    }

    @Bean
    public Queue cvResultQueue(@Value("${cv.rabbit.result-queue}") String resultQueue) {
        return new Queue(resultQueue, true);
    }

    @Bean
    public Queue cvParseDlq(@Value("${cv.rabbit.dlq-queue}") String dlqQueue) {
        return new Queue(dlqQueue, true);
    }

    @Bean
    public Binding parseBinding(
            Queue cvParseQueue,
            DirectExchange cvExchange,
            @Value("${cv.rabbit.parse-queue}") String parseQueue) {
        return BindingBuilder.bind(cvParseQueue).to(cvExchange).with(parseQueue);
    }

    @Bean
    public Binding dlqBinding(
            Queue cvParseDlq,
            DirectExchange cvExchange,
            @Value("${cv.rabbit.dlq-queue}") String dlqQueue) {
        return BindingBuilder.bind(cvParseDlq).to(cvExchange).with(dlqQueue);
    }

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory,
            @Value("${cv.retry.max-attempts}") int maxAttempts,
            @Value("${cv.retry.initial-interval-ms}") long initialInterval,
            @Value("${cv.retry.multiplier}") double multiplier,
            @Value("${cv.retry.max-interval-ms}") long maxInterval) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setDefaultRequeueRejected(false);
        factory.setAdviceChain(
                RetryInterceptorBuilder.stateless()
                        .maxAttempts(maxAttempts)
                        .backOffOptions(initialInterval, multiplier, maxInterval)
                        .recoverer(new RejectAndDontRequeueRecoverer())
                        .build());
        return factory;
    }

    @Bean
    public RetryTemplate publishRetryTemplate(
            @Value("${cv.retry.max-attempts}") int maxAttempts,
            @Value("${cv.retry.initial-interval-ms}") long initialInterval,
            @Value("${cv.retry.multiplier}") double multiplier,
            @Value("${cv.retry.max-interval-ms}") long maxInterval) {
        RetryTemplate template = new RetryTemplate();
        org.springframework.retry.backoff.ExponentialBackOffPolicy backOff = new org.springframework.retry.backoff.ExponentialBackOffPolicy();
        backOff.setInitialInterval(initialInterval);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxInterval);
        template.setBackOffPolicy(backOff);
        org.springframework.retry.policy.SimpleRetryPolicy retryPolicy = new org.springframework.retry.policy.SimpleRetryPolicy(maxAttempts);
        template.setRetryPolicy(retryPolicy);
        return template;
    }
}
