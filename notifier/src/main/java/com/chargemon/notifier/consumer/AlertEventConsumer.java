package com.chargemon.notifier.consumer;

import com.chargemon.alert.AlertEvent;
import com.chargemon.notifier.channel.DeliveryException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * Consumes the alerts topic. Transient delivery failures go through non-blocking
 * retry topics ({@code alerts-retry-*}) and finally {@code alerts-dlq}; malformed
 * records are logged and acknowledged (they can never succeed).
 */
@Component
public class AlertEventConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(AlertEventConsumer.class);

    private final ObjectMapper json;
    private final NotificationDispatcher dispatcher;

    public AlertEventConsumer(ObjectMapper json, NotificationDispatcher dispatcher) {
        this.json = json;
        this.dispatcher = dispatcher;
    }

    @RetryableTopic(
            attempts = "${notifier.retry.attempts:4}",
            backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 60000),
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            dltTopicSuffix = "-dlq",
            retryTopicSuffix = "-retry",
            include = DeliveryException.class)
    @KafkaListener(topics = "${notifier.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void onAlert(String payload, Acknowledgment ack) {
        AlertEvent event;
        try {
            event = json.readValue(payload, AlertEvent.class);
        } catch (IOException e) {
            LOG.error("Dropping malformed alert event: {}", e.getMessage());
            ack.acknowledge();
            return;
        }
        try {
            dispatcher.dispatch(event);
            ack.acknowledge();
        } catch (DeliveryException e) {
            if (e.isTransient()) {
                throw e;                  // -> retry topic / DLQ
            }
            ack.acknowledge();
        }
    }
}
