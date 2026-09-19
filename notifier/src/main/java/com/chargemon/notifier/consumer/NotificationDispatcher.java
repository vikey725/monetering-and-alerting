package com.chargemon.notifier.consumer;

import com.chargemon.alert.AlertEvent;
import com.chargemon.alert.ChannelRef;
import com.chargemon.notifier.channel.ChannelRegistry;
import com.chargemon.notifier.channel.DeliveryException;
import com.chargemon.notifier.channel.Notification;
import com.chargemon.notifier.channel.NotificationChannel;
import com.chargemon.notifier.ledger.DeliveryLedger;
import com.chargemon.notifier.routing.NotificationRouter;
import com.chargemon.notifier.template.MessageFormatter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Routes one alert event to its channels. Each (event, channel) is claimed in the
 * ledger first, so redelivered Kafka records never notify twice. Transient failures
 * are re-thrown so the listener's retry/DLQ policy kicks in; permanent ones are recorded and skipped.
 */
@Service
public class NotificationDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(NotificationDispatcher.class);

    private final NotificationRouter router;
    private final ChannelRegistry channels;
    private final MessageFormatter formatter;
    private final DeliveryLedger ledger;
    private final MeterRegistry metrics;

    public NotificationDispatcher(NotificationRouter router, ChannelRegistry channels, MessageFormatter formatter,
                                  DeliveryLedger ledger, MeterRegistry metrics) {
        this.router = router;
        this.channels = channels;
        this.formatter = formatter;
        this.ledger = ledger;
        this.metrics = metrics;
    }

    /** @throws DeliveryException with {@code isTransient()} when at least one channel should be retried */
    public void dispatch(AlertEvent event) {
        Notification n = formatter.format(event);
        List<String> transientFailures = new ArrayList<>();
        for (ChannelRef ref : router.route(event)) {
            Optional<NotificationChannel> channel = channels.resolve(ref);
            if (channel.isEmpty()) {
                LOG.warn("No channel implementation for {} (alert {})", ref, event.alertId());
                count("skipped", ref);
                continue;
            }
            if (!ledger.claim(event.alertEventId(), ref)) {
                count("duplicate", ref);
                continue;
            }
            try {
                channel.get().send(n, ref.target());
                ledger.markDelivered(event.alertEventId(), ref);
                count("delivered", ref);
            } catch (DeliveryException e) {
                ledger.markFailed(event.alertEventId(), ref, e.getMessage(), e.isTransient());
                count(e.isTransient() ? "retry" : "failed", ref);
                if (e.isTransient()) {
                    transientFailures.add(ref + ": " + e.getMessage());
                } else {
                    LOG.error("Permanent delivery failure for {} via {}: {}", event.alertId(), ref, e.getMessage());
                }
            }
        }
        if (!transientFailures.isEmpty()) {
            throw new DeliveryException("transient failures: " + transientFailures, true);
        }
    }

    private void count(String outcome, ChannelRef ref) {
        Counter.builder("notifier.deliveries").tag("outcome", outcome).tag("channel", ref.type()).register(metrics).increment();
    }
}
