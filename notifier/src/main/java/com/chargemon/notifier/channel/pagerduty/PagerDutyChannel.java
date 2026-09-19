package com.chargemon.notifier.channel.pagerduty;

import com.chargemon.alert.AlertEventType;
import com.chargemon.notifier.channel.DeliveryException;
import com.chargemon.notifier.channel.Notification;
import com.chargemon.notifier.channel.NotificationChannel;
import com.chargemon.notifier.config.NotifierProperties;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

/** PagerDuty Events API v2; {@code dedup_key} = alertId so RESOLVED closes the incident. */
@Component
public class PagerDutyChannel implements NotificationChannel {

    private final RestClient http;
    private final NotifierProperties props;

    public PagerDutyChannel(RestClient http, NotifierProperties props) {
        this.http = http;
        this.props = props;
    }

    @Override
    public String type() {
        return "pagerduty";
    }

    @Override
    public void send(Notification n, String target) {
        String routingKey = props.getPagerduty().getRoutingKeys().get(target);
        if (routingKey == null) {
            throw DeliveryException.permanent("no pagerduty routing key for " + target);
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("routing_key", routingKey);
        body.put("dedup_key", n.event().alertId());
        body.put("event_action", n.event().type() == AlertEventType.OPENED ? "trigger" : "resolve");
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("summary", n.title());
        payload.put("source", n.event().subjectId());
        payload.put("severity", switch (n.event().severity()) {
            case CRITICAL -> "critical";
            case HIGH -> "error";
            case MEDIUM -> "warning";
            case LOW, INFO -> "info";
        });
        payload.put("custom_details", Map.of("body", n.body(), "rule", n.event().ruleName(), "context", n.event().context()));
        body.put("payload", payload);
        try {
            http.post().uri(props.getPagerduty().getEventsUrl()).contentType(MediaType.APPLICATION_JSON).body(body)
                    .retrieve().toBodilessEntity();
        } catch (HttpServerErrorException | ResourceAccessException e) {
            throw DeliveryException.transientError("pagerduty " + target + ": " + e.getMessage(), e);
        } catch (RestClientResponseException e) {
            if (e.getStatusCode().value() == 429) {
                throw DeliveryException.transientError("pagerduty rate limited", e);
            }
            throw DeliveryException.permanent("pagerduty " + target + " rejected: " + e.getStatusCode());
        }
    }
}
