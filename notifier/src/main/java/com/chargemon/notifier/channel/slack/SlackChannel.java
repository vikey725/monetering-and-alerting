package com.chargemon.notifier.channel.slack;

import com.chargemon.notifier.channel.DeliveryException;
import com.chargemon.notifier.channel.Notification;
import com.chargemon.notifier.channel.NotificationChannel;
import com.chargemon.notifier.config.NotifierProperties;
import java.util.Map;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

/** Slack incoming webhook per target ({@code slack:#ops}). */
@Component
public class SlackChannel implements NotificationChannel {

    private final RestClient http;
    private final NotifierProperties props;

    public SlackChannel(RestClient http, NotifierProperties props) {
        this.http = http;
        this.props = props;
    }

    @Override
    public String type() {
        return "slack";
    }

    @Override
    public void send(Notification n, String target) {
        String url = props.getSlack().getWebhooks().get(target);
        if (url == null) {
            throw DeliveryException.permanent("no slack webhook configured for " + target);
        }
        try {
            http.post().uri(url).contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("text", "*" + n.title() + "*\n" + n.body()))
                    .retrieve().toBodilessEntity();
        } catch (HttpServerErrorException | ResourceAccessException e) {
            throw DeliveryException.transientError("slack " + target + ": " + e.getMessage(), e);
        } catch (RestClientResponseException e) {
            throw DeliveryException.permanent("slack " + target + " rejected: " + e.getStatusCode());
        }
    }
}
