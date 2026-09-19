package com.chargemon.notifier.channel.webhook;

import com.chargemon.notifier.channel.DeliveryException;
import com.chargemon.notifier.channel.Notification;
import com.chargemon.notifier.channel.NotificationChannel;
import com.chargemon.notifier.config.NotifierProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

/** Generic JSON POST of the AlertEvent; signed with HMAC-SHA256 when a secret is configured. */
@Component
public class WebhookChannel implements NotificationChannel {

    public static final String SIGNATURE_HEADER = "X-Chargemon-Signature";
    public static final String EVENT_ID_HEADER = "X-Chargemon-Event-Id";

    private final RestClient http;
    private final NotifierProperties props;
    private final ObjectMapper json;

    public WebhookChannel(RestClient http, NotifierProperties props, ObjectMapper json) {
        this.http = http;
        this.props = props;
        this.json = json;
    }

    @Override
    public String type() {
        return "webhook";
    }

    @Override
    public void send(Notification n, String target) {
        byte[] body;
        try {
            body = json.writeValueAsBytes(n.event());
        } catch (JsonProcessingException e) {
            throw DeliveryException.permanent("cannot serialize alert: " + e.getMessage());
        }
        try {
            var req = http.post().uri(target).contentType(MediaType.APPLICATION_JSON)
                    .header(EVENT_ID_HEADER, n.event().alertEventId());
            if (!props.getWebhook().getHmacSecret().isBlank()) {
                req = req.header(SIGNATURE_HEADER, "sha256=" + sign(body, props.getWebhook().getHmacSecret()));
            }
            req.body(body).retrieve().toBodilessEntity();
        } catch (HttpServerErrorException | ResourceAccessException e) {
            throw DeliveryException.transientError("webhook " + target + ": " + e.getMessage(), e);
        } catch (RestClientResponseException e) {
            throw DeliveryException.permanent("webhook " + target + " rejected: " + e.getStatusCode());
        }
    }

    static String sign(byte[] body, String secret) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(body));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
