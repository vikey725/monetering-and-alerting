package com.chargemon.notifier.config;

import jakarta.validation.constraints.NotBlank;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "notifier")
public class NotifierProperties {

    @NotBlank
    private String topic = "alerts";
    private Retry retry = new Retry();
    private Map<String, String> fallback = new LinkedHashMap<>();
    private Slack slack = new Slack();
    private PagerDuty pagerduty = new PagerDuty();
    private Email email = new Email();
    private Webhook webhook = new Webhook();

    public static class Retry {
        private int attempts = 4;
        private Duration initialBackoff = Duration.ofSeconds(2);
        private double multiplier = 2.0;

        public int getAttempts() {
            return attempts;
        }

        public void setAttempts(int attempts) {
            this.attempts = attempts;
        }

        public Duration getInitialBackoff() {
            return initialBackoff;
        }

        public void setInitialBackoff(Duration initialBackoff) {
            this.initialBackoff = initialBackoff;
        }

        public double getMultiplier() {
            return multiplier;
        }

        public void setMultiplier(double multiplier) {
            this.multiplier = multiplier;
        }
    }

    public static class Slack {
        private Map<String, String> webhooks = new LinkedHashMap<>();

        public Map<String, String> getWebhooks() {
            return webhooks;
        }

        public void setWebhooks(Map<String, String> webhooks) {
            this.webhooks = webhooks;
        }
    }

    public static class PagerDuty {
        private String eventsUrl = "https://events.pagerduty.com/v2/enqueue";
        private Map<String, String> routingKeys = new LinkedHashMap<>();

        public String getEventsUrl() {
            return eventsUrl;
        }

        public void setEventsUrl(String eventsUrl) {
            this.eventsUrl = eventsUrl;
        }

        public Map<String, String> getRoutingKeys() {
            return routingKeys;
        }

        public void setRoutingKeys(Map<String, String> routingKeys) {
            this.routingKeys = routingKeys;
        }
    }

    public static class Email {
        private String from = "alerts@chargemon.local";

        public String getFrom() {
            return from;
        }

        public void setFrom(String from) {
            this.from = from;
        }
    }

    public static class Webhook {
        private String hmacSecret = "";
        private Duration timeout = Duration.ofSeconds(5);

        public String getHmacSecret() {
            return hmacSecret;
        }

        public void setHmacSecret(String hmacSecret) {
            this.hmacSecret = hmacSecret;
        }

        public Duration getTimeout() {
            return timeout;
        }

        public void setTimeout(Duration timeout) {
            this.timeout = timeout;
        }
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Retry getRetry() {
        return retry;
    }

    public void setRetry(Retry retry) {
        this.retry = retry;
    }

    public Map<String, String> getFallback() {
        return fallback;
    }

    public void setFallback(Map<String, String> fallback) {
        this.fallback = fallback;
    }

    public Slack getSlack() {
        return slack;
    }

    public void setSlack(Slack slack) {
        this.slack = slack;
    }

    public PagerDuty getPagerduty() {
        return pagerduty;
    }

    public void setPagerduty(PagerDuty pagerduty) {
        this.pagerduty = pagerduty;
    }

    public Email getEmail() {
        return email;
    }

    public void setEmail(Email email) {
        this.email = email;
    }

    public Webhook getWebhook() {
        return webhook;
    }

    public void setWebhook(Webhook webhook) {
        this.webhook = webhook;
    }
}
