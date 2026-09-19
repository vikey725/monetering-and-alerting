package com.chargemon.notifier.channel;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.chargemon.alert.Severity;
import com.chargemon.common.json.JsonMapperFactory;
import com.chargemon.notifier.Fixtures;
import com.chargemon.notifier.channel.pagerduty.PagerDutyChannel;
import com.chargemon.notifier.channel.slack.SlackChannel;
import com.chargemon.notifier.channel.webhook.WebhookChannel;
import com.chargemon.notifier.config.NotifierProperties;
import com.chargemon.notifier.template.MessageFormatter;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

class HttpChannelsTest {

    static WireMockServer wm = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    static NotifierProperties props = new NotifierProperties();
    static RestClient http = RestClient.builder()
            .requestFactory(new org.springframework.http.client.SimpleClientHttpRequestFactory()).build();
    static Notification n = new MessageFormatter().format(Fixtures.opened(Severity.CRITICAL, List.of()));

    @BeforeAll
    static void start() {
        wm.start();
        props.getSlack().setWebhooks(Map.of("#ops", wm.baseUrl() + "/slack"));
        props.getPagerduty().setEventsUrl(wm.baseUrl() + "/pd");
        props.getPagerduty().setRoutingKeys(Map.of("svc", "KEY"));
        props.getWebhook().setHmacSecret("s3cret");
    }

    @AfterAll
    static void stop() {
        wm.stop();
    }

    @Test
    void slackPostsTextAndClassifiesFailures() {
        wm.stubFor(post(urlEqualTo("/slack")).willReturn(aResponse().withStatus(200)));
        new SlackChannel(http, props).send(n, "#ops");
        wm.verify(postRequestedFor(urlEqualTo("/slack")).withRequestBody(matching(".*OPEN: station ST-1.*")));

        wm.stubFor(post(urlEqualTo("/slack")).willReturn(aResponse().withStatus(503)));
        assertThatThrownBy(() -> new SlackChannel(http, props).send(n, "#ops"))
                .isInstanceOf(DeliveryException.class).matches(e -> ((DeliveryException) e).isTransient());
        wm.stubFor(post(urlEqualTo("/slack")).willReturn(aResponse().withStatus(400)));
        assertThatThrownBy(() -> new SlackChannel(http, props).send(n, "#ops"))
                .isInstanceOf(DeliveryException.class).matches(e -> !((DeliveryException) e).isTransient());
        assertThatThrownBy(() -> new SlackChannel(http, props).send(n, "#unknown"))
                .isInstanceOf(DeliveryException.class).matches(e -> !((DeliveryException) e).isTransient());
    }

    @Test
    void pagerDutyUsesAlertIdAsDedupKey() {
        wm.stubFor(post(urlEqualTo("/pd")).willReturn(aResponse().withStatus(202)));
        new PagerDutyChannel(http, props).send(n, "svc");
        wm.verify(postRequestedFor(urlEqualTo("/pd"))
                .withRequestBody(matching(".*\"dedup_key\":\"" + n.event().alertId() + "\".*"))
                .withRequestBody(matching(".*\"event_action\":\"trigger\".*"))
                .withRequestBody(matching(".*\"severity\":\"critical\".*")));
    }

    @Test
    void webhookSignsBody() throws Exception {
        wm.stubFor(post(urlEqualTo("/hook")).willReturn(aResponse().withStatus(204)));
        new WebhookChannel(http, props, JsonMapperFactory.standard()).send(n, wm.baseUrl() + "/hook");
        var requests = wm.findAll(postRequestedFor(urlEqualTo("/hook")));
        assertThat(requests).hasSize(1);
        String sig = requests.get(0).getHeader(WebhookChannel.SIGNATURE_HEADER);
        byte[] body = requests.get(0).getBody();
        assertThat(sig).isEqualTo("sha256=" + hmac(body, "s3cret"));
        assertThat(requests.get(0).getHeader(WebhookChannel.EVENT_ID_HEADER)).isEqualTo(n.event().alertEventId());
        assertThat(JsonMapperFactory.standard().readTree(body).get("alertId").asText()).isEqualTo(n.event().alertId());
    }

    private static String hmac(byte[] body, String secret) throws Exception {
        javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
        mac.init(new javax.crypto.spec.SecretKeySpec(secret.getBytes(), "HmacSHA256"));
        return java.util.HexFormat.of().formatHex(mac.doFinal(body));
    }
}
