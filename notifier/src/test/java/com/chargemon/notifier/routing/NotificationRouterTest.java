package com.chargemon.notifier.routing;

import static org.assertj.core.api.Assertions.assertThat;

import com.chargemon.alert.ChannelRef;
import com.chargemon.alert.Severity;
import com.chargemon.notifier.Fixtures;
import com.chargemon.notifier.config.NotifierProperties;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class NotificationRouterTest {

    @Test
    void ruleChannelsWinOverFallback() {
        NotifierProperties p = new NotifierProperties();
        p.setFallback(Map.of("CRITICAL", "pagerduty:svc,slack:#ops", "DEFAULT", "email:ops@x.io"));
        NotificationRouter r = new NotificationRouter(p);
        assertThat(r.route(Fixtures.opened(Severity.CRITICAL, List.of(ChannelRef.parse("webhook:https://h")))))
                .containsExactly(ChannelRef.parse("webhook:https://h"));
        assertThat(r.route(Fixtures.opened(Severity.CRITICAL, List.of())))
                .containsExactly(ChannelRef.parse("pagerduty:svc"), ChannelRef.parse("slack:#ops"));
        assertThat(r.route(Fixtures.opened(Severity.LOW, List.of()))).containsExactly(ChannelRef.parse("email:ops@x.io"));
    }
}
