package com.chargemon.notifier.template;

import static org.assertj.core.api.Assertions.assertThat;

import com.chargemon.alert.Severity;
import com.chargemon.notifier.Fixtures;
import com.chargemon.notifier.channel.Notification;
import java.util.List;
import org.junit.jupiter.api.Test;

class MessageFormatterTest {

    @Test
    void rendersTitleAndBody() {
        Notification n = new MessageFormatter().format(Fixtures.opened(Severity.HIGH, List.of()));
        assertThat(n.title()).isEqualTo("[HIGH] OPEN: station ST-1");
        assertThat(n.body()).contains("Rule: No heartbeat (ABSENCE)").contains("expectedAction = Heartbeat").contains("Groups: site:1");
    }
}
