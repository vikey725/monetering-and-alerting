package com.chargemon.alert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ChannelRefTest {

    @Test
    void parsesTypeAndTargetKeepingColonsInTarget() {
        ChannelRef r = ChannelRef.parse("Webhook:https://x.example/hook?a=1");
        assertThat(r.type()).isEqualTo("webhook");
        assertThat(r.target()).isEqualTo("https://x.example/hook?a=1");
        assertThat(r.toString()).isEqualTo("webhook:https://x.example/hook?a=1");
    }

    @Test
    void rejectsMalformed() {
        assertThatThrownBy(() -> ChannelRef.parse("slack")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ChannelRef.parse("slack:")).isInstanceOf(IllegalArgumentException.class);
    }
}
