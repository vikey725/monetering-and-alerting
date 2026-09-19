package com.chargemon.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.chargemon.common.id.Ids;
import com.chargemon.common.time.Durations;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class IdsAndDurationsTest {

    @Test
    void uuidV7IsVersion7AndTimeOrdered() {
        UUID a = Ids.uuidV7(1_000L);
        UUID b = Ids.uuidV7(2_000L);
        assertThat(a.version()).isEqualTo(7);
        assertThat(a.toString()).isLessThan(b.toString());
    }

    @Test
    void parsesIsoDurations() {
        assertThat(Durations.parse("PT10M")).isEqualTo(Duration.ofMinutes(10));
        assertThat(Durations.parseOptional(" ")).isEmpty();
        assertThatThrownBy(() -> Durations.parse("10 minutes")).isInstanceOf(IllegalArgumentException.class);
    }
}
