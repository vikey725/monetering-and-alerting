package com.chargemon.notifier;

import com.chargemon.alert.AlertEvent;
import com.chargemon.alert.AlertEventType;
import com.chargemon.alert.ChannelRef;
import com.chargemon.alert.Severity;
import com.chargemon.alert.SubjectType;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public final class Fixtures {

    private Fixtures() {
    }

    public static AlertEvent opened(Severity severity, List<ChannelRef> channels) {
        Instant t = Instant.parse("2026-01-01T00:00:00Z");
        return new AlertEvent(UUID.randomUUID().toString(), UUID.randomUUID().toString(), "r|STATION|ST-1", 1,
                AlertEventType.OPENED, "r", 1, "No heartbeat", "ABSENCE", severity, channels, SubjectType.STATION, "ST-1",
                Set.of("site:1"), t, t.plusSeconds(60), null, null, Map.of("expectedAction", "Heartbeat"), t.plusSeconds(60), 1);
    }
}
