package com.chargemon.flink.energy;

import java.time.Instant;
import java.util.Set;

/** Per-session scratch state kept until the session ends. */
public record SessionTrack(Instant startedAt, Long meterStart, Long lastRegister, Set<String> groupIds) {

    public SessionTrack {
        groupIds = groupIds == null ? Set.of() : Set.copyOf(groupIds);
    }

    public SessionTrack withRegister(long register) {
        return new SessionTrack(startedAt, meterStart == null ? register : meterStart, register, groupIds);
    }
}
