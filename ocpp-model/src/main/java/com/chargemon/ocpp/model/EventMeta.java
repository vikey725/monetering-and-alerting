package com.chargemon.ocpp.model;

import java.time.Instant;
import java.util.Objects;

/**
 * Envelope-level metadata shared by every canonical event. Built once by the
 * envelope parser; mappers never construct it themselves.
 *
 * @param eventTime payload timestamp when the action carries one, else {@code receivedAt}
 * @param sourceRef opaque origin pointer (topic-partition-offset) for tracing
 */
public record EventMeta(
        String stationId,
        OcppVersion version,
        Direction direction,
        String uniqueId,
        String action,
        Instant receivedAt,
        Instant eventTime,
        String sourceRef) {

    public EventMeta {
        Objects.requireNonNull(stationId, "stationId");
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(direction, "direction");
        Objects.requireNonNull(receivedAt, "receivedAt");
        if (eventTime == null) {
            eventTime = receivedAt;
        }
    }

    public EventMeta withAction(String newAction) {
        return new EventMeta(stationId, version, direction, uniqueId, newAction, receivedAt, eventTime, sourceRef);
    }

    public EventMeta withEventTime(Instant newEventTime) {
        return new EventMeta(stationId, version, direction, uniqueId, action, receivedAt, newEventTime, sourceRef);
    }
}
