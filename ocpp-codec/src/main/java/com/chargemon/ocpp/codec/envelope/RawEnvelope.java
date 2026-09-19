package com.chargemon.ocpp.codec.envelope;

import com.chargemon.ocpp.model.Direction;
import com.chargemon.ocpp.model.OcppVersion;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

/**
 * One record from {@code common-broker}: transport metadata plus the untouched OCPP-J frame.
 *
 * @param frame the JSON array {@code [2, id, action, payload]} etc.
 * @param sourceRef topic-partition-offset for tracing
 */
public record RawEnvelope(
        String stationId,
        OcppVersion ocppVersion,
        Direction direction,
        Instant receivedAt,
        JsonNode frame,
        String sourceRef) {
}
