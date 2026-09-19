package com.chargemon.ocpp.codec.correlate;

import com.chargemon.ocpp.model.Direction;
import com.chargemon.ocpp.model.OcppVersion;
import java.time.Instant;

/**
 * A CALL awaiting its response. Stored (per station) in keyed state by the
 * correlation operator; payload kept as JSON text so it serializes trivially.
 */
public record PendingCall(
        String stationId,
        OcppVersion version,
        Direction direction,
        String uniqueId,
        String action,
        String payloadJson,
        Instant sentAt,
        String sourceRef) {

    /** State key: direction + uniqueId, since both sides may reuse ids independently. */
    public static String key(Direction direction, String uniqueId) {
        return direction.name() + "|" + uniqueId;
    }

    public String key() {
        return key(direction, uniqueId);
    }
}
