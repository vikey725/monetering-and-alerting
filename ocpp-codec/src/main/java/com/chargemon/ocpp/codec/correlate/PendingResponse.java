package com.chargemon.ocpp.codec.correlate;

import com.chargemon.ocpp.model.Direction;
import java.time.Instant;

/**
 * A CALLRESULT / CALLERROR that arrived before its CALL (possible when both
 * directions are on different Kafka partitions). Held briefly until the CALL shows up.
 *
 * @param errorCode null for CALLRESULT
 */
public record PendingResponse(
        Direction direction,
        String uniqueId,
        String payloadJson,
        String errorCode,
        String errorDescription,
        String errorDetailsJson,
        Instant receivedAt) {

    public boolean isError() {
        return errorCode != null;
    }

    /** Key under the direction of the CALL it answers. */
    public String callKey() {
        return PendingCall.key(direction.opposite(), uniqueId);
    }
}
