package com.chargemon.ocpp.codec.mapper;

import com.chargemon.ocpp.model.CorrelatedEvent;
import com.chargemon.ocpp.model.EventMeta;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

/**
 * Joins a CALL with its CALLRESULT into a synthesized event. Keyed by the
 * request's (version, action).
 */
public interface CorrelatedMapper<E extends CorrelatedEvent> {

    MapperKey key();

    /** Synthetic action name reported on the produced event (e.g. {@code BootCompleted}). */
    String producedAction();

    E map(EventMeta callMeta, JsonNode callPayload, JsonNode resultPayload, Instant resultReceivedAt) throws MappingException;

    /** Meta for the synthesized event: same request identity, event time = response arrival. */
    default EventMeta correlatedMeta(EventMeta callMeta, Instant resultReceivedAt) {
        return new EventMeta(callMeta.stationId(), callMeta.version(), callMeta.direction(), callMeta.uniqueId(),
                producedAction(), resultReceivedAt, resultReceivedAt, callMeta.sourceRef());
    }
}
