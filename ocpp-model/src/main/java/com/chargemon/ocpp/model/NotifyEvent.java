package com.chargemon.ocpp.model;

import java.time.Instant;
import java.util.List;

/** OCPP 2.0.1 NotifyEvent: device-model monitoring events. */
public record NotifyEvent(EventMeta meta, int seqNo, Instant generatedAt, boolean tbc, List<EventDatum> eventData)
        implements StationMessage {

    public record EventDatum(
            int eventId,
            Instant timestamp,
            String trigger,
            String actualValue,
            String eventNotificationType,
            String componentName,
            Integer evseId,
            String variableName,
            String techCode,
            String techInfo,
            Boolean cleared) {
    }
}
