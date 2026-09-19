package com.chargemon.ocpp.model;

import java.time.Instant;
import java.util.List;

/** OCPP 1.6 StopTransaction CALL. */
public record StopTransaction(
        EventMeta meta,
        int transactionId,
        long meterStop,
        Instant timestamp,
        String reason,
        String idTag,
        List<MeterValue> transactionData) implements StationMessage {

    public com.chargemon.ocpp.model.session.SessionId sessionId() {
        return com.chargemon.ocpp.model.session.SessionId.of(meta.version(), meta.stationId(), Integer.toString(transactionId));
    }
}
