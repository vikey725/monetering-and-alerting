package com.chargemon.ocpp.model;

import java.time.Instant;

/** OCPP 1.6 StartTransaction CALL (transactionId arrives only in the CSMS response). */
public record StartTransaction(
        EventMeta meta,
        int connectorId,
        String idTag,
        long meterStart,
        Instant timestamp,
        Integer reservationId) implements StationMessage {
}
