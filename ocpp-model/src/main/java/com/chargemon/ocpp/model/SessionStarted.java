package com.chargemon.ocpp.model;

import com.chargemon.ocpp.model.session.SessionId;
import java.time.Instant;

/** 1.6 StartTransaction CALL joined with the CSMS response carrying the transactionId. */
public record SessionStarted(
        EventMeta meta,
        SessionId sessionId,
        int connectorId,
        String idTag,
        long meterStart,
        Instant startedAt,
        AuthStatus idTagStatus) implements CorrelatedEvent {
}
