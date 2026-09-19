package com.chargemon.ocpp.model;

import java.time.Instant;

/** A CALL that received no CALLRESULT / CALLERROR within the correlation timeout. */
public record CallTimedOut(EventMeta meta, String requestAction, Instant sentAt, Direction requestDirection)
        implements CorrelatedEvent {
}
