package com.chargemon.ocpp.model;

/**
 * A CALL answered with CALLERROR. {@code requestDirection} tells whether the
 * station or the CSMS issued the failing request.
 */
public record CallFailed(
        EventMeta meta,
        String requestAction,
        String errorCode,
        String description,
        String detailsJson,
        Direction requestDirection) implements CorrelatedEvent {
}
