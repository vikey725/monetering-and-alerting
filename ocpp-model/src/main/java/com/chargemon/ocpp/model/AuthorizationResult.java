package com.chargemon.ocpp.model;

/** Authorize CALL joined with its CALLRESULT. */
public record AuthorizationResult(EventMeta meta, String idToken, AuthStatus status) implements CorrelatedEvent {
}
