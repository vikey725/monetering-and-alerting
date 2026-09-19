package com.chargemon.ocpp.model;

/**
 * Fallback for any action without a dedicated mapper. Still fully rule-evaluable:
 * the raw payload is exposed to the rule engine under {@code event.payload.*}.
 */
public record GenericOcppEvent(EventMeta meta, MessageType messageType, String rawPayloadJson) implements OcppEvent {
}
