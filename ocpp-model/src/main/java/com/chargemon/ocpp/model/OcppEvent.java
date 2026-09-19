package com.chargemon.ocpp.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Root of the canonical event model. Every OCPP message, from any protocol
 * version, is normalized into one of these before the rule engine sees it.
 *
 * <p>The {@code type} discriminator equals the OCPP action name (or the
 * synthetic name for correlated events) so rules can filter on it directly.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = BootNotification.class, name = "BootNotification"),
    @JsonSubTypes.Type(value = Heartbeat.class, name = "Heartbeat"),
    @JsonSubTypes.Type(value = StatusNotification.class, name = "StatusNotification"),
    @JsonSubTypes.Type(value = NotifyEvent.class, name = "NotifyEvent"),
    @JsonSubTypes.Type(value = SecurityEventNotification.class, name = "SecurityEventNotification"),
    @JsonSubTypes.Type(value = StartTransaction.class, name = "StartTransaction"),
    @JsonSubTypes.Type(value = StopTransaction.class, name = "StopTransaction"),
    @JsonSubTypes.Type(value = TransactionEvent.class, name = "TransactionEvent"),
    @JsonSubTypes.Type(value = MeterValues.class, name = "MeterValues"),
    @JsonSubTypes.Type(value = Authorize.class, name = "Authorize"),
    @JsonSubTypes.Type(value = BootCompleted.class, name = "BootCompleted"),
    @JsonSubTypes.Type(value = SessionStarted.class, name = "SessionStarted"),
    @JsonSubTypes.Type(value = AuthorizationResult.class, name = "AuthorizationResult"),
    @JsonSubTypes.Type(value = CallFailed.class, name = "CallFailed"),
    @JsonSubTypes.Type(value = CallTimedOut.class, name = "CallTimedOut"),
    @JsonSubTypes.Type(value = GenericOcppEvent.class, name = "Generic"),
})
public sealed interface OcppEvent permits StationMessage, CorrelatedEvent, GenericOcppEvent {

    EventMeta meta();

    default String stationId() {
        return meta().stationId();
    }

    /** Rule-facing action name; equals the OCPP action or the synthetic correlated name. */
    default String action() {
        return meta().action();
    }
}
