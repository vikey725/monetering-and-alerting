package com.chargemon.ocpp.model;

import java.time.Instant;

/** BootNotification CALL joined with its CALLRESULT. */
public record BootCompleted(EventMeta meta, RegistrationStatus status, int intervalSec, Instant csmsTime,
                            String vendor, String model, String firmwareVersion) implements CorrelatedEvent {
}
