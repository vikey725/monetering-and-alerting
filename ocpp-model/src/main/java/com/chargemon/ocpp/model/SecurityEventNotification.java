package com.chargemon.ocpp.model;

import java.time.Instant;

public record SecurityEventNotification(EventMeta meta, String type, Instant timestamp, String techInfo)
        implements StationMessage {
}
