package com.chargemon.ocpp.model;

import java.time.Instant;

/**
 * 1.6: connectorId/status/errorCode. 2.0.1: evseId/connectorId/connectorStatus.
 * {@code rawStatus} keeps the exact wire value for rules that care about the version-specific vocabulary.
 */
public record StatusNotification(
        EventMeta meta,
        int evseId,
        int connectorId,
        ConnectorStatus status,
        String rawStatus,
        String errorCode,
        String vendorErrorCode,
        Instant timestamp) implements StationMessage {
}
