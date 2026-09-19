package com.chargemon.ocpp.model;

import java.util.Locale;

public enum Direction {
    STATION_TO_CSMS,
    CSMS_TO_STATION;

    public static Direction parse(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("direction is null");
        }
        String s = raw.trim().toUpperCase(Locale.ROOT).replace('-', '_');
        return switch (s) {
            case "STATION_TO_CSMS", "FROM_STATION", "INBOUND", "IN", "CP_TO_CS", "STATION" -> STATION_TO_CSMS;
            case "CSMS_TO_STATION", "TO_STATION", "OUTBOUND", "OUT", "CS_TO_CP", "CSMS" -> CSMS_TO_STATION;
            default -> throw new IllegalArgumentException("Unsupported direction: " + raw);
        };
    }

    public Direction opposite() {
        return this == STATION_TO_CSMS ? CSMS_TO_STATION : STATION_TO_CSMS;
    }
}
