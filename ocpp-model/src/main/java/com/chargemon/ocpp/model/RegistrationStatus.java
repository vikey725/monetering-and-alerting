package com.chargemon.ocpp.model;

import java.util.Locale;

public enum RegistrationStatus {
    ACCEPTED, PENDING, REJECTED, UNKNOWN;

    public static RegistrationStatus parse(String raw) {
        if (raw == null) {
            return UNKNOWN;
        }
        return switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "accepted" -> ACCEPTED;
            case "pending" -> PENDING;
            case "rejected" -> REJECTED;
            default -> UNKNOWN;
        };
    }
}
