package com.chargemon.ocpp.model;

import java.util.Locale;

/** Union of 1.6 AuthorizationStatus and 2.0.1 AuthorizationStatusEnumType. */
public enum AuthStatus {
    ACCEPTED, BLOCKED, EXPIRED, INVALID, CONCURRENT_TX, NO_CREDIT, NOT_ALLOWED_TYPE_EVSE, NOT_AT_THIS_LOCATION,
    NOT_AT_THIS_TIME, UNKNOWN;

    public static AuthStatus parse(String raw) {
        if (raw == null) {
            return UNKNOWN;
        }
        return switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "accepted" -> ACCEPTED;
            case "blocked" -> BLOCKED;
            case "expired" -> EXPIRED;
            case "invalid" -> INVALID;
            case "concurrenttx" -> CONCURRENT_TX;
            case "nocredit" -> NO_CREDIT;
            case "notallowedtypeevse" -> NOT_ALLOWED_TYPE_EVSE;
            case "notatthislocation" -> NOT_AT_THIS_LOCATION;
            case "notatthistime" -> NOT_AT_THIS_TIME;
            default -> UNKNOWN;
        };
    }
}
