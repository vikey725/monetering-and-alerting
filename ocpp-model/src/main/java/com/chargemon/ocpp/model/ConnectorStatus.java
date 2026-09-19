package com.chargemon.ocpp.model;

import java.util.Locale;

/** Union of OCPP 1.6 ChargePointStatus and 2.0.1 ConnectorStatusEnumType. */
public enum ConnectorStatus {
    AVAILABLE, PREPARING, CHARGING, SUSPENDED_EVSE, SUSPENDED_EV, FINISHING, RESERVED, UNAVAILABLE, FAULTED,
    OCCUPIED, UNKNOWN;

    public static ConnectorStatus parse(String raw) {
        if (raw == null) {
            return UNKNOWN;
        }
        return switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "available" -> AVAILABLE;
            case "preparing" -> PREPARING;
            case "charging" -> CHARGING;
            case "suspendedevse" -> SUSPENDED_EVSE;
            case "suspendedev" -> SUSPENDED_EV;
            case "finishing" -> FINISHING;
            case "reserved" -> RESERVED;
            case "unavailable" -> UNAVAILABLE;
            case "faulted" -> FAULTED;
            case "occupied" -> OCCUPIED;
            default -> UNKNOWN;
        };
    }
}
