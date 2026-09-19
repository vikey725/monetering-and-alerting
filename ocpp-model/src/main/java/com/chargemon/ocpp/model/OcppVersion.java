package com.chargemon.ocpp.model;

import java.util.Locale;

/** Supported OCPP protocol versions. Adding a version = new constant + new mapper package. */
public enum OcppVersion {
    V16("1.6"),
    V201("2.0.1");

    private final String wire;

    OcppVersion(String wire) {
        this.wire = wire;
    }

    public String wire() {
        return wire;
    }

    /** Accepts "1.6", "ocpp1.6", "OCPP2.0.1", "2.0.1", "V16", "V201". */
    public static OcppVersion parse(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("ocppVersion is null");
        }
        String s = raw.trim().toLowerCase(Locale.ROOT).replace("ocpp", "").replace("v", "");
        return switch (s) {
            case "1.6", "16" -> V16;
            case "2.0.1", "201", "2.0", "20" -> V201;
            default -> throw new IllegalArgumentException("Unsupported OCPP version: " + raw);
        };
    }
}
