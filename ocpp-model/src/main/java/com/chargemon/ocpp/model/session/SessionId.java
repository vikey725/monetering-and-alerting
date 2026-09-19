package com.chargemon.ocpp.model.session;

import com.chargemon.ocpp.model.OcppVersion;
import java.util.Objects;

/** Version-agnostic charging-session identity. */
public record SessionId(OcppVersion version, String stationId, String transactionId) {

    public SessionId {
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(stationId, "stationId");
        Objects.requireNonNull(transactionId, "transactionId");
    }

    public static SessionId of(OcppVersion version, String stationId, String transactionId) {
        return new SessionId(version, stationId, transactionId);
    }

    public String canonical() {
        return version.wire() + ":" + stationId + ":" + transactionId;
    }
}
