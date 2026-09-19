package com.chargemon.ocpp.model;

import com.chargemon.ocpp.model.session.SessionId;
import java.time.Instant;
import java.util.List;

/** OCPP 2.0.1 TransactionEvent (Started / Updated / Ended). */
public record TransactionEvent(
        EventMeta meta,
        TxEventType eventType,
        int seqNo,
        String triggerReason,
        Instant timestamp,
        boolean offline,
        TransactionInfo transactionInfo,
        EvseRef evse,
        IdToken idToken,
        List<MeterValue> meterValues) implements StationMessage {

    public enum TxEventType { STARTED, UPDATED, ENDED }

    public record TransactionInfo(String transactionId, String chargingState, Long timeSpentCharging, String stoppedReason,
                                  Integer remoteStartId) {
    }

    public record EvseRef(int id, Integer connectorId) {
    }

    public record IdToken(String idToken, String type) {
    }

    public SessionId sessionId() {
        return SessionId.of(meta.version(), meta.stationId(), transactionInfo.transactionId());
    }
}
