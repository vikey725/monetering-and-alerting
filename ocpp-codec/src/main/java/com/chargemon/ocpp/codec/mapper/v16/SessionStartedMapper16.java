package com.chargemon.ocpp.codec.mapper.v16;

import com.chargemon.ocpp.codec.mapper.CorrelatedMapper;
import com.chargemon.ocpp.codec.mapper.Json;
import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.model.AuthStatus;
import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.SessionStarted;
import com.chargemon.ocpp.model.session.SessionId;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

/** StartTransaction CALL + CSMS response -> canonical session start with the CSMS-assigned transactionId. */
public final class SessionStartedMapper16 implements CorrelatedMapper<SessionStarted> {

    @Override
    public MapperKey key() {
        return V16.key("StartTransaction");
    }

    @Override
    public String producedAction() {
        return "SessionStarted";
    }

    @Override
    public SessionStarted map(EventMeta callMeta, JsonNode call, JsonNode result, Instant at) {
        long txId = Json.requireLong(result, "transactionId");
        JsonNode idTagInfo = Json.obj(result, "idTagInfo");
        Instant startedAt = Json.instant(call, "timestamp");
        return new SessionStarted(correlatedMeta(callMeta, at),
                SessionId.of(callMeta.version(), callMeta.stationId(), Long.toString(txId)),
                Json.intOr(call, "connectorId", 0),
                Json.text(call, "idTag"),
                Json.requireLong(call, "meterStart"),
                startedAt == null ? callMeta.receivedAt() : startedAt,
                AuthStatus.parse(Json.text(idTagInfo, "status")));
    }
}
