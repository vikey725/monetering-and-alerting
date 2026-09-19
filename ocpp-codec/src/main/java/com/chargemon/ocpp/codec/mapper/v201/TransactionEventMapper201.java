package com.chargemon.ocpp.codec.mapper.v201;

import com.chargemon.ocpp.codec.mapper.Json;
import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.codec.mapper.MappingException;
import com.chargemon.ocpp.codec.mapper.MeterValueParser;
import com.chargemon.ocpp.codec.mapper.OcppActionMapper;
import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.TransactionEvent;
import com.chargemon.ocpp.model.TransactionEvent.EvseRef;
import com.chargemon.ocpp.model.TransactionEvent.IdToken;
import com.chargemon.ocpp.model.TransactionEvent.TransactionInfo;
import com.chargemon.ocpp.model.TransactionEvent.TxEventType;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.Locale;

public final class TransactionEventMapper201 implements OcppActionMapper<TransactionEvent> {

    @Override
    public MapperKey key() {
        return V201.key("TransactionEvent");
    }

    @Override
    public TransactionEvent map(EventMeta meta, JsonNode p) {
        Instant ts = Json.instant(p, "timestamp");
        JsonNode tx = Json.obj(p, "transactionInfo");
        if (tx == null || Json.text(tx, "transactionId") == null) {
            throw new MappingException("TransactionEvent missing transactionInfo.transactionId");
        }
        JsonNode evse = Json.obj(p, "evse");
        JsonNode idToken = Json.obj(p, "idToken");
        return new TransactionEvent(ts == null ? meta : meta.withEventTime(ts),
                parseType(Json.requireText(p, "eventType")),
                Json.intOr(p, "seqNo", 0),
                Json.text(p, "triggerReason"),
                ts,
                Boolean.TRUE.equals(Json.bool(p, "offline")),
                new TransactionInfo(Json.text(tx, "transactionId"), Json.text(tx, "chargingState"),
                        Json.longVal(tx, "timeSpentCharging"), Json.text(tx, "stoppedReason"),
                        Json.integer(tx, "remoteStartId")),
                evse == null ? null : new EvseRef(Json.intOr(evse, "id", 0), Json.integer(evse, "connectorId")),
                idToken == null ? null : new IdToken(Json.text(idToken, "idToken"), Json.text(idToken, "type")),
                MeterValueParser.parseList(p.get("meterValue")));
    }

    private static TxEventType parseType(String raw) {
        try {
            return TxEventType.valueOf(raw.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new MappingException("unknown TransactionEvent.eventType: " + raw);
        }
    }
}
