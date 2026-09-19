package com.chargemon.ocpp.codec.mapper.v16;

import com.chargemon.ocpp.codec.mapper.Json;
import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.codec.mapper.OcppActionMapper;
import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.StartTransaction;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

public final class StartTransactionMapper16 implements OcppActionMapper<StartTransaction> {

    @Override
    public MapperKey key() {
        return V16.key("StartTransaction");
    }

    @Override
    public StartTransaction map(EventMeta meta, JsonNode p) {
        Instant ts = Json.instant(p, "timestamp");
        return new StartTransaction(ts == null ? meta : meta.withEventTime(ts),
                Json.intOr(p, "connectorId", 0),
                Json.requireText(p, "idTag"),
                Json.requireLong(p, "meterStart"),
                ts,
                Json.integer(p, "reservationId"));
    }
}
