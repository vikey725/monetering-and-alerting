package com.chargemon.ocpp.codec.mapper.v16;

import com.chargemon.ocpp.codec.mapper.Json;
import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.codec.mapper.MeterValueParser;
import com.chargemon.ocpp.codec.mapper.OcppActionMapper;
import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.StopTransaction;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

public final class StopTransactionMapper16 implements OcppActionMapper<StopTransaction> {

    @Override
    public MapperKey key() {
        return V16.key("StopTransaction");
    }

    @Override
    public StopTransaction map(EventMeta meta, JsonNode p) {
        Instant ts = Json.instant(p, "timestamp");
        return new StopTransaction(ts == null ? meta : meta.withEventTime(ts),
                (int) Json.requireLong(p, "transactionId"),
                Json.requireLong(p, "meterStop"),
                ts,
                Json.text(p, "reason"),
                Json.text(p, "idTag"),
                MeterValueParser.parseList(p.get("transactionData")));
    }
}
