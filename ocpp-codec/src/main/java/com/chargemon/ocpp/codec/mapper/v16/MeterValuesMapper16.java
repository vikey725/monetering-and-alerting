package com.chargemon.ocpp.codec.mapper.v16;

import com.chargemon.ocpp.codec.mapper.Json;
import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.codec.mapper.MeterValueParser;
import com.chargemon.ocpp.codec.mapper.OcppActionMapper;
import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.MeterValues;
import com.fasterxml.jackson.databind.JsonNode;

public final class MeterValuesMapper16 implements OcppActionMapper<MeterValues> {

    @Override
    public MapperKey key() {
        return V16.key("MeterValues");
    }

    @Override
    public MeterValues map(EventMeta meta, JsonNode p) {
        return new MeterValues(meta, Json.intOr(p, "connectorId", 0), Json.integer(p, "transactionId"),
                MeterValueParser.parseList(p.get("meterValue")));
    }
}
