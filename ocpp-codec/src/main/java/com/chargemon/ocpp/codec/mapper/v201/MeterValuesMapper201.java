package com.chargemon.ocpp.codec.mapper.v201;

import com.chargemon.ocpp.codec.mapper.Json;
import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.codec.mapper.MeterValueParser;
import com.chargemon.ocpp.codec.mapper.OcppActionMapper;
import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.MeterValues;
import com.fasterxml.jackson.databind.JsonNode;

public final class MeterValuesMapper201 implements OcppActionMapper<MeterValues> {

    @Override
    public MapperKey key() {
        return V201.key("MeterValues");
    }

    @Override
    public MeterValues map(EventMeta meta, JsonNode p) {
        return new MeterValues(meta, Json.intOr(p, "evseId", 0), null, MeterValueParser.parseList(p.get("meterValue")));
    }
}
