package com.chargemon.ocpp.codec.mapper.v16;

import com.chargemon.ocpp.codec.mapper.Json;
import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.codec.mapper.OcppActionMapper;
import com.chargemon.ocpp.model.Authorize;
import com.chargemon.ocpp.model.EventMeta;
import com.fasterxml.jackson.databind.JsonNode;

public final class AuthorizeMapper16 implements OcppActionMapper<Authorize> {

    @Override
    public MapperKey key() {
        return V16.key("Authorize");
    }

    @Override
    public Authorize map(EventMeta meta, JsonNode p) {
        return new Authorize(meta, Json.requireText(p, "idTag"), "ISO14443");
    }
}
