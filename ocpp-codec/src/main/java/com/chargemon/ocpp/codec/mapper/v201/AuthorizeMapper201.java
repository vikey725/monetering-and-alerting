package com.chargemon.ocpp.codec.mapper.v201;

import com.chargemon.ocpp.codec.mapper.Json;
import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.codec.mapper.OcppActionMapper;
import com.chargemon.ocpp.model.Authorize;
import com.chargemon.ocpp.model.EventMeta;
import com.fasterxml.jackson.databind.JsonNode;

public final class AuthorizeMapper201 implements OcppActionMapper<Authorize> {

    @Override
    public MapperKey key() {
        return V201.key("Authorize");
    }

    @Override
    public Authorize map(EventMeta meta, JsonNode p) {
        JsonNode t = Json.obj(p, "idToken");
        return new Authorize(meta, Json.text(t, "idToken"), Json.text(t, "type"));
    }
}
