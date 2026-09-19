package com.chargemon.ocpp.codec.mapper.v16;

import com.chargemon.ocpp.codec.mapper.CorrelatedMapper;
import com.chargemon.ocpp.codec.mapper.Json;
import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.model.AuthStatus;
import com.chargemon.ocpp.model.AuthorizationResult;
import com.chargemon.ocpp.model.EventMeta;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

public final class AuthorizationResultMapper16 implements CorrelatedMapper<AuthorizationResult> {

    @Override
    public MapperKey key() {
        return V16.key("Authorize");
    }

    @Override
    public String producedAction() {
        return "AuthorizationResult";
    }

    @Override
    public AuthorizationResult map(EventMeta callMeta, JsonNode call, JsonNode result, Instant at) {
        return new AuthorizationResult(correlatedMeta(callMeta, at), Json.text(call, "idTag"),
                AuthStatus.parse(Json.text(Json.obj(result, "idTagInfo"), "status")));
    }
}
