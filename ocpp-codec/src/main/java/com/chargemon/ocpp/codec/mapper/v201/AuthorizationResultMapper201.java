package com.chargemon.ocpp.codec.mapper.v201;

import com.chargemon.ocpp.codec.mapper.CorrelatedMapper;
import com.chargemon.ocpp.codec.mapper.Json;
import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.model.AuthStatus;
import com.chargemon.ocpp.model.AuthorizationResult;
import com.chargemon.ocpp.model.EventMeta;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

public final class AuthorizationResultMapper201 implements CorrelatedMapper<AuthorizationResult> {

    @Override
    public MapperKey key() {
        return V201.key("Authorize");
    }

    @Override
    public String producedAction() {
        return "AuthorizationResult";
    }

    @Override
    public AuthorizationResult map(EventMeta callMeta, JsonNode call, JsonNode result, Instant at) {
        return new AuthorizationResult(correlatedMeta(callMeta, at),
                Json.text(Json.obj(call, "idToken"), "idToken"),
                AuthStatus.parse(Json.text(Json.obj(result, "idTokenInfo"), "status")));
    }
}
