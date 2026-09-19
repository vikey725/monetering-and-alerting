package com.chargemon.ocpp.codec.mapper.v201;

import com.chargemon.ocpp.codec.mapper.CorrelatedMapper;
import com.chargemon.ocpp.codec.mapper.Json;
import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.model.BootCompleted;
import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.RegistrationStatus;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

public final class BootCompletedMapper201 implements CorrelatedMapper<BootCompleted> {

    @Override
    public MapperKey key() {
        return V201.key("BootNotification");
    }

    @Override
    public String producedAction() {
        return "BootCompleted";
    }

    @Override
    public BootCompleted map(EventMeta callMeta, JsonNode call, JsonNode result, Instant at) {
        JsonNode cs = Json.obj(call, "chargingStation");
        return new BootCompleted(correlatedMeta(callMeta, at),
                RegistrationStatus.parse(Json.text(result, "status")),
                Json.intOr(result, "interval", 0),
                Json.instant(result, "currentTime"),
                Json.text(cs, "vendorName"),
                Json.text(cs, "model"),
                Json.text(cs, "firmwareVersion"));
    }
}
