package com.chargemon.ocpp.codec.mapper.v16;

import com.chargemon.ocpp.codec.mapper.CorrelatedMapper;
import com.chargemon.ocpp.codec.mapper.Json;
import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.model.BootCompleted;
import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.RegistrationStatus;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

public final class BootCompletedMapper16 implements CorrelatedMapper<BootCompleted> {

    @Override
    public MapperKey key() {
        return V16.key("BootNotification");
    }

    @Override
    public String producedAction() {
        return "BootCompleted";
    }

    @Override
    public BootCompleted map(EventMeta callMeta, JsonNode call, JsonNode result, Instant at) {
        return new BootCompleted(correlatedMeta(callMeta, at),
                RegistrationStatus.parse(Json.text(result, "status")),
                Json.intOr(result, "interval", 0),
                Json.instant(result, "currentTime"),
                Json.text(call, "chargePointVendor"),
                Json.text(call, "chargePointModel"),
                Json.text(call, "firmwareVersion"));
    }
}
