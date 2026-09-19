package com.chargemon.ocpp.codec.mapper.v201;

import com.chargemon.ocpp.codec.mapper.Json;
import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.codec.mapper.OcppActionMapper;
import com.chargemon.ocpp.model.BootNotification;
import com.chargemon.ocpp.model.EventMeta;
import com.fasterxml.jackson.databind.JsonNode;

public final class BootNotificationMapper201 implements OcppActionMapper<BootNotification> {

    @Override
    public MapperKey key() {
        return V201.key("BootNotification");
    }

    @Override
    public BootNotification map(EventMeta meta, JsonNode p) {
        JsonNode cs = Json.obj(p, "chargingStation");
        return new BootNotification(meta,
                Json.text(cs, "vendorName"),
                Json.text(cs, "model"),
                Json.text(cs, "serialNumber"),
                Json.text(cs, "firmwareVersion"),
                Json.text(p, "reason"));
    }
}
