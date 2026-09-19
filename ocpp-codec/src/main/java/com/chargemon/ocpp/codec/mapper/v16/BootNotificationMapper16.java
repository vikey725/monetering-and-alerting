package com.chargemon.ocpp.codec.mapper.v16;

import com.chargemon.ocpp.codec.mapper.Json;
import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.codec.mapper.OcppActionMapper;
import com.chargemon.ocpp.model.BootNotification;
import com.chargemon.ocpp.model.EventMeta;
import com.fasterxml.jackson.databind.JsonNode;

public final class BootNotificationMapper16 implements OcppActionMapper<BootNotification> {

    @Override
    public MapperKey key() {
        return V16.key("BootNotification");
    }

    @Override
    public BootNotification map(EventMeta meta, JsonNode p) {
        return new BootNotification(meta,
                Json.text(p, "chargePointVendor"),
                Json.text(p, "chargePointModel"),
                Json.text(p, "chargePointSerialNumber"),
                Json.text(p, "firmwareVersion"),
                null);
    }
}
