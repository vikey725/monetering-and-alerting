package com.chargemon.ocpp.codec.mapper.v201;

import com.chargemon.ocpp.codec.mapper.Json;
import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.codec.mapper.OcppActionMapper;
import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.SecurityEventNotification;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

public final class SecurityEventNotificationMapper201 implements OcppActionMapper<SecurityEventNotification> {

    @Override
    public MapperKey key() {
        return V201.key("SecurityEventNotification");
    }

    @Override
    public SecurityEventNotification map(EventMeta meta, JsonNode p) {
        Instant ts = Json.instant(p, "timestamp");
        return new SecurityEventNotification(ts == null ? meta : meta.withEventTime(ts),
                Json.requireText(p, "type"), ts, Json.text(p, "techInfo"));
    }
}
