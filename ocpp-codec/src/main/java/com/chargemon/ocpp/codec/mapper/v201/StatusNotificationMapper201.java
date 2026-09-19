package com.chargemon.ocpp.codec.mapper.v201;

import com.chargemon.ocpp.codec.mapper.Json;
import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.codec.mapper.OcppActionMapper;
import com.chargemon.ocpp.model.ConnectorStatus;
import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.StatusNotification;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

public final class StatusNotificationMapper201 implements OcppActionMapper<StatusNotification> {

    @Override
    public MapperKey key() {
        return V201.key("StatusNotification");
    }

    @Override
    public StatusNotification map(EventMeta meta, JsonNode p) {
        Instant ts = Json.instant(p, "timestamp");
        String raw = Json.requireText(p, "connectorStatus");
        return new StatusNotification(ts == null ? meta : meta.withEventTime(ts),
                Json.intOr(p, "evseId", 0), Json.intOr(p, "connectorId", 0),
                ConnectorStatus.parse(raw), raw, null, null, ts);
    }
}
