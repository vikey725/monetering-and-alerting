package com.chargemon.ocpp.codec.mapper.v16;

import com.chargemon.ocpp.codec.mapper.Json;
import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.codec.mapper.OcppActionMapper;
import com.chargemon.ocpp.model.ConnectorStatus;
import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.StatusNotification;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

public final class StatusNotificationMapper16 implements OcppActionMapper<StatusNotification> {

    @Override
    public MapperKey key() {
        return V16.key("StatusNotification");
    }

    @Override
    public StatusNotification map(EventMeta meta, JsonNode p) {
        Instant ts = Json.instant(p, "timestamp");
        String raw = Json.requireText(p, "status");
        int connectorId = Json.intOr(p, "connectorId", 0);
        return new StatusNotification(ts == null ? meta : meta.withEventTime(ts),
                connectorId, connectorId, ConnectorStatus.parse(raw), raw,
                Json.text(p, "errorCode"), Json.text(p, "vendorErrorCode"), ts);
    }
}
