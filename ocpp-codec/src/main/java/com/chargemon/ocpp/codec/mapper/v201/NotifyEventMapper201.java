package com.chargemon.ocpp.codec.mapper.v201;

import com.chargemon.ocpp.codec.mapper.Json;
import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.codec.mapper.OcppActionMapper;
import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.NotifyEvent;
import com.chargemon.ocpp.model.NotifyEvent.EventDatum;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class NotifyEventMapper201 implements OcppActionMapper<NotifyEvent> {

    @Override
    public MapperKey key() {
        return V201.key("NotifyEvent");
    }

    @Override
    public NotifyEvent map(EventMeta meta, JsonNode p) {
        Instant generated = Json.instant(p, "generatedAt");
        List<EventDatum> data = new ArrayList<>();
        JsonNode arr = p.get("eventData");
        if (arr != null && arr.isArray()) {
            for (JsonNode d : arr) {
                JsonNode component = Json.obj(d, "component");
                JsonNode variable = Json.obj(d, "variable");
                JsonNode evse = component == null ? null : Json.obj(component, "evse");
                data.add(new EventDatum(
                        Json.intOr(d, "eventId", 0),
                        Json.instant(d, "timestamp"),
                        Json.text(d, "trigger"),
                        Json.text(d, "actualValue"),
                        Json.text(d, "eventNotificationType"),
                        Json.text(component, "name"),
                        evse == null ? null : Json.integer(evse, "id"),
                        Json.text(variable, "name"),
                        Json.text(d, "techCode"),
                        Json.text(d, "techInfo"),
                        Json.bool(d, "cleared")));
            }
        }
        return new NotifyEvent(generated == null ? meta : meta.withEventTime(generated),
                Json.intOr(p, "seqNo", 0), generated, Boolean.TRUE.equals(Json.bool(p, "tbc")), List.copyOf(data));
    }
}
