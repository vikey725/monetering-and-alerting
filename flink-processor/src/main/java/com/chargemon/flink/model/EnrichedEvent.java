package com.chargemon.flink.model;

import com.chargemon.flink.serde.JsonTypeInfoFactory;
import com.chargemon.ocpp.model.OcppEvent;
import com.chargemon.ocpp.model.station.StationContext;
import org.apache.flink.api.common.typeinfo.TypeInfo;

/** Canonical event plus station master data (group closure included). */
@TypeInfo(JsonTypeInfoFactory.class)
public record EnrichedEvent(OcppEvent event, StationContext station) {

    public String stationId() {
        return event.stationId();
    }
}
