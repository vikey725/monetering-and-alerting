package com.chargemon.flink.model;

import com.chargemon.flink.serde.JsonTypeInfoFactory;
import com.chargemon.ocpp.model.OcppEvent;
import com.chargemon.ocpp.model.station.StationRecord;
import org.apache.flink.api.common.typeinfo.TypeInfo;

/** Union of the two station-keyed inputs to enrichment: an event or a master-data update. */
@TypeInfo(JsonTypeInfoFactory.class)
public record StationStreamElement(OcppEvent event, StationRecord station) {

    public static StationStreamElement of(OcppEvent e) {
        return new StationStreamElement(e, null);
    }

    public static StationStreamElement of(StationRecord s) {
        return new StationStreamElement(null, s);
    }

    public String stationId() {
        return event != null ? event.stationId() : station.stationId();
    }
}
