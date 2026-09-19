package com.chargemon.ocpp.codec.mapper.v16;

import com.chargemon.ocpp.codec.mapper.MapperKey;
import com.chargemon.ocpp.codec.mapper.OcppActionMapper;
import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.Heartbeat;
import com.fasterxml.jackson.databind.JsonNode;

public final class HeartbeatMapper16 implements OcppActionMapper<Heartbeat> {

    @Override
    public MapperKey key() {
        return V16.key("Heartbeat");
    }

    @Override
    public Heartbeat map(EventMeta meta, JsonNode p) {
        return new Heartbeat(meta);
    }
}
