package com.chargemon.ocpp.codec.mapper;

import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.GenericOcppEvent;
import com.chargemon.ocpp.model.MessageType;
import com.fasterxml.jackson.databind.JsonNode;

/** Fallback for actions without a dedicated mapper. */
public final class GenericEventMapper {

    public GenericOcppEvent map(EventMeta meta, MessageType type, JsonNode payload) {
        return new GenericOcppEvent(meta, type, payload == null ? "{}" : payload.toString());
    }
}
