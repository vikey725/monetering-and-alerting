package com.chargemon.ocpp.codec.mapper;

import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.OcppEvent;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Maps one (version, action) CALL payload to a canonical event.
 * Discovered via {@link java.util.ServiceLoader}; one class per action.
 */
public interface OcppActionMapper<E extends OcppEvent> {

    MapperKey key();

    E map(EventMeta meta, JsonNode payload) throws MappingException;
}
