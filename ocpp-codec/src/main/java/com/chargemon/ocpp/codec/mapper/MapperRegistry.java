package com.chargemon.ocpp.codec.mapper;

import com.chargemon.ocpp.model.CorrelatedEvent;
import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.MessageType;
import com.chargemon.ocpp.model.OcppEvent;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * Lookup table (version, action) -> mapper. Unknown actions fall back to
 * {@link GenericEventMapper}, so no message is ever dropped for lack of a mapper.
 */
public final class MapperRegistry {

    private final Map<MapperKey, OcppActionMapper<?>> callMappers;
    private final Map<MapperKey, CorrelatedMapper<?>> correlatedMappers;
    private final GenericEventMapper generic = new GenericEventMapper();

    private MapperRegistry(Map<MapperKey, OcppActionMapper<?>> callMappers,
                           Map<MapperKey, CorrelatedMapper<?>> correlatedMappers) {
        this.callMappers = Map.copyOf(callMappers);
        this.correlatedMappers = Map.copyOf(correlatedMappers);
    }

    public static MapperRegistry fromServiceLoader() {
        List<OcppActionMapper<?>> calls = new ArrayList<>();
        ServiceLoader.load(OcppActionMapper.class).forEach(calls::add);
        List<CorrelatedMapper<?>> correlated = new ArrayList<>();
        ServiceLoader.load(CorrelatedMapper.class).forEach(correlated::add);
        return of(calls, correlated);
    }

    public static MapperRegistry of(Collection<? extends OcppActionMapper<?>> calls,
                                    Collection<? extends CorrelatedMapper<?>> correlated) {
        Map<MapperKey, OcppActionMapper<?>> c = new HashMap<>();
        for (OcppActionMapper<?> m : calls) {
            if (c.putIfAbsent(m.key(), m) != null) {
                throw new IllegalStateException("Duplicate call mapper for " + m.key());
            }
        }
        Map<MapperKey, CorrelatedMapper<?>> r = new HashMap<>();
        for (CorrelatedMapper<?> m : correlated) {
            if (r.putIfAbsent(m.key(), m) != null) {
                throw new IllegalStateException("Duplicate correlated mapper for " + m.key());
            }
        }
        return new MapperRegistry(c, r);
    }

    public OcppEvent mapCall(EventMeta meta, JsonNode payload) {
        OcppActionMapper<?> m = callMappers.get(new MapperKey(meta.version(), meta.action()));
        if (m == null) {
            return generic.map(meta, MessageType.CALL, payload);
        }
        return m.map(meta, payload);
    }

    public boolean hasCorrelatedMapper(MapperKey key) {
        return correlatedMappers.containsKey(key);
    }

    public Optional<CorrelatedEvent> mapResult(EventMeta callMeta, JsonNode callPayload, JsonNode resultPayload,
                                               Instant resultReceivedAt) {
        CorrelatedMapper<?> m = correlatedMappers.get(new MapperKey(callMeta.version(), callMeta.action()));
        if (m == null) {
            return Optional.empty();
        }
        return Optional.of(m.map(callMeta, callPayload, resultPayload, resultReceivedAt));
    }

    public Set<MapperKey> callKeys() {
        return callMappers.keySet();
    }

    public Set<MapperKey> correlatedKeys() {
        return correlatedMappers.keySet();
    }
}
