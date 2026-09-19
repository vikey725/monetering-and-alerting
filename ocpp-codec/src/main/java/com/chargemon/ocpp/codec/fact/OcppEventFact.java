package com.chargemon.ocpp.codec.fact;

import com.chargemon.common.json.JsonMapperFactory;
import com.chargemon.ocpp.model.GenericOcppEvent;
import com.chargemon.ocpp.model.OcppEvent;
import com.chargemon.ocpp.model.station.StationContext;
import com.chargemon.rules.condition.Fact;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Adapts an enriched OCPP event to the rule engine's {@link Fact} view.
 *
 * <p>Namespaces: {@code event.*} (typed record components, {@code event.type} = action),
 * {@code event.payload.*} (raw payload of generic events), {@code station.*},
 * {@code agg.<source>.<window>}, {@code now}.
 */
public final class OcppEventFact implements Fact {

    private static final String EVENT = "event";
    private static final String STATION = "station";
    private static final String AGG = "agg";
    private static final String NOW = "now";

    private final OcppEvent event;
    private final StationContext station;
    private final Map<String, Number> aggregates;
    private final Instant now;
    private final ObjectMapper mapper;
    private JsonNode eventTree;
    private JsonNode stationTree;

    public OcppEventFact(OcppEvent event, StationContext station, Map<String, Number> aggregates, Instant now) {
        this(event, station, aggregates, now, JsonMapperFactory.standard());
    }

    public OcppEventFact(OcppEvent event, StationContext station, Map<String, Number> aggregates, Instant now,
                         ObjectMapper mapper) {
        this.event = event;                  // null for aggregate-only inputs
        this.station = station == null ? StationContext.unknown(event == null ? "" : event.stationId()) : station;
        this.aggregates = aggregates == null ? Map.of() : aggregates;
        this.now = now;
        this.mapper = mapper;
    }

    public OcppEvent event() {
        return event;
    }

    public StationContext station() {
        return station;
    }

    @Override
    public Optional<Object> get(String path) {
        int dot = path.indexOf('.');
        String ns = dot < 0 ? path : path.substring(0, dot);
        String rest = dot < 0 ? "" : path.substring(dot + 1);
        return switch (ns) {
            case EVENT -> event == null ? Optional.empty()
                    : rest.isEmpty() ? Optional.of(event) : FieldPathResolver.resolve(eventTree(), rest);
            case STATION -> rest.isEmpty() ? Optional.of(station) : FieldPathResolver.resolve(stationTree(), rest);
            case AGG -> Optional.ofNullable(aggregates.get(rest));
            case NOW -> Optional.ofNullable(now);
            default -> Optional.empty();
        };
    }

    private JsonNode eventTree() {
        if (eventTree == null) {
            ObjectNode tree = mapper.valueToTree(event);
            if (event instanceof GenericOcppEvent g) {
                tree.set("payload", parse(g.rawPayloadJson()));
            }
            // Flatten meta.* one level up so `event.stationId` and `event.action` work as expected.
            JsonNode meta = tree.get("meta");
            if (meta != null && meta.isObject()) {
                meta.properties().forEach(e -> tree.putIfAbsent(e.getKey(), e.getValue()));
            }
            eventTree = tree;
        }
        return eventTree;
    }

    private JsonNode stationTree() {
        if (stationTree == null) {
            stationTree = mapper.valueToTree(station);
        }
        return stationTree;
    }

    private JsonNode parse(String json) {
        try {
            return mapper.readTree(json == null ? "{}" : json);
        } catch (IOException e) {
            return mapper.createObjectNode();
        }
    }
}
