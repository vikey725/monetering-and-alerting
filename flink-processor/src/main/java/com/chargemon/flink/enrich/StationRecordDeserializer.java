package com.chargemon.flink.enrich;

import com.chargemon.common.json.JsonMapperFactory;
import com.chargemon.flink.serde.JsonTypes;
import com.chargemon.ocpp.model.station.StationRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Adapter isolating the upstream registry's shape. Tolerates common field
 * aliases; a tombstone (or {@code deleted:true}) yields a deleted record.
 */
public final class StationRecordDeserializer implements KafkaRecordDeserializationSchema<StationRecord> {

    private static final long serialVersionUID = 1L;
    private transient ObjectMapper json;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> r, Collector<StationRecord> out) throws IOException {
        String key = r.key() == null ? null : new String(r.key(), StandardCharsets.UTF_8);
        if (r.value() == null) {
            if (key != null) {
                out.collect(new StationRecord(key, null, null, null, null, null, Map.of(), Set.of(), true));
            }
            return;
        }
        out.collect(parse(mapper().readTree(r.value()), key));
    }

    public static StationRecord parse(JsonNode n, String fallbackId) {
        if (n.has("payload")) {                                  // Debezium/CDC envelope
            JsonNode p = n.get("payload");
            n = p.hasNonNull("after") ? p.get("after") : p;
        }
        String id = first(n, "stationId", "station_id", "chargePointId", "id");
        if (id == null) {
            id = fallbackId;
        }
        Map<String, String> attrs = new HashMap<>();
        JsonNode a = n.get("attributes");
        if (a != null && a.isObject()) {
            a.properties().forEach(e -> attrs.put(e.getKey(), e.getValue().isValueNode() ? e.getValue().asText() : e.getValue().toString()));
        }
        Set<String> groups = new HashSet<>();
        JsonNode g = n.has("groupIds") ? n.get("groupIds") : n.has("groups") ? n.get("groups") : n.get("group_ids");
        if (g != null && g.isArray()) {
            g.forEach(x -> groups.add(x.isTextual() ? x.asText() : x.path("id").asText()));
        }
        boolean deleted = n.path("deleted").asBoolean(false) || "true".equals(n.path("__deleted").asText());
        return new StationRecord(id,
                first(n, "name"),
                first(n, "vendor", "chargePointVendor", "vendorName"),
                first(n, "model", "chargePointModel"),
                first(n, "firmware", "firmwareVersion"),
                first(n, "ocppVersion", "protocol"),
                attrs, groups, deleted);
    }

    private static String first(JsonNode n, String... keys) {
        for (String k : keys) {
            JsonNode v = n.get(k);
            if (v != null && !v.isNull()) {
                return v.asText();
            }
        }
        return null;
    }

    private ObjectMapper mapper() {
        if (json == null) {
            json = JsonMapperFactory.standard();
        }
        return json;
    }

    @Override
    public TypeInformation<StationRecord> getProducedType() {
        return JsonTypes.of(StationRecord.class);
    }
}
