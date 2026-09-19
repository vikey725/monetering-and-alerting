package com.chargemon.flink.enrich;

import com.chargemon.common.json.JsonMapperFactory;
import com.chargemon.flink.serde.JsonTypes;
import com.chargemon.ocpp.model.station.GroupRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class GroupRecordDeserializer implements KafkaRecordDeserializationSchema<GroupRecord> {

    private static final long serialVersionUID = 1L;
    private transient ObjectMapper json;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> r, Collector<GroupRecord> out) throws IOException {
        String key = r.key() == null ? null : new String(r.key(), StandardCharsets.UTF_8);
        if (r.value() == null) {
            if (key != null) {
                out.collect(new GroupRecord(key, null, null, null, Map.of(), true));
            }
            return;
        }
        out.collect(parse(mapper().readTree(r.value()), key));
    }

    public static GroupRecord parse(JsonNode n, String fallbackId) {
        if (n.has("payload")) {
            JsonNode p = n.get("payload");
            n = p.hasNonNull("after") ? p.get("after") : p;
        }
        String id = first(n, "groupId", "group_id", "id");
        Map<String, String> attrs = new HashMap<>();
        JsonNode a = n.get("attributes");
        if (a != null && a.isObject()) {
            a.properties().forEach(e -> attrs.put(e.getKey(), e.getValue().isValueNode() ? e.getValue().asText() : e.getValue().toString()));
        }
        boolean deleted = n.path("deleted").asBoolean(false) || "true".equals(n.path("__deleted").asText());
        return new GroupRecord(id == null ? fallbackId : id, first(n, "parentId", "parent_id", "parent"),
                first(n, "name"), first(n, "level", "type"), attrs, deleted);
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
    public TypeInformation<GroupRecord> getProducedType() {
        return JsonTypes.of(GroupRecord.class);
    }
}
