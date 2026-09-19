package com.chargemon.flink.control;

import com.chargemon.common.json.JsonMapperFactory;
import com.chargemon.flink.model.RuleChange;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;

/**
 * Accepts either a Debezium-unwrapped row, a full Debezium envelope ({@code payload.after}),
 * or a plain rule document. Tombstones and {@code __deleted} rows become deletes.
 */
public final class RuleChangeDeserializer implements KafkaRecordDeserializationSchema<RuleChange> {

    private static final long serialVersionUID = 1L;
    private transient ObjectMapper json;

    private ObjectMapper json() {
        if (json == null) {
            json = JsonMapperFactory.standard();
        }
        return json;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RuleChange> out) throws IOException {
        String key = record.key() == null ? null : new String(record.key(), StandardCharsets.UTF_8);
        if (record.value() == null) {
            if (key != null) {
                out.collect(new RuleChange(unwrapKey(key), null));
            }
            return;
        }
        JsonNode root = json().readTree(record.value());
        JsonNode row = root;
        if (root.has("payload")) {                       // full Debezium envelope
            JsonNode payload = root.get("payload");
            row = payload.hasNonNull("after") ? payload.get("after") : null;
            if (row == null || "d".equals(payload.path("op").asText())) {
                String id = idOf(payload.get("before"), key);
                out.collect(new RuleChange(id, null));
                return;
            }
        }
        if (row.path("__deleted").asText("false").equals("true")) {
            out.collect(new RuleChange(idOf(row, key), null));
            return;
        }
        out.collect(new RuleChange(idOf(row, key), row.toString()));
    }

    private String idOf(JsonNode row, String key) {
        if (row != null && row.hasNonNull("id")) {
            return row.get("id").asText();
        }
        return unwrapKey(key);
    }

    /** Debezium JSON keys look like {"id":"..."} or {"payload":{"id":"..."}}; plain keys are the id itself. */
    private String unwrapKey(String key) {
        if (key == null) {
            return null;
        }
        String k = key.trim();
        if (k.startsWith("{")) {
            try {
                JsonNode n = json().readTree(k);
                if (n.has("payload")) {
                    n = n.get("payload");
                }
                return n.path("id").asText(k);
            } catch (IOException e) {
                return k;
            }
        }
        return k;
    }

    @Override
    public TypeInformation<RuleChange> getProducedType() {
        return Types.POJO(RuleChange.class);
    }
}
