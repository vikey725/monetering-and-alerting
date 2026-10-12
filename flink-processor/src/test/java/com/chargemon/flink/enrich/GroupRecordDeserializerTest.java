package com.chargemon.flink.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import com.chargemon.ocpp.model.station.GroupRecord;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class GroupRecordDeserializerTest {

    private final GroupRecordDeserializer deserializer = new GroupRecordDeserializer();

    private List<GroupRecord> read(String key, String value) throws Exception {
        List<GroupRecord> out = new ArrayList<>();
        deserializer.deserialize(new ConsumerRecord<>("groups", 0, 0L,
                key.getBytes(StandardCharsets.UTF_8),
                value == null ? null : value.getBytes(StandardCharsets.UTF_8)), new ListCollector<>(out));
        return out;
    }

    @Test
    void debeziumUnwrappedRowWithJsonbAttributesAsText() throws Exception {
        List<GroupRecord> out = read("region:de1", """
                {"group_id":"region:de1","parent_id":"country:de","name":"de1","level":"region",
                 "attributes":"{\\"tz\\": \\"Europe/Berlin\\"}","deleted":false,
                 "updated_at":"2026-10-12T10:00:00Z","__deleted":"false"}""");

        assertThat(out).containsExactly(new GroupRecord("region:de1", "country:de", "de1", "region",
                Map.of("tz", "Europe/Berlin"), false));
    }

    @Test
    void debeziumDeleteMarkerBecomesDeletedRecord() throws Exception {
        List<GroupRecord> out = read("site:de1-1", """
                {"group_id":"site:de1-1","parent_id":"region:de1","name":"de1-1","level":"site",
                 "attributes":"{}","deleted":false,"__deleted":"true"}""");

        assertThat(out).singleElement().satisfies(g -> {
            assertThat(g.groupId()).isEqualTo("site:de1-1");
            assertThat(g.deleted()).isTrue();
        });
    }

    @Test
    void tombstoneBecomesDeletedRecordFromKey() throws Exception {
        assertThat(read("site:de1-1", null))
                .containsExactly(new GroupRecord("site:de1-1", null, null, null, Map.of(), true));
    }

    @Test
    void plainCamelCaseDocumentStillParses() throws Exception {
        List<GroupRecord> out = read("country:de", """
                {"groupId":"country:de","parentId":null,"name":"de","level":"country","attributes":{"iso":"DE"}}""");

        assertThat(out).containsExactly(new GroupRecord("country:de", null, "de", "country", Map.of("iso", "DE"), false));
    }
}
