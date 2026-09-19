package com.chargemon.generator;

import com.chargemon.ocpp.codec.envelope.RawEnvelope;
import com.chargemon.ocpp.codec.fixtures.Frames;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/** Thin Kafka producer wrapper; keys by station so per-station order is preserved. */
final class Publisher implements AutoCloseable {

    private final KafkaProducer<String, String> producer;

    Publisher(String bootstrap) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        p.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        p.put(ProducerConfig.ACKS_CONFIG, "1");
        producer = new KafkaProducer<>(p);
    }

    void envelope(String topic, RawEnvelope e) {
        producer.send(new ProducerRecord<>(topic, e.stationId(), Frames.envelopeJson(e)));
    }

    void publishMasterData(Fleet fleet, String stationsTopic, String groupsTopic) {
        for (Map.Entry<String, String> g : fleet.groups().entrySet()) {
            String[] parts = g.getKey().split(":", 2);
            String json = Frames.obj("groupId", g.getKey(), "parentId", g.getValue(), "name", parts[1], "level", parts[0]).toString();
            producer.send(new ProducerRecord<>(groupsTopic, g.getKey(), json));
        }
        for (StationSim s : fleet.stations()) {
            String json = Frames.obj("stationId", s.id(), "name", s.id(), "vendor", s.vendor(), "model", s.model(),
                    "firmware", "1.0.0", "ocppVersion", s.version().wire(), "groupIds", new String[] {s.site()}).toString();
            producer.send(new ProducerRecord<>(stationsTopic, s.id(), json));
        }
        producer.flush();
        System.out.printf("seeded %d groups, %d stations%n", fleet.groups().size(), fleet.stations().size());
    }

    void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
