package com.chargemon.flink.sink;

import com.chargemon.common.json.JsonMapperFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.util.function.SerializableFunction;

/** Builds an at-least-once JSON Kafka sink with a caller-supplied key function. */
public final class JsonKafkaSink {

    private JsonKafkaSink() {
    }

    public static <T> KafkaSink<T> build(String bootstrap, String topic, SerializableFunction<T, String> key) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(bootstrap)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
                        .setTopic(topic)
                        .setKeySerializationSchema(new KeySchema<>(key))
                        .setValueSerializationSchema(new JsonSchema<>())
                        .build())
                .build();
    }

    private record KeySchema<T>(SerializableFunction<T, String> key) implements SerializationSchema<T> {
        @Override
        public byte[] serialize(T element) {
            String k = key.apply(element);
            return k == null ? null : k.getBytes(StandardCharsets.UTF_8);
        }
    }

    private static final class JsonSchema<T> implements SerializationSchema<T> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public byte[] serialize(T element) {
            if (mapper == null) {
                mapper = JsonMapperFactory.standard();
            }
            try {
                return mapper.writeValueAsBytes(element);
            } catch (JsonProcessingException e) {
                throw new IllegalStateException("cannot serialize " + element.getClass(), e);
            }
        }
    }
}
