package com.chargemon.flink.model;

/** Raw Kafka record; parsing happens downstream so bad payloads become dead letters, not source failures. */
public record KafkaRecord(String key, byte[] value, long timestamp, String sourceRef) {
}
