package com.chargemon.flink.source;

import com.chargemon.flink.config.JobConfig;
import com.chargemon.flink.control.KafkaRuleLoader;
import com.chargemon.flink.control.RuleChangeDeserializer;
import com.chargemon.flink.control.RuleLoader;
import com.chargemon.flink.enrich.GroupRecordDeserializer;
import com.chargemon.flink.enrich.StationRecordDeserializer;
import com.chargemon.flink.model.KafkaRecord;
import com.chargemon.flink.model.RuleChange;
import com.chargemon.ocpp.model.station.GroupRecord;
import com.chargemon.ocpp.model.station.StationRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public final class KafkaSources implements Sources {

    private final JobConfig cfg;

    public KafkaSources(JobConfig cfg) {
        this.cfg = cfg;
    }

    @Override
    public DataStream<KafkaRecord> events(StreamExecutionEnvironment env) {
        KafkaSource<KafkaRecord> source = KafkaSource.<KafkaRecord>builder()
                .setBootstrapServers(cfg.kafkaBootstrap())
                .setTopics(cfg.eventsTopic())
                .setGroupId(cfg.consumerGroup())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new KafkaRecordDeserializer())
                .build();
        WatermarkStrategy<KafkaRecord> wm = WatermarkStrategy
                .<KafkaRecord>forBoundedOutOfOrderness(cfg.maxOutOfOrderness())
                .withIdleness(cfg.sourceIdleness());
        return env.fromSource(source, wm, "common-broker").uid("src-events");
    }

    @Override
    public DataStream<RuleChange> rules(StreamExecutionEnvironment env) {
        return compacted(env, cfg.rulesTopic(), "rules", new RuleChangeDeserializer());
    }

    @Override
    public DataStream<StationRecord> stations(StreamExecutionEnvironment env) {
        return compacted(env, cfg.stationsTopic(), "stations", new StationRecordDeserializer());
    }

    @Override
    public DataStream<GroupRecord> groups(StreamExecutionEnvironment env) {
        return compacted(env, cfg.groupsTopic(), "groups", new GroupRecordDeserializer());
    }

    @Override
    public RuleLoader ruleLoader() {
        return new KafkaRuleLoader(cfg.kafkaBootstrap(), cfg.rulesTopic());
    }

    /** Compacted control topics: always replay from the beginning, never emit watermarks. */
    private <T> DataStream<T> compacted(StreamExecutionEnvironment env, String topic, String name,
                                        KafkaRecordDeserializationSchema<T> schema) {
        KafkaSource<T> source = KafkaSource.<T>builder()
                .setBootstrapServers(cfg.kafkaBootstrap())
                .setTopics(topic)
                .setGroupId(cfg.consumerGroup() + "-" + name)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(schema)
                .build();
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), name).uid("src-" + name);
    }
}
