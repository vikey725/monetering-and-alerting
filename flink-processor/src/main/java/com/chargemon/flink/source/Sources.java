package com.chargemon.flink.source;

import com.chargemon.flink.control.RuleLoader;
import com.chargemon.flink.model.KafkaRecord;
import com.chargemon.flink.model.RuleChange;
import com.chargemon.ocpp.model.station.GroupRecord;
import com.chargemon.ocpp.model.station.StationRecord;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Source port so the topology can be built against Kafka in production and collections in tests. */
public interface Sources {

    DataStream<KafkaRecord> events(StreamExecutionEnvironment env);

    DataStream<RuleChange> rules(StreamExecutionEnvironment env);

    DataStream<StationRecord> stations(StreamExecutionEnvironment env);

    DataStream<GroupRecord> groups(StreamExecutionEnvironment env);

    /** Snapshot of rules for operator start-up (see {@link RuleLoader}). */
    RuleLoader ruleLoader();
}
