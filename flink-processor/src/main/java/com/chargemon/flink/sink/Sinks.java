package com.chargemon.flink.sink;

import com.chargemon.alert.AlertEvent;
import com.chargemon.flink.aggregate.LateSession;
import com.chargemon.flink.decode.DeadLetter;
import com.chargemon.flink.model.AggregateSnapshot;
import com.chargemon.ocpp.model.station.GroupRecord;
import com.chargemon.ocpp.model.station.StationRecord;
import org.apache.flink.streaming.api.datastream.DataStream;

/** Sink port: Kafka + JDBC in production, collectors in tests. */
public interface Sinks {

    void alerts(DataStream<AlertEvent> alerts);

    void deadLetters(DataStream<DeadLetter> deadLetters);

    void stationMirror(DataStream<StationRecord> stations);

    void groupMirror(DataStream<GroupRecord> groups);

    void aggregates(DataStream<AggregateSnapshot> snapshots);

    void lateSessions(DataStream<LateSession> late);
}
