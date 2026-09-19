package com.chargemon.flink.sink;

import com.chargemon.alert.AlertEvent;
import com.chargemon.flink.config.JobConfig;
import com.chargemon.flink.aggregate.LateSession;
import com.chargemon.flink.decode.DeadLetter;
import com.chargemon.flink.model.AggregateSnapshot;
import com.chargemon.rules.window.WindowSpec;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import com.chargemon.ocpp.model.station.GroupRecord;
import com.chargemon.ocpp.model.station.StationRecord;
import org.apache.flink.streaming.api.datastream.DataStream;

/** Kafka for events, Postgres for bookkeeping. Alerts go to both. */
public final class ProductionSinks implements Sinks {

    private final JobConfig cfg;
    private final JdbcSinks jdbc;

    public ProductionSinks(JobConfig cfg) {
        this.cfg = cfg;
        this.jdbc = new JdbcSinks(cfg);
    }

    @Override
    public void alerts(DataStream<AlertEvent> alerts) {
        alerts.sinkTo(JsonKafkaSink.build(cfg.kafkaBootstrap(), cfg.alertsTopic(), AlertEvent::alertKey))
                .uid("sink-alerts-kafka").name("alerts -> kafka");
        alerts.sinkTo(jdbc.alerts()).uid("sink-alerts-jdbc").name("alerts -> postgres");
    }

    @Override
    public void deadLetters(DataStream<DeadLetter> deadLetters) {
        deadLetters.sinkTo(JsonKafkaSink.build(cfg.kafkaBootstrap(), cfg.deadLetterTopic(), DeadLetter::sourceRef))
                .uid("sink-dead-letter").name("dead-letter");
    }

    @Override
    public void stationMirror(DataStream<StationRecord> stations) {
        stations.sinkTo(jdbc.stationMirror()).uid("sink-stations-jdbc").name("stations -> postgres");
    }

    @Override
    public void groupMirror(DataStream<GroupRecord> groups) {
        groups.sinkTo(jdbc.groupMirror()).uid("sink-groups-jdbc").name("groups -> postgres");
    }

    @Override
    public void aggregates(DataStream<AggregateSnapshot> snapshots) {
        List<WindowSpec> specs = WindowSpec.parseList(cfg.zeroEnergyWindows(), ZoneOffset.UTC);
        DataStream<JdbcSinks.WindowRow> rows = snapshots
                .flatMap((AggregateSnapshot s, org.apache.flink.util.Collector<JdbcSinks.WindowRow> out) ->
                        JdbcSinks.rows(s, specs).forEach(out::collect))
                .returns(Types.POJO(JdbcSinks.WindowRow.class))
                .uid("aggregate-rows").name("aggregate rows");
        rows.filter(r -> !r.rolling()).sinkTo(jdbc.tumblingAggregates()).uid("sink-agg-tumbling").name("tumbling -> postgres");
        rows.filter(JdbcSinks.WindowRow::rolling).sinkTo(jdbc.rollingAggregates()).uid("sink-agg-rolling").name("rolling -> postgres");
    }

    @Override
    public void lateSessions(DataStream<LateSession> late) {
        late.sinkTo(JsonKafkaSink.build(cfg.kafkaBootstrap(), cfg.lateEventsTopic(), LateSession::subjectId))
                .uid("sink-late").name("late-events");
    }
}
