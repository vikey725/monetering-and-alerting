package com.chargemon.flink.topology;

import com.chargemon.alert.AlertEvent;
import com.chargemon.flink.aggregate.LateSession;
import com.chargemon.flink.decode.DeadLetter;
import com.chargemon.flink.model.AggregateSnapshot;
import com.chargemon.flink.sink.Sinks;
import com.chargemon.ocpp.model.station.GroupRecord;
import com.chargemon.ocpp.model.station.StationRecord;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/** Static queues keyed by test id; MiniCluster sinks run in-process so this works. */
final class CollectingSinks implements Sinks {

    static final Map<String, Queue<AlertEvent>> ALERTS = new ConcurrentHashMap<>();
    static final Map<String, Queue<DeadLetter>> DEAD = new ConcurrentHashMap<>();
    static final Map<String, Queue<AggregateSnapshot>> SNAPSHOTS = new ConcurrentHashMap<>();

    private final String id;

    CollectingSinks(String id) {
        this.id = id;
        ALERTS.put(id, new ConcurrentLinkedQueue<>());
        DEAD.put(id, new ConcurrentLinkedQueue<>());
        SNAPSHOTS.put(id, new ConcurrentLinkedQueue<>());
    }

    List<AggregateSnapshot> snapshots() {
        return List.copyOf(SNAPSHOTS.get(id));
    }

    List<AlertEvent> alerts() {
        return List.copyOf(ALERTS.get(id));
    }

    List<DeadLetter> dead() {
        return List.copyOf(DEAD.get(id));
    }

    @Override
    public void alerts(DataStream<AlertEvent> alerts) {
        String key = id;
        alerts.addSink(new SinkFunction<>() {
            @Override
            public void invoke(AlertEvent value, Context context) {
                ALERTS.get(key).add(value);
            }
        });
    }

    @Override
    public void stationMirror(DataStream<StationRecord> stations) {
        stations.addSink(new SinkFunction<>() {
        });
    }

    @Override
    public void groupMirror(DataStream<GroupRecord> groups) {
        groups.addSink(new SinkFunction<>() {
        });
    }

    @Override
    public void aggregates(DataStream<AggregateSnapshot> snapshots) {
        String key = id;
        snapshots.addSink(new SinkFunction<>() {
            @Override
            public void invoke(AggregateSnapshot value, Context context) {
                SNAPSHOTS.get(key).add(value);
            }
        });
    }

    @Override
    public void lateSessions(DataStream<LateSession> late) {
        late.addSink(new SinkFunction<>() {
        });
    }

    @Override
    public void deadLetters(DataStream<DeadLetter> deadLetters) {
        String key = id;
        deadLetters.addSink(new SinkFunction<>() {
            @Override
            public void invoke(DeadLetter value, Context context) {
                DEAD.get(key).add(value);
            }
        });
    }
}
