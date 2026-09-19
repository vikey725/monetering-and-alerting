package com.chargemon.flink.topology;

import com.chargemon.flink.control.RuleLoader;
import com.chargemon.flink.model.KafkaRecord;
import com.chargemon.flink.model.RuleChange;
import com.chargemon.flink.serde.JsonTypes;
import com.chargemon.flink.source.Sources;
import com.chargemon.ocpp.model.station.GroupRecord;
import com.chargemon.ocpp.model.station.StationRecord;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * In-memory sources for MiniCluster tests. The event source stays open after
 * emitting (like Kafka would) so processing-time timers keep firing; events are
 * held back briefly so the rules broadcast lands first.
 */
final class ListSources implements Sources {

    private final List<String> envelopes;
    private final List<RuleChange> rules;
    private final List<StationRecord> stations;
    private final List<GroupRecord> groups;
    private final long eventDelayMillis;
    /** Rules source counts down once rules are emitted; the event source waits on it (same JVM in MiniCluster). */
    static final Map<String, CountDownLatch> RULES_EMITTED = new ConcurrentHashMap<>();
    private final String gate = UUID.randomUUID().toString();

    ListSources(List<String> envelopes, List<RuleChange> rules, long eventDelayMillis) {
        this(envelopes, rules, List.of(), List.of(), eventDelayMillis);
    }

    ListSources(List<String> envelopes, List<RuleChange> rules, List<StationRecord> stations, List<GroupRecord> groups,
                long eventDelayMillis) {
        this.envelopes = envelopes;
        this.rules = rules;
        this.stations = stations;
        this.groups = groups;
        this.eventDelayMillis = eventDelayMillis;
        RULES_EMITTED.put(gate, new CountDownLatch(1));
    }

    @SuppressWarnings("deprecation")
    @Override
    public DataStream<KafkaRecord> events(StreamExecutionEnvironment env) {
        return env.addSource(new OpenEndedSource(envelopes, eventDelayMillis, gate), Types.POJO(KafkaRecord.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<KafkaRecord>forMonotonousTimestamps()
                        .withTimestampAssigner((r, ts) -> r.timestamp()));
    }

    @Override
    public RuleLoader ruleLoader() {
        return RuleLoader.of(rules);
    }

    @SuppressWarnings("deprecation")
    @Override
    public DataStream<RuleChange> rules(StreamExecutionEnvironment env) {
        String g = gate;
        List<RuleChange> els = rules;
        return env.addSource(new SourceFunction<RuleChange>() {
            @Override
            public void run(SourceContext<RuleChange> ctx) {
                els.forEach(ctx::collect);
                RULES_EMITTED.get(g).countDown();
            }

            @Override
            public void cancel() {
            }
        }, Types.POJO(RuleChange.class));
    }

    /** Flink refuses empty element sources; a deleted sentinel record is a harmless stand-in. */
    @Override
    public DataStream<StationRecord> stations(StreamExecutionEnvironment env) {
        List<StationRecord> els = stations.isEmpty()
                ? List.of(new StationRecord("__none__", null, null, null, null, null, Map.of(), Set.of(), true))
                : stations;
        return env.fromData(JsonTypes.of(StationRecord.class), els.toArray(StationRecord[]::new));
    }

    @Override
    public DataStream<GroupRecord> groups(StreamExecutionEnvironment env) {
        List<GroupRecord> els = groups.isEmpty()
                ? List.of(new GroupRecord("__none__", null, null, null, Map.of(), true))
                : groups;
        return env.fromData(JsonTypes.of(GroupRecord.class), els.toArray(GroupRecord[]::new));
    }

    @SuppressWarnings("deprecation")
    private static final class OpenEndedSource implements SourceFunction<KafkaRecord> {
        private final List<String> envelopes;
        private final long delay;
        private final String gate;
        private volatile boolean running = true;

        OpenEndedSource(List<String> envelopes, long delay, String gate) {
            this.envelopes = envelopes;
            this.delay = delay;
            this.gate = gate;
        }

        @Override
        public void run(SourceContext<KafkaRecord> ctx) throws Exception {
            RULES_EMITTED.get(gate).await(30, TimeUnit.SECONDS);
            Thread.sleep(delay);        // let the broadcast propagate through the graph
            int i = 0;
            for (String json : envelopes) {
                ctx.collect(new KafkaRecord(null, json.getBytes(StandardCharsets.UTF_8), System.currentTimeMillis(),
                        "test-0-" + (i++)));
            }
            while (running) {
                Thread.sleep(50);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
