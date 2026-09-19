package com.chargemon.flink.aggregate;

import com.chargemon.flink.model.AggregateSnapshot;
import com.chargemon.rules.window.HourlyBucketAggregator;
import com.chargemon.rules.window.WindowSpec;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keyed by subject (station or group). Hourly buckets in state; every configured
 * window is derived on demand. Event-time timers re-emit snapshots when a window
 * boundary passes so downstream rules can clear. Emits only when values change.
 */
public final class ZeroEnergyAggregator extends KeyedProcessFunction<SubjectKey, SubjectSession, AggregateSnapshot> {

    private static final Logger LOG = LoggerFactory.getLogger(ZeroEnergyAggregator.class);

    public static final String SOURCE = "zeroEnergy";
    public static final OutputTag<LateSession> LATE = new OutputTag<>("late-sessions") {
    };

    private final String windowConfig;
    private final String zoneId;

    private transient HourlyBucketAggregator agg;
    private transient MapState<Long, Integer> buckets;
    private transient ValueState<Map<String, Long>> lastValues;
    private transient ValueState<List<String>> lastGroups;
    private transient Counter late;

    public ZeroEnergyAggregator(String windowConfig, ZoneId zone) {
        this.windowConfig = windowConfig;
        this.zoneId = zone.getId();
    }

    @Override
    public void open(OpenContext ctx) {
        agg = new HourlyBucketAggregator(WindowSpec.parseList(windowConfig, ZoneId.of(zoneId)));
        buckets = getRuntimeContext().getMapState(new MapStateDescriptor<>("buckets", Types.LONG, Types.INT));
        lastValues = getRuntimeContext().getState(new ValueStateDescriptor<>("lastValues", Types.MAP(Types.STRING, Types.LONG)));
        lastGroups = getRuntimeContext().getState(new ValueStateDescriptor<>("lastGroups", Types.LIST(Types.STRING)));
        late = getRuntimeContext().getMetricGroup().counter("lateSessions");
    }

    @Override
    public void processElement(SubjectSession s, Context ctx, Collector<AggregateSnapshot> out) throws Exception {
        long eventHour = HourlyBucketAggregator.hourOf(s.endedAt());
        long wm = ctx.timerService().currentWatermark();
        long nowHour = wm == Long.MIN_VALUE ? eventHour : Math.max(eventHour, Math.floorDiv(wm, 3_600_000L));
        LOG.debug("aggregate subject={} session={} eventHour={} wm={} nowHour={} accepted={}", ctx.getCurrentKey(), s.sessionId(),
                eventHour, wm, nowHour, agg.accepts(eventHour, nowHour));
        if (!agg.accepts(eventHour, nowHour)) {
            late.inc();
            ctx.output(LATE, new LateSession(s.subject().subjectType(), s.subject().subjectId(), s.sessionId(),
                    s.endedAt().toEpochMilli(), wm));
            return;
        }
        Integer cur = buckets.get(eventHour);
        buckets.put(eventHour, cur == null ? 1 : cur + 1);
        if (s.subject().type() == com.chargemon.alert.SubjectType.STATION) {
            lastGroups.update(List.copyOf(s.groupIds()));
        }
        refresh(ctx.getCurrentKey(), nowHour, ctx.timerService()::registerEventTimeTimer, out);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AggregateSnapshot> out) throws Exception {
        long nowHour = Math.floorDiv(timestamp, 3_600_000L);
        refresh(ctx.getCurrentKey(), nowHour, ctx.timerService()::registerEventTimeTimer, out);
    }

    private void refresh(SubjectKey key, long nowHour, java.util.function.LongConsumer registerTimer,
                         Collector<AggregateSnapshot> out) throws Exception {
        TreeMap<Long, Integer> local = new TreeMap<>();
        for (Map.Entry<Long, Integer> e : buckets.entries()) {
            local.put(e.getKey(), e.getValue());
        }
        int removed = agg.expire(local, nowHour);
        if (removed > 0) {
            buckets.clear();
            buckets.putAll(local);
        }
        Map<String, Long> values = agg.values(local, nowHour);
        Map<String, Long> previous = lastValues.value();
        LOG.debug("snapshot subject={} values={} previous={}", key, values, previous);
        if (!values.equals(previous)) {
            lastValues.update(new HashMap<>(values));
            List<String> groups = lastGroups.value();
            out.collect(new AggregateSnapshot(SOURCE, key.type(), key.subjectId(), values,
                    Instant.ofEpochSecond(nowHour * 3600), groups == null ? Set.of() : Set.copyOf(groups)));
        }
        OptionalLong next = agg.nextChangeHour(local, nowHour);
        if (next.isPresent()) {
            registerTimer.accept(next.getAsLong() * 3_600_000L);
        } else if (local.isEmpty()) {
            lastValues.clear();
        }
    }

    public List<WindowSpec> windows() {
        return agg.windows();
    }
}
