package com.chargemon.flink.rules;

import com.chargemon.alert.ConditionSignal;
import com.chargemon.alert.SubjectRef;
import com.chargemon.flink.control.RuleBroadcast;
import com.chargemon.flink.control.RuleLoader;
import com.chargemon.flink.model.AggregateSnapshot;
import com.chargemon.flink.model.EnrichedEvent;
import com.chargemon.flink.model.RuleChange;
import com.chargemon.flink.model.RuleInput;
import com.chargemon.flink.serde.JsonTypes;
import com.chargemon.ocpp.codec.fact.OcppEventFact;
import com.chargemon.ocpp.model.station.StationContext;
import com.chargemon.rules.condition.EvalContext;
import com.chargemon.rules.condition.Fact;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.eval.EvaluatorRegistry;
import com.chargemon.rules.eval.StationInput;
import com.chargemon.rules.eval.StationRuleKindEvaluator;
import com.chargemon.rules.eval.TimerRef;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stage-1 rule evaluation, keyed by station. Rules arrive via broadcast; each
 * enabled stage-1 rule that targets the station is dispatched to its kind evaluator.
 *
 * <p>Aggregate snapshots are remembered per station so event-triggered rules can
 * also read {@code agg.*} facts.
 */
public final class StationRuleEvaluatorOperator
        extends KeyedBroadcastProcessFunction<String, RuleInput, RuleChange, ConditionSignal> {

    private static final Logger LOG = LoggerFactory.getLogger(StationRuleEvaluatorOperator.class);

    private final Duration keyTtl;
    private final RuleLoader loader;

    private transient RuleBroadcast rules;
    private transient EvaluatorRegistry evaluators;
    private transient MapState<String, byte[]> blobs;
    private transient MapState<String, Long> timers;
    private transient MapState<String, Long> aggregates;     // "source.window" -> value
    private transient ValueState<StationContext> lastStation;
    private transient Counter evaluated;
    private transient Counter signals;

    public StationRuleEvaluatorOperator(Duration keyTtl, RuleLoader loader) {
        this.keyTtl = keyTtl;
        this.loader = loader;
    }

    @Override
    public void open(OpenContext ctx) {
        rules = RuleBroadcast.create();
        rules.seed(loader);
        evaluators = EvaluatorRegistry.fromServiceLoader();
        StateTtlConfig ttl = StateTtlConfig.newBuilder(keyTtl)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupInRocksdbCompactFilter(1000)
                .build();
        MapStateDescriptor<String, byte[]> blobDesc = new MapStateDescriptor<>("ruleState", Types.STRING,
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        blobDesc.enableTimeToLive(ttl);
        blobs = getRuntimeContext().getMapState(blobDesc);
        MapStateDescriptor<String, Long> timerDesc = new MapStateDescriptor<>("ruleTimers", Types.STRING, Types.LONG);
        timerDesc.enableTimeToLive(ttl);
        timers = getRuntimeContext().getMapState(timerDesc);
        MapStateDescriptor<String, Long> aggDesc = new MapStateDescriptor<>("aggregates", Types.STRING, Types.LONG);
        aggDesc.enableTimeToLive(ttl);
        aggregates = getRuntimeContext().getMapState(aggDesc);
        ValueStateDescriptor<StationContext> stDesc = new ValueStateDescriptor<>("lastStation",
                JsonTypes.of(StationContext.class));
        stDesc.enableTimeToLive(ttl);
        lastStation = getRuntimeContext().getState(stDesc);
        evaluated = getRuntimeContext().getMetricGroup().counter("rulesEvaluated");
        signals = getRuntimeContext().getMetricGroup().counter("signalsEmitted");
    }

    @Override
    public void processElement(RuleInput input, ReadOnlyContext ctx, Collector<ConditionSignal> out) throws Exception {
        Instant now = Instant.ofEpochMilli(ctx.timerService().currentProcessingTime());
        StationInput si;
        StationContext station;
        if (input.event() != null) {
            EnrichedEvent e = input.event();
            station = e.station();
            lastStation.update(station);
            Fact fact = new OcppEventFact(e.event(), station, aggregateFacts(), now);
            si = StationInput.event(e.event().action(), fact, e.event().meta().eventTime(), station.allGroupIds());
        } else {
            AggregateSnapshot s = input.snapshot();
            for (Map.Entry<String, Long> w : s.windows().entrySet()) {
                aggregates.put(s.source() + "." + w.getKey(), w.getValue());
            }
            station = Optional.ofNullable(lastStation.value()).orElse(StationContext.unknown(s.subjectId()));
            Fact fact = new OcppEventFact(null, station, aggregateFacts(), now);
            si = StationInput.aggregate(s.source(), fact, s.asOf(), s.groupIds().isEmpty() ? station.allGroupIds() : s.groupIds());
        }
        FlinkRuleContext rc = new FlinkRuleContext(SubjectRef.station(ctx.getCurrentKey()), station.allGroupIds(), blobs,
                timers, ctx.timerService(), counting(out), now);
        EvalContext ec = EvalContext.at(now);
        Map<String, RuleDefinition> active = rules.all(ctx.getBroadcastState(RuleBroadcast.DESCRIPTOR));
        LOG.debug("evaluate station={} input={}/{} rules={}", ctx.getCurrentKey(), si.kind(), si.name(), active.keySet());
        for (RuleDefinition rule : active.values()) {
            if (!applies(rule, station, si.fact(), ec)) {
                continue;
            }
            Optional<StationRuleKindEvaluator<?>> ev = evaluators.station(rule.kind());
            if (ev.isPresent()) {
                evaluated.inc();
                ev.get().dispatchInput(rule, si, rc);
            }
        }
    }

    @Override
    public void processBroadcastElement(RuleChange change, Context ctx, Collector<ConditionSignal> out) throws Exception {
        rules.apply(change, ctx.getBroadcastState(RuleBroadcast.DESCRIPTOR));
        // Per-station cleanup for removed rules is lazy: stale timers are ignored on firing and state expires via TTL.
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ConditionSignal> out) throws Exception {
        Instant now = Instant.ofEpochMilli(timestamp);
        List<TimerRef> due = new ArrayList<>();
        for (Map.Entry<String, Long> e : timers.entries()) {
            if (e.getValue() == timestamp) {
                due.add(TimerRef.parse(e.getKey()));
            }
        }
        if (due.isEmpty()) {
            return;
        }
        StationContext station = Optional.ofNullable(lastStation.value())
                .orElse(StationContext.unknown(ctx.getCurrentKey()));
        FlinkRuleContext rc = new FlinkRuleContext(SubjectRef.station(ctx.getCurrentKey()), station.allGroupIds(), blobs,
                timers, ctx.timerService(), counting(out), now);
        for (TimerRef ref : due) {
            timers.remove(ref.key());
            Optional<RuleDefinition> rule = rules.lookup(ref.ruleId(), ctx.getBroadcastState(RuleBroadcast.DESCRIPTOR));
            if (rule.isEmpty() || !rule.get().enabled()) {
                rc.state().clear(ref.ruleId());
                continue;
            }
            evaluators.station(rule.get().kind()).ifPresent(ev -> ev.dispatchTimer(rule.get(), ref, rc));
        }
    }

    private boolean applies(RuleDefinition rule, StationContext station, Fact fact, EvalContext ec) {
        if (!rule.enabled() || rule.kind().stage() != 1) {
            return false;
        }
        if (!rule.appliesToGroups(station.allGroupIds())) {
            return false;
        }
        return rule.stationFilter() == null || rule.stationFilter().test(fact, ec);
    }

    private Map<String, Number> aggregateFacts() throws Exception {
        Map<String, Number> m = new HashMap<>();
        for (Map.Entry<String, Long> e : aggregates.entries()) {
            m.put(e.getKey(), e.getValue());
        }
        return m;
    }

    private Collector<ConditionSignal> counting(Collector<ConditionSignal> out) {
        return new Collector<>() {
            @Override
            public void collect(ConditionSignal record) {
                signals.inc();
                LOG.debug("signal {} rule={} subject={}", record.kind(), record.ruleId(), record.subject().id());
                out.collect(record);
            }

            @Override
            public void close() {
            }
        };
    }

    static Set<String> groups(StationContext s) {
        return s == null ? Set.of() : s.allGroupIds();
    }
}
