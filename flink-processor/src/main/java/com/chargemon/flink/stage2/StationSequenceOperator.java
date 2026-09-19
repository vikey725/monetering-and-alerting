package com.chargemon.flink.stage2;

import com.chargemon.alert.AlertEvent;
import com.chargemon.alert.ConditionSignal;
import com.chargemon.alert.SubjectRef;
import com.chargemon.flink.control.RuleBroadcast;
import com.chargemon.flink.control.RuleLoader;
import com.chargemon.flink.model.RuleChange;
import com.chargemon.flink.rules.FlinkRuleContext;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.definition.RuleKind;
import com.chargemon.rules.eval.TimerRef;
import com.chargemon.rules.eval.stage2.SequenceEvaluator;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/** Keyed by station id; evaluates SEQUENCE rules over the station's stage-1 alerts. */
public final class StationSequenceOperator
        extends KeyedBroadcastProcessFunction<String, AlertEvent, RuleChange, ConditionSignal> {

    private final Duration keyTtl;
    private final RuleLoader loader;
    private transient RuleBroadcast rules;
    private transient SequenceEvaluator evaluator;
    private transient MapState<String, byte[]> blobs;
    private transient MapState<String, Long> timers;

    public StationSequenceOperator(Duration keyTtl, RuleLoader loader) {
        this.keyTtl = keyTtl;
        this.loader = loader;
    }

    @Override
    public void open(OpenContext ctx) {
        rules = RuleBroadcast.create();
        rules.seed(loader);
        evaluator = new SequenceEvaluator();
        blobs = getRuntimeContext().getMapState(Stage2Support.blobs(keyTtl));
        timers = getRuntimeContext().getMapState(Stage2Support.timers(keyTtl));
    }

    @Override
    public void processElement(AlertEvent alert, ReadOnlyContext ctx, Collector<ConditionSignal> out) throws Exception {
        Instant now = Instant.ofEpochMilli(ctx.timerService().currentProcessingTime());
        FlinkRuleContext rc = new FlinkRuleContext(SubjectRef.station(ctx.getCurrentKey()), alert.groupIds(), blobs, timers,
                ctx.timerService(), out, now);
        for (RuleDefinition rule : rules.all(ctx.getBroadcastState(RuleBroadcast.DESCRIPTOR)).values()) {
            if (rule.enabled() && rule.kind() == RuleKind.SEQUENCE && rule.appliesToGroups(alert.groupIds())) {
                evaluator.dispatchAlert(rule, alert, rc);
            }
        }
    }

    @Override
    public void processBroadcastElement(RuleChange change, Context ctx, Collector<ConditionSignal> out) throws Exception {
        rules.apply(change, ctx.getBroadcastState(RuleBroadcast.DESCRIPTOR));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ConditionSignal> out) throws Exception {
        List<TimerRef> due = new ArrayList<>();
        for (Map.Entry<String, Long> e : timers.entries()) {
            if (e.getValue() == timestamp) {
                due.add(TimerRef.parse(e.getKey()));
            }
        }
        for (TimerRef ref : due) {
            timers.remove(ref.key());
        }
    }
}
