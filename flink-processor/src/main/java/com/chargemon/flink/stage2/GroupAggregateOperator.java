package com.chargemon.flink.stage2;

import com.chargemon.alert.ConditionSignal;
import com.chargemon.alert.SubjectRef;
import com.chargemon.flink.control.RuleBroadcast;
import com.chargemon.flink.control.RuleLoader;
import com.chargemon.flink.model.RuleChange;
import com.chargemon.flink.rules.FlinkRuleContext;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.definition.RuleKind;
import com.chargemon.rules.eval.TimerRef;
import com.chargemon.rules.eval.stage2.GroupAggregateEvaluator;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Keyed by group id. Tracks membership (for percentage thresholds) and dispatches
 * member alerts / aggregates to GROUP_AGGREGATE rules targeting the group.
 */
public final class GroupAggregateOperator
        extends KeyedBroadcastProcessFunction<String, GroupInput, RuleChange, ConditionSignal> {

    private final Duration keyTtl;
    private final RuleLoader loader;
    private transient RuleBroadcast rules;
    private transient GroupAggregateEvaluator evaluator;
    private transient MapState<String, byte[]> blobs;
    private transient MapState<String, Long> timers;
    private transient MapState<String, Boolean> members;
    private transient ValueState<Integer> memberCount;

    public GroupAggregateOperator(Duration keyTtl, RuleLoader loader) {
        this.keyTtl = keyTtl;
        this.loader = loader;
    }

    @Override
    public void open(OpenContext ctx) {
        rules = RuleBroadcast.create();
        rules.seed(loader);
        evaluator = new GroupAggregateEvaluator();
        blobs = getRuntimeContext().getMapState(Stage2Support.blobs(keyTtl));
        timers = getRuntimeContext().getMapState(Stage2Support.timers(keyTtl));
        members = getRuntimeContext().getMapState(new MapStateDescriptor<>("members", Types.STRING, Types.BOOLEAN));
        memberCount = getRuntimeContext().getState(new ValueStateDescriptor<>("memberCount", Types.INT));
    }

    @Override
    public void processElement(GroupInput in, ReadOnlyContext ctx, Collector<ConditionSignal> out) throws Exception {
        Instant now = Instant.ofEpochMilli(ctx.timerService().currentProcessingTime());
        String groupId = ctx.getCurrentKey();
        if (in.delta() != null) {
            boolean changed = in.delta().added() ? members.get(in.delta().stationId()) == null : members.contains(in.delta().stationId());
            if (in.delta().added()) {
                members.put(in.delta().stationId(), Boolean.TRUE);
            } else {
                members.remove(in.delta().stationId());
            }
            if (changed) {
                int count = Optional.ofNullable(memberCount.value()).orElse(0) + (in.delta().added() ? 1 : -1);
                memberCount.update(Math.max(0, count));
                FlinkRuleContext rc = context(groupId, ctx, out, now);
                for (RuleDefinition rule : applicable(groupId, ctx)) {
                    evaluator.dispatchMemberCount(rule, memberCount.value(), rc);
                }
            }
            return;
        }
        int count = Optional.ofNullable(memberCount.value()).orElse(0);
        FlinkRuleContext rc = context(groupId, ctx, out, now);
        for (RuleDefinition rule : applicable(groupId, ctx)) {
            if (in.alert() != null) {
                evaluator.dispatchAlert(rule, in.alert(), count, rc);
            } else if (in.snapshot() != null) {
                evaluator.dispatchAggregate(rule, in.snapshot().source(), in.snapshot().windows(), count, rc);
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
        if (due.isEmpty()) {
            return;
        }
        Instant now = Instant.ofEpochMilli(timestamp);
        int count = Optional.ofNullable(memberCount.value()).orElse(0);
        FlinkRuleContext rc = new FlinkRuleContext(SubjectRef.group(ctx.getCurrentKey()), Set.of(ctx.getCurrentKey()), blobs,
                timers, ctx.timerService(), out, now);
        for (TimerRef ref : due) {
            timers.remove(ref.key());
            Optional<RuleDefinition> rule = rules.lookup(ref.ruleId(), ctx.getBroadcastState(RuleBroadcast.DESCRIPTOR));
            if (rule.isPresent() && rule.get().enabled() && rule.get().kind() == RuleKind.GROUP_AGGREGATE) {
                evaluator.dispatchTimer(rule.get(), ref, count, rc);
            } else {
                rc.state().clear(ref.ruleId());
            }
        }
    }

    private List<RuleDefinition> applicable(String groupId, ReadOnlyContext ctx) throws Exception {
        List<RuleDefinition> out = new ArrayList<>();
        for (RuleDefinition rule : rules.all(ctx.getBroadcastState(RuleBroadcast.DESCRIPTOR)).values()) {
            if (rule.enabled() && rule.kind() == RuleKind.GROUP_AGGREGATE && rule.appliesToGroup(groupId)) {
                out.add(rule);
            }
        }
        return out;
    }

    private FlinkRuleContext context(String groupId, ReadOnlyContext ctx, Collector<ConditionSignal> out, Instant now) {
        return new FlinkRuleContext(SubjectRef.group(groupId), Set.of(groupId), blobs, timers, ctx.timerService(), out, now);
    }
}
