package com.chargemon.flink.lifecycle;

import com.chargemon.alert.AlertEvent;
import com.chargemon.alert.ConditionSignal;
import com.chargemon.alert.ResolveReason;
import com.chargemon.flink.control.RuleBroadcast;
import com.chargemon.flink.control.RuleLoader;
import com.chargemon.flink.model.AlertKey;
import com.chargemon.flink.model.RuleChange;
import com.chargemon.flink.serde.JsonTypes;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.lifecycle.AlertLifecycle;
import com.chargemon.rules.lifecycle.LifecycleInput;
import com.chargemon.rules.lifecycle.LifecycleState;
import com.chargemon.rules.lifecycle.TimerKind;
import com.chargemon.rules.lifecycle.TimerRequest;
import java.time.Instant;
import java.util.Optional;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keyed by (rule, subject). Owns one {@link LifecycleState} per key and drives the
 * pure {@link AlertLifecycle} with signals, timers and rule removals.
 */
public final class AlertLifecycleOperator
        extends KeyedBroadcastProcessFunction<AlertKey, ConditionSignal, RuleChange, AlertEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(AlertLifecycleOperator.class);

    private final RuleLoader loader;

    private transient RuleBroadcast rules;
    private transient AlertLifecycle lifecycle;
    private transient ValueState<LifecycleState> state;
    private transient Counter opened;
    private transient Counter resolved;

    public AlertLifecycleOperator(RuleLoader loader) {
        this.loader = loader;
    }

    @Override
    public void open(OpenContext ctx) {
        rules = RuleBroadcast.create();
        rules.seed(loader);
        lifecycle = new AlertLifecycle();
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("lifecycle", JsonTypes.of(LifecycleState.class)));
        opened = getRuntimeContext().getMetricGroup().counter("alertsOpened");
        resolved = getRuntimeContext().getMetricGroup().counter("alertsResolved");
    }

    @Override
    public void processElement(ConditionSignal signal, ReadOnlyContext ctx, Collector<AlertEvent> out) throws Exception {
        Optional<RuleDefinition> rule = rules.lookup(signal.ruleId(), ctx.getBroadcastState(RuleBroadcast.DESCRIPTOR));
        LOG.debug("lifecycle key={} signal={} rulePresent={}", ctx.getCurrentKey(), signal.kind(), rule.isPresent());
        if (rule.isEmpty() || !rule.get().enabled()) {
            return;
        }
        Instant now = Instant.ofEpochMilli(ctx.timerService().currentProcessingTime());
        apply(new LifecycleInput.Signal(signal), rule.get(), ctx.getCurrentKey(), now, ctx.timerService(), out);
    }

    @Override
    public void processBroadcastElement(RuleChange change, Context ctx, Collector<AlertEvent> out) throws Exception {
        RuleBroadcast.Change c = rules.apply(change, ctx.getBroadcastState(RuleBroadcast.DESCRIPTOR));
        if (!c.deactivated()) {
            return;
        }
        RuleDefinition gone = c.before().orElseThrow();
        ResolveReason reason = c.after().isPresent() ? ResolveReason.RULE_DISABLED : ResolveReason.RULE_REMOVED;
        Instant now = Instant.ofEpochMilli(ctx.currentProcessingTime());
        ValueStateDescriptor<LifecycleState> desc = new ValueStateDescriptor<>("lifecycle", JsonTypes.of(LifecycleState.class));
        ctx.applyToKeyedState(desc, (AlertKey key, ValueState<LifecycleState> s) -> {
            LifecycleState cur = s.value();
            if (cur == null || cur.isIdle() || !key.ruleId().equals(gone.id())) {
                return;
            }
            AlertLifecycle.Decision d = lifecycle.on(cur, new LifecycleInput.RuleGone(reason), gone, key.subject(), now);
            s.update(d.next());
            d.emits().forEach(e -> emit(e, out));
        });
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AlertEvent> out) throws Exception {
        LifecycleState cur = state.value();
        if (cur == null || cur.isIdle()) {
            return;
        }
        Optional<RuleDefinition> rule = rules.lookup(ctx.getCurrentKey().ruleId(), ctx.getBroadcastState(RuleBroadcast.DESCRIPTOR));
        if (rule.isEmpty()) {
            return;
        }
        Instant at = Instant.ofEpochMilli(timestamp);
        Instant now = at;
        for (TimerKind kind : TimerKind.values()) {
            Instant deadline = switch (kind) {
                case GRACE -> cur.graceDeadline();
                case SUPPRESSION -> cur.suppressedUntil();
                case AUTO_RESOLVE -> cur.autoResolveAt();
            };
            if (deadline != null && deadline.toEpochMilli() == timestamp) {
                apply(new LifecycleInput.TimerFired(kind, at), rule.get(), ctx.getCurrentKey(), now, ctx.timerService(), out);
                cur = state.value();
            }
        }
    }

    private void apply(LifecycleInput in, RuleDefinition rule, AlertKey key, Instant now,
                       org.apache.flink.streaming.api.TimerService timerService, Collector<AlertEvent> out) throws Exception {
        LifecycleState cur = Optional.ofNullable(state.value()).orElse(LifecycleState.idle());
        AlertLifecycle.Decision d = lifecycle.on(cur, in, rule, key.subject(), now);
        LOG.debug("lifecycle key={} {} -> {} emits={}", key, cur.phase(), d.next().phase(), d.emits().size());
        state.update(d.next().isIdle() && d.next().seq() == 0 ? null : d.next());
        for (TimerRequest t : d.timers()) {
            if (!t.isCancel()) {
                timerService.registerProcessingTimeTimer(t.at().toEpochMilli());
            }
            // Cancels are implicit: a fired timer whose timestamp no longer matches a stored deadline is ignored.
        }
        d.emits().forEach(e -> emit(e, out));
    }

    private void emit(AlertEvent e, Collector<AlertEvent> out) {
        switch (e.type()) {
            case OPENED -> opened.inc();
            case RESOLVED -> resolved.inc();
        }
        out.collect(e);
    }
}
