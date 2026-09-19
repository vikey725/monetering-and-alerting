package com.chargemon.rules.eval.stage2;

import com.chargemon.alert.AlertEvent;
import com.chargemon.alert.AlertEventType;
import com.chargemon.alert.ConditionSignal;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.definition.RuleKind;
import com.chargemon.rules.definition.RuleSpec.AggregateSource;
import com.chargemon.rules.definition.RuleSpec.GroupAggregateSpec;
import com.chargemon.rules.definition.RuleSpec.Threshold;
import com.chargemon.rules.eval.RuleContext;
import com.chargemon.rules.eval.TimerRef;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Group-level threshold over either (a) currently-open stage-1 alerts of a given rule
 * whose OPENED lies within {@code window}, or (b) the group's zero-energy aggregate window.
 * A timer re-evaluates when the oldest counted alert ages out of the window.
 */
public final class GroupAggregateEvaluator implements GroupRuleKindEvaluator<GroupAggregateSpec> {

    static final String SCOPE = "";
    static final String TIMER = "age-out";

    /** Open source alerts by station -> openedAt; last aggregate value; whether currently triggered. */
    public record State(Map<String, Instant> open, Long aggregate, boolean triggered) {
        public State {
            open = open == null ? Map.of() : Map.copyOf(open);
        }

        static State empty() {
            return new State(Map.of(), null, false);
        }
    }

    @Override
    public RuleKind kind() {
        return RuleKind.GROUP_AGGREGATE;
    }

    @Override
    public void onAlert(RuleDefinition rule, GroupAggregateSpec spec, AlertEvent alert, int members, RuleContext ctx) {
        if (!AggregateSource.ALERTS.equals(spec.source().type()) || !spec.source().ruleId().equals(alert.ruleId())) {
            return;
        }
        State prev = state(rule, ctx);
        Map<String, Instant> open = new HashMap<>(prev.open());
        if (alert.type() == AlertEventType.OPENED) {
            open.put(alert.subjectId(), alert.openedAt());
        } else {
            open.remove(alert.subjectId());
        }
        evaluate(rule, spec, new State(open, prev.aggregate(), prev.triggered()), members, ctx);
    }

    @Override
    public void onMemberCount(RuleDefinition rule, GroupAggregateSpec spec, int members, RuleContext ctx) {
        if (spec.threshold().percent() != null) {
            evaluate(rule, spec, state(rule, ctx), members, ctx);
        }
    }

    @Override
    public void onAggregate(RuleDefinition rule, GroupAggregateSpec spec, String source, Map<String, Long> windows,
                            int members, RuleContext ctx) {
        if (!AggregateSource.ZERO_ENERGY.equals(spec.source().type()) || !AggregateSource.ZERO_ENERGY.equals(source)) {
            return;
        }
        Long value = windows.get(spec.source().window());
        if (value == null) {
            return;
        }
        State prev = state(rule, ctx);
        evaluate(rule, spec, new State(prev.open(), value, prev.triggered()), members, ctx);
    }

    @Override
    public void onTimer(RuleDefinition rule, GroupAggregateSpec spec, TimerRef ref, int members, RuleContext ctx) {
        evaluate(rule, spec, state(rule, ctx), members, ctx);
    }

    private void evaluate(RuleDefinition rule, GroupAggregateSpec spec, State state, int members, RuleContext ctx) {
        Instant now = ctx.now();
        long count;
        Instant oldestCounted = null;
        if (AggregateSource.ALERTS.equals(spec.source().type())) {
            Instant cutoff = now.minus(spec.window());
            count = 0;
            for (Instant at : state.open().values()) {
                if (!at.isBefore(cutoff)) {
                    count++;
                    oldestCounted = oldestCounted == null || at.isBefore(oldestCounted) ? at : oldestCounted;
                }
            }
        } else {
            count = state.aggregate() == null ? 0 : state.aggregate();
        }
        boolean over = crosses(spec.threshold(), count, members);
        State next = new State(state.open(), state.aggregate(), over);
        if (next.open().isEmpty() && next.aggregate() == null && !over) {
            ctx.state().remove(rule.id(), SCOPE);
        } else {
            ctx.state().put(rule.id(), SCOPE, next);
        }
        if (over && !state.triggered()) {
            ctx.emit(ConditionSignal.triggered(rule.id(), rule.version(), ctx.subject(), now, Map.of(
                    "count", Long.toString(count),
                    "members", Integer.toString(members),
                    "source", spec.source().type()), ctx.groupIds()));
        } else if (!over && state.triggered()) {
            ctx.emit(ConditionSignal.cleared(rule.id(), rule.version(), ctx.subject(), now));
        }
        TimerRef timer = new TimerRef(rule.id(), SCOPE, TIMER);
        if (oldestCounted != null) {
            ctx.timers().schedule(timer, oldestCounted.plus(spec.window()).plusMillis(1));
        } else {
            ctx.timers().cancel(timer);
        }
    }

    static boolean crosses(Threshold t, long count, int members) {
        if (t.count() != null) {
            return count >= t.count();
        }
        if (members <= 0) {
            return false;
        }
        return count * 100.0 / members >= t.percent();
    }

    private static State state(RuleDefinition rule, RuleContext ctx) {
        return ctx.state().get(rule.id(), SCOPE, State.class).orElse(State.empty());
    }
}
