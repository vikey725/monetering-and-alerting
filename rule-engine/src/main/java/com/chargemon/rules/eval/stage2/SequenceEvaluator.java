package com.chargemon.rules.eval.stage2;

import com.chargemon.alert.AlertEvent;
import com.chargemon.alert.AlertEventType;
import com.chargemon.alert.ConditionSignal;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.definition.RuleKind;
import com.chargemon.rules.definition.RuleSpec.SequenceSpec;
import com.chargemon.rules.eval.RuleContext;
import com.chargemon.rules.eval.TimerRef;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * "All of these stage-1 alerts are open on the same station, and they opened within
 * {@code within} of each other." Triggered while that holds; cleared when any of them resolves.
 */
public final class SequenceEvaluator implements AlertRuleKindEvaluator<SequenceSpec> {

    static final String SCOPE = "";

    /** Open source alerts by rule id -> openedAt, plus whether we are currently triggered. */
    public record State(Map<String, Instant> open, boolean triggered, Set<String> groupIds) {
        public State {
            open = open == null ? Map.of() : Map.copyOf(open);
            groupIds = groupIds == null ? Set.of() : Set.copyOf(groupIds);
        }
    }

    @Override
    public RuleKind kind() {
        return RuleKind.SEQUENCE;
    }

    @Override
    public void onAlert(RuleDefinition rule, SequenceSpec spec, AlertEvent alert, RuleContext ctx) {
        if (!spec.allOf().contains(alert.ruleId())) {
            return;
        }
        State prev = ctx.state().get(rule.id(), SCOPE, State.class).orElse(new State(Map.of(), false, Set.of()));
        Map<String, Instant> open = new HashMap<>(prev.open());
        if (alert.type() == AlertEventType.OPENED) {
            open.put(alert.ruleId(), alert.openedAt());
        } else {
            open.remove(alert.ruleId());
        }
        boolean satisfied = satisfied(spec, open);
        State next = new State(open, satisfied, alert.groupIds().isEmpty() ? prev.groupIds() : alert.groupIds());
        if (open.isEmpty()) {
            ctx.state().remove(rule.id(), SCOPE);
        } else {
            ctx.state().put(rule.id(), SCOPE, next);
        }
        if (satisfied && !prev.triggered()) {
            Map<String, String> context = new LinkedHashMap<>();
            open.forEach((k, v) -> context.put("alert." + k + ".openedAt", String.valueOf(v)));
            ctx.emit(ConditionSignal.triggered(rule.id(), rule.version(), ctx.subject(), ctx.now(), context, next.groupIds()));
        } else if (!satisfied && prev.triggered()) {
            ctx.emit(ConditionSignal.cleared(rule.id(), rule.version(), ctx.subject(), ctx.now()));
        }
    }

    static boolean satisfied(SequenceSpec spec, Map<String, Instant> open) {
        Instant min = null;
        Instant max = null;
        for (String required : spec.allOf()) {
            Instant at = open.get(required);
            if (at == null) {
                return false;
            }
            min = min == null || at.isBefore(min) ? at : min;
            max = max == null || at.isAfter(max) ? at : max;
        }
        return min != null && !java.time.Duration.between(min, max).minus(spec.within()).isPositive();
    }

    @Override
    public void onTimer(RuleDefinition rule, SequenceSpec spec, TimerRef ref, RuleContext ctx) {
        // Sequence rules react to alert edges only.
    }
}
