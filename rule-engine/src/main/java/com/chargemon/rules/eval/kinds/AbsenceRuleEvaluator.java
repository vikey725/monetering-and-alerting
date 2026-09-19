package com.chargemon.rules.eval.kinds;

import com.chargemon.rules.condition.EvalContext;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.definition.RuleKind;
import com.chargemon.rules.definition.RuleSpec.AbsenceSpec;
import com.chargemon.rules.eval.RuleContext;
import com.chargemon.rules.eval.StationInput;
import com.chargemon.rules.eval.StationRuleKindEvaluator;
import com.chargemon.rules.eval.TimerRef;
import java.time.Instant;
import java.util.Map;

/**
 * "No {@code expectedAction} for {@code within}". Every matching input re-arms a
 * processing-time timer; when it fires with no newer input, the condition triggers.
 * A subsequent matching input clears it.
 */
public final class AbsenceRuleEvaluator implements StationRuleKindEvaluator<AbsenceSpec> {

    static final String SCOPE = "";
    static final String TIMER = "absence";

    public record State(Instant lastSeen, boolean triggered) {
    }

    @Override
    public RuleKind kind() {
        return RuleKind.ABSENCE;
    }

    @Override
    public Class<AbsenceSpec> specType() {
        return AbsenceSpec.class;
    }

    @Override
    public void onInput(RuleDefinition rule, AbsenceSpec spec, StationInput input, RuleContext ctx) {
        if (input.kind() != StationInput.Kind.EVENT) {
            return;
        }
        boolean expected = "*".equals(spec.expectedAction()) || spec.expectedAction().equals(input.name());
        if (!expected) {
            return;
        }
        if (spec.onlyIf() != null && !spec.onlyIf().test(input.fact(), EvalContext.at(ctx.now()))) {
            return;
        }
        State prev = ctx.state().get(rule.id(), SCOPE, State.class).orElse(null);
        if (prev != null && prev.triggered()) {
            EvaluatorSupport.cleared(rule, ctx);
        }
        ctx.state().put(rule.id(), SCOPE, new State(ctx.now(), false));
        ctx.timers().schedule(new TimerRef(rule.id(), SCOPE, TIMER), ctx.now().plus(spec.within()));
    }

    @Override
    public void onTimer(RuleDefinition rule, AbsenceSpec spec, TimerRef ref, RuleContext ctx) {
        State prev = ctx.state().get(rule.id(), SCOPE, State.class).orElse(null);
        if (prev == null || prev.triggered()) {
            return;
        }
        ctx.state().put(rule.id(), SCOPE, new State(prev.lastSeen(), true));
        EvaluatorSupport.triggered(rule, ctx, null, Map.of(
                "expectedAction", spec.expectedAction(),
                "lastSeen", String.valueOf(prev.lastSeen()),
                "within", spec.within().toString()));
    }
}
