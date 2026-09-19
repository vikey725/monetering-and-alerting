package com.chargemon.rules.eval.kinds;

import com.chargemon.rules.condition.EvalContext;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.definition.RuleKind;
import com.chargemon.rules.definition.RuleSpec.StateDurationSpec;
import com.chargemon.rules.eval.RuleContext;
import com.chargemon.rules.eval.StationInput;
import com.chargemon.rules.eval.StationRuleKindEvaluator;
import com.chargemon.rules.eval.TimerRef;
import java.time.Instant;
import java.util.Map;

/**
 * "Stuck in a state for too long", per scope (connector / EVSE / whole station).
 * Enter starts a timer; exit cancels it (and clears if triggered); timer fires => triggered.
 */
public final class StateDurationRuleEvaluator implements StationRuleKindEvaluator<StateDurationSpec> {

    static final String TIMER = "stuck";

    public record State(Instant enteredAt, boolean triggered) {
    }

    @Override
    public RuleKind kind() {
        return RuleKind.STATE_DURATION;
    }

    @Override
    public Class<StateDurationSpec> specType() {
        return StateDurationSpec.class;
    }

    @Override
    public void onInput(RuleDefinition rule, StateDurationSpec spec, StationInput input, RuleContext ctx) {
        if (input.kind() != StationInput.Kind.EVENT) {
            return;
        }
        String scope = scope(spec, input);
        EvalContext ec = EvalContext.at(ctx.now());
        State prev = ctx.state().get(rule.id(), scope, State.class).orElse(null);
        boolean exit = spec.exit().test(input.fact(), ec);
        boolean enter = !exit && spec.enter().test(input.fact(), ec);
        if (enter && prev == null) {
            ctx.state().put(rule.id(), scope, new State(ctx.now(), false));
            ctx.timers().schedule(new TimerRef(rule.id(), scope, TIMER), ctx.now().plus(spec.maxDuration()));
        } else if (exit && prev != null) {
            ctx.state().remove(rule.id(), scope);
            ctx.timers().cancel(new TimerRef(rule.id(), scope, TIMER));
            if (prev.triggered()) {
                EvaluatorSupport.cleared(rule, ctx);
            }
        }
    }

    @Override
    public void onTimer(RuleDefinition rule, StateDurationSpec spec, TimerRef ref, RuleContext ctx) {
        State prev = ctx.state().get(rule.id(), ref.scope(), State.class).orElse(null);
        if (prev == null || prev.triggered()) {
            return;
        }
        ctx.state().put(rule.id(), ref.scope(), new State(prev.enteredAt(), true));
        EvaluatorSupport.triggered(rule, ctx, null, Map.of(
                "scope", ref.scope(),
                "enteredAt", String.valueOf(prev.enteredAt()),
                "maxDuration", spec.maxDuration().toString()));
    }

    private static String scope(StateDurationSpec spec, StationInput input) {
        if (spec.scopeField() == null) {
            return "";
        }
        return input.fact().get(spec.scopeField()).map(String::valueOf).orElse("");
    }
}
