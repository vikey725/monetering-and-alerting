package com.chargemon.rules.eval.kinds;

import com.chargemon.rules.condition.EvalContext;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.definition.RuleKind;
import com.chargemon.rules.definition.RuleSpec.AbsenceSpec;
import com.chargemon.rules.definition.RuleSpec.Rearm;
import com.chargemon.rules.eval.RuleContext;
import com.chargemon.rules.eval.StationInput;
import com.chargemon.rules.eval.StationRuleKindEvaluator;
import com.chargemon.rules.eval.TimerRef;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * "No {@code expectedAction} for {@code within}". Every matching input re-arms a
 * processing-time timer; when it fires with no newer input, the condition triggers.
 * A subsequent matching input clears it.
 *
 * <p>{@code rearmOn} inputs (e.g. Reset, BootNotification) only move the deadline: they open a
 * longer window while the station is expected to be quiet, but do not clear a triggered condition.
 */
public final class AbsenceRuleEvaluator implements StationRuleKindEvaluator<AbsenceSpec> {

    static final String SCOPE = "";
    static final String TIMER = "absence";

    /**
     * @param lastSeen last {@code expectedAction} (null if only re-armed so far)
     * @param armedBy  action that set the current deadline
     */
    public record State(Instant lastSeen, boolean triggered, String armedBy, Instant armedAt) {
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
        EvalContext ec = EvalContext.at(ctx.now());
        State prev = ctx.state().get(rule.id(), SCOPE, State.class).orElse(null);
        boolean expected = "*".equals(spec.expectedAction()) || spec.expectedAction().equals(input.name());
        if (expected && (spec.onlyIf() == null || spec.onlyIf().test(input.fact(), ec))) {
            if (prev != null && prev.triggered()) {
                EvaluatorSupport.cleared(rule, ctx);
            }
            arm(rule, ctx, spec.within(), new State(ctx.now(), false, input.name(), ctx.now()));
            return;
        }
        Rearm rearm = spec.rearmOn().stream()
                .filter(r -> input.name().equals(r.action()))
                .filter(r -> r.when() == null || r.when().test(input.fact(), ec))
                .findFirst().orElse(null);
        if (rearm == null || (prev != null && prev.triggered())) {
            return;
        }
        arm(rule, ctx, rearm.within(),
                new State(prev == null ? null : prev.lastSeen(), false, input.name(), ctx.now()));
    }

    private static void arm(RuleDefinition rule, RuleContext ctx, Duration window, State next) {
        ctx.state().put(rule.id(), SCOPE, next);
        ctx.timers().schedule(new TimerRef(rule.id(), SCOPE, TIMER), ctx.now().plus(window));
    }

    @Override
    public void onTimer(RuleDefinition rule, AbsenceSpec spec, TimerRef ref, RuleContext ctx) {
        State prev = ctx.state().get(rule.id(), SCOPE, State.class).orElse(null);
        if (prev == null || prev.triggered()) {
            return;
        }
        ctx.state().put(rule.id(), SCOPE, new State(prev.lastSeen(), true, prev.armedBy(), prev.armedAt()));
        EvaluatorSupport.triggered(rule, ctx, null, Map.of(
                "expectedAction", spec.expectedAction(),
                "lastSeen", String.valueOf(prev.lastSeen()),
                "within", spec.within().toString(),
                "armedBy", String.valueOf(prev.armedBy()),
                "armedAt", String.valueOf(prev.armedAt())));
    }
}
