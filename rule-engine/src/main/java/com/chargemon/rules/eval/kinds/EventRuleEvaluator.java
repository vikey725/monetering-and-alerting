package com.chargemon.rules.eval.kinds;

import com.chargemon.rules.condition.EvalContext;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.definition.RuleKind;
import com.chargemon.rules.definition.RuleSpec.EventSpec;
import com.chargemon.rules.eval.RuleContext;
import com.chargemon.rules.eval.StationInput;
import com.chargemon.rules.eval.StationRuleKindEvaluator;
import com.chargemon.rules.eval.TimerRef;
import java.util.LinkedHashMap;
import java.util.Map;

/** Stateless predicate over matching inputs; remembers only whether it is currently triggered. */
public final class EventRuleEvaluator implements StationRuleKindEvaluator<EventSpec> {

    static final String SCOPE = "";

    public record State(boolean triggered) {
    }

    @Override
    public RuleKind kind() {
        return RuleKind.EVENT;
    }

    @Override
    public Class<EventSpec> specType() {
        return EventSpec.class;
    }

    @Override
    public void onInput(RuleDefinition rule, EventSpec spec, StationInput input, RuleContext ctx) {
        boolean relevant = switch (input.kind()) {
            case EVENT -> spec.trigger().matchesAction(input.name());
            case AGGREGATE -> spec.trigger().matchesAggregate(input.name());
        };
        if (!relevant) {
            return;
        }
        EvalContext ec = EvalContext.at(ctx.now());
        boolean was = ctx.state().get(rule.id(), SCOPE, State.class).map(State::triggered).orElse(false);
        boolean matches = spec.condition().test(input.fact(), ec);
        if (matches && !was) {
            ctx.state().put(rule.id(), SCOPE, new State(true));
            EvaluatorSupport.triggered(rule, ctx, input, context(input));
            return;
        }
        boolean clears = spec.clear() == null ? !matches : spec.clear().test(input.fact(), ec);
        if (was && clears && !(matches && spec.clear() == null)) {
            ctx.state().remove(rule.id(), SCOPE);
            EvaluatorSupport.cleared(rule, ctx);
        }
    }

    @Override
    public void onTimer(RuleDefinition rule, EventSpec spec, TimerRef ref, RuleContext ctx) {
        // EVENT rules do not use timers.
    }

    private static Map<String, String> context(StationInput input) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("input", input.name());
        input.fact().get("event.type").ifPresent(v -> m.put("event.type", String.valueOf(v)));
        input.fact().get("event.status").ifPresent(v -> m.put("event.status", String.valueOf(v)));
        input.fact().get("event.errorCode").ifPresent(v -> m.put("event.errorCode", String.valueOf(v)));
        input.fact().get("event.connectorId").ifPresent(v -> m.put("event.connectorId", String.valueOf(v)));
        return m;
    }
}
