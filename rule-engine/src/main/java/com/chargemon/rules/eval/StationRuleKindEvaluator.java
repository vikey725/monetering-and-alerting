package com.chargemon.rules.eval;

import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.definition.RuleKind;
import com.chargemon.rules.definition.RuleSpec;

/**
 * Strategy per stage-1 {@link RuleKind}. Implementations are stateless; all
 * per-subject state lives in {@link RuleContext#state()}. Emit only on transitions.
 */
public interface StationRuleKindEvaluator<S extends RuleSpec> {

    RuleKind kind();

    Class<S> specType();

    void onInput(RuleDefinition rule, S spec, StationInput input, RuleContext ctx);

    void onTimer(RuleDefinition rule, S spec, TimerRef ref, RuleContext ctx);

    /** Rule deleted / disabled: drop state and timers. Lifecycle handles resolving open alerts. */
    default void onRuleRemoved(RuleDefinition rule, RuleContext ctx) {
        ctx.state().clear(rule.id());
        ctx.timers().cancelAll(rule.id());
    }

    @SuppressWarnings("unchecked")
    default void dispatchInput(RuleDefinition rule, StationInput input, RuleContext ctx) {
        onInput(rule, (S) rule.spec(), input, ctx);
    }

    @SuppressWarnings("unchecked")
    default void dispatchTimer(RuleDefinition rule, TimerRef ref, RuleContext ctx) {
        onTimer(rule, (S) rule.spec(), ref, ctx);
    }
}
