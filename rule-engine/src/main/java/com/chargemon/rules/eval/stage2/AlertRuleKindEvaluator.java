package com.chargemon.rules.eval.stage2;

import com.chargemon.alert.AlertEvent;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.definition.RuleKind;
import com.chargemon.rules.definition.RuleSpec;
import com.chargemon.rules.eval.RuleContext;
import com.chargemon.rules.eval.TimerRef;

/** Stage-2 evaluator fed by stage-1 alerts of one station (subject = station). */
public interface AlertRuleKindEvaluator<S extends RuleSpec> {

    RuleKind kind();

    void onAlert(RuleDefinition rule, S spec, AlertEvent alert, RuleContext ctx);

    void onTimer(RuleDefinition rule, S spec, TimerRef ref, RuleContext ctx);

    @SuppressWarnings("unchecked")
    default void dispatchAlert(RuleDefinition rule, AlertEvent alert, RuleContext ctx) {
        onAlert(rule, (S) rule.spec(), alert, ctx);
    }

    @SuppressWarnings("unchecked")
    default void dispatchTimer(RuleDefinition rule, TimerRef ref, RuleContext ctx) {
        onTimer(rule, (S) rule.spec(), ref, ctx);
    }
}
