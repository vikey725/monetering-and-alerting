package com.chargemon.rules.eval.kinds;

import com.chargemon.alert.ConditionSignal;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.eval.RuleContext;
import com.chargemon.rules.eval.StationInput;
import java.util.Map;

/** Uniform signal construction for all evaluators. */
final class EvaluatorSupport {

    private EvaluatorSupport() {
    }

    static void triggered(RuleDefinition rule, RuleContext ctx, StationInput input, Map<String, String> context) {
        ctx.emit(ConditionSignal.triggered(rule.id(), rule.version(), ctx.subject(), ctx.now(), context,
                input == null ? ctx.groupIds() : input.groupIds()));
    }

    static void cleared(RuleDefinition rule, RuleContext ctx) {
        ctx.emit(ConditionSignal.cleared(rule.id(), rule.version(), ctx.subject(), ctx.now()));
    }
}
