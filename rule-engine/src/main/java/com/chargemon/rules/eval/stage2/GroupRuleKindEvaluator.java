package com.chargemon.rules.eval.stage2;

import com.chargemon.alert.AlertEvent;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.definition.RuleKind;
import com.chargemon.rules.definition.RuleSpec;
import com.chargemon.rules.eval.RuleContext;
import com.chargemon.rules.eval.TimerRef;
import java.util.Map;

/** Stage-2 evaluator over one group (subject = group): alerts of members, membership changes, group aggregates. */
public interface GroupRuleKindEvaluator<S extends RuleSpec> {

    RuleKind kind();

    void onAlert(RuleDefinition rule, S spec, AlertEvent alert, int memberCount, RuleContext ctx);

    void onMemberCount(RuleDefinition rule, S spec, int memberCount, RuleContext ctx);

    void onAggregate(RuleDefinition rule, S spec, String source, Map<String, Long> windows, int memberCount, RuleContext ctx);

    void onTimer(RuleDefinition rule, S spec, TimerRef ref, int memberCount, RuleContext ctx);

    @SuppressWarnings("unchecked")
    default void dispatchAlert(RuleDefinition rule, AlertEvent alert, int members, RuleContext ctx) {
        onAlert(rule, (S) rule.spec(), alert, members, ctx);
    }

    @SuppressWarnings("unchecked")
    default void dispatchMemberCount(RuleDefinition rule, int members, RuleContext ctx) {
        onMemberCount(rule, (S) rule.spec(), members, ctx);
    }

    @SuppressWarnings("unchecked")
    default void dispatchAggregate(RuleDefinition rule, String source, Map<String, Long> windows, int members, RuleContext ctx) {
        onAggregate(rule, (S) rule.spec(), source, windows, members, ctx);
    }

    @SuppressWarnings("unchecked")
    default void dispatchTimer(RuleDefinition rule, TimerRef ref, int members, RuleContext ctx) {
        onTimer(rule, (S) rule.spec(), ref, members, ctx);
    }
}
