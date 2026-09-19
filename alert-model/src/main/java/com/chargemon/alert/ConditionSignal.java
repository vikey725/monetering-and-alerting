package com.chargemon.alert;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

/**
 * Edge-triggered output of a rule evaluator: the condition for (rule, subject)
 * became true (TRIGGERED) or false (CLEARED). Evaluators only emit on transitions.
 */
public record ConditionSignal(
        String ruleId,
        int ruleVersion,
        SubjectRef subject,
        Kind kind,
        Instant at,
        Map<String, String> context,
        Set<String> groupIds) {

    public enum Kind { TRIGGERED, CLEARED }

    public ConditionSignal {
        context = context == null ? Map.of() : Map.copyOf(context);
        groupIds = groupIds == null ? Set.of() : Set.copyOf(groupIds);
    }

    public static ConditionSignal triggered(String ruleId, int ruleVersion, SubjectRef subject, Instant at,
                                            Map<String, String> context, Set<String> groupIds) {
        return new ConditionSignal(ruleId, ruleVersion, subject, Kind.TRIGGERED, at, context, groupIds);
    }

    public static ConditionSignal cleared(String ruleId, int ruleVersion, SubjectRef subject, Instant at) {
        return new ConditionSignal(ruleId, ruleVersion, subject, Kind.CLEARED, at, Map.of(), Set.of());
    }

    public String alertKey() {
        return ruleId + "|" + subject.key();
    }
}
