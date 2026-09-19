package com.chargemon.rules.eval;

import java.util.Optional;

/**
 * Per-(subject, rule) scratch state for evaluators. Values are JSON-serializable
 * records; the Flink adapter persists them in keyed MapState.
 */
public interface RuleStateStore {

    <T> Optional<T> get(String ruleId, String scope, Class<T> type);

    <T> void put(String ruleId, String scope, T value);

    void remove(String ruleId, String scope);

    /** Drop every scope for the rule (rule removed / disabled). */
    void clear(String ruleId);
}
