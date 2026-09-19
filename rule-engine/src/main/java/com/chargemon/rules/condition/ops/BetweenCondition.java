package com.chargemon.rules.condition.ops;

import com.chargemon.rules.condition.EvalContext;
import com.chargemon.rules.condition.Values;

/** Inclusive range check. */
public record BetweenCondition(String field, Object min, Object max) implements FieldCondition {

    public static final String OP = "between";

    @Override
    public boolean testValue(Object actual, EvalContext ctx) {
        return Values.compare(actual, min).map(c -> c >= 0).orElse(false)
                && Values.compare(actual, max).map(c -> c <= 0).orElse(false);
    }
}
