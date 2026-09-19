package com.chargemon.rules.condition.ops;

import com.chargemon.rules.condition.EvalContext;
import com.chargemon.rules.condition.Values;

public record NeCondition(String field, Object value) implements FieldCondition {

    public static final String OP = "ne";

    @Override
    public boolean testValue(Object actual, EvalContext ctx) {
        return !Values.equal(actual, value);
    }
}
