package com.chargemon.rules.condition.ops;

import com.chargemon.rules.condition.EvalContext;
import com.chargemon.rules.condition.Values;

public record EqCondition(String field, Object value) implements FieldCondition {

    public static final String OP = "eq";

    @Override
    public boolean testValue(Object actual, EvalContext ctx) {
        return Values.equal(actual, value);
    }
}
