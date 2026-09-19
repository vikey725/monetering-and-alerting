package com.chargemon.rules.condition.ops;

import com.chargemon.rules.condition.EvalContext;
import com.chargemon.rules.condition.Values;

public record StartsWithCondition(String field, String value) implements FieldCondition {

    public static final String OP = "startsWith";

    @Override
    public boolean testValue(Object actual, EvalContext ctx) {
        String s = Values.string(actual);
        return s != null && value != null && s.startsWith(value);
    }
}
