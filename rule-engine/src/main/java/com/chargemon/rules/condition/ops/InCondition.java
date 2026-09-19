package com.chargemon.rules.condition.ops;

import com.chargemon.rules.condition.EvalContext;
import com.chargemon.rules.condition.Values;
import java.util.List;

public record InCondition(String field, List<Object> values) implements FieldCondition {

    public static final String OP = "in";

    public InCondition {
        values = values == null ? List.of() : List.copyOf(values);
    }

    @Override
    public boolean testValue(Object actual, EvalContext ctx) {
        return Values.in(actual, values);
    }
}
