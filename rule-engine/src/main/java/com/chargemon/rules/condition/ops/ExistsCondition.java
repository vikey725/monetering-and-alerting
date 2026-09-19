package com.chargemon.rules.condition.ops;

import com.chargemon.rules.condition.Condition;
import com.chargemon.rules.condition.EvalContext;
import com.chargemon.rules.condition.Fact;

public record ExistsCondition(String field) implements Condition {

    public static final String OP = "exists";

    @Override
    public boolean test(Fact fact, EvalContext ctx) {
        return fact.get(field).isPresent();
    }
}
