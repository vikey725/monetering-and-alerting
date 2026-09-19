package com.chargemon.rules.condition.ops;

import com.chargemon.rules.condition.Condition;
import com.chargemon.rules.condition.EvalContext;
import com.chargemon.rules.condition.Fact;

public record NotCondition(Condition arg) implements Condition {

    public static final String OP = "not";

    @Override
    public boolean test(Fact fact, EvalContext ctx) {
        return arg == null || !arg.test(fact, ctx);
    }
}
