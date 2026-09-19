package com.chargemon.rules.condition.ops;

import com.chargemon.rules.condition.Condition;
import com.chargemon.rules.condition.EvalContext;
import com.chargemon.rules.condition.Fact;
import java.util.List;

public record OrCondition(List<Condition> args) implements Condition {

    public static final String OP = "or";

    public OrCondition {
        args = args == null ? List.of() : List.copyOf(args);
    }

    @Override
    public boolean test(Fact fact, EvalContext ctx) {
        for (Condition c : args) {
            if (c.test(fact, ctx)) {
                return true;
            }
        }
        return false;
    }
}
