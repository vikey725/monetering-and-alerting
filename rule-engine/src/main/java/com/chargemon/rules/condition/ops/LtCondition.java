package com.chargemon.rules.condition.ops;

import java.util.function.IntPredicate;

public record LtCondition(String field, Object value) implements CompareCondition {

    public static final String OP = "lt";

    @Override
    public IntPredicate accept() {
        return c -> c < 0;
    }
}
