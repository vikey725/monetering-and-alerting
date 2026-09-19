package com.chargemon.rules.condition.ops;

import java.util.function.IntPredicate;

public record GteCondition(String field, Object value) implements CompareCondition {

    public static final String OP = "gte";

    @Override
    public IntPredicate accept() {
        return c -> c >= 0;
    }
}
