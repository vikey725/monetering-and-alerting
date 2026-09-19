package com.chargemon.rules.condition.ops;

import java.util.function.IntPredicate;

public record GtCondition(String field, Object value) implements CompareCondition {

    public static final String OP = "gt";

    @Override
    public IntPredicate accept() {
        return c -> c > 0;
    }
}
