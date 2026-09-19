package com.chargemon.rules.condition.ops;

import com.chargemon.rules.condition.EvalContext;
import com.chargemon.rules.condition.Values;
import java.util.function.IntPredicate;

/** Shared implementation for gt/gte/lt/lte. */
interface CompareCondition extends FieldCondition {

    Object value();

    IntPredicate accept();

    @Override
    default boolean testValue(Object actual, EvalContext ctx) {
        return Values.compare(actual, value()).map(c -> accept().test(c)).orElse(false);
    }
}
