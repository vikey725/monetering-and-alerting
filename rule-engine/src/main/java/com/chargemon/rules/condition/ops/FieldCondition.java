package com.chargemon.rules.condition.ops;

import com.chargemon.rules.condition.Condition;
import com.chargemon.rules.condition.EvalContext;
import com.chargemon.rules.condition.Fact;
import java.util.Optional;

/** Base for single-field operators: resolves the field once, missing field => false. */
public interface FieldCondition extends Condition {

    String field();

    boolean testValue(Object actual, EvalContext ctx);

    @Override
    default boolean test(Fact fact, EvalContext ctx) {
        Optional<Object> v = fact.get(field());
        return v.isPresent() && testValue(v.get(), ctx);
    }
}
