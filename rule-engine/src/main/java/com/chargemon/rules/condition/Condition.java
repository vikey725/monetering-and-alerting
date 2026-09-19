package com.chargemon.rules.condition;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * A predicate over a {@link Fact}. Implementations form a JSON AST; the
 * discriminator is the {@code op} property. New operators are added by
 * implementing this and registering a {@link ConditionOperatorProvider}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "op")
public interface Condition {

    boolean test(Fact fact, EvalContext ctx);
}
