package com.chargemon.rules.condition;

import java.util.Map;

/** ServiceLoader hook: contributes {@code op} name -> {@link Condition} class mappings. */
public interface ConditionOperatorProvider {

    Map<String, Class<? extends Condition>> operators();
}
