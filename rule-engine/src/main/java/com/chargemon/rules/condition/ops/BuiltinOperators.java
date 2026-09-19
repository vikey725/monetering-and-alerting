package com.chargemon.rules.condition.ops;

import com.chargemon.rules.condition.Condition;
import com.chargemon.rules.condition.ConditionOperatorProvider;
import java.util.LinkedHashMap;
import java.util.Map;

/** Registers the built-in operator set. Third parties add their own providers via ServiceLoader. */
public final class BuiltinOperators implements ConditionOperatorProvider {

    @Override
    public Map<String, Class<? extends Condition>> operators() {
        Map<String, Class<? extends Condition>> m = new LinkedHashMap<>();
        m.put(AndCondition.OP, AndCondition.class);
        m.put(OrCondition.OP, OrCondition.class);
        m.put(NotCondition.OP, NotCondition.class);
        m.put(EqCondition.OP, EqCondition.class);
        m.put(NeCondition.OP, NeCondition.class);
        m.put(GtCondition.OP, GtCondition.class);
        m.put(GteCondition.OP, GteCondition.class);
        m.put(LtCondition.OP, LtCondition.class);
        m.put(LteCondition.OP, LteCondition.class);
        m.put(InCondition.OP, InCondition.class);
        m.put(NinCondition.OP, NinCondition.class);
        m.put(BetweenCondition.OP, BetweenCondition.class);
        m.put(MatchesCondition.OP, MatchesCondition.class);
        m.put(ExistsCondition.OP, ExistsCondition.class);
        m.put(StartsWithCondition.OP, StartsWithCondition.class);
        m.put(ContainsCondition.OP, ContainsCondition.class);
        return m;
    }
}
