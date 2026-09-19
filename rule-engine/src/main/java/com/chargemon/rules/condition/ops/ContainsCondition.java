package com.chargemon.rules.condition.ops;

import com.chargemon.rules.condition.EvalContext;
import com.chargemon.rules.condition.Values;
import java.util.Collection;

/** Substring for strings, membership for collections. */
public record ContainsCondition(String field, Object value) implements FieldCondition {

    public static final String OP = "contains";

    @Override
    public boolean testValue(Object actual, EvalContext ctx) {
        if (actual instanceof Collection<?> c) {
            return c.stream().anyMatch(x -> Values.equal(x, value));
        }
        String s = Values.string(actual);
        String v = Values.string(value);
        return s != null && v != null && s.contains(v);
    }
}
