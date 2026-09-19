package com.chargemon.rules.condition.ops;

import com.chargemon.rules.condition.EvalContext;
import com.chargemon.rules.condition.Values;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Regex match (find semantics) against the string form of the field. Pattern compiled once at parse time. */
public final class MatchesCondition implements FieldCondition {

    public static final String OP = "matches";

    private final String field;
    private final String pattern;
    @JsonIgnore
    private final Pattern compiled;

    @JsonCreator
    public MatchesCondition(@JsonProperty("field") String field, @JsonProperty("pattern") String pattern) {
        this.field = Objects.requireNonNull(field, "field");
        this.pattern = Objects.requireNonNull(pattern, "pattern");
        try {
            this.compiled = Pattern.compile(pattern);
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("invalid regex '" + pattern + "': " + e.getDescription(), e);
        }
    }

    @Override
    @JsonProperty
    public String field() {
        return field;
    }

    @JsonProperty
    public String pattern() {
        return pattern;
    }

    @Override
    public boolean testValue(Object actual, EvalContext ctx) {
        String s = Values.string(actual);
        return s != null && compiled.matcher(s).find();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof MatchesCondition m && field.equals(m.field) && pattern.equals(m.pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, pattern);
    }
}
