package com.chargemon.rules.condition;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

/** Comparison semantics shared by field operators: numeric when both sides parse as numbers, else string. */
public final class Values {

    private Values() {
    }

    public static Optional<BigDecimal> number(Object v) {
        return switch (v) {
            case null -> Optional.empty();
            case BigDecimal bd -> Optional.of(bd);
            case Number n -> Optional.of(new BigDecimal(n.toString()));
            case String s -> {
                try {
                    yield Optional.of(new BigDecimal(s.trim()));
                } catch (NumberFormatException e) {
                    yield Optional.empty();
                }
            }
            default -> Optional.empty();
        };
    }

    public static String string(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Enum<?> e) {
            return e.name();
        }
        return v.toString();
    }

    /** Equality: numeric if both numeric, boolean-aware, else case-sensitive string equality. */
    public static boolean equal(Object actual, Object expected) {
        if (actual == null || expected == null) {
            return actual == null && expected == null;
        }
        if (actual instanceof Boolean || expected instanceof Boolean) {
            return Objects.equals(string(actual), string(expected));
        }
        Optional<BigDecimal> a = number(actual);
        Optional<BigDecimal> b = number(expected);
        if (a.isPresent() && b.isPresent()) {
            return a.get().compareTo(b.get()) == 0;
        }
        return Objects.equals(string(actual), string(expected));
    }

    /** Ordering; empty when either side is not comparable. */
    public static Optional<Integer> compare(Object actual, Object expected) {
        Optional<BigDecimal> a = number(actual);
        Optional<BigDecimal> b = number(expected);
        if (a.isPresent() && b.isPresent()) {
            return Optional.of(a.get().compareTo(b.get()));
        }
        if (actual instanceof Comparable<?> && expected != null) {
            String sa = string(actual);
            String sb = string(expected);
            return Optional.of(sa.compareTo(sb));
        }
        return Optional.empty();
    }

    /** True when {@code actual} equals any element, or (when actual is a collection) any of its elements equals any element. */
    public static boolean in(Object actual, Collection<?> values) {
        if (actual instanceof Collection<?> c) {
            return c.stream().anyMatch(x -> values.stream().anyMatch(v -> equal(x, v)));
        }
        return values.stream().anyMatch(v -> equal(actual, v));
    }
}
