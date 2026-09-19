package com.chargemon.common.time;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Optional;

/** ISO-8601 duration helpers shared by rule definitions and config. */
public final class Durations {

    private Durations() {
    }

    /** Parses an ISO-8601 duration such as {@code PT10M}; blank or null yields empty. */
    public static Optional<Duration> parseOptional(String iso) {
        if (iso == null || iso.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(parse(iso));
    }

    public static Duration parse(String iso) {
        try {
            return Duration.parse(iso.trim());
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid ISO-8601 duration: '" + iso + "'", e);
        }
    }

    public static String format(Duration d) {
        return d == null ? null : d.toString();
    }
}
