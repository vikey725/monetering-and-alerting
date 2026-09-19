package com.chargemon.alert;

import java.time.Duration;
import java.util.Objects;

/**
 * The two windows every rule carries.
 *
 * @param graceWindow       how long a TRIGGERED condition must persist before an alert opens
 * @param suppressionWindow after an alert opens, how long re-triggers on the same subject stay muted
 * @param autoResolveAfter  optional: force-resolve an open alert after this long (null = never)
 */
public record RuleTiming(Duration graceWindow, Duration suppressionWindow, Duration autoResolveAfter) {

    public RuleTiming {
        graceWindow = Objects.requireNonNullElse(graceWindow, Duration.ZERO);
        suppressionWindow = Objects.requireNonNullElse(suppressionWindow, Duration.ZERO);
        if (graceWindow.isNegative() || suppressionWindow.isNegative()
                || (autoResolveAfter != null && autoResolveAfter.isNegative())) {
            throw new IllegalArgumentException("rule windows must not be negative");
        }
    }

    public static RuleTiming none() {
        return new RuleTiming(Duration.ZERO, Duration.ZERO, null);
    }
}
