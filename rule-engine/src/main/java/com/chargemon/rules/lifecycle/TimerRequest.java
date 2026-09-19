package com.chargemon.rules.lifecycle;

import java.time.Instant;

/** Schedule (at != null) or cancel (at == null) the timer of the given kind. */
public record TimerRequest(TimerKind kind, Instant at) {

    public static TimerRequest schedule(TimerKind kind, Instant at) {
        return new TimerRequest(kind, at);
    }

    public static TimerRequest cancel(TimerKind kind) {
        return new TimerRequest(kind, null);
    }

    public boolean isCancel() {
        return at == null;
    }
}
