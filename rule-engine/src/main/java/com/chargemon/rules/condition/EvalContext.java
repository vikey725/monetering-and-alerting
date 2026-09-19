package com.chargemon.rules.condition;

import java.time.Instant;

/** Per-evaluation context; kept minimal so conditions stay pure. */
public record EvalContext(Instant now) {

    public static EvalContext at(Instant now) {
        return new EvalContext(now);
    }
}
