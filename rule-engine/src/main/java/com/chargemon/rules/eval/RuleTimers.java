package com.chargemon.rules.eval;

import java.time.Instant;

/** Timer port: processing-time timers keyed by {@link TimerRef}. */
public interface RuleTimers {

    void schedule(TimerRef ref, Instant at);

    void cancel(TimerRef ref);

    void cancelAll(String ruleId);
}
