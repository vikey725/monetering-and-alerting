package com.chargemon.rules.lifecycle;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

/**
 * Persistent state per alert key. Deadlines are the source of truth for timers:
 * a timer firing with a different timestamp is stale and ignored.
 */
public record LifecycleState(
        Phase phase,
        String alertId,
        long seq,
        Instant triggeredAt,
        Instant openedAt,
        Instant graceDeadline,
        Instant suppressedUntil,
        Instant autoResolveAt,
        Map<String, String> context,
        Set<String> groupIds) {

    public LifecycleState {
        context = context == null ? Map.of() : Map.copyOf(context);
        groupIds = groupIds == null ? Set.of() : Set.copyOf(groupIds);
    }

    public static LifecycleState idle() {
        return new LifecycleState(Phase.IDLE, null, 0, null, null, null, null, null, Map.of(), Set.of());
    }

    public boolean isIdle() {
        return phase == Phase.IDLE;
    }

    public LifecycleState withSeq(long newSeq) {
        return new LifecycleState(phase, alertId, newSeq, triggeredAt, openedAt, graceDeadline, suppressedUntil,
                autoResolveAt, context, groupIds);
    }

    public LifecycleState withSuppressedUntil(Instant until) {
        return new LifecycleState(phase, alertId, seq, triggeredAt, openedAt, graceDeadline, until, autoResolveAt,
                context, groupIds);
    }
}
