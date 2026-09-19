package com.chargemon.alert;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The contract between the Flink job and everything downstream (alerts topic,
 * Postgres, notifier). Additive changes only; bump {@code schemaVersion} on shape changes.
 *
 * @param alertEventId unique per emitted event (idempotency key for the notifier)
 * @param alertId      identity of the alert instance; shared by its OPENED and RESOLVED events
 * @param alertKey     {@code ruleId|subjectType|subjectId}; one open alert at a time per key
 * @param seq          monotonically increasing per alertKey; guards out-of-order DB writes
 */
public record AlertEvent(
        String alertEventId,
        String alertId,
        String alertKey,
        long seq,
        AlertEventType type,
        String ruleId,
        int ruleVersion,
        String ruleName,
        String kind,
        Severity severity,
        List<ChannelRef> channels,
        SubjectType subjectType,
        String subjectId,
        Set<String> groupIds,
        Instant triggeredAt,
        Instant openedAt,
        Instant resolvedAt,
        ResolveReason resolveReason,
        Map<String, String> context,
        Instant emittedAt,
        int schemaVersion) {

    public static final int CURRENT_SCHEMA_VERSION = 1;

    public AlertEvent {
        channels = channels == null ? List.of() : List.copyOf(channels);
        groupIds = groupIds == null ? Set.of() : Set.copyOf(groupIds);
        context = context == null ? Map.of() : Map.copyOf(context);
    }

    public SubjectRef subject() {
        return new SubjectRef(subjectType, subjectId);
    }
}
