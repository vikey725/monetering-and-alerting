package com.chargemon.flink.aggregate;

/** Session that arrived too late to be counted in any window. */
public record LateSession(String subjectType, String subjectId, String sessionId, long endedAtMillis, long watermarkMillis) {
}
