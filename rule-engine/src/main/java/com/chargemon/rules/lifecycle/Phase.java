package com.chargemon.rules.lifecycle;

public enum Phase {
    /** No condition, no alert. */
    IDLE,
    /** Condition true, waiting out the grace window. */
    PENDING,
    /** Alert raised and still active. */
    OPEN,
    /** Alert resolved; suppression window may still be running. */
    RESOLVED,
    /** Condition re-triggered inside the suppression window; no new alert. */
    SUPPRESSED
}
