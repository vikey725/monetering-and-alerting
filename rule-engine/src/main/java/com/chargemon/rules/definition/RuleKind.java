package com.chargemon.rules.definition;

/**
 * Stage 1 kinds evaluate raw events per station. Stage 2 kinds evaluate the
 * alerts produced by stage 1 (composite / group rules).
 */
public enum RuleKind {
    EVENT(1), ABSENCE(1), STATE_DURATION(1), SEQUENCE(2), GROUP_AGGREGATE(2);

    private final int stage;

    RuleKind(int stage) {
        this.stage = stage;
    }

    public int stage() {
        return stage;
    }
}
