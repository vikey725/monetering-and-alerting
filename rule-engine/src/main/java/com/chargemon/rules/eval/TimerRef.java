package com.chargemon.rules.eval;

/** Logical timer identity; re-scheduling the same ref replaces the previous deadline. */
public record TimerRef(String ruleId, String scope, String tag) {

    public String key() {
        return ruleId + "|" + scope + "|" + tag;
    }

    public static TimerRef parse(String key) {
        String[] p = key.split("\\|", -1);
        return new TimerRef(p[0], p.length > 1 ? p[1] : "", p.length > 2 ? p[2] : "");
    }
}
