package com.chargemon.flink.model;

/** One record from the compacted rules topic. {@code ruleJson == null} means delete. */
public record RuleChange(String ruleId, String ruleJson) implements java.io.Serializable {

    public boolean isDelete() {
        return ruleJson == null;
    }
}
