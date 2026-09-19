package com.chargemon.flink.model;

import com.chargemon.alert.SubjectRef;
import com.chargemon.alert.SubjectType;

/** Key of the lifecycle operator: one state machine per (rule, subject). */
public record AlertKey(String ruleId, String subjectType, String subjectId) {

    public static AlertKey of(String ruleId, SubjectRef subject) {
        return new AlertKey(ruleId, subject.type().name(), subject.id());
    }

    public SubjectRef subject() {
        return new SubjectRef(SubjectType.valueOf(subjectType), subjectId);
    }
}
