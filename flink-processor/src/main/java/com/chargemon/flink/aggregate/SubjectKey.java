package com.chargemon.flink.aggregate;

import com.chargemon.alert.SubjectType;

public record SubjectKey(String subjectType, String subjectId) {

    public static SubjectKey station(String id) {
        return new SubjectKey(SubjectType.STATION.name(), id);
    }

    public static SubjectKey group(String id) {
        return new SubjectKey(SubjectType.GROUP.name(), id);
    }

    public SubjectType type() {
        return SubjectType.valueOf(subjectType);
    }
}
