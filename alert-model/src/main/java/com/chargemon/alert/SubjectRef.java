package com.chargemon.alert;

import java.util.Objects;

/** What an alert is about: one station or one group. */
public record SubjectRef(SubjectType type, String id) {

    public SubjectRef {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(id, "id");
    }

    public static SubjectRef station(String id) {
        return new SubjectRef(SubjectType.STATION, id);
    }

    public static SubjectRef group(String id) {
        return new SubjectRef(SubjectType.GROUP, id);
    }

    public String key() {
        return type.name() + "|" + id;
    }
}
