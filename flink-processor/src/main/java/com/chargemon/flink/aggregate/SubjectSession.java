package com.chargemon.flink.aggregate;

import com.chargemon.flink.serde.JsonTypeInfoFactory;
import java.time.Instant;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInfo;

/** A zero-energy session attributed to one subject (the station itself or one of its groups). */
@TypeInfo(JsonTypeInfoFactory.class)
public record SubjectSession(SubjectKey subject, String sessionId, Instant endedAt, Set<String> groupIds) {

    public SubjectSession {
        groupIds = groupIds == null ? Set.of() : Set.copyOf(groupIds);
    }
}
