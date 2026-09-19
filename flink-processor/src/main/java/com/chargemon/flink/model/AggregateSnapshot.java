package com.chargemon.flink.model;

import com.chargemon.alert.SubjectType;
import com.chargemon.flink.serde.JsonTypeInfoFactory;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInfo;

/**
 * Current values of every configured window for one subject and aggregate source
 * (e.g. zeroEnergy: hourly=2, daily=5, rolling7d=9, rolling30d=20).
 */
@TypeInfo(JsonTypeInfoFactory.class)
public record AggregateSnapshot(String source, SubjectType subjectType, String subjectId, Map<String, Long> windows,
                                Instant asOf, Set<String> groupIds) {

    public AggregateSnapshot {
        windows = windows == null ? Map.of() : Map.copyOf(windows);
        groupIds = groupIds == null ? Set.of() : Set.copyOf(groupIds);
    }
}
