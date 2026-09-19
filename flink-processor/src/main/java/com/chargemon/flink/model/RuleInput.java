package com.chargemon.flink.model;

import com.chargemon.flink.serde.JsonTypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInfo;

/** Union input for the station rule evaluator: exactly one of event / snapshot is set. */
@TypeInfo(JsonTypeInfoFactory.class)
public record RuleInput(EnrichedEvent event, AggregateSnapshot snapshot) {

    public static RuleInput of(EnrichedEvent e) {
        return new RuleInput(e, null);
    }

    public static RuleInput of(AggregateSnapshot s) {
        return new RuleInput(null, s);
    }

    public String stationId() {
        return event != null ? event.stationId() : snapshot.subjectId();
    }
}
