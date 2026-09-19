package com.chargemon.rules.eval;

import com.chargemon.rules.condition.Fact;
import java.time.Instant;
import java.util.Set;

/**
 * One unit of input to station-scoped evaluators: either an event (name = action)
 * or an aggregate snapshot (name = source, e.g. {@code zeroEnergy}).
 *
 * @param eventTime  event time of the input (for context / windows)
 * @param groupIds   station's transitive group ids (copied onto signals)
 */
public record StationInput(Kind kind, String name, Fact fact, Instant eventTime, Set<String> groupIds) {

    public enum Kind { EVENT, AGGREGATE }

    public StationInput {
        groupIds = groupIds == null ? Set.of() : Set.copyOf(groupIds);
    }

    public static StationInput event(String action, Fact fact, Instant eventTime, Set<String> groupIds) {
        return new StationInput(Kind.EVENT, action, fact, eventTime, groupIds);
    }

    public static StationInput aggregate(String source, Fact fact, Instant at, Set<String> groupIds) {
        return new StationInput(Kind.AGGREGATE, source, fact, at, groupIds);
    }
}
