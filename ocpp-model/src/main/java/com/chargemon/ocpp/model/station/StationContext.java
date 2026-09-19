package com.chargemon.ocpp.model.station;

import java.util.Map;
import java.util.Set;

/**
 * Station metadata attached to every enriched event, including the transitive
 * closure of group memberships so rules and aggregators never walk the hierarchy.
 */
public record StationContext(
        String id,
        String vendor,
        String model,
        String firmware,
        Map<String, String> attributes,
        Set<String> directGroupIds,
        Set<String> allGroupIds,
        boolean known) {

    public StationContext {
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
        directGroupIds = directGroupIds == null ? Set.of() : Set.copyOf(directGroupIds);
        allGroupIds = allGroupIds == null ? Set.of() : Set.copyOf(allGroupIds);
    }

    public static StationContext unknown(String id) {
        return new StationContext(id, null, null, null, Map.of(), Set.of(), Set.of(), false);
    }

    public static StationContext from(StationRecord r, Set<String> allGroupIds) {
        return new StationContext(r.stationId(), r.vendor(), r.model(), r.firmware(), r.attributes(), r.groupIds(),
                allGroupIds, true);
    }
}
