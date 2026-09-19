package com.chargemon.ocpp.model.station;

import java.util.Map;
import java.util.Set;

/** Station master data as received from the upstream registry topic (already adapted to our shape). */
public record StationRecord(
        String stationId,
        String name,
        String vendor,
        String model,
        String firmware,
        String ocppVersion,
        Map<String, String> attributes,
        Set<String> groupIds,
        boolean deleted) {

    public StationRecord {
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
        groupIds = groupIds == null ? Set.of() : Set.copyOf(groupIds);
    }
}
