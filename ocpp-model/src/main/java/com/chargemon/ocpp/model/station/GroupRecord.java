package com.chargemon.ocpp.model.station;

import java.util.Map;

/** Station group node; {@code parentId} null for roots. */
public record GroupRecord(String groupId, String parentId, String name, String level, Map<String, String> attributes,
                          boolean deleted) {

    public GroupRecord {
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }
}
