package com.chargemon.flink.energy;

import com.chargemon.flink.serde.JsonTypeInfoFactory;
import java.time.Instant;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInfo;

/** A completed charging session with the energy it delivered. */
@TypeInfo(JsonTypeInfoFactory.class)
public record SessionEnergy(String sessionId, String stationId, Instant startedAt, Instant endedAt, long energyWh,
                            Set<String> groupIds) {

    public SessionEnergy {
        groupIds = groupIds == null ? Set.of() : Set.copyOf(groupIds);
    }

    public boolean isZeroEnergy() {
        return energyWh == 0;
    }
}
