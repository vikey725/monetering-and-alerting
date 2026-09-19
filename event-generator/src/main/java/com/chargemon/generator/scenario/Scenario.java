package com.chargemon.generator.scenario;

import com.chargemon.generator.StationSim;
import com.chargemon.ocpp.codec.envelope.RawEnvelope;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Behaviour profile of a simulated station. Implementations keep per-station scratch data in {@link State}. */
public interface Scenario {

    String name();

    List<RawEnvelope> tick(StationSim station, Instant now);

    /** Free-form per-station scratch state. */
    final class State {
        public final Map<String, Object> values = new HashMap<>();
        public int nextUniqueId = 1;

        @SuppressWarnings("unchecked")
        public <T> T get(String key, T def) {
            return (T) values.getOrDefault(key, def);
        }

        public void put(String key, Object v) {
            values.put(key, v);
        }

        public String uniqueId() {
            return Integer.toString(nextUniqueId++);
        }
    }
}
