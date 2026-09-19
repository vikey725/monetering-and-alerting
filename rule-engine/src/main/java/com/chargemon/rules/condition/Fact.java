package com.chargemon.rules.condition;

import java.util.Optional;

/**
 * Read-only view of the data a condition can inspect, addressed by dotted path
 * ({@code event.status}, {@code station.vendor}, {@code agg.zeroEnergy.rolling7d}).
 * Keeps the rule engine independent of any concrete event model.
 */
@FunctionalInterface
public interface Fact {

    Optional<Object> get(String path);

    static Fact empty() {
        return path -> Optional.empty();
    }
}
