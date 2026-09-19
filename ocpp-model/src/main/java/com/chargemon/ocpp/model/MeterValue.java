package com.chargemon.ocpp.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public record MeterValue(Instant timestamp, List<SampledValue> sampledValues) {

    public static final String ENERGY_ACTIVE_IMPORT_REGISTER = "Energy.Active.Import.Register";

    public record SampledValue(BigDecimal value, String measurand, String context, String unit, String location, String phase) {
    }

    /** First sampled value for the given measurand (defaults to energy register when measurand is absent, per spec). */
    public Optional<SampledValue> find(String measurand) {
        if (sampledValues == null) {
            return Optional.empty();
        }
        return sampledValues.stream()
                .filter(sv -> measurand.equals(sv.measurand() == null ? ENERGY_ACTIVE_IMPORT_REGISTER : sv.measurand()))
                .findFirst();
    }
}
