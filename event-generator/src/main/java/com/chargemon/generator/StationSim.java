package com.chargemon.generator;

import com.chargemon.generator.scenario.Scenario;
import com.chargemon.ocpp.codec.envelope.RawEnvelope;
import com.chargemon.ocpp.model.OcppVersion;
import java.time.Instant;
import java.util.List;
import java.util.Random;

/** One simulated station: identity + scenario + scratch state the scenario owns. */
public final class StationSim {

    private final String id;
    private final OcppVersion version;
    private final String vendor;
    private final String model;
    private final String site;
    private final Scenario scenario;
    private final Random rnd;
    private final Scenario.State state = new Scenario.State();

    StationSim(String id, OcppVersion version, String vendor, String model, String site, Scenario scenario, long seed) {
        this.id = id;
        this.version = version;
        this.vendor = vendor;
        this.model = model;
        this.site = site;
        this.scenario = scenario;
        this.rnd = new Random(seed);
    }

    public String id() {
        return id;
    }

    public OcppVersion version() {
        return version;
    }

    public String vendor() {
        return vendor;
    }

    public String model() {
        return model;
    }

    public String site() {
        return site;
    }

    public Random random() {
        return rnd;
    }

    public Scenario.State state() {
        return state;
    }

    /** Next envelopes for this station at simulated time {@code now}; may be empty. */
    List<RawEnvelope> tick(Instant now) {
        return scenario.tick(this, now);
    }
}
