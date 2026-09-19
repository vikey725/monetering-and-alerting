package com.chargemon.generator.scenario;

import com.chargemon.generator.StationSim;
import com.chargemon.ocpp.codec.envelope.RawEnvelope;
import com.chargemon.ocpp.codec.fixtures.Frames;
import com.chargemon.ocpp.model.Direction;
import com.fasterxml.jackson.databind.JsonNode;

/** Envelope builders shared by scenarios. */
final class Emit {

    private Emit() {
    }

    static RawEnvelope call(StationSim s, java.time.Instant at, String uid, String action, JsonNode payload) {
        return new RawEnvelope(s.id(), s.version(), Direction.STATION_TO_CSMS, at, Frames.call(uid, action, payload), null);
    }

    static RawEnvelope result(StationSim s, java.time.Instant at, String uid, JsonNode payload) {
        return new RawEnvelope(s.id(), s.version(), Direction.CSMS_TO_STATION, at, Frames.callResult(uid, payload), null);
    }

    static RawEnvelope error(StationSim s, java.time.Instant at, String uid, String code, String description) {
        return new RawEnvelope(s.id(), s.version(), Direction.CSMS_TO_STATION, at, Frames.callError(uid, code, description), null);
    }

    static RawEnvelope csmsCall(StationSim s, java.time.Instant at, String uid, String action, JsonNode payload) {
        return new RawEnvelope(s.id(), s.version(), Direction.CSMS_TO_STATION, at, Frames.call(uid, action, payload), null);
    }
}
