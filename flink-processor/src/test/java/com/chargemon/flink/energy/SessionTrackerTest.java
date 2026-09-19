package com.chargemon.flink.energy;

import static org.assertj.core.api.Assertions.assertThat;

import com.chargemon.ocpp.codec.fixtures.Frames;
import com.chargemon.ocpp.codec.mapper.MapperRegistry;
import com.chargemon.ocpp.model.Direction;
import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.OcppEvent;
import com.chargemon.ocpp.model.OcppVersion;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SessionTrackerTest {

    private final MapperRegistry registry = MapperRegistry.fromServiceLoader();
    private final SessionTracker tracker = new SessionTracker();

    private OcppEvent event(OcppVersion v, String action, com.fasterxml.jackson.databind.JsonNode payload) {
        return registry.mapCall(new EventMeta("ST", v, Direction.STATION_TO_CSMS, "1", action, Frames.T0, null, "x"), payload);
    }

    @Test
    void v201StartedUpdatedEndedComputesDelta() {
        OcppEvent started = event(OcppVersion.V201, "TransactionEvent", Frames.txEvent201("Started", "t1", 0, Frames.T0, "Authorized", 100L, 1));
        OcppEvent updated = event(OcppVersion.V201, "TransactionEvent", Frames.txEvent201("Updated", "t1", 1, Frames.T0.plusSeconds(60), "MeterValuePeriodic", 150L, 1));
        OcppEvent ended = event(OcppVersion.V201, "TransactionEvent", Frames.txEvent201("Ended", "t1", 2, Frames.T0.plusSeconds(120), "EVDisconnected", 180L, 1));

        SessionTracker.Step a = tracker.apply(started, null, Set.of("g"));
        SessionTracker.Step b = tracker.apply(updated, a.track(), Set.of());
        SessionTracker.Step c = tracker.apply(ended, b.track(), Set.of());
        assertThat(c.ended()).isPresent();
        assertThat(c.ended().get().energyWh()).isEqualTo(80);
        assertThat(c.ended().get().groupIds()).containsExactly("g");
        assertThat(c.track()).isNull();
    }

    @Test
    void v201EndedWithoutStartUsesLastRegisterOnly_thenZeroIfNoBegin() {
        OcppEvent ended = event(OcppVersion.V201, "TransactionEvent", Frames.txEvent201("Ended", "t2", 2, Frames.T0, "EVDisconnected", 500L, 1));
        SessionTracker.Step s = tracker.apply(ended, null, Set.of());
        assertThat(s.ended()).isEmpty();          // no start reading: energy unknown, not reported
    }

    @Test
    void v16StopMinusStartAndZeroEnergyFlag() {
        OcppEvent stop = event(OcppVersion.V16, "StopTransaction", Frames.stopTx16(77, 1000, "Local", Frames.T0.plusSeconds(30)));
        SessionTrack track = new SessionTrack(Frames.T0, 1000L, 1000L, Set.of("site:1"));
        SessionTracker.Step s = tracker.apply(stop, track, Set.of());
        assertThat(s.sessionId()).isEqualTo("1.6:ST:77");
        assertThat(s.ended().get().isZeroEnergy()).isTrue();
    }
}
