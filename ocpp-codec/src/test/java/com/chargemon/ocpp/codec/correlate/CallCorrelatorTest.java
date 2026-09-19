package com.chargemon.ocpp.codec.correlate;

import static org.assertj.core.api.Assertions.assertThat;

import com.chargemon.ocpp.codec.envelope.RawEnvelope;
import com.chargemon.ocpp.codec.fixtures.Frames;
import com.chargemon.ocpp.codec.fixtures.InMemoryPendingCallStore;
import com.chargemon.ocpp.codec.frame.FrameParser;
import com.chargemon.ocpp.codec.frame.RawFrame;
import com.chargemon.ocpp.codec.mapper.MapperRegistry;
import com.chargemon.ocpp.model.AuthStatus;
import com.chargemon.ocpp.model.BootCompleted;
import com.chargemon.ocpp.model.CallFailed;
import com.chargemon.ocpp.model.CallTimedOut;
import com.chargemon.ocpp.model.Direction;
import com.chargemon.ocpp.model.OcppVersion;
import com.chargemon.ocpp.model.RegistrationStatus;
import com.chargemon.ocpp.model.SessionStarted;
import com.chargemon.ocpp.model.StartTransaction;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class CallCorrelatorTest {

    private final CallCorrelator correlator = new CallCorrelator(MapperRegistry.fromServiceLoader(), Duration.ofSeconds(60));
    private final FrameParser frames = new FrameParser();
    private final InMemoryPendingCallStore store = new InMemoryPendingCallStore();

    private CorrelationOutcome feed(RawEnvelope env) {
        RawFrame f = ((com.chargemon.common.result.Result.Ok<RawFrame, String>) frames.parse(env.frame())).value();
        CorrelationOutcome out = correlator.onFrame(env, f, store);
        out.registered().ifPresent(r -> store.put(r.key(), r.call()));
        out.releasedKey().ifPresent(store::remove);
        out.earlyResponse().ifPresent(e -> store.putResponse(e.callKey(), e.response()));
        out.releasedResponseKey().ifPresent(store::removeResponse);
        return out;
    }

    @Test
    void joinsStartTransactionWithResponseIntoSessionStarted() {
        JsonNode call = Frames.call("10", "StartTransaction", Frames.startTx16(1, "TAG", 100, Frames.T0));
        CorrelationOutcome a = feed(Frames.fromStation("ST-1", OcppVersion.V16, Frames.T0, call));
        assertThat(a.events()).singleElement().isInstanceOf(StartTransaction.class);
        assertThat(a.registered()).isPresent();
        assertThat(store.size()).isEqualTo(1);

        JsonNode resp = Frames.callResult("10", Frames.startTxResponse16(555, "Accepted"));
        CorrelationOutcome b = feed(Frames.fromCsms("ST-1", OcppVersion.V16, Frames.T0.plusSeconds(1), resp));
        assertThat(b.events()).singleElement().isInstanceOf(SessionStarted.class);
        SessionStarted ss = (SessionStarted) b.events().get(0);
        assertThat(ss.sessionId().canonical()).isEqualTo("1.6:ST-1:555");
        assertThat(ss.idTagStatus()).isEqualTo(AuthStatus.ACCEPTED);
        assertThat(ss.meterStart()).isEqualTo(100);
        assertThat(ss.meta().receivedAt()).isEqualTo(Frames.T0.plusSeconds(1));
        assertThat(store.size()).isZero();
    }

    @Test
    void bootRejectedSurfacesAsBootCompleted() {
        feed(Frames.fromStation("ST-1", OcppVersion.V201, Frames.T0,
                Frames.call("1", "BootNotification", Frames.boot201("ACME", "X1", "1.0", "PowerUp"))));
        CorrelationOutcome b = feed(Frames.fromCsms("ST-1", OcppVersion.V201, Frames.T0,
                Frames.callResult("1", Frames.bootResponse("Rejected", 30, Frames.T0))));
        BootCompleted bc = (BootCompleted) b.events().get(0);
        assertThat(bc.status()).isEqualTo(RegistrationStatus.REJECTED);
        assertThat(bc.vendor()).isEqualTo("ACME");
    }

    @Test
    void callErrorProducesCallFailedWithRequestAction() {
        feed(Frames.fromStation("ST-1", OcppVersion.V201, Frames.T0,
                Frames.call("5", "NotifyEvent", Frames.obj("seqNo", 0))));
        CorrelationOutcome b = feed(Frames.fromCsms("ST-1", OcppVersion.V201, Frames.T0,
                Frames.callError("5", "NotImplemented", "nope")));
        CallFailed cf = (CallFailed) b.events().get(0);
        assertThat(cf.requestAction()).isEqualTo("NotifyEvent");
        assertThat(cf.errorCode()).isEqualTo("NotImplemented");
        assertThat(cf.requestDirection()).isEqualTo(Direction.STATION_TO_CSMS);
    }

    @Test
    void responseBeforeCallIsParkedAndJoinedWhenCallArrives() {
        CorrelationOutcome b = feed(Frames.fromCsms("ST-1", OcppVersion.V16, Frames.T0,
                Frames.callResult("10", Frames.startTxResponse16(555, "Accepted"))));
        assertThat(b.events()).isEmpty();
        assertThat(b.earlyResponse()).isPresent();
        assertThat(store.parked()).isEqualTo(1);

        CorrelationOutcome a = feed(Frames.fromStation("ST-1", OcppVersion.V16, Frames.T0.plusSeconds(1),
                Frames.call("10", "StartTransaction", Frames.startTx16(1, "TAG", 100, Frames.T0))));
        assertThat(a.events()).hasSize(2);
        assertThat(a.events().get(0)).isInstanceOf(StartTransaction.class);
        assertThat(a.events().get(1)).isInstanceOf(SessionStarted.class);
        assertThat(a.registered()).isEmpty();
        assertThat(store.parked()).isZero();
        assertThat(store.size()).isZero();
    }

    @Test
    void sameUniqueIdInBothDirectionsDoesNotCollide() {
        feed(Frames.fromStation("ST-1", OcppVersion.V16, Frames.T0, Frames.call("7", "Heartbeat", Frames.heartbeat())));
        feed(Frames.fromCsms("ST-1", OcppVersion.V16, Frames.T0, Frames.call("7", "Reset", Frames.obj("type", "Soft"))));
        assertThat(store.size()).isEqualTo(2);
        CorrelationOutcome r = feed(Frames.fromStation("ST-1", OcppVersion.V16, Frames.T0,
                Frames.callResult("7", Frames.obj("status", "Accepted"))));
        assertThat(r.releasedKey()).contains(PendingCall.key(Direction.CSMS_TO_STATION, "7"));
        assertThat(store.size()).isEqualTo(1);
    }

    @Test
    void timeoutYieldsCallTimedOut() {
        CorrelationOutcome a = feed(Frames.fromCsms("ST-1", OcppVersion.V16, Frames.T0,
                Frames.call("9", "RemoteStartTransaction", Frames.obj("idTag", "T"))));
        Instant later = Frames.T0.plusSeconds(61);
        CallTimedOut t = correlator.onTimeout(a.registered().get().call(), later);
        assertThat(t.requestAction()).isEqualTo("RemoteStartTransaction");
        assertThat(t.requestDirection()).isEqualTo(Direction.CSMS_TO_STATION);
        assertThat(a.registered().get().deadline()).isEqualTo(Frames.T0.plusSeconds(60));
    }
}
