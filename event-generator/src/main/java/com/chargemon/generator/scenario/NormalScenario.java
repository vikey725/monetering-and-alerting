package com.chargemon.generator.scenario;

import com.chargemon.generator.StationSim;
import com.chargemon.ocpp.codec.envelope.RawEnvelope;
import com.chargemon.ocpp.codec.fixtures.Frames;
import com.chargemon.ocpp.model.OcppVersion;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Healthy station: boot once, heartbeat every 30s, a charging session every few
 * minutes with real energy. Subclasses tweak knobs to produce faults.
 */
public class NormalScenario implements Scenario {

    static final Duration HEARTBEAT = Duration.ofSeconds(30);
    static final Duration SESSION_GAP = Duration.ofMinutes(4);
    static final Duration SESSION_LENGTH = Duration.ofMinutes(2);

    @Override
    public String name() {
        return "normal";
    }

    @Override
    public List<RawEnvelope> tick(StationSim s, Instant now) {
        List<RawEnvelope> out = new ArrayList<>(4);
        State st = s.state();
        if (!st.get("booted", false)) {
            boot(s, now, out);
            st.put("booted", true);
            st.put("lastHb", now);
            st.put("nextSession", now.plusSeconds(10 + s.random().nextInt(60)));
            return out;
        }
        if (heartbeatsEnabled(s, now) && Duration.between(st.get("lastHb", now), now).compareTo(HEARTBEAT) >= 0) {
            String uid = st.uniqueId();
            out.add(Emit.call(s, now, uid, "Heartbeat", Frames.heartbeat()));
            out.add(Emit.result(s, now.plusMillis(50), uid, Frames.obj("currentTime", now.toString())));
            st.put("lastHb", now);
        }
        session(s, now, out);
        extra(s, now, out);
        return out;
    }

    protected boolean heartbeatsEnabled(StationSim s, Instant now) {
        return true;
    }

    protected long sessionEnergyWh(StationSim s) {
        return 500 + s.random().nextInt(20_000);
    }

    protected void extra(StationSim s, Instant now, List<RawEnvelope> out) {
    }

    protected void boot(StationSim s, Instant now, List<RawEnvelope> out) {
        String uid = s.state().uniqueId();
        if (s.version() == OcppVersion.V16) {
            out.add(Emit.call(s, now, uid, "BootNotification", Frames.boot16(s.vendor(), s.model(), "1.0.0")));
        } else {
            out.add(Emit.call(s, now, uid, "BootNotification", Frames.boot201(s.vendor(), s.model(), "1.0.0", "PowerUp")));
        }
        out.add(Emit.result(s, now.plusMillis(80), uid, Frames.bootResponse(bootStatus(), 30, now)));
        String uid2 = s.state().uniqueId();
        out.add(Emit.call(s, now.plusMillis(200), uid2, "StatusNotification", status(s, "Available", now)));
        out.add(Emit.result(s, now.plusMillis(250), uid2, Frames.obj()));
    }

    protected String bootStatus() {
        return "Accepted";
    }

    private void session(StationSim s, Instant now, List<RawEnvelope> out) {
        State st = s.state();
        Instant next = st.get("nextSession", now);
        boolean inSession = st.get("inSession", false);
        if (!inSession && !now.isBefore(next)) {
            startSession(s, now, out);
        } else if (inSession && !now.isBefore(st.get("sessionEnd", now))) {
            endSession(s, now, out);
        }
    }

    private void startSession(StationSim s, Instant now, List<RawEnvelope> out) {
        State st = s.state();
        long meterStart = st.get("meter", 0L);
        st.put("meterStart", meterStart);
        st.put("inSession", true);
        st.put("sessionEnd", now.plus(SESSION_LENGTH));
        String uidPrep = st.uniqueId();
        out.add(Emit.call(s, now, uidPrep, "StatusNotification", status(s, "Preparing", now)));
        out.add(Emit.result(s, now.plusMillis(30), uidPrep, Frames.obj()));
        if (s.version() == OcppVersion.V16) {
            String uid = st.uniqueId();
            out.add(Emit.call(s, now.plusSeconds(1), uid, "StartTransaction", Frames.startTx16(1, "TAG-" + s.id(), meterStart, now)));
            int txId = st.get("txCounter", 1000) + 1;
            st.put("txCounter", txId);
            st.put("txId", Integer.toString(txId));
            out.add(Emit.result(s, now.plusSeconds(1).plusMillis(60), uid, Frames.startTxResponse16(txId, "Accepted")));
        } else {
            String txId = s.id() + "-" + now.getEpochSecond();
            st.put("txId", txId);
            String uid = st.uniqueId();
            out.add(Emit.call(s, now.plusSeconds(1), uid, "TransactionEvent",
                    Frames.txEvent201("Started", txId, 0, now, "Authorized", meterStart, 1)));
            out.add(Emit.result(s, now.plusSeconds(1).plusMillis(60), uid, Frames.obj()));
        }
        String uidCh = st.uniqueId();
        out.add(Emit.call(s, now.plusSeconds(2), uidCh, "StatusNotification", status(s, "Charging", now.plusSeconds(2))));
        out.add(Emit.result(s, now.plusSeconds(2).plusMillis(30), uidCh, Frames.obj()));
    }

    private void endSession(StationSim s, Instant now, List<RawEnvelope> out) {
        State st = s.state();
        long meterStart = st.get("meterStart", 0L);
        long meterStop = meterStart + sessionEnergyWh(s);
        st.put("meter", meterStop);
        st.put("inSession", false);
        st.put("nextSession", now.plus(SESSION_GAP).plusSeconds(s.random().nextInt(120)));
        String txId = st.get("txId", "0");
        String uid = st.uniqueId();
        if (s.version() == OcppVersion.V16) {
            out.add(Emit.call(s, now, uid, "StopTransaction", Frames.stopTx16(Integer.parseInt(txId), meterStop, "EVDisconnected", now)));
        } else {
            out.add(Emit.call(s, now, uid, "TransactionEvent",
                    Frames.txEvent201("Ended", txId, 2, now, "EVCommunicationLost", meterStop, 1)));
        }
        out.add(Emit.result(s, now.plusMillis(60), uid, Frames.obj()));
        String uidAv = st.uniqueId();
        out.add(Emit.call(s, now.plusSeconds(1), uidAv, "StatusNotification", status(s, "Available", now.plusSeconds(1))));
        out.add(Emit.result(s, now.plusSeconds(1).plusMillis(30), uidAv, Frames.obj()));
    }

    static com.fasterxml.jackson.databind.node.ObjectNode status(StationSim s, String status, Instant at) {
        if (s.version() == OcppVersion.V16) {
            return Frames.status16(1, status, "NoError", at);
        }
        return Frames.status201(1, 1, status.equals("Charging") || status.equals("Preparing") ? "Occupied" : status, at);
    }
}
