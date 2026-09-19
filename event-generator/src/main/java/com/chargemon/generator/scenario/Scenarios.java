package com.chargemon.generator.scenario;

import com.chargemon.generator.StationSim;
import com.chargemon.ocpp.codec.envelope.RawEnvelope;
import com.chargemon.ocpp.codec.fixtures.Frames;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

/** Catalogue of profiles; each is a small variation on {@link NormalScenario}. */
public final class Scenarios {

    private Scenarios() {
    }

    public static Scenario byName(String name) {
        return switch (name.trim().toLowerCase(Locale.ROOT)) {
            case "normal" -> new NormalScenario();
            case "heartbeat-drop" -> new HeartbeatDrop();
            case "stuck-preparing" -> new StuckPreparing();
            case "zero-energy" -> new ZeroEnergy();
            case "boot-rejected" -> new BootRejected();
            case "call-error" -> new CallError();
            default -> throw new IllegalArgumentException("unknown profile: " + name);
        };
    }

    /** Heartbeats stop 90s after boot and never resume. */
    static final class HeartbeatDrop extends NormalScenario {
        @Override
        public String name() {
            return "heartbeat-drop";
        }

        @Override
        protected boolean heartbeatsEnabled(StationSim s, Instant now) {
            Instant first = s.state().get("firstTick", null);
            if (first == null) {
                s.state().put("firstTick", now);
                return true;
            }
            return Duration.between(first, now).compareTo(Duration.ofSeconds(90)) < 0;
        }
    }

    /** Sends Preparing once and then never progresses. */
    static final class StuckPreparing extends NormalScenario {
        @Override
        public String name() {
            return "stuck-preparing";
        }

        @Override
        protected void extra(StationSim s, Instant now, List<RawEnvelope> out) {
            if (!s.state().get("stuck", false)) {
                String uid = s.state().uniqueId();
                out.add(Emit.call(s, now, uid, "StatusNotification", status(s, "Preparing", now)));
                out.add(Emit.result(s, now.plusMillis(30), uid, Frames.obj()));
                s.state().put("stuck", true);
                s.state().put("nextSession", Instant.MAX);        // no sessions, stays stuck
            }
        }
    }

    /** Sessions complete normally but deliver 0 Wh. */
    static final class ZeroEnergy extends NormalScenario {
        @Override
        public String name() {
            return "zero-energy";
        }

        @Override
        protected long sessionEnergyWh(StationSim s) {
            return 0;
        }
    }

    /** CSMS rejects the boot. */
    static final class BootRejected extends NormalScenario {
        @Override
        public String name() {
            return "boot-rejected";
        }

        @Override
        protected String bootStatus() {
            return "Rejected";
        }
    }

    /** Every minute the station sends an unsupported action and the CSMS answers CALLERROR. */
    static final class CallError extends NormalScenario {
        @Override
        public String name() {
            return "call-error";
        }

        @Override
        protected void extra(StationSim s, Instant now, List<RawEnvelope> out) {
            Instant last = s.state().get("lastErr", Instant.EPOCH);
            if (Duration.between(last, now).compareTo(Duration.ofMinutes(1)) >= 0) {
                String uid = s.state().uniqueId();
                out.add(Emit.call(s, now, uid, "NotifyChargingLimit", Frames.obj("evseId", 1)));
                out.add(Emit.error(s, now.plusMillis(40), uid, "InternalError", "simulated failure"));
                s.state().put("lastErr", now);
            }
        }
    }
}
