package com.chargemon.flink.energy;

import com.chargemon.ocpp.model.MeterValue;
import com.chargemon.ocpp.model.MeterValues;
import com.chargemon.ocpp.model.OcppEvent;
import com.chargemon.ocpp.model.SessionStarted;
import com.chargemon.ocpp.model.StopTransaction;
import com.chargemon.ocpp.model.TransactionEvent;
import com.chargemon.ocpp.model.session.SessionId;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Version-agnostic session energy bookkeeping. Pure: given an event and the
 * current track, returns the next track and (on session end) the result.
 *
 * <ul>
 *   <li>1.6: {@code SessionStarted.meterStart} .. {@code StopTransaction.meterStop}</li>
 *   <li>2.0.1: energy register in TransactionEvent Started/Updated/Ended meter values</li>
 * </ul>
 */
public final class SessionTracker {

    public record Step(String sessionId, SessionTrack track, Optional<SessionEnergy> ended) {
        static Step none() {
            return new Step(null, null, Optional.empty());
        }

        public boolean touches() {
            return sessionId != null;
        }
    }

    public Step apply(OcppEvent event, SessionTrack current, Set<String> groupIds) {
        return switch (event) {
            case SessionStarted s -> new Step(s.sessionId().canonical(),
                    new SessionTrack(s.startedAt(), s.meterStart(), s.meterStart(), groupIds), Optional.empty());
            case StopTransaction s -> {
                String id = s.sessionId().canonical();
                Long start = current == null ? beginFrom(s.transactionData()) : current.meterStart();
                if (start == null) {
                    yield new Step(id, null, Optional.empty());     // cannot compute energy; drop track
                }
                Instant startedAt = current == null ? s.timestamp() : current.startedAt();
                yield new Step(id, null, Optional.of(new SessionEnergy(id, s.stationId(), startedAt, s.timestamp(),
                        Math.max(0, s.meterStop() - start), groups(current, groupIds))));
            }
            case MeterValues m when m.transactionId() != null -> {
                String id = SessionId.of(m.meta().version(), m.stationId(), Integer.toString(m.transactionId())).canonical();
                if (current == null) {
                    yield Step.none();
                }
                Optional<Long> reg = register(m.meterValues());
                yield new Step(id, reg.map(current::withRegister).orElse(current), Optional.empty());
            }
            case TransactionEvent t -> onTransactionEvent(t, current, groupIds);
            default -> Step.none();
        };
    }

    /** Session id the event refers to, if any (lets the operator load the right track before applying). */
    public Optional<String> sessionIdOf(OcppEvent event) {
        return switch (event) {
            case SessionStarted s -> Optional.of(s.sessionId().canonical());
            case StopTransaction s -> Optional.of(s.sessionId().canonical());
            case MeterValues m when m.transactionId() != null ->
                    Optional.of(SessionId.of(m.meta().version(), m.stationId(), Integer.toString(m.transactionId())).canonical());
            case TransactionEvent t -> Optional.of(t.sessionId().canonical());
            default -> Optional.empty();
        };
    }

    private Step onTransactionEvent(TransactionEvent t, SessionTrack current, Set<String> groupIds) {
        String id = t.sessionId().canonical();
        Optional<Long> reg = register(t.meterValues());
        Instant at = t.timestamp() == null ? t.meta().receivedAt() : t.timestamp();
        return switch (t.eventType()) {
            case STARTED -> new Step(id, new SessionTrack(at, reg.orElse(null), reg.orElse(null), groupIds), Optional.empty());
            case UPDATED -> {
                SessionTrack base = current == null ? new SessionTrack(at, null, null, groupIds) : current;
                yield new Step(id, reg.map(base::withRegister).orElse(base), Optional.empty());
            }
            case ENDED -> {
                Long start = current == null ? beginFrom(t.meterValues()) : current.meterStart();
                Long end = reg.orElse(current == null ? null : current.lastRegister());
                if (start == null || end == null) {
                    yield new Step(id, null, Optional.empty());
                }
                Instant startedAt = current == null ? at : current.startedAt();
                yield new Step(id, null, Optional.of(new SessionEnergy(id, t.stationId(), startedAt, at,
                        Math.max(0, end - start), groups(current, groupIds))));
            }
        };
    }

    /** Latest energy register reading in the list (any context). */
    static Optional<Long> register(List<MeterValue> values) {
        if (values == null) {
            return Optional.empty();
        }
        Optional<Long> last = Optional.empty();
        for (MeterValue mv : values) {
            Optional<Long> v = mv.find(MeterValue.ENERGY_ACTIVE_IMPORT_REGISTER).map(sv -> toWh(sv));
            if (v.isPresent()) {
                last = v;
            }
        }
        return last;
    }

    /** Energy register flagged as Transaction.Begin, for sessions whose start we never saw. */
    static Long beginFrom(List<MeterValue> values) {
        if (values == null) {
            return null;
        }
        for (MeterValue mv : values) {
            for (MeterValue.SampledValue sv : mv.sampledValues()) {
                boolean energy = sv.measurand() == null || MeterValue.ENERGY_ACTIVE_IMPORT_REGISTER.equals(sv.measurand());
                if (energy && "Transaction.Begin".equals(sv.context())) {
                    return toWh(sv);
                }
            }
        }
        return null;
    }

    private static long toWh(MeterValue.SampledValue sv) {
        if (sv.value() == null) {
            return 0;
        }
        return "kWh".equalsIgnoreCase(sv.unit()) ? sv.value().movePointRight(3).longValue() : sv.value().longValue();
    }

    private static Set<String> groups(SessionTrack current, Set<String> fallback) {
        return current != null && !current.groupIds().isEmpty() ? current.groupIds() : fallback;
    }
}
