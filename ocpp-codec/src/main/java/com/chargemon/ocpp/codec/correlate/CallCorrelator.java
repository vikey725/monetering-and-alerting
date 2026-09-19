package com.chargemon.ocpp.codec.correlate;

import com.chargemon.common.json.JsonMapperFactory;
import com.chargemon.ocpp.codec.envelope.RawEnvelope;
import com.chargemon.ocpp.codec.frame.RawFrame;
import com.chargemon.ocpp.codec.mapper.MapperRegistry;
import com.chargemon.ocpp.codec.mapper.MappingException;
import com.chargemon.ocpp.model.CallFailed;
import com.chargemon.ocpp.model.CallTimedOut;
import com.chargemon.ocpp.model.CorrelatedEvent;
import com.chargemon.ocpp.model.Direction;
import com.chargemon.ocpp.model.EventMeta;
import com.chargemon.ocpp.model.OcppEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Pure request/response correlation for one station. Framework-free so it is
 * unit-testable; the Flink operator supplies the {@link PendingCallStore} and timers.
 *
 * <ul>
 *   <li>CALL: emit the mapped event immediately, register as pending. If the response already
 *       arrived (parked), join right away.</li>
 *   <li>CALLRESULT: join with the pending CALL (opposite direction, same uniqueId) via a correlated
 *       mapper; if the CALL has not been seen yet, park the response.</li>
 *   <li>CALLERROR: emit {@link CallFailed} (parked likewise when the CALL is unknown).</li>
 *   <li>timeout: emit {@link CallTimedOut}.</li>
 * </ul>
 */
public final class CallCorrelator {

    public static final String CALL_FAILED = "CallFailed";
    public static final String CALL_TIMED_OUT = "CallTimedOut";

    private final MapperRegistry registry;
    private final Duration timeout;
    private final ObjectMapper json;

    public CallCorrelator(MapperRegistry registry, Duration timeout) {
        this(registry, timeout, JsonMapperFactory.standard());
    }

    public CallCorrelator(MapperRegistry registry, Duration timeout, ObjectMapper json) {
        this.registry = registry;
        this.timeout = timeout;
        this.json = json;
    }

    public CorrelationOutcome onFrame(RawEnvelope env, RawFrame frame, PendingCallStore store) {
        return switch (frame) {
            case RawFrame.Call call -> onCall(env, call, store);
            case RawFrame.CallResult result -> onResponse(env, toPending(env, result), store);
            case RawFrame.CallError error -> onResponse(env, toPending(env, error), store);
        };
    }

    public CallTimedOut onTimeout(PendingCall pending, Instant now) {
        EventMeta meta = new EventMeta(pending.stationId(), pending.version(), pending.direction(), pending.uniqueId(),
                CALL_TIMED_OUT, now, now, pending.sourceRef());
        return new CallTimedOut(meta, pending.action(), pending.sentAt(), pending.direction());
    }

    // ------------------------------------------------------------------ CALL

    private CorrelationOutcome onCall(RawEnvelope env, RawFrame.Call call, PendingCallStore store) {
        EventMeta meta = meta(env, call.uniqueId(), call.action());
        OcppEvent event;
        try {
            event = registry.mapCall(meta, call.payload());
        } catch (MappingException e) {
            return CorrelationOutcome.drop(CorrelationOutcome.Drop.MAPPING_FAILED);
        }
        PendingCall pending = new PendingCall(env.stationId(), env.ocppVersion(), env.direction(), call.uniqueId(),
                call.action(), call.payload().toString(), env.receivedAt(), env.sourceRef());
        Optional<PendingResponse> early = store.getResponse(pending.key());
        if (early.isPresent()) {
            List<OcppEvent> events = new ArrayList<>(2);
            events.add(event);
            CorrelationOutcome joined = join(pending, early.get(), env);
            events.addAll(joined.events());
            CorrelationOutcome out = CorrelationOutcome.events(events).withReleasedResponse(pending.key());
            return joined.drop().map(out::withDrop).orElse(out);
        }
        return CorrelationOutcome.eventAndRegister(event,
                new CorrelationOutcome.Registered(pending.key(), pending, env.receivedAt().plus(timeout)));
    }

    // ------------------------------------------------------------------ CALLRESULT / CALLERROR

    private CorrelationOutcome onResponse(RawEnvelope env, PendingResponse response, PendingCallStore store) {
        String callKey = response.callKey();
        Optional<PendingCall> pending = store.get(callKey);
        if (pending.isEmpty()) {
            if (response.isError()) {
                // Even without the CALL, an error is worth surfacing; park too so a late CALL can enrich it.
                CallFailed failed = callFailed(env, response, null);
                return new CorrelationOutcome(List.of(failed), Optional.empty(), Optional.empty(),
                        Optional.of(new CorrelationOutcome.EarlyResponse(callKey, response, env.receivedAt().plus(timeout))),
                        Optional.empty(), Optional.empty());
            }
            return CorrelationOutcome.park(
                    new CorrelationOutcome.EarlyResponse(callKey, response, env.receivedAt().plus(timeout)));
        }
        CorrelationOutcome joined = join(pending.get(), response, env);
        CorrelationOutcome out = CorrelationOutcome.eventsAndRelease(joined.events(), callKey);
        return joined.drop().map(out::withDrop).orElse(out);
    }

    /** Joins a CALL with its response into zero or more events. */
    private CorrelationOutcome join(PendingCall call, PendingResponse response, RawEnvelope env) {
        if (response.isError()) {
            return CorrelationOutcome.events(List.of(callFailed(env, response, call.action())));
        }
        try {
            Optional<CorrelatedEvent> joined = registry.mapResult(callMeta(call), parse(call.payloadJson()),
                    parse(response.payloadJson()), response.receivedAt());
            return CorrelationOutcome.events(joined.map(e -> List.<OcppEvent>of(e)).orElse(List.of()));
        } catch (MappingException e) {
            return CorrelationOutcome.drop(CorrelationOutcome.Drop.MAPPING_FAILED);
        }
    }

    private CallFailed callFailed(RawEnvelope env, PendingResponse r, String requestAction) {
        EventMeta meta = new EventMeta(env.stationId(), env.ocppVersion(), r.direction(), r.uniqueId(), CALL_FAILED,
                r.receivedAt(), r.receivedAt(), env.sourceRef());
        return new CallFailed(meta, requestAction, r.errorCode(), r.errorDescription(), r.errorDetailsJson(),
                r.direction().opposite());
    }

    private static PendingResponse toPending(RawEnvelope env, RawFrame.CallResult r) {
        return new PendingResponse(env.direction(), r.uniqueId(), r.payload().toString(), null, null, null, env.receivedAt());
    }

    private static PendingResponse toPending(RawEnvelope env, RawFrame.CallError e) {
        return new PendingResponse(env.direction(), e.uniqueId(), null, e.errorCode(), e.description(),
                e.details().toString(), env.receivedAt());
    }

    private static EventMeta meta(RawEnvelope env, String uniqueId, String action) {
        return new EventMeta(env.stationId(), env.ocppVersion(), env.direction(), uniqueId, action, env.receivedAt(),
                env.receivedAt(), env.sourceRef());
    }

    private static EventMeta callMeta(PendingCall p) {
        return new EventMeta(p.stationId(), p.version(), p.direction(), p.uniqueId(), p.action(), p.sentAt(), p.sentAt(),
                p.sourceRef());
    }

    private JsonNode parse(String payloadJson) {
        try {
            return json.readTree(payloadJson);
        } catch (IOException e) {
            throw new MappingException("stored pending payload is not JSON", e);
        }
    }

    static Direction requestDirectionOf(PendingResponse r) {
        return r.direction().opposite();
    }
}
