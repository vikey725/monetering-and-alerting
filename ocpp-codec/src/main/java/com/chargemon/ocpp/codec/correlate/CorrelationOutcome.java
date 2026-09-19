package com.chargemon.ocpp.codec.correlate;

import com.chargemon.ocpp.model.OcppEvent;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * What the correlator decided for one frame: events to emit, a pending call to
 * register (with its timeout deadline), a pending call released, or a drop.
 */
public record CorrelationOutcome(
        List<OcppEvent> events,
        Optional<Registered> registered,
        Optional<String> releasedKey,
        Optional<EarlyResponse> earlyResponse,
        Optional<String> releasedResponseKey,
        Optional<Drop> drop) {

    public record Registered(String key, PendingCall call, Instant deadline) {
    }

    /** A response with no CALL yet; parked under the CALL's key until it arrives or the deadline passes. */
    public record EarlyResponse(String callKey, PendingResponse response, Instant deadline) {
    }

    public enum Drop { MAPPING_FAILED }

    public static CorrelationOutcome events(List<OcppEvent> events) {
        return new CorrelationOutcome(events, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty());
    }

    public static CorrelationOutcome eventAndRegister(OcppEvent event, Registered registered) {
        return new CorrelationOutcome(List.of(event), Optional.of(registered), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
    }

    public static CorrelationOutcome eventsAndRelease(List<OcppEvent> events, String releasedKey) {
        return new CorrelationOutcome(events, Optional.empty(), Optional.of(releasedKey), Optional.empty(),
                Optional.empty(), Optional.empty());
    }

    public static CorrelationOutcome park(EarlyResponse early) {
        return new CorrelationOutcome(List.of(), Optional.empty(), Optional.empty(), Optional.of(early), Optional.empty(),
                Optional.empty());
    }

    public static CorrelationOutcome drop(Drop reason) {
        return new CorrelationOutcome(List.of(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.of(reason));
    }

    public CorrelationOutcome withReleasedResponse(String callKey) {
        return new CorrelationOutcome(events, registered, releasedKey, earlyResponse, Optional.of(callKey), drop);
    }

    public CorrelationOutcome withDrop(Drop reason) {
        return new CorrelationOutcome(events, registered, releasedKey, earlyResponse, releasedResponseKey,
                Optional.of(reason));
    }
}
