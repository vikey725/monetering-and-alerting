package com.chargemon.flink.correlate;

import com.chargemon.flink.model.DecodedFrame;
import com.chargemon.flink.serde.JsonTypes;
import com.chargemon.ocpp.codec.correlate.CallCorrelator;
import com.chargemon.ocpp.codec.correlate.CorrelationOutcome;
import com.chargemon.ocpp.codec.correlate.PendingCall;
import com.chargemon.ocpp.codec.correlate.PendingCallStore;
import com.chargemon.ocpp.codec.correlate.PendingResponse;
import com.chargemon.ocpp.codec.mapper.MapperRegistry;
import com.chargemon.ocpp.model.OcppEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keyed by station. Thin adapter: state and timers are Flink's, the decisions
 * are {@link CallCorrelator}'s. One processing-time timer per pending CALL.
 */
public final class CallCorrelationOperator extends KeyedProcessFunction<String, DecodedFrame, OcppEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(CallCorrelationOperator.class);

    private final Duration timeout;
    private final Duration ttl;

    private transient CallCorrelator correlator;
    private transient MapState<String, PendingCall> pending;
    private transient MapState<String, PendingResponse> parked;
    private transient Counter timedOut;
    private transient Counter parkedResponses;
    private transient Counter parkedExpired;
    private transient Counter mappingFailed;

    public CallCorrelationOperator(Duration timeout, Duration ttl) {
        this.timeout = timeout;
        this.ttl = ttl;
    }

    @Override
    public void open(OpenContext ctx) {
        correlator = new CallCorrelator(MapperRegistry.fromServiceLoader(), timeout);
        MapStateDescriptor<String, PendingCall> desc =
                new MapStateDescriptor<>("pendingCalls", Types.STRING, JsonTypes.of(PendingCall.class));
        desc.enableTimeToLive(StateTtlConfig.newBuilder(ttl)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupInRocksdbCompactFilter(1000)
                .build());
        pending = getRuntimeContext().getMapState(desc);
        MapStateDescriptor<String, PendingResponse> parkedDesc =
                new MapStateDescriptor<>("parkedResponses", Types.STRING, JsonTypes.of(PendingResponse.class));
        parkedDesc.enableTimeToLive(StateTtlConfig.newBuilder(ttl)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupInRocksdbCompactFilter(1000)
                .build());
        parked = getRuntimeContext().getMapState(parkedDesc);
        var metrics = getRuntimeContext().getMetricGroup();
        timedOut = metrics.counter("callsTimedOut");
        parkedResponses = metrics.counter("responsesParked");
        parkedExpired = metrics.counter("responsesExpiredUnmatched");
        mappingFailed = metrics.counter("mappingFailed");
    }

    @Override
    public void processElement(DecodedFrame frame, Context ctx, Collector<OcppEvent> out) throws Exception {
        CorrelationOutcome outcome = correlator.onFrame(frame.envelope(), frame.frame(), new MapStateStore(pending, parked));
        if (LOG.isDebugEnabled()) {
            LOG.debug("correlate station={} frame={} -> {} event(s) {}", frame.envelope().stationId(), frame.frame().type(),
                    outcome.events().size(), outcome.events().stream().map(OcppEvent::action).toList());
        }
        outcome.events().forEach(out::collect);
        if (outcome.registered().isPresent()) {
            CorrelationOutcome.Registered r = outcome.registered().get();
            pending.put(r.key(), r.call());
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + timeout.toMillis());
        }
        if (outcome.releasedKey().isPresent()) {
            pending.remove(outcome.releasedKey().get());
        }
        if (outcome.earlyResponse().isPresent()) {
            CorrelationOutcome.EarlyResponse e = outcome.earlyResponse().get();
            parked.put(e.callKey(), e.response());
            parkedResponses.inc();
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + timeout.toMillis());
        }
        if (outcome.releasedResponseKey().isPresent()) {
            parked.remove(outcome.releasedResponseKey().get());
        }
        outcome.drop().ifPresent(d -> mappingFailed.inc());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OcppEvent> out) throws Exception {
        Instant now = Instant.ofEpochMilli(timestamp);
        List<String> expired = new ArrayList<>();
        for (Map.Entry<String, PendingCall> e : pending.entries()) {
            // Deadline is measured in processing time from registration, so compare against wall time elapsed.
            if (!e.getValue().sentAt().plus(timeout).isAfter(now) || registeredBefore(e.getValue(), timestamp)) {
                expired.add(e.getKey());
                out.collect(correlator.onTimeout(e.getValue(), now));
            }
        }
        for (String k : expired) {
            pending.remove(k);
        }
        timedOut.inc(expired.size());
        List<String> stale = new ArrayList<>();
        for (Map.Entry<String, PendingResponse> e : parked.entries()) {
            if (e.getValue().receivedAt().toEpochMilli() + timeout.toMillis() <= timestamp) {
                stale.add(e.getKey());
            }
        }
        for (String k : stale) {
            parked.remove(k);
        }
        parkedExpired.inc(stale.size());
    }

    /** Envelope time may lag wall time during replays; treat any call older than the timer horizon as expired. */
    private boolean registeredBefore(PendingCall call, long timerTs) {
        return call.sentAt().toEpochMilli() + timeout.toMillis() <= timerTs;
    }

    /** Adapts Flink MapState to the codec's storage port. */
    private record MapStateStore(MapState<String, PendingCall> state, MapState<String, PendingResponse> responses)
            implements PendingCallStore {
        @Override
        public Optional<PendingCall> get(String key) {
            try {
                return Optional.ofNullable(state.get(key));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void put(String key, PendingCall call) {
            try {
                state.put(key, call);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void remove(String key) {
            try {
                state.remove(key);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public Optional<PendingResponse> getResponse(String callKey) {
            try {
                return Optional.ofNullable(responses.get(callKey));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void putResponse(String callKey, PendingResponse response) {
            try {
                responses.put(callKey, response);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void removeResponse(String callKey) {
            try {
                responses.remove(callKey);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
