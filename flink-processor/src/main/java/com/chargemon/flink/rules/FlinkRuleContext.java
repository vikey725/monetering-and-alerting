package com.chargemon.flink.rules;

import com.chargemon.alert.ConditionSignal;
import com.chargemon.alert.SubjectRef;
import com.chargemon.common.json.JsonMapperFactory;
import com.chargemon.rules.eval.RuleContext;
import com.chargemon.rules.eval.RuleStateStore;
import com.chargemon.rules.eval.RuleTimers;
import com.chargemon.rules.eval.TimerRef;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;

/**
 * Adapts Flink keyed state + timer service to the rule engine's ports.
 * Evaluator state blobs are Smile-encoded; timers are recorded as ref -> deadline.
 */
public final class FlinkRuleContext implements RuleContext {

    private static final ObjectMapper SMILE = JsonMapperFactory.smile();

    private final SubjectRef subject;
    private final Set<String> groupIds;
    private final MapState<String, byte[]> blobs;
    private final MapState<String, Long> timers;
    private final TimerService timerService;
    private final Collector<ConditionSignal> out;
    private final Instant now;

    public FlinkRuleContext(SubjectRef subject, Set<String> groupIds, MapState<String, byte[]> blobs,
                     MapState<String, Long> timers, TimerService timerService, Collector<ConditionSignal> out,
                     Instant now) {
        this.subject = subject;
        this.groupIds = groupIds;
        this.blobs = blobs;
        this.timers = timers;
        this.timerService = timerService;
        this.out = out;
        this.now = now;
    }

    @Override
    public SubjectRef subject() {
        return subject;
    }

    @Override
    public Set<String> groupIds() {
        return groupIds;
    }

    @Override
    public RuleStateStore state() {
        return new RuleStateStore() {
            @Override
            public <T> Optional<T> get(String ruleId, String scope, Class<T> type) {
                try {
                    byte[] b = blobs.get(ruleId + "|" + scope);
                    return b == null ? Optional.empty() : Optional.of(SMILE.readValue(b, type));
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public <T> void put(String ruleId, String scope, T value) {
                try {
                    blobs.put(ruleId + "|" + scope, SMILE.writeValueAsBytes(value));
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public void remove(String ruleId, String scope) {
                try {
                    blobs.remove(ruleId + "|" + scope);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public void clear(String ruleId) {
                try {
                    List<String> keys = new ArrayList<>();
                    for (String k : blobs.keys()) {
                        if (k.startsWith(ruleId + "|")) {
                            keys.add(k);
                        }
                    }
                    for (String k : keys) {
                        blobs.remove(k);
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        };
    }

    @Override
    public RuleTimers timers() {
        return new RuleTimers() {
            @Override
            public void schedule(TimerRef ref, Instant at) {
                try {
                    long ts = at.toEpochMilli();
                    Long previous = timers.get(ref.key());
                    if (previous != null && !stillReferenced(previous, ref.key())) {
                        timerService.deleteProcessingTimeTimer(previous);
                    }
                    timers.put(ref.key(), ts);
                    timerService.registerProcessingTimeTimer(ts);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public void cancel(TimerRef ref) {
                try {
                    Long previous = timers.get(ref.key());
                    if (previous != null) {
                        timers.remove(ref.key());
                        if (!stillReferenced(previous, ref.key())) {
                            timerService.deleteProcessingTimeTimer(previous);
                        }
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public void cancelAll(String ruleId) {
                try {
                    List<String> keys = new ArrayList<>();
                    for (String k : timers.keys()) {
                        if (k.startsWith(ruleId + "|")) {
                            keys.add(k);
                        }
                    }
                    for (String k : keys) {
                        cancel(TimerRef.parse(k));
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }

            /** Another ref may share the same timestamp; only delete the Flink timer when nobody else needs it. */
            private boolean stillReferenced(long ts, String exceptKey) throws Exception {
                for (Map.Entry<String, Long> e : timers.entries()) {
                    if (e.getValue() == ts && !e.getKey().equals(exceptKey)) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    @Override
    public void emit(ConditionSignal signal) {
        out.collect(signal);
    }

    @Override
    public Instant now() {
        return now;
    }

    static byte[] encode(Object value) throws IOException {
        return SMILE.writeValueAsBytes(value);
    }
}
