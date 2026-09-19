package com.chargemon.rules.fixtures;

import com.chargemon.alert.ConditionSignal;
import com.chargemon.alert.SubjectRef;
import com.chargemon.rules.eval.RuleContext;
import com.chargemon.rules.eval.RuleStateStore;
import com.chargemon.rules.eval.RuleTimers;
import com.chargemon.rules.eval.TimerRef;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/** In-memory RuleContext with a manual clock and inspectable timers / emissions. */
public final class FakeRuleContext implements RuleContext {

    private final SubjectRef subject;
    private Instant now;
    private Set<String> groupIds = Set.of();
    public final List<ConditionSignal> emitted = new ArrayList<>();
    public final Map<String, Instant> timers = new TreeMap<>();
    private final Map<String, Object> state = new HashMap<>();

    public FakeRuleContext(SubjectRef subject, Instant now) {
        this.subject = subject;
        this.now = now;
    }

    public FakeRuleContext advance(java.time.Duration d) {
        now = now.plus(d);
        return this;
    }

    public FakeRuleContext at(Instant t) {
        now = t;
        return this;
    }

    public FakeRuleContext groups(Set<String> g) {
        groupIds = g;
        return this;
    }

    /** Timers due at or before now, in order. */
    public List<TimerRef> dueTimers() {
        List<TimerRef> due = new ArrayList<>();
        timers.forEach((k, at) -> {
            if (!at.isAfter(now)) {
                due.add(TimerRef.parse(k));
            }
        });
        due.forEach(r -> timers.remove(r.key()));
        return due;
    }

    public List<ConditionSignal> drain() {
        List<ConditionSignal> out = List.copyOf(emitted);
        emitted.clear();
        return out;
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
            @SuppressWarnings("unchecked")
            public <T> Optional<T> get(String ruleId, String scope, Class<T> type) {
                return Optional.ofNullable((T) state.get(ruleId + "|" + scope));
            }

            @Override
            public <T> void put(String ruleId, String scope, T value) {
                state.put(ruleId + "|" + scope, value);
            }

            @Override
            public void remove(String ruleId, String scope) {
                state.remove(ruleId + "|" + scope);
            }

            @Override
            public void clear(String ruleId) {
                state.keySet().removeIf(k -> k.startsWith(ruleId + "|"));
            }
        };
    }

    @Override
    public RuleTimers timers() {
        return new RuleTimers() {
            @Override
            public void schedule(TimerRef ref, Instant at) {
                timers.put(ref.key(), at);
            }

            @Override
            public void cancel(TimerRef ref) {
                timers.remove(ref.key());
            }

            @Override
            public void cancelAll(String ruleId) {
                timers.keySet().removeIf(k -> k.startsWith(ruleId + "|"));
            }
        };
    }

    @Override
    public void emit(ConditionSignal signal) {
        emitted.add(signal);
    }

    @Override
    public Instant now() {
        return now;
    }
}
