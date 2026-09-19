package com.chargemon.rules.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import com.chargemon.alert.AlertEvent;
import com.chargemon.alert.AlertEventType;
import com.chargemon.alert.ConditionSignal;
import com.chargemon.alert.SubjectRef;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.fixtures.RuleFixtures;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/** Invariants that must hold for any interleaving of triggers, clears and time. */
class AlertLifecycleProperties {

    sealed interface Step permits Trigger, Clear, Wait {
    }

    record Trigger() implements Step {
    }

    record Clear() implements Step {
    }

    record Wait(int seconds) implements Step {
    }

    @Provide
    Arbitrary<List<Step>> steps() {
        Arbitrary<Step> step = Arbitraries.oneOf(
                Arbitraries.just(new Trigger()),
                Arbitraries.just(new Clear()),
                Arbitraries.integers().between(1, 600).map(Wait::new));
        return step.list().ofMinSize(1).ofMaxSize(60);
    }

    @Property(tries = 300)
    void openedAndResolvedAlternate_noOpenWhileSuppressed_noOpenBeforeGrace(
            @ForAll("steps") List<Step> steps,
            @ForAll("grace") int graceSec,
            @ForAll("suppression") int suppressionSec) {
        RuleDefinition rule = RuleFixtures.parse(RuleFixtures.faultedRule("r", "PT" + graceSec + "S", "PT" + suppressionSec + "S"));
        AlertLifecycle lifecycle = new AlertLifecycle();
        SubjectRef st = SubjectRef.station("S");
        LifecycleState state = LifecycleState.idle();
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        Map<TimerKind, Instant> timers = new EnumMap<>(TimerKind.class);
        List<AlertEvent> events = new ArrayList<>();
        Instant lastTrigger = null;
        Instant lastOpen = null;

        for (Step s : steps) {
            List<LifecycleInput> inputs = new ArrayList<>();
            if (s instanceof Wait w) {
                Instant target = now.plusSeconds(w.seconds());
                while (true) {
                    var due = timers.entrySet().stream().filter(e -> !e.getValue().isAfter(target))
                            .min(Map.Entry.comparingByValue());
                    if (due.isEmpty()) {
                        break;
                    }
                    now = due.get().getValue();
                    TimerKind k = due.get().getKey();
                    timers.remove(k);
                    state = apply(lifecycle, state, new LifecycleInput.TimerFired(k, now), rule, st, now, timers, events);
                }
                now = target;
                continue;
            }
            if (s instanceof Trigger) {
                if (state.isIdle() || state.phase() == Phase.RESOLVED) {
                    lastTrigger = now;
                }
                inputs.add(new LifecycleInput.Signal(ConditionSignal.triggered("r", 1, st, now, Map.of(), Set.of())));
            } else {
                inputs.add(new LifecycleInput.Signal(ConditionSignal.cleared("r", 1, st, now)));
            }
            int before = events.size();
            for (LifecycleInput in : inputs) {
                state = apply(lifecycle, state, in, rule, st, now, timers, events);
            }
            for (AlertEvent e : events.subList(before, events.size())) {
                if (e.type() == AlertEventType.OPENED) {
                    if (lastOpen != null) {
                        assertThat(Duration.between(lastOpen, now).getSeconds())
                                .as("no re-open inside suppression").isGreaterThanOrEqualTo(suppressionSec);
                    }
                    assertThat(Duration.between(lastTrigger, now).getSeconds())
                            .as("open only after grace").isGreaterThanOrEqualTo(graceSec);
                    lastOpen = now;
                }
            }
        }
        // strict alternation OPENED, RESOLVED, OPENED, ...
        for (int i = 0; i < events.size(); i++) {
            assertThat(events.get(i).type()).isEqualTo(i % 2 == 0 ? AlertEventType.OPENED : AlertEventType.RESOLVED);
            if (i > 0) {
                assertThat(events.get(i).seq()).isGreaterThan(events.get(i - 1).seq());
            }
            if (i % 2 == 1) {
                assertThat(events.get(i).alertId()).isEqualTo(events.get(i - 1).alertId());
            }
        }
    }

    @Provide
    Arbitrary<Integer> grace() {
        return Arbitraries.of(0, 30, 120);
    }

    @Provide
    Arbitrary<Integer> suppression() {
        return Arbitraries.of(0, 60, 900);
    }

    private static LifecycleState apply(AlertLifecycle lc, LifecycleState state, LifecycleInput in, RuleDefinition rule,
                                        SubjectRef st, Instant now, Map<TimerKind, Instant> timers, List<AlertEvent> out) {
        AlertLifecycle.Decision d = lc.on(state, in, rule, st, now);
        out.addAll(d.emits());
        for (TimerRequest t : d.timers()) {
            if (t.isCancel()) {
                timers.remove(t.kind());
            } else {
                timers.put(t.kind(), t.at());
            }
        }
        return d.next();
    }
}
