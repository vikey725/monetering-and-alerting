package com.chargemon.rules.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import com.chargemon.alert.AlertEvent;
import com.chargemon.alert.AlertEventType;
import com.chargemon.alert.ConditionSignal;
import com.chargemon.alert.ResolveReason;
import com.chargemon.alert.SubjectRef;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.fixtures.RuleFixtures;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class AlertLifecycleTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final SubjectRef ST = SubjectRef.station("ST-1");

    /** Drives the state machine with a simulated clock and timer table. */
    static final class Harness {
        final AlertLifecycle lifecycle;
        final RuleDefinition rule;
        LifecycleState state = LifecycleState.idle();
        Instant now = T0;
        final Map<TimerKind, Instant> timers = new java.util.EnumMap<>(TimerKind.class);
        final List<AlertEvent> events = new ArrayList<>();

        Harness(RuleDefinition rule) {
            AtomicInteger n = new AtomicInteger();
            this.lifecycle = new AlertLifecycle(() -> "id-" + n.incrementAndGet());
            this.rule = rule;
        }

        void apply(LifecycleInput in) {
            AlertLifecycle.Decision d = lifecycle.on(state, in, rule, ST, now);
            state = d.next();
            events.addAll(d.emits());
            for (TimerRequest t : d.timers()) {
                if (t.isCancel()) {
                    timers.remove(t.kind());
                } else {
                    timers.put(t.kind(), t.at());
                }
            }
        }

        void trigger() {
            apply(new LifecycleInput.Signal(ConditionSignal.triggered(rule.id(), 1, ST, now, Map.of("k", "v"), Set.of("g"))));
        }

        void clear() {
            apply(new LifecycleInput.Signal(ConditionSignal.cleared(rule.id(), 1, ST, now)));
        }

        /** Advance clock, firing any timers that come due, in time order. */
        void advance(Duration d) {
            Instant target = now.plus(d);
            while (true) {
                var due = timers.entrySet().stream().filter(e -> !e.getValue().isAfter(target))
                        .min(Map.Entry.comparingByValue());
                if (due.isEmpty()) {
                    break;
                }
                now = due.get().getValue();
                TimerKind k = due.get().getKey();
                timers.remove(k);
                apply(new LifecycleInput.TimerFired(k, now));
            }
            now = target;
        }

        List<AlertEventType> types() {
            return events.stream().map(AlertEvent::type).toList();
        }
    }

    private static Harness harness(String grace, String suppression) {
        return new Harness(RuleFixtures.parse(RuleFixtures.faultedRule("r", grace, suppression)));
    }

    @Test
    void clearedWithinGraceNeverOpens() {
        Harness h = harness("PT2M", "PT1H");
        h.trigger();
        h.advance(Duration.ofMinutes(1));
        h.clear();
        h.advance(Duration.ofHours(2));
        assertThat(h.events).isEmpty();
        assertThat(h.state.phase()).isEqualTo(Phase.IDLE);
        assertThat(h.timers).isEmpty();
    }

    @Test
    void opensAfterGraceAndResolvesOnClear() {
        Harness h = harness("PT2M", "PT1H");
        h.trigger();
        h.advance(Duration.ofMinutes(3));
        assertThat(h.types()).containsExactly(AlertEventType.OPENED);
        AlertEvent opened = h.events.get(0);
        assertThat(opened.openedAt()).isEqualTo(T0.plus(Duration.ofMinutes(2)));
        assertThat(opened.triggeredAt()).isEqualTo(T0);
        assertThat(opened.context()).containsEntry("k", "v");
        assertThat(opened.groupIds()).containsExactly("g");
        assertThat(opened.alertKey()).isEqualTo("r|STATION|ST-1");
        assertThat(opened.seq()).isEqualTo(1);

        h.clear();
        assertThat(h.types()).containsExactly(AlertEventType.OPENED, AlertEventType.RESOLVED);
        AlertEvent resolved = h.events.get(1);
        assertThat(resolved.alertId()).isEqualTo(opened.alertId());
        assertThat(resolved.seq()).isEqualTo(2);
        assertThat(resolved.resolveReason()).isEqualTo(ResolveReason.CONDITION_CLEARED);
        assertThat(h.state.phase()).isEqualTo(Phase.RESOLVED);   // suppression still running
    }

    @Test
    void reTriggerInsideSuppressionIsMuted_thenNewGraceAfterwards() {
        Harness h = harness("PT2M", "PT1H");
        h.trigger();
        h.advance(Duration.ofMinutes(2));
        h.clear();
        h.advance(Duration.ofMinutes(10));
        h.trigger();                                   // muted
        assertThat(h.state.phase()).isEqualTo(Phase.SUPPRESSED);
        h.advance(Duration.ofMinutes(10));
        assertThat(h.types()).containsExactly(AlertEventType.OPENED, AlertEventType.RESOLVED);

        h.advance(Duration.ofHours(1));                // suppression ends while still triggered -> new grace -> opens
        assertThat(h.types()).containsExactly(AlertEventType.OPENED, AlertEventType.RESOLVED, AlertEventType.OPENED);
        assertThat(h.events.get(2).alertId()).isNotEqualTo(h.events.get(0).alertId());
        assertThat(h.events.get(2).openedAt()).isEqualTo(T0.plus(Duration.ofMinutes(2 + 60 + 2)));
    }

    @Test
    void suppressedThenClearedGoesIdleAfterWindow() {
        Harness h = harness("PT0S", "PT30M");
        h.trigger();
        assertThat(h.types()).containsExactly(AlertEventType.OPENED);
        h.clear();
        h.trigger();
        h.clear();
        h.advance(Duration.ofHours(1));
        assertThat(h.types()).containsExactly(AlertEventType.OPENED, AlertEventType.RESOLVED);
        assertThat(h.state.phase()).isEqualTo(Phase.IDLE);
        h.trigger();
        assertThat(h.types()).hasSize(3);
    }

    @Test
    void zeroWindowsBehaveAsImmediateOpenAndRepeat() {
        Harness h = harness("PT0S", "PT0S");
        h.trigger();
        h.clear();
        h.trigger();
        h.clear();
        assertThat(h.types()).containsExactly(AlertEventType.OPENED, AlertEventType.RESOLVED,
                AlertEventType.OPENED, AlertEventType.RESOLVED);
        assertThat(h.events.get(3).seq()).isEqualTo(4);
    }

    @Test
    void autoResolveAndRuleRemoval() {
        RuleDefinition r = RuleFixtures.parse("""
            {"id":"r","name":"n","kind":"EVENT","spec":{"trigger":"X","condition":{"op":"exists","field":"a"}},
             "graceWindow":"PT0S","suppressionWindow":"PT0S","autoResolveAfter":"PT5M","severity":"LOW"}
            """);
        Harness h = new Harness(r);
        h.trigger();
        h.advance(Duration.ofMinutes(6));
        assertThat(h.types()).containsExactly(AlertEventType.OPENED, AlertEventType.RESOLVED);
        assertThat(h.events.get(1).resolveReason()).isEqualTo(ResolveReason.AUTO_RESOLVE_TIMEOUT);

        h.trigger();
        h.apply(new LifecycleInput.RuleGone(ResolveReason.RULE_DISABLED));
        assertThat(h.events.get(3).resolveReason()).isEqualTo(ResolveReason.RULE_DISABLED);
        assertThat(h.state.phase()).isEqualTo(Phase.IDLE);
        assertThat(h.timers).isEmpty();
    }

    @Test
    void staleTimerIsIgnored() {
        Harness h = harness("PT2M", "PT0S");
        h.trigger();
        h.apply(new LifecycleInput.TimerFired(TimerKind.GRACE, T0.plusSeconds(1)));   // wrong deadline
        assertThat(h.events).isEmpty();
        assertThat(h.state.phase()).isEqualTo(Phase.PENDING);
    }
}
