package com.chargemon.rules.eval;

import static org.assertj.core.api.Assertions.assertThat;

import com.chargemon.alert.ConditionSignal;
import com.chargemon.alert.SubjectRef;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.fixtures.FakeRuleContext;
import com.chargemon.rules.fixtures.MapFact;
import com.chargemon.rules.fixtures.RuleFixtures;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.Test;

class StationEvaluatorsTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private final EvaluatorRegistry registry = EvaluatorRegistry.fromServiceLoader();

    private StationRuleKindEvaluator<?> evaluator(RuleDefinition r) {
        return registry.station(r.kind()).orElseThrow();
    }

    private static StationInput status(String status, int connector, Instant at) {
        return StationInput.event("StatusNotification",
                MapFact.of("event.type", "StatusNotification", "event.status", status, "event.connectorId", connector),
                at, Set.of("site:1"));
    }

    @Test
    void eventRuleEmitsOnlyOnTransitions() {
        RuleDefinition r = RuleFixtures.parse(RuleFixtures.faultedRule("r", "PT0S", "PT0S"));
        FakeRuleContext ctx = new FakeRuleContext(SubjectRef.station("ST-1"), T0);
        StationRuleKindEvaluator<?> ev = evaluator(r);

        ev.dispatchInput(r, status("FAULTED", 1, T0), ctx);
        ev.dispatchInput(r, status("FAULTED", 1, T0), ctx);
        ev.dispatchInput(r, StationInput.event("Heartbeat", MapFact.of("event.type", "Heartbeat"), T0, Set.of()), ctx);
        ev.dispatchInput(r, status("AVAILABLE", 1, T0), ctx);
        ev.dispatchInput(r, status("AVAILABLE", 1, T0), ctx);

        assertThat(ctx.emitted).extracting(ConditionSignal::kind)
                .containsExactly(ConditionSignal.Kind.TRIGGERED, ConditionSignal.Kind.CLEARED);
        assertThat(ctx.emitted.get(0).groupIds()).containsExactly("site:1");
        assertThat(ctx.emitted.get(0).context()).containsEntry("event.status", "FAULTED");
    }

    @Test
    void aggregateTriggeredEventRule() {
        RuleDefinition r = RuleFixtures.parse(RuleFixtures.zeroEnergyRule("z", "rolling24h", 3));
        FakeRuleContext ctx = new FakeRuleContext(SubjectRef.station("ST-1"), T0);
        StationRuleKindEvaluator<?> ev = evaluator(r);
        ev.dispatchInput(r, StationInput.aggregate("zeroEnergy", MapFact.of("agg.zeroEnergy.rolling24h", 2), T0, Set.of()), ctx);
        ev.dispatchInput(r, StationInput.aggregate("zeroEnergy", MapFact.of("agg.zeroEnergy.rolling24h", 3), T0, Set.of()), ctx);
        ev.dispatchInput(r, StationInput.aggregate("zeroEnergy", MapFact.of("agg.zeroEnergy.rolling24h", 0), T0, Set.of()), ctx);
        assertThat(ctx.emitted).extracting(ConditionSignal::kind)
                .containsExactly(ConditionSignal.Kind.TRIGGERED, ConditionSignal.Kind.CLEARED);
    }

    @Test
    void absenceRuleTriggersOnTimerAndClearsOnReturn() {
        RuleDefinition r = RuleFixtures.parse(RuleFixtures.heartbeatAbsenceRule("h", "PT10M"));
        FakeRuleContext ctx = new FakeRuleContext(SubjectRef.station("ST-1"), T0).groups(Set.of("g"));
        StationRuleKindEvaluator<?> ev = evaluator(r);
        StationInput hb = StationInput.event("Heartbeat", MapFact.of("event.type", "Heartbeat"), T0, Set.of("g"));

        ev.dispatchInput(r, hb, ctx);
        ctx.advance(Duration.ofMinutes(5));
        ev.dispatchInput(r, hb, ctx);                       // re-arms
        ctx.advance(Duration.ofMinutes(6));
        assertThat(ctx.dueTimers()).isEmpty();              // 11 min after first, 6 after second: not yet
        ctx.advance(Duration.ofMinutes(5));
        for (TimerRef t : ctx.dueTimers()) {
            ev.dispatchTimer(r, t, ctx);
        }
        assertThat(ctx.emitted).extracting(ConditionSignal::kind).containsExactly(ConditionSignal.Kind.TRIGGERED);
        assertThat(ctx.emitted.get(0).groupIds()).containsExactly("g");

        ev.dispatchInput(r, hb, ctx);
        assertThat(ctx.emitted).extracting(ConditionSignal::kind)
                .containsExactly(ConditionSignal.Kind.TRIGGERED, ConditionSignal.Kind.CLEARED);
        assertThat(ctx.timers).hasSize(1);
    }

    @Test
    void stateDurationTracksPerConnectorScope() {
        RuleDefinition r = RuleFixtures.parse(RuleFixtures.stuckPreparingRule("p", "PT30M"));
        FakeRuleContext ctx = new FakeRuleContext(SubjectRef.station("ST-1"), T0);
        StationRuleKindEvaluator<?> ev = evaluator(r);

        ev.dispatchInput(r, status("PREPARING", 1, T0), ctx);
        ev.dispatchInput(r, status("PREPARING", 2, T0), ctx);
        ctx.advance(Duration.ofMinutes(10));
        ev.dispatchInput(r, status("CHARGING", 2, T0), ctx);   // connector 2 leaves in time
        ctx.advance(Duration.ofMinutes(25));
        for (TimerRef t : ctx.dueTimers()) {
            ev.dispatchTimer(r, t, ctx);
        }
        assertThat(ctx.emitted).hasSize(1);
        assertThat(ctx.emitted.get(0).context()).containsEntry("scope", "1");

        ev.dispatchInput(r, status("AVAILABLE", 1, T0), ctx);
        assertThat(ctx.emitted).extracting(ConditionSignal::kind)
                .containsExactly(ConditionSignal.Kind.TRIGGERED, ConditionSignal.Kind.CLEARED);
    }

    @Test
    void ruleRemovalDropsStateAndTimers() {
        RuleDefinition r = RuleFixtures.parse(RuleFixtures.heartbeatAbsenceRule("h", "PT10M"));
        FakeRuleContext ctx = new FakeRuleContext(SubjectRef.station("ST-1"), T0);
        StationRuleKindEvaluator<?> ev = evaluator(r);
        ev.dispatchInput(r, StationInput.event("Heartbeat", MapFact.of(), T0, Set.of()), ctx);
        assertThat(ctx.timers).hasSize(1);
        ev.onRuleRemoved(r, ctx);
        assertThat(ctx.timers).isEmpty();
        assertThat(ctx.state().get("h", "", Object.class)).isEmpty();
    }
}
