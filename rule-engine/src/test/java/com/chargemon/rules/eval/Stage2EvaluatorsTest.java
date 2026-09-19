package com.chargemon.rules.eval;

import static org.assertj.core.api.Assertions.assertThat;

import com.chargemon.alert.AlertEvent;
import com.chargemon.alert.AlertEventType;
import com.chargemon.alert.ConditionSignal;
import com.chargemon.alert.Severity;
import com.chargemon.alert.SubjectRef;
import com.chargemon.alert.SubjectType;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.eval.stage2.GroupAggregateEvaluator;
import com.chargemon.rules.eval.stage2.SequenceEvaluator;
import com.chargemon.rules.fixtures.FakeRuleContext;
import com.chargemon.rules.fixtures.RuleFixtures;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class Stage2EvaluatorsTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private static AlertEvent alert(String ruleId, String station, AlertEventType type, Instant openedAt) {
        return new AlertEvent("e", "a-" + ruleId + station, ruleId + "|STATION|" + station, 1, type, ruleId, 1, ruleId, "EVENT",
                Severity.HIGH, List.of(), SubjectType.STATION, station, Set.of("region:eu"), openedAt, openedAt,
                type == AlertEventType.RESOLVED ? openedAt.plusSeconds(1) : null, null, Map.of(), openedAt, 1);
    }

    @Test
    void sequenceTriggersWhenAllOpenWithinWindowAndClearsOnResolve() {
        RuleDefinition r = RuleFixtures.parse(RuleFixtures.sequenceRule("seq", "a", "b", "PT15M"));
        SequenceEvaluator ev = new SequenceEvaluator();
        FakeRuleContext ctx = new FakeRuleContext(SubjectRef.station("ST"), T0);

        ev.dispatchAlert(r, alert("a", "ST", AlertEventType.OPENED, T0), ctx);
        ev.dispatchAlert(r, alert("x", "ST", AlertEventType.OPENED, T0), ctx);          // unrelated rule
        assertThat(ctx.emitted).isEmpty();
        ev.dispatchAlert(r, alert("b", "ST", AlertEventType.OPENED, T0.plus(Duration.ofMinutes(10))), ctx);
        assertThat(ctx.emitted).extracting(ConditionSignal::kind).containsExactly(ConditionSignal.Kind.TRIGGERED);
        assertThat(ctx.emitted.get(0).groupIds()).containsExactly("region:eu");
        ev.dispatchAlert(r, alert("a", "ST", AlertEventType.RESOLVED, T0), ctx);
        assertThat(ctx.emitted).extracting(ConditionSignal::kind)
                .containsExactly(ConditionSignal.Kind.TRIGGERED, ConditionSignal.Kind.CLEARED);
    }

    @Test
    void sequenceIgnoresAlertsTooFarApart() {
        RuleDefinition r = RuleFixtures.parse(RuleFixtures.sequenceRule("seq", "a", "b", "PT15M"));
        SequenceEvaluator ev = new SequenceEvaluator();
        FakeRuleContext ctx = new FakeRuleContext(SubjectRef.station("ST"), T0);
        ev.dispatchAlert(r, alert("a", "ST", AlertEventType.OPENED, T0), ctx);
        ev.dispatchAlert(r, alert("b", "ST", AlertEventType.OPENED, T0.plus(Duration.ofMinutes(16))), ctx);
        assertThat(ctx.emitted).isEmpty();
    }

    @Test
    void groupCountThresholdWithAgeOutTimer() {
        RuleDefinition r = RuleFixtures.parse(RuleFixtures.groupAlertsRule("grp", "offline", "PT1H", 2));
        GroupAggregateEvaluator ev = new GroupAggregateEvaluator();
        FakeRuleContext ctx = new FakeRuleContext(SubjectRef.group("region:eu"), T0);

        ev.dispatchAlert(r, alert("offline", "S1", AlertEventType.OPENED, T0), 10, ctx);
        assertThat(ctx.emitted).isEmpty();
        ctx.advance(Duration.ofMinutes(30));
        ev.dispatchAlert(r, alert("offline", "S2", AlertEventType.OPENED, T0.plus(Duration.ofMinutes(30))), 10, ctx);
        assertThat(ctx.emitted).extracting(ConditionSignal::kind).containsExactly(ConditionSignal.Kind.TRIGGERED);
        assertThat(ctx.emitted.get(0).context()).containsEntry("count", "2");
        assertThat(ctx.timers).hasSize(1);

        ctx.advance(Duration.ofMinutes(31));                 // S1 ages out of the 1h window
        for (TimerRef t : ctx.dueTimers()) {
            ev.dispatchTimer(r, t, 10, ctx);
        }
        assertThat(ctx.emitted).extracting(ConditionSignal::kind)
                .containsExactly(ConditionSignal.Kind.TRIGGERED, ConditionSignal.Kind.CLEARED);
    }

    @Test
    void groupPercentThresholdUsesMemberCount() {
        RuleDefinition r = RuleFixtures.parse("""
            {"id":"pct","name":"20% offline","kind":"GROUP_AGGREGATE","subjectType":"GROUP",
             "spec":{"source":{"type":"alerts","ruleId":"offline"},"window":"PT1H","threshold":{"percent":20}},
             "severity":"CRITICAL"}
            """);
        GroupAggregateEvaluator ev = new GroupAggregateEvaluator();
        FakeRuleContext ctx = new FakeRuleContext(SubjectRef.group("g"), T0);
        ev.dispatchAlert(r, alert("offline", "S1", AlertEventType.OPENED, T0), 10, ctx);
        assertThat(ctx.emitted).isEmpty();                   // 10%
        ev.dispatchAlert(r, alert("offline", "S2", AlertEventType.OPENED, T0), 10, ctx);
        assertThat(ctx.emitted).hasSize(1);                  // 20%
        ev.dispatchMemberCount(r, 20, ctx);                  // group grew: 10% again
        assertThat(ctx.emitted).extracting(ConditionSignal::kind)
                .containsExactly(ConditionSignal.Kind.TRIGGERED, ConditionSignal.Kind.CLEARED);
    }

    @Test
    void groupZeroEnergyAggregateSource() {
        RuleDefinition r = RuleFixtures.parse("""
            {"id":"gz","name":"group zero energy","kind":"GROUP_AGGREGATE","subjectType":"GROUP",
             "spec":{"source":{"type":"zeroEnergy","window":"rolling7d"},"threshold":{"count":5}},"severity":"LOW"}
            """);
        GroupAggregateEvaluator ev = new GroupAggregateEvaluator();
        FakeRuleContext ctx = new FakeRuleContext(SubjectRef.group("g"), T0);
        ev.dispatchAggregate(r, "zeroEnergy", Map.of("rolling7d", 4L), 3, ctx);
        ev.dispatchAggregate(r, "zeroEnergy", Map.of("rolling7d", 5L), 3, ctx);
        ev.dispatchAggregate(r, "zeroEnergy", Map.of("rolling7d", 1L), 3, ctx);
        assertThat(ctx.emitted).extracting(ConditionSignal::kind)
                .containsExactly(ConditionSignal.Kind.TRIGGERED, ConditionSignal.Kind.CLEARED);
    }
}
