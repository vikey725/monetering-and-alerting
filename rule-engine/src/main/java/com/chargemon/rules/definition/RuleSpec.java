package com.chargemon.rules.definition;

import com.chargemon.rules.condition.Condition;
import java.time.Duration;
import java.util.List;
import java.util.Set;

/** Kind-specific parameters. One record per {@link RuleKind}. */
public sealed interface RuleSpec permits RuleSpec.EventSpec, RuleSpec.AbsenceSpec, RuleSpec.StateDurationSpec,
        RuleSpec.SequenceSpec, RuleSpec.GroupAggregateSpec {

    RuleKind kind();

    /**
     * Fires when a matching input satisfies {@code condition}. Clears when {@code clear}
     * is satisfied, or (if {@code clear} is null) when a matching input no longer satisfies {@code condition}.
     *
     * @param trigger which inputs are evaluated: event actions and/or an aggregate source name
     */
    record EventSpec(Trigger trigger, Condition condition, Condition clear) implements RuleSpec {
        @Override
        public RuleKind kind() {
            return RuleKind.EVENT;
        }
    }

    /** Fires when {@code expectedAction} ("*" = any) is not seen for {@code within}. */
    record AbsenceSpec(String expectedAction, Duration within, Condition onlyIf) implements RuleSpec {
        @Override
        public RuleKind kind() {
            return RuleKind.ABSENCE;
        }
    }

    /**
     * Fires when a scope (e.g. a connector) stays in a state for longer than {@code maxDuration}.
     *
     * @param scopeField fact path whose value distinguishes scopes, e.g. {@code event.connectorId}; null = whole station
     */
    record StateDurationSpec(Condition enter, Condition exit, Duration maxDuration, String scopeField) implements RuleSpec {
        @Override
        public RuleKind kind() {
            return RuleKind.STATE_DURATION;
        }
    }

    /** Fires when all listed stage-1 rules are open on the same station within {@code within}. */
    record SequenceSpec(List<String> allOf, Duration within) implements RuleSpec {
        public SequenceSpec {
            allOf = allOf == null ? List.of() : List.copyOf(allOf);
        }

        @Override
        public RuleKind kind() {
            return RuleKind.SEQUENCE;
        }
    }

    /**
     * Fires when a group-level count/percentage crosses a threshold within {@code window}.
     *
     * @param source {@code alerts} of a given stage-1 rule, or a {@code zeroEnergy} aggregate window
     */
    record GroupAggregateSpec(AggregateSource source, Duration window, Threshold threshold) implements RuleSpec {
        @Override
        public RuleKind kind() {
            return RuleKind.GROUP_AGGREGATE;
        }
    }

    record Trigger(Set<String> actions, String aggregate) {
        public Trigger {
            actions = actions == null ? Set.of() : Set.copyOf(actions);
        }

        public boolean matchesAction(String action) {
            return actions.contains("*") || actions.contains(action);
        }

        public boolean matchesAggregate(String source) {
            return aggregate != null && aggregate.equals(source);
        }
    }

    record AggregateSource(String type, String ruleId, String window) {
        public static final String ALERTS = "alerts";
        public static final String ZERO_ENERGY = "zeroEnergy";
    }

    /** Exactly one of count / percent is set. */
    record Threshold(Integer count, Double percent) {
    }
}
