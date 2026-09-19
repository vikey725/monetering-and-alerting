package com.chargemon.rules.definition;

import com.chargemon.alert.SubjectType;
import com.chargemon.rules.definition.RuleSpec.AggregateSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Structural validation of a single rule plus optional cross-rule reference checks. */
public final class RuleValidator {

    public record ValidationResult(List<String> errors) {
        public boolean valid() {
            return errors.isEmpty();
        }
    }

    public ValidationResult validate(RuleDefinition r) {
        List<String> e = new ArrayList<>();
        if (r.name() == null || r.name().isBlank()) {
            e.add("name is blank");
        }
        if (r.kind() == RuleKind.GROUP_AGGREGATE && r.subjectType() != SubjectType.GROUP) {
            e.add("GROUP_AGGREGATE rules must have subjectType GROUP");
        }
        if (r.kind() != RuleKind.GROUP_AGGREGATE && r.subjectType() != SubjectType.STATION) {
            e.add(r.kind() + " rules must have subjectType STATION");
        }
        if (r.spec().kind() != r.kind()) {
            e.add("spec kind " + r.spec().kind() + " does not match rule kind " + r.kind());
        }
        switch (r.spec()) {
            case RuleSpec.EventSpec s -> {
                if (s.condition() == null) {
                    e.add("EVENT.condition is required");
                }
                if (s.trigger().actions().isEmpty() && s.trigger().aggregate() == null) {
                    e.add("EVENT.trigger needs actions or aggregate");
                }
            }
            case RuleSpec.AbsenceSpec s -> {
                if (s.within() == null || s.within().isZero() || s.within().isNegative()) {
                    e.add("ABSENCE.within must be a positive duration");
                }
            }
            case RuleSpec.StateDurationSpec s -> {
                if (s.enter() == null) {
                    e.add("STATE_DURATION.enter is required");
                }
                if (s.exit() == null) {
                    e.add("STATE_DURATION.exit is required");
                }
                if (s.maxDuration() == null || s.maxDuration().isZero() || s.maxDuration().isNegative()) {
                    e.add("STATE_DURATION.maxDuration must be a positive duration");
                }
            }
            case RuleSpec.SequenceSpec s -> {
                if (s.allOf().size() < 2) {
                    e.add("SEQUENCE.allOf needs at least two rule ids");
                }
                if (s.within() == null || s.within().isZero() || s.within().isNegative()) {
                    e.add("SEQUENCE.within must be a positive duration");
                }
            }
            case RuleSpec.GroupAggregateSpec s -> {
                if (s.source() == null || s.source().type() == null) {
                    e.add("GROUP_AGGREGATE.source.type is required");
                } else if (AggregateSource.ALERTS.equals(s.source().type()) && s.source().ruleId() == null) {
                    e.add("GROUP_AGGREGATE.source.ruleId is required for alerts source");
                } else if (AggregateSource.ZERO_ENERGY.equals(s.source().type()) && s.source().window() == null) {
                    e.add("GROUP_AGGREGATE.source.window is required for zeroEnergy source");
                }
                if (s.threshold() == null || (s.threshold().count() == null) == (s.threshold().percent() == null)) {
                    e.add("GROUP_AGGREGATE.threshold needs exactly one of count / percent");
                } else if (s.threshold().percent() != null && s.source() != null
                        && AggregateSource.ZERO_ENERGY.equals(s.source().type())) {
                    e.add("GROUP_AGGREGATE.threshold.percent is only valid for the alerts source");
                }
                if (s.source() != null && AggregateSource.ALERTS.equals(s.source().type())
                        && (s.window() == null || s.window().isZero() || s.window().isNegative())) {
                    e.add("GROUP_AGGREGATE.window must be a positive duration for alerts source");
                }
            }
        }
        return new ValidationResult(List.copyOf(e));
    }

    /** Stage-2 rules may only reference stage-1 rules. Missing references are tolerated (may arrive later). */
    public ValidationResult validateReferences(RuleDefinition r, Map<String, RuleDefinition> known) {
        List<String> e = new ArrayList<>();
        List<String> refs = switch (r.spec()) {
            case RuleSpec.SequenceSpec s -> s.allOf();
            case RuleSpec.GroupAggregateSpec s ->
                    s.source() != null && s.source().ruleId() != null ? List.of(s.source().ruleId()) : List.of();
            default -> List.of();
        };
        for (String ref : refs) {
            RuleDefinition target = known.get(ref);
            if (target != null && target.kind().stage() != 1) {
                e.add("rule " + r.id() + " references non stage-1 rule " + ref);
            }
            if (ref.equals(r.id())) {
                e.add("rule " + r.id() + " references itself");
            }
        }
        return new ValidationResult(List.copyOf(e));
    }
}
