package com.chargemon.rules.eval;

import com.chargemon.rules.definition.RuleKind;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

public final class EvaluatorRegistry {

    private final Map<RuleKind, StationRuleKindEvaluator<?>> station;

    private EvaluatorRegistry(Map<RuleKind, StationRuleKindEvaluator<?>> station) {
        this.station = new EnumMap<>(station);
    }

    public static EvaluatorRegistry fromServiceLoader() {
        List<StationRuleKindEvaluator<?>> found = new ArrayList<>();
        ServiceLoader.load(StationRuleKindEvaluator.class).forEach(found::add);
        return of(found);
    }

    public static EvaluatorRegistry of(Collection<? extends StationRuleKindEvaluator<?>> evaluators) {
        Map<RuleKind, StationRuleKindEvaluator<?>> m = new EnumMap<>(RuleKind.class);
        for (StationRuleKindEvaluator<?> e : evaluators) {
            if (m.putIfAbsent(e.kind(), e) != null) {
                throw new IllegalStateException("duplicate evaluator for " + e.kind());
            }
        }
        return new EvaluatorRegistry(m);
    }

    public Optional<StationRuleKindEvaluator<?>> station(RuleKind kind) {
        return Optional.ofNullable(station.get(kind));
    }
}
