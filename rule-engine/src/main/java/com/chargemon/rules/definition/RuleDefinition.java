package com.chargemon.rules.definition;

import com.chargemon.alert.ChannelRef;
import com.chargemon.alert.RuleTiming;
import com.chargemon.alert.Severity;
import com.chargemon.alert.SubjectType;
import com.chargemon.rules.condition.Condition;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A parsed, immutable rule. {@code targetGroupIds} empty = applies everywhere;
 * otherwise the subject (or the station's group closure) must intersect it.
 */
public record RuleDefinition(
        String id,
        String name,
        RuleKind kind,
        SubjectType subjectType,
        Set<String> targetGroupIds,
        Condition stationFilter,
        RuleSpec spec,
        RuleTiming timing,
        Severity severity,
        List<ChannelRef> channels,
        boolean enabled,
        int version) {

    public RuleDefinition {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(spec, "spec");
        targetGroupIds = targetGroupIds == null ? Set.of() : Set.copyOf(targetGroupIds);
        channels = channels == null ? List.of() : List.copyOf(channels);
        timing = timing == null ? RuleTiming.none() : timing;
        subjectType = subjectType == null ? SubjectType.STATION : subjectType;
    }

    /** True when the rule targets this station given its transitive group membership. */
    public boolean appliesToGroups(Set<String> stationGroupIds) {
        if (targetGroupIds.isEmpty()) {
            return true;
        }
        for (String g : targetGroupIds) {
            if (stationGroupIds.contains(g)) {
                return true;
            }
        }
        return false;
    }

    public boolean appliesToGroup(String groupId) {
        return targetGroupIds.isEmpty() || targetGroupIds.contains(groupId);
    }
}
