package com.chargemon.flink.enrich;

import com.chargemon.ocpp.model.station.GroupRecord;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Computes the transitive ancestor closure of group ids. Cache is invalidated
 * wholesale on any group change (changes are rare, lookups are hot).
 */
public final class GroupHierarchy {

    private final Map<String, Set<String>> closureCache = new HashMap<>();

    public void invalidate() {
        closureCache.clear();
    }

    /** Direct groups plus all their ancestors. Unknown ids are kept as-is. */
    public Set<String> closure(Set<String> direct, Function<String, GroupRecord> lookup) {
        Set<String> all = new HashSet<>();
        for (String g : direct) {
            all.addAll(ancestorsOf(g, lookup));
        }
        return all;
    }

    private Set<String> ancestorsOf(String groupId, Function<String, GroupRecord> lookup) {
        Set<String> cached = closureCache.get(groupId);
        if (cached != null) {
            return cached;
        }
        Set<String> chain = new HashSet<>();
        String cur = groupId;
        int guard = 0;
        while (cur != null && chain.add(cur) && guard++ < 64) {     // guard against cycles / runaway depth
            GroupRecord g = lookup.apply(cur);
            cur = g == null || g.deleted() ? null : g.parentId();
        }
        Set<String> result = Set.copyOf(chain);
        closureCache.put(groupId, result);
        return result;
    }
}
