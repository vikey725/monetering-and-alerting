package com.chargemon.flink.control;

import com.chargemon.common.result.Result;
import com.chargemon.flink.model.RuleChange;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.definition.RuleDefinitionParser;
import com.chargemon.rules.definition.RuleValidator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared handling of the rules broadcast state: raw JSON lives in Flink state,
 * parsed definitions live in a per-operator cache keyed by (id, version).
 * Invalid rows are logged and skipped so the last good version keeps running.
 */
public final class RuleBroadcast {

    private static final Logger LOG = LoggerFactory.getLogger(RuleBroadcast.class);

    public static final MapStateDescriptor<String, String> DESCRIPTOR =
            new MapStateDescriptor<>("rules", Types.STRING, Types.STRING);

    private final RuleDefinitionParser parser;
    private final RuleValidator validator = new RuleValidator();
    private final Map<String, RuleDefinition> cache = new HashMap<>();
    private final Map<String, String> cachedJson = new HashMap<>();
    /** Rules known before the broadcast delivered anything; superseded key by key as broadcast elements arrive. */
    private final Map<String, RuleDefinition> seeded = new HashMap<>();
    /** Snapshot of {@link #all} reused across events until the broadcast changes (or first use after restore). */
    private Map<String, RuleDefinition> activeSnapshot;

    public RuleBroadcast(RuleDefinitionParser parser) {
        this.parser = parser;
    }

    public static RuleBroadcast create() {
        return new RuleBroadcast(RuleDefinitionParser.fromServiceLoader());
    }

    /** Called from {@code open()}: makes the initial rule set visible before the broadcast catches up. */
    public void seed(RuleLoader loader) {
        for (RuleChange c : loader.load()) {
            if (!c.isDelete()) {
                parse(c.ruleId(), c.ruleJson()).ifPresent(d -> seeded.put(c.ruleId(), d));
            }
        }
    }

    /** Applies a change to broadcast state. Returns the previous definition (if any) and the new one. */
    public Change apply(RuleChange change, BroadcastState<String, String> state) throws Exception {
        activeSnapshot = null;
        Optional<RuleDefinition> before = lookup(change.ruleId(), state);
        seeded.remove(change.ruleId());                 // broadcast is authoritative from now on for this id
        if (change.isDelete()) {
            state.remove(change.ruleId());
            cache.remove(change.ruleId());
            cachedJson.remove(change.ruleId());
            return new Change(before, Optional.empty());
        }
        Optional<RuleDefinition> parsed = parse(change.ruleId(), change.ruleJson());
        if (parsed.isEmpty()) {
            return new Change(before, before);          // keep last good version
        }
        state.put(change.ruleId(), change.ruleJson());
        cache.put(change.ruleId(), parsed.get());
        cachedJson.put(change.ruleId(), change.ruleJson());
        return new Change(before, parsed);
    }

    public Optional<RuleDefinition> lookup(String ruleId, ReadOnlyBroadcastState<String, String> state) throws Exception {
        String json = state.get(ruleId);
        if (json == null) {
            cache.remove(ruleId);
            cachedJson.remove(ruleId);
            return Optional.ofNullable(seeded.get(ruleId));
        }
        if (json.equals(cachedJson.get(ruleId))) {
            return Optional.ofNullable(cache.get(ruleId));
        }
        Optional<RuleDefinition> parsed = parse(ruleId, json);
        parsed.ifPresent(d -> {
            cache.put(ruleId, d);
            cachedJson.put(ruleId, json);
        });
        return parsed;
    }

    /** All currently known rules (parsed). Computed once per broadcast change, not per event. */
    public Map<String, RuleDefinition> all(ReadOnlyBroadcastState<String, String> state) throws Exception {
        if (activeSnapshot != null) {
            return activeSnapshot;
        }
        Map<String, RuleDefinition> out = new HashMap<>(seeded);
        for (Map.Entry<String, String> e : state.immutableEntries()) {
            lookup(e.getKey(), state).ifPresent(d -> out.put(e.getKey(), d));
        }
        activeSnapshot = Map.copyOf(out);
        return activeSnapshot;
    }

    private Optional<RuleDefinition> parse(String ruleId, String json) {
        Result<RuleDefinition, String> r = parser.parse(json);
        if (r instanceof Result.Err<RuleDefinition, String> err) {
            LOG.warn("Rejecting rule {}: {}", ruleId, err.error());
            return Optional.empty();
        }
        RuleDefinition def = ((Result.Ok<RuleDefinition, String>) r).value();
        RuleValidator.ValidationResult v = validator.validate(def);
        if (!v.valid()) {
            LOG.warn("Rejecting rule {} ({}): {}", ruleId, def.name(), v.errors());
            return Optional.empty();
        }
        return Optional.of(def);
    }

    public record Change(Optional<RuleDefinition> before, Optional<RuleDefinition> after) {
        /** True when an active rule stopped applying (deleted or disabled). */
        public boolean deactivated() {
            return before.map(RuleDefinition::enabled).orElse(false) && !after.map(RuleDefinition::enabled).orElse(false);
        }
    }
}
