package com.chargemon.rules.definition;

import com.chargemon.alert.ChannelRef;
import com.chargemon.alert.RuleTiming;
import com.chargemon.alert.Severity;
import com.chargemon.alert.SubjectType;
import com.chargemon.common.json.JsonMapperFactory;
import com.chargemon.common.result.Result;
import com.chargemon.common.time.Durations;
import com.chargemon.rules.condition.Condition;
import com.chargemon.rules.condition.ConditionParser;
import com.chargemon.rules.definition.RuleSpec.AggregateSource;
import com.chargemon.rules.definition.RuleSpec.Threshold;
import com.chargemon.rules.definition.RuleSpec.Trigger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Parses the rule row shape (Postgres row via Debezium, or a JSON document) into
 * a {@link RuleDefinition}. Explicit per-kind switch keeps spec parsing obvious.
 */
public final class RuleDefinitionParser {

    private final ConditionParser conditions;
    private final ObjectMapper json;

    public RuleDefinitionParser(ConditionParser conditions) {
        this(conditions, JsonMapperFactory.standard());
    }

    public RuleDefinitionParser(ConditionParser conditions, ObjectMapper json) {
        this.conditions = conditions;
        this.json = json;
    }

    public static RuleDefinitionParser fromServiceLoader() {
        return new RuleDefinitionParser(ConditionParser.fromServiceLoader());
    }

    public Result<RuleDefinition, String> parse(String jsonText) {
        try {
            return parse(json.readTree(jsonText));
        } catch (IOException e) {
            return Result.err("rule is not JSON: " + e.getMessage());
        }
    }

    public Result<RuleDefinition, String> parse(JsonNode row) {
        row = camelCaseKeys(row);
        try {
            RuleKind kind = RuleKind.valueOf(req(row, "kind").toUpperCase(Locale.ROOT));
            SubjectType subjectType = row.hasNonNull("subjectType")
                    ? SubjectType.valueOf(row.get("subjectType").asText().toUpperCase(Locale.ROOT))
                    : (kind == RuleKind.GROUP_AGGREGATE ? SubjectType.GROUP : SubjectType.STATION);
            RuleTiming timing = new RuleTiming(
                    duration(row, "graceWindow"),
                    duration(row, "suppressionWindow"),
                    duration(row, "autoResolveAfter"));
            List<ChannelRef> channels = new ArrayList<>();
            for (String s : strings(row.get("channels"))) {
                channels.add(ChannelRef.parse(s));
            }
            RuleDefinition def = new RuleDefinition(
                    req(row, "id"),
                    row.path("name").asText(""),
                    kind,
                    subjectType,
                    new HashSet<>(strings(row.get("targetGroupIds"))),
                    condition(row.get("stationFilter")),
                    parseSpec(kind, specNode(row)),
                    timing,
                    Severity.valueOf(req(row, "severity").toUpperCase(Locale.ROOT)),
                    channels,
                    row.path("enabled").asBoolean(true),
                    row.path("version").asInt(1));
            return Result.ok(def);
        } catch (RuntimeException e) {
            return Result.err("invalid rule: " + e.getMessage());
        }
    }

    /** Debezium emits Postgres column names (snake_case); normalize to the document shape. */
    private JsonNode camelCaseKeys(JsonNode row) {
        if (row == null || !row.isObject()) {
            return row;
        }
        var out = json.createObjectNode();
        row.properties().forEach(e -> out.set(toCamel(e.getKey()), e.getValue()));
        return out;
    }

    private static String toCamel(String key) {
        if (key.indexOf('_') < 0) {
            return key;
        }
        StringBuilder sb = new StringBuilder(key.length());
        boolean up = false;
        for (char c : key.toCharArray()) {
            if (c == '_') {
                up = true;
            } else {
                sb.append(up ? Character.toUpperCase(c) : c);
                up = false;
            }
        }
        return sb.toString();
    }

    private JsonNode specNode(JsonNode row) {
        JsonNode spec = row.get("spec");
        if (spec == null || spec.isNull()) {
            throw new IllegalArgumentException("missing spec");
        }
        if (spec.isTextual()) {       // Debezium may deliver jsonb as a string
            try {
                return json.readTree(spec.asText());
            } catch (IOException e) {
                throw new IllegalArgumentException("spec is not JSON: " + e.getMessage());
            }
        }
        return spec;
    }

    private RuleSpec parseSpec(RuleKind kind, JsonNode s) {
        return switch (kind) {
            case EVENT -> new RuleSpec.EventSpec(trigger(s.get("trigger")), condition(s.get("condition")),
                    condition(s.get("clear")));
            case ABSENCE -> new RuleSpec.AbsenceSpec(s.path("expectedAction").asText("*"), duration(s, "within"),
                    condition(s.get("onlyIf")));
            case STATE_DURATION -> new RuleSpec.StateDurationSpec(condition(s.get("enter")), condition(s.get("exit")),
                    duration(s, "maxDuration"), s.hasNonNull("scopeField") ? s.get("scopeField").asText() : null);
            case SEQUENCE -> new RuleSpec.SequenceSpec(strings(s.get("allOf")), duration(s, "within"));
            case GROUP_AGGREGATE -> {
                JsonNode src = s.get("source");
                JsonNode th = s.get("threshold");
                yield new RuleSpec.GroupAggregateSpec(
                        src == null ? null : new AggregateSource(src.path("type").asText(null),
                                src.hasNonNull("ruleId") ? src.get("ruleId").asText() : null,
                                src.hasNonNull("window") ? src.get("window").asText() : null),
                        duration(s, "window"),
                        th == null ? null : new Threshold(th.hasNonNull("count") ? th.get("count").asInt() : null,
                                th.hasNonNull("percent") ? th.get("percent").asDouble() : null));
            }
        };
    }

    private Trigger trigger(JsonNode t) {
        if (t == null || t.isNull()) {
            return new Trigger(Set.of(), null);
        }
        if (t.isTextual()) {
            return new Trigger(Set.of(t.asText()), null);
        }
        return new Trigger(new HashSet<>(strings(t.get("actions"))),
                t.hasNonNull("aggregate") ? t.get("aggregate").asText() : null);
    }

    private Condition condition(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isTextual()) {
            return conditions.parse(node.asText());
        }
        return conditions.parse(node);
    }

    private static Duration duration(JsonNode n, String field) {
        JsonNode v = n == null ? null : n.get(field);
        if (v == null || v.isNull()) {
            return null;
        }
        return Durations.parse(v.asText());
    }

    private static String req(JsonNode n, String field) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull() || v.asText().isBlank()) {
            throw new IllegalArgumentException("missing " + field);
        }
        return v.asText();
    }

    private List<String> strings(JsonNode n) {
        if (n == null || n.isNull()) {
            return List.of();
        }
        if (n.isTextual()) {                  // Postgres array rendered as text: {a,b} or JSON string
            String s = n.asText().trim();
            if (s.startsWith("[")) {
                try {
                    return strings(json.readTree(s));
                } catch (IOException e) {
                    throw new IllegalArgumentException("bad array: " + s);
                }
            }
            if (s.startsWith("{") && s.endsWith("}")) {
                s = s.substring(1, s.length() - 1);
            }
            List<String> out = new ArrayList<>();
            for (String p : s.split(",")) {
                if (!p.isBlank()) {
                    out.add(p.trim().replace("\"", ""));
                }
            }
            return out;
        }
        List<String> out = new ArrayList<>();
        n.forEach(e -> out.add(e.asText()));
        return out;
    }
}
