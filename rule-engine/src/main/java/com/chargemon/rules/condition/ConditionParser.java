package com.chargemon.rules.condition;

import com.chargemon.common.json.JsonMapperFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;

/** Builds {@link Condition} trees from JSON and back. Operators discovered via ServiceLoader. */
public final class ConditionParser {

    private final ObjectMapper mapper;

    private ConditionParser(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public static ConditionParser fromServiceLoader() {
        List<ConditionOperatorProvider> providers = new ArrayList<>();
        ServiceLoader.load(ConditionOperatorProvider.class).forEach(providers::add);
        return of(providers);
    }

    public static ConditionParser of(Collection<? extends ConditionOperatorProvider> providers) {
        ObjectMapper m = JsonMapperFactory.standard().copy();
        for (ConditionOperatorProvider p : providers) {
            p.operators().forEach((op, type) -> m.registerSubtypes(new NamedType(type, op)));
        }
        return new ConditionParser(m);
    }

    public Condition parse(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        try {
            return mapper.treeToValue(node, Condition.class);
        } catch (Exception e) {
            throw new ConditionParseException("invalid condition: " + e.getMessage(), e);
        }
    }

    public Condition parse(String json) {
        try {
            return parse(mapper.readTree(json));
        } catch (java.io.IOException e) {
            throw new ConditionParseException("condition is not JSON: " + e.getMessage(), e);
        }
    }

    public JsonNode toJson(Condition c) {
        return mapper.valueToTree(c);
    }

    public static final class ConditionParseException extends RuntimeException {
        public ConditionParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
