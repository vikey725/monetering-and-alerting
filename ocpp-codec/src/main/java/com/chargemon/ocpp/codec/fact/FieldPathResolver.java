package com.chargemon.ocpp.codec.fact;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Resolves dotted paths with optional indexes ({@code meterValues[0].sampledValues[1].value})
 * against a JsonNode tree and converts leaves to plain Java values.
 */
public final class FieldPathResolver {

    private FieldPathResolver() {
    }

    public static Optional<Object> resolve(JsonNode root, String path) {
        JsonNode node = node(root, path);
        return node == null ? Optional.empty() : Optional.ofNullable(toJava(node));
    }

    public static JsonNode node(JsonNode root, String path) {
        if (root == null || path == null || path.isEmpty()) {
            return null;
        }
        JsonNode cur = root;
        for (String seg : path.split("\\.")) {
            if (cur == null || cur.isNull() || cur.isMissingNode()) {
                return null;
            }
            int bracket = seg.indexOf('[');
            String name = bracket < 0 ? seg : seg.substring(0, bracket);
            if (!name.isEmpty()) {
                cur = cur.get(name);
            }
            while (bracket >= 0 && cur != null) {
                int close = seg.indexOf(']', bracket);
                int idx = Integer.parseInt(seg.substring(bracket + 1, close));
                cur = cur.isArray() ? cur.get(idx) : null;
                bracket = seg.indexOf('[', close);
            }
        }
        return cur == null || cur.isNull() || cur.isMissingNode() ? null : cur;
    }

    public static Object toJava(JsonNode n) {
        if (n == null || n.isNull()) {
            return null;
        }
        if (n.isTextual()) {
            return n.asText();
        }
        if (n.isBoolean()) {
            return n.asBoolean();
        }
        if (n.isIntegralNumber()) {
            return n.asLong();
        }
        if (n.isNumber()) {
            return n.decimalValue();
        }
        if (n.isArray()) {
            List<Object> out = new ArrayList<>(n.size());
            n.forEach(e -> out.add(toJava(e)));
            return out;
        }
        return n;    // objects are returned as-is; conditions on objects use `exists`
    }

    public static Optional<BigDecimal> asNumber(Object v) {
        if (v instanceof BigDecimal bd) {
            return Optional.of(bd);
        }
        if (v instanceof Number num) {
            return Optional.of(new BigDecimal(num.toString()));
        }
        if (v instanceof String s) {
            try {
                return Optional.of(new BigDecimal(s.trim()));
            } catch (NumberFormatException e) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }
}
