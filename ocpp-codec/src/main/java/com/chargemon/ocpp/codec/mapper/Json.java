package com.chargemon.ocpp.codec.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.format.DateTimeParseException;

/** Null-safe payload accessors shared by all mappers. */
public final class Json {

    private Json() {
    }

    public static String text(JsonNode n, String field) {
        JsonNode v = n == null ? null : n.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    public static String requireText(JsonNode n, String field) {
        String v = text(n, field);
        if (v == null) {
            throw new MappingException("missing required field '" + field + "'");
        }
        return v;
    }

    public static Integer integer(JsonNode n, String field) {
        JsonNode v = n == null ? null : n.get(field);
        return v == null || v.isNull() ? null : v.asInt();
    }

    public static int intOr(JsonNode n, String field, int def) {
        Integer v = integer(n, field);
        return v == null ? def : v;
    }

    public static Long longVal(JsonNode n, String field) {
        JsonNode v = n == null ? null : n.get(field);
        return v == null || v.isNull() ? null : v.asLong();
    }

    public static long requireLong(JsonNode n, String field) {
        Long v = longVal(n, field);
        if (v == null) {
            throw new MappingException("missing required field '" + field + "'");
        }
        return v;
    }

    public static Boolean bool(JsonNode n, String field) {
        JsonNode v = n == null ? null : n.get(field);
        return v == null || v.isNull() ? null : v.asBoolean();
    }

    public static BigDecimal decimal(JsonNode n, String field) {
        JsonNode v = n == null ? null : n.get(field);
        if (v == null || v.isNull()) {
            return null;
        }
        try {
            return v.isNumber() ? v.decimalValue() : new BigDecimal(v.asText().trim());
        } catch (NumberFormatException e) {
            throw new MappingException("field '" + field + "' is not numeric: " + v.asText());
        }
    }

    public static Instant instant(JsonNode n, String field) {
        String s = text(n, field);
        if (s == null) {
            return null;
        }
        try {
            return Instant.parse(s);
        } catch (DateTimeParseException e) {
            try {
                return java.time.OffsetDateTime.parse(s).toInstant();
            } catch (DateTimeParseException e2) {
                throw new MappingException("field '" + field + "' is not an ISO-8601 timestamp: " + s);
            }
        }
    }

    public static JsonNode obj(JsonNode n, String field) {
        JsonNode v = n == null ? null : n.get(field);
        return v == null || v.isNull() ? null : v;
    }
}
