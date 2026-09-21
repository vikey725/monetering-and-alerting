package com.chargemon.ocpp.codec.envelope;

import com.chargemon.common.json.JsonMapperFactory;
import com.chargemon.common.result.Result;
import com.chargemon.ocpp.model.Direction;
import com.chargemon.ocpp.model.OcppVersion;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * Parses the upstream envelope. The station id is the Kafka record <em>key</em>, not a body
 * field; callers pass it in. Body field names are tolerant to the common variants so a
 * producer change is a one-line adjustment here, not a job change.
 *
 * <pre>key:   ST-1
 * value: {"ocppVersion":"2.0.1","direction":"STATION_TO_CSMS",
 *         "receivedAt":"2026-01-01T00:00:00Z","message":[2,"42","Heartbeat",{}]}</pre>
 */
public final class EnvelopeParser {

    private static final List<String> VERSION_KEYS = List.of("ocppVersion", "protocol", "version");
    private static final List<String> DIRECTION_KEYS = List.of("direction", "dir");
    private static final List<String> TIME_KEYS = List.of("receivedAt", "timestamp", "ts");
    private static final List<String> FRAME_KEYS = List.of("message", "frame", "payload", "ocppMessage");

    private final ObjectMapper mapper;

    public EnvelopeParser() {
        this(JsonMapperFactory.standard());
    }

    public EnvelopeParser(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /** @param stationId the Kafka record key; blank or null is an error */
    public Result<RawEnvelope, String> parse(String stationId, byte[] bytes, String sourceRef) {
        if (isBlank(stationId)) {
            return Result.err("record key missing stationId");
        }
        try {
            return parse(stationId, mapper.readTree(bytes), sourceRef);
        } catch (IOException e) {
            return Result.err("envelope is not valid JSON: " + e.getMessage());
        }
    }

    public Result<RawEnvelope, String> parse(String stationId, String json, String sourceRef) {
        if (isBlank(stationId)) {
            return Result.err("record key missing stationId");
        }
        try {
            return parse(stationId, mapper.readTree(json), sourceRef);
        } catch (IOException e) {
            return Result.err("envelope is not valid JSON: " + e.getMessage());
        }
    }

    public Result<RawEnvelope, String> parse(String stationId, JsonNode root, String sourceRef) {
        if (isBlank(stationId)) {
            return Result.err("record key missing stationId");
        }
        if (root == null || !root.isObject()) {
            return Result.err("envelope root is not an object");
        }
        JsonNode frame = first(root, FRAME_KEYS);
        if (frame == null) {
            return Result.err("envelope missing message frame");
        }
        if (frame.isTextual()) {
            try {
                frame = mapper.readTree(frame.asText());
            } catch (IOException e) {
                return Result.err("frame string is not valid JSON: " + e.getMessage());
            }
        }
        if (!frame.isArray()) {
            return Result.err("frame is not a JSON array");
        }
        try {
            OcppVersion version = OcppVersion.parse(text(first(root, VERSION_KEYS)));
            Direction direction = parseDirection(first(root, DIRECTION_KEYS));
            JsonNode time = first(root, TIME_KEYS);
            Instant receivedAt = time == null ? Instant.now() : parseInstant(time);
            return Result.ok(new RawEnvelope(stationId, version, direction, receivedAt, frame, sourceRef));
        } catch (IllegalArgumentException | java.time.DateTimeException e) {
            return Result.err(e.getMessage());
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    private static Direction parseDirection(JsonNode node) {
        return node == null ? Direction.STATION_TO_CSMS : Direction.parse(node.asText());
    }

    private static Instant parseInstant(JsonNode node) {
        return node.isNumber() ? Instant.ofEpochMilli(node.asLong()) : Instant.parse(node.asText());
    }

    private static JsonNode first(JsonNode root, List<String> keys) {
        for (String k : keys) {
            JsonNode n = root.get(k);
            if (n != null && !n.isNull()) {
                return n;
            }
        }
        return null;
    }

    private static String text(JsonNode n) {
        return n == null ? null : n.asText();
    }
}
