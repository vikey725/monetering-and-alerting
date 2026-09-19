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
 * Parses the upstream envelope. Field names are tolerant to the common
 * variants so a producer change is a one-line adjustment here, not a job change.
 *
 * <pre>{"stationId":"ST-1","ocppVersion":"2.0.1","direction":"STATION_TO_CSMS",
 *       "receivedAt":"2026-01-01T00:00:00Z","message":[2,"42","Heartbeat",{}]}</pre>
 */
public final class EnvelopeParser {

    private static final List<String> STATION_KEYS = List.of("stationId", "chargePointId", "chargingStationId", "cpId");
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

    public Result<RawEnvelope, String> parse(byte[] bytes, String sourceRef) {
        try {
            return parse(mapper.readTree(bytes), sourceRef);
        } catch (IOException e) {
            return Result.err("envelope is not valid JSON: " + e.getMessage());
        }
    }

    public Result<RawEnvelope, String> parse(String json, String sourceRef) {
        try {
            return parse(mapper.readTree(json), sourceRef);
        } catch (IOException e) {
            return Result.err("envelope is not valid JSON: " + e.getMessage());
        }
    }

    public Result<RawEnvelope, String> parse(JsonNode root, String sourceRef) {
        if (root == null || !root.isObject()) {
            return Result.err("envelope root is not an object");
        }
        JsonNode station = first(root, STATION_KEYS);
        if (station == null || station.asText().isBlank()) {
            return Result.err("envelope missing stationId");
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
            return Result.ok(new RawEnvelope(station.asText(), version, direction, receivedAt, frame, sourceRef));
        } catch (IllegalArgumentException | java.time.DateTimeException e) {
            return Result.err(e.getMessage());
        }
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
