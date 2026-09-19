package com.chargemon.ocpp.codec.frame;

import com.chargemon.common.result.Result;
import com.chargemon.ocpp.model.MessageType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/** Turns the OCPP-J JSON array into a typed {@link RawFrame}. */
public final class FrameParser {

    public Result<RawFrame, String> parse(JsonNode array) {
        if (array == null || !array.isArray() || array.size() < 3) {
            return Result.err("frame must be an array with at least 3 elements");
        }
        if (!array.get(0).isInt()) {
            return Result.err("frame[0] must be the numeric message type");
        }
        MessageType type;
        try {
            type = MessageType.fromId(array.get(0).asInt());
        } catch (IllegalArgumentException e) {
            return Result.err(e.getMessage());
        }
        String uniqueId = array.get(1).asText();
        if (uniqueId.isBlank()) {
            return Result.err("frame[1] uniqueId is blank");
        }
        return switch (type) {
            case CALL -> {
                if (array.size() < 4 || !array.get(2).isTextual()) {
                    yield Result.err("CALL requires [2, id, action, payload]");
                }
                yield Result.ok(new RawFrame.Call(uniqueId, array.get(2).asText(), orEmpty(array.get(3))));
            }
            case CALLRESULT -> Result.ok(new RawFrame.CallResult(uniqueId, orEmpty(array.get(2))));
            case CALLERROR -> {
                if (array.size() < 4) {
                    yield Result.err("CALLERROR requires [4, id, code, description, details]");
                }
                JsonNode details = array.size() > 4 ? orEmpty(array.get(4)) : JsonNodeFactory.instance.objectNode();
                yield Result.ok(new RawFrame.CallError(uniqueId, array.get(2).asText(), array.get(3).asText(), details));
            }
        };
    }

    private static JsonNode orEmpty(JsonNode n) {
        return n == null || n.isNull() ? JsonNodeFactory.instance.objectNode() : n;
    }
}
