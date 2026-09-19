package com.chargemon.ocpp.codec.frame;

import com.chargemon.ocpp.model.MessageType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;

/** Decoded OCPP-J frame; payloads stay as JsonNode until a mapper claims them. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes({
    @JsonSubTypes.Type(value = RawFrame.Call.class, name = "call"),
    @JsonSubTypes.Type(value = RawFrame.CallResult.class, name = "result"),
    @JsonSubTypes.Type(value = RawFrame.CallError.class, name = "error"),
})
public sealed interface RawFrame permits RawFrame.Call, RawFrame.CallResult, RawFrame.CallError {

    String uniqueId();

    @JsonIgnore
    MessageType type();

    record Call(String uniqueId, String action, JsonNode payload) implements RawFrame {
        @Override
        public MessageType type() {
            return MessageType.CALL;
        }
    }

    record CallResult(String uniqueId, JsonNode payload) implements RawFrame {
        @Override
        public MessageType type() {
            return MessageType.CALLRESULT;
        }
    }

    record CallError(String uniqueId, String errorCode, String description, JsonNode details) implements RawFrame {
        @Override
        public MessageType type() {
            return MessageType.CALLERROR;
        }
    }
}
