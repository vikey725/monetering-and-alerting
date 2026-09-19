package com.chargemon.ocpp.model;

/** OCPP-J framing type id (first element of the JSON array). */
public enum MessageType {
    CALL(2),
    CALLRESULT(3),
    CALLERROR(4);

    private final int id;

    MessageType(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public static MessageType fromId(int id) {
        for (MessageType t : values()) {
            if (t.id == id) {
                return t;
            }
        }
        throw new IllegalArgumentException("Unknown OCPP message type id: " + id);
    }
}
