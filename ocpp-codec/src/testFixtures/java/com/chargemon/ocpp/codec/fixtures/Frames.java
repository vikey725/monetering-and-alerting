package com.chargemon.ocpp.codec.fixtures;

import com.chargemon.common.json.JsonMapperFactory;
import com.chargemon.ocpp.codec.envelope.RawEnvelope;
import com.chargemon.ocpp.model.Direction;
import com.chargemon.ocpp.model.OcppVersion;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;

/** Builders for OCPP-J frames and envelopes used across module tests and the generator. */
public final class Frames {

    public static final ObjectMapper JSON = JsonMapperFactory.standard();
    public static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private Frames() {
    }

    public static ArrayNode call(String uniqueId, String action, JsonNode payload) {
        ArrayNode a = JSON.createArrayNode();
        a.add(2).add(uniqueId).add(action).add(payload);
        return a;
    }

    public static ArrayNode callResult(String uniqueId, JsonNode payload) {
        ArrayNode a = JSON.createArrayNode();
        a.add(3).add(uniqueId).add(payload);
        return a;
    }

    public static ArrayNode callError(String uniqueId, String code, String description) {
        ArrayNode a = JSON.createArrayNode();
        a.add(4).add(uniqueId).add(code).add(description).add(JSON.createObjectNode());
        return a;
    }

    public static ObjectNode obj(Object... kv) {
        ObjectNode o = JSON.createObjectNode();
        for (int i = 0; i < kv.length; i += 2) {
            o.set((String) kv[i], JSON.valueToTree(kv[i + 1]));
        }
        return o;
    }

    public static RawEnvelope envelope(String stationId, OcppVersion v, Direction d, Instant receivedAt, JsonNode frame) {
        return new RawEnvelope(stationId, v, d, receivedAt, frame, "test-0-0");
    }

    public static RawEnvelope fromStation(String stationId, OcppVersion v, Instant at, JsonNode frame) {
        return envelope(stationId, v, Direction.STATION_TO_CSMS, at, frame);
    }

    public static RawEnvelope fromCsms(String stationId, OcppVersion v, Instant at, JsonNode frame) {
        return envelope(stationId, v, Direction.CSMS_TO_STATION, at, frame);
    }

    /** JSON body of an envelope as it appears on common-broker. The record key is {@code e.stationId()}; the body has no station id. */
    public static String envelopeJson(RawEnvelope e) {
        ObjectNode o = JSON.createObjectNode();
        o.put("ocppVersion", e.ocppVersion().wire());
        o.put("direction", e.direction().name());
        o.put("receivedAt", e.receivedAt().toString());
        o.set("message", e.frame());
        return o.toString();
    }

    // ---- canned payloads ----

    public static ObjectNode heartbeat() {
        return JSON.createObjectNode();
    }

    public static ObjectNode boot16(String vendor, String model, String fw) {
        return obj("chargePointVendor", vendor, "chargePointModel", model, "firmwareVersion", fw);
    }

    public static ObjectNode boot201(String vendor, String model, String fw, String reason) {
        return obj("chargingStation", obj("vendorName", vendor, "model", model, "firmwareVersion", fw), "reason", reason);
    }

    public static ObjectNode bootResponse(String status, int interval, Instant now) {
        return obj("status", status, "interval", interval, "currentTime", now.toString());
    }

    public static ObjectNode status16(int connectorId, String status, String errorCode, Instant ts) {
        return obj("connectorId", connectorId, "status", status, "errorCode", errorCode, "timestamp", ts.toString());
    }

    public static ObjectNode status201(int evseId, int connectorId, String status, Instant ts) {
        return obj("evseId", evseId, "connectorId", connectorId, "connectorStatus", status, "timestamp", ts.toString());
    }

    public static ObjectNode startTx16(int connectorId, String idTag, long meterStart, Instant ts) {
        return obj("connectorId", connectorId, "idTag", idTag, "meterStart", meterStart, "timestamp", ts.toString());
    }

    public static ObjectNode startTxResponse16(int transactionId, String status) {
        return obj("transactionId", transactionId, "idTagInfo", obj("status", status));
    }

    public static ObjectNode stopTx16(int transactionId, long meterStop, String reason, Instant ts) {
        return obj("transactionId", transactionId, "meterStop", meterStop, "reason", reason, "timestamp", ts.toString());
    }

    public static ObjectNode sampledEnergy(Instant ts, long wh) {
        return obj("timestamp", ts.toString(), "sampledValue", JSON.createArrayNode().add(
                obj("value", Long.toString(wh), "measurand", "Energy.Active.Import.Register", "unit", "Wh")));
    }

    public static ObjectNode txEvent201(String eventType, String txId, int seqNo, Instant ts, String trigger,
                                        Long energyWh, Integer evseId) {
        ObjectNode o = obj("eventType", eventType, "seqNo", seqNo, "timestamp", ts.toString(), "triggerReason", trigger,
                "transactionInfo", obj("transactionId", txId));
        if (evseId != null) {
            o.set("evse", obj("id", evseId, "connectorId", 1));
        }
        if (energyWh != null) {
            o.set("meterValue", JSON.createArrayNode().add(obj("timestamp", ts.toString(), "sampledValue",
                    JSON.createArrayNode().add(obj("value", energyWh, "measurand", "Energy.Active.Import.Register",
                            "unitOfMeasure", obj("unit", "Wh"))))));
        }
        return o;
    }

    public static ObjectNode authorize16(String idTag) {
        return obj("idTag", idTag);
    }

    public static ObjectNode authorizeResponse16(String status) {
        return obj("idTagInfo", obj("status", status));
    }

    public static ObjectNode securityEvent201(String type, Instant ts) {
        return obj("type", type, "timestamp", ts.toString());
    }
}
