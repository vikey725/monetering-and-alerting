package com.chargemon.ocpp.codec.mapper;

import com.chargemon.ocpp.model.MeterValue;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;

/** Shared parser: MeterValue shape is identical in 1.6 and 2.0.1 except the value type (string vs number). */
public final class MeterValueParser {

    private MeterValueParser() {
    }

    public static List<MeterValue> parseList(JsonNode array) {
        if (array == null || !array.isArray()) {
            return List.of();
        }
        List<MeterValue> out = new ArrayList<>(array.size());
        for (JsonNode mv : array) {
            out.add(parseOne(mv));
        }
        return List.copyOf(out);
    }

    public static MeterValue parseOne(JsonNode mv) {
        List<MeterValue.SampledValue> sampled = new ArrayList<>();
        JsonNode sv = mv.get("sampledValue");
        if (sv != null && sv.isArray()) {
            for (JsonNode s : sv) {
                sampled.add(new MeterValue.SampledValue(
                        Json.decimal(s, "value"),
                        Json.text(s, "measurand"),
                        Json.text(s, "context"),
                        unit(s),
                        Json.text(s, "location"),
                        Json.text(s, "phase")));
            }
        }
        return new MeterValue(Json.instant(mv, "timestamp"), List.copyOf(sampled));
    }

    private static String unit(JsonNode s) {
        JsonNode u = s.get("unitOfMeasure");           // 2.0.1: {"unit":"kWh","multiplier":0}
        if (u != null && u.isObject()) {
            return Json.text(u, "unit");
        }
        return Json.text(s, "unit");                    // 1.6
    }
}
