package com.chargemon.rules.condition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.chargemon.rules.fixtures.MapFact;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ConditionParserTest {

    private final ConditionParser parser = ConditionParser.fromServiceLoader();
    private final EvalContext ctx = EvalContext.at(Instant.EPOCH);
    private final Fact fact = MapFact.of(
            "event.status", "FAULTED", "event.connectorId", 2L, "event.value", new BigDecimal("12.5"),
            "event.flag", true, "station.vendor", "ACME Corp", "station.groups", List.of("site:1", "region:eu"));

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "{\"op\":\"eq\",\"field\":\"event.status\",\"value\":\"FAULTED\"}|true",
        "{\"op\":\"eq\",\"field\":\"event.connectorId\",\"value\":2}|true",
        "{\"op\":\"eq\",\"field\":\"event.connectorId\",\"value\":\"2\"}|true",
        "{\"op\":\"eq\",\"field\":\"event.flag\",\"value\":true}|true",
        "{\"op\":\"ne\",\"field\":\"event.status\",\"value\":\"AVAILABLE\"}|true",
        "{\"op\":\"gt\",\"field\":\"event.value\",\"value\":12}|true",
        "{\"op\":\"gte\",\"field\":\"event.value\",\"value\":12.5}|true",
        "{\"op\":\"lt\",\"field\":\"event.value\",\"value\":12}|false",
        "{\"op\":\"lte\",\"field\":\"event.connectorId\",\"value\":2}|true",
        "{\"op\":\"in\",\"field\":\"event.status\",\"values\":[\"FAULTED\",\"UNAVAILABLE\"]}|true",
        "{\"op\":\"nin\",\"field\":\"event.status\",\"values\":[\"FAULTED\"]}|false",
        "{\"op\":\"in\",\"field\":\"station.groups\",\"values\":[\"region:eu\"]}|true",
        "{\"op\":\"between\",\"field\":\"event.value\",\"min\":10,\"max\":13}|true",
        "{\"op\":\"matches\",\"field\":\"station.vendor\",\"pattern\":\"^ACME\"}|true",
        "{\"op\":\"exists\",\"field\":\"event.status\"}|true",
        "{\"op\":\"exists\",\"field\":\"event.missing\"}|false",
        "{\"op\":\"eq\",\"field\":\"event.missing\",\"value\":1}|false",
        "{\"op\":\"startsWith\",\"field\":\"station.vendor\",\"value\":\"ACME\"}|true",
        "{\"op\":\"contains\",\"field\":\"station.vendor\",\"value\":\"Corp\"}|true",
        "{\"op\":\"contains\",\"field\":\"station.groups\",\"value\":\"site:1\"}|true",
        "{\"op\":\"not\",\"arg\":{\"op\":\"eq\",\"field\":\"event.status\",\"value\":\"FAULTED\"}}|false",
        "{\"op\":\"and\",\"args\":[{\"op\":\"eq\",\"field\":\"event.status\",\"value\":\"FAULTED\"},{\"op\":\"gt\",\"field\":\"event.connectorId\",\"value\":1}]}|true",
        "{\"op\":\"or\",\"args\":[{\"op\":\"eq\",\"field\":\"event.status\",\"value\":\"X\"},{\"op\":\"gt\",\"field\":\"event.connectorId\",\"value\":1}]}|true",
    })
    void evaluatesOperators(String json, boolean expected) {
        assertThat(parser.parse(json).test(fact, ctx)).as(json).isEqualTo(expected);
    }

    @Test
    void roundTripsToJson() {
        String json = "{\"op\":\"and\",\"args\":[{\"op\":\"matches\",\"field\":\"a\",\"pattern\":\"x+\"},{\"op\":\"in\",\"field\":\"b\",\"values\":[1,\"z\"]}]}";
        Condition c = parser.parse(json);
        Condition again = parser.parse(parser.toJson(c));
        assertThat(again).isEqualTo(c);
    }

    @Test
    void rejectsUnknownOperatorAndBadRegex() {
        assertThatThrownBy(() -> parser.parse("{\"op\":\"nope\",\"field\":\"a\"}"))
                .isInstanceOf(ConditionParser.ConditionParseException.class);
        assertThatThrownBy(() -> parser.parse("{\"op\":\"matches\",\"field\":\"a\",\"pattern\":\"(\"}"))
                .isInstanceOf(ConditionParser.ConditionParseException.class);
    }
}
