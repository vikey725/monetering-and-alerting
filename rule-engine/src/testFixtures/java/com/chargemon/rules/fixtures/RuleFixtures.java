package com.chargemon.rules.fixtures;

import com.chargemon.common.result.Result;
import com.chargemon.rules.definition.RuleDefinition;
import com.chargemon.rules.definition.RuleDefinitionParser;

/** Canned rule JSON documents for tests. */
public final class RuleFixtures {

    public static final RuleDefinitionParser PARSER = RuleDefinitionParser.fromServiceLoader();

    private RuleFixtures() {
    }

    public static RuleDefinition parse(String json) {
        return switch (PARSER.parse(json)) {
            case Result.Ok<RuleDefinition, String> ok -> ok.value();
            case Result.Err<RuleDefinition, String> err -> throw new IllegalArgumentException(err.error());
        };
    }

    public static String faultedRule(String id, String grace, String suppression) {
        return """
            {"id":"%s","name":"Connector faulted","kind":"EVENT","subjectType":"STATION",
             "spec":{"trigger":{"actions":["StatusNotification"]},
                     "condition":{"op":"eq","field":"event.status","value":"FAULTED"}},
             "graceWindow":"%s","suppressionWindow":"%s","severity":"HIGH","channels":["slack:#ops"],"enabled":true,"version":1}
            """.formatted(id, grace, suppression);
    }

    public static String heartbeatAbsenceRule(String id, String within) {
        return """
            {"id":"%s","name":"No heartbeat","kind":"ABSENCE",
             "spec":{"expectedAction":"Heartbeat","within":"%s"},
             "graceWindow":"PT0S","suppressionWindow":"PT0S","severity":"CRITICAL","channels":["pagerduty:svc"]}
            """.formatted(id, within);
    }

    public static String stuckPreparingRule(String id, String max) {
        return """
            {"id":"%s","name":"Stuck preparing","kind":"STATE_DURATION",
             "spec":{"enter":{"op":"eq","field":"event.status","value":"PREPARING"},
                     "exit":{"op":"ne","field":"event.status","value":"PREPARING"},
                     "maxDuration":"%s","scopeField":"event.connectorId"},
             "severity":"MEDIUM"}
            """.formatted(id, max);
    }

    public static String zeroEnergyRule(String id, String window, int min) {
        return """
            {"id":"%s","name":"Zero energy sessions","kind":"EVENT",
             "spec":{"trigger":{"aggregate":"zeroEnergy"},
                     "condition":{"op":"gte","field":"agg.zeroEnergy.%s","value":%d}},
             "severity":"LOW"}
            """.formatted(id, window, min);
    }

    public static String sequenceRule(String id, String a, String b, String within) {
        return """
            {"id":"%s","name":"Faulted and zero energy","kind":"SEQUENCE",
             "spec":{"allOf":["%s","%s"],"within":"%s"},"severity":"HIGH"}
            """.formatted(id, a, b, within);
    }

    public static String groupAlertsRule(String id, String sourceRule, String window, int count) {
        return """
            {"id":"%s","name":"Many offline in group","kind":"GROUP_AGGREGATE","subjectType":"GROUP",
             "targetGroupIds":["region:eu"],
             "spec":{"source":{"type":"alerts","ruleId":"%s"},"window":"%s","threshold":{"count":%d}},
             "severity":"CRITICAL"}
            """.formatted(id, sourceRule, window, count);
    }
}
