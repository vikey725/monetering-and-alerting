package com.chargemon.rules.definition;

import static org.assertj.core.api.Assertions.assertThat;

import com.chargemon.alert.ChannelRef;
import com.chargemon.alert.Severity;
import com.chargemon.alert.SubjectType;
import com.chargemon.common.result.Result;
import com.chargemon.rules.fixtures.RuleFixtures;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RuleDefinitionParserTest {

    private final RuleValidator validator = new RuleValidator();

    @Test
    void parsesEventRule() {
        RuleDefinition r = RuleFixtures.parse(RuleFixtures.faultedRule("r1", "PT2M", "PT1H"));
        assertThat(r.kind()).isEqualTo(RuleKind.EVENT);
        assertThat(r.subjectType()).isEqualTo(SubjectType.STATION);
        assertThat(r.timing().graceWindow()).isEqualTo(Duration.ofMinutes(2));
        assertThat(r.timing().suppressionWindow()).isEqualTo(Duration.ofHours(1));
        assertThat(r.severity()).isEqualTo(Severity.HIGH);
        assertThat(r.channels()).containsExactly(ChannelRef.parse("slack:#ops"));
        assertThat(((RuleSpec.EventSpec) r.spec()).trigger().matchesAction("StatusNotification")).isTrue();
        assertThat(validator.validate(r).valid()).isTrue();
    }

    @Test
    void parsesEveryKindAndDefaultsSubjectType() {
        RuleDefinition abs = RuleFixtures.parse(RuleFixtures.heartbeatAbsenceRule("a", "PT10M"));
        RuleDefinition stuck = RuleFixtures.parse(RuleFixtures.stuckPreparingRule("s", "PT30M"));
        RuleDefinition seq = RuleFixtures.parse(RuleFixtures.sequenceRule("q", "a", "s", "PT15M"));
        RuleDefinition grp = RuleFixtures.parse(RuleFixtures.groupAlertsRule("g", "a", "PT1H", 10));
        assertThat(abs.spec()).isInstanceOf(RuleSpec.AbsenceSpec.class);
        assertThat(stuck.spec()).isInstanceOf(RuleSpec.StateDurationSpec.class);
        assertThat(seq.spec()).isInstanceOf(RuleSpec.SequenceSpec.class);
        assertThat(grp.subjectType()).isEqualTo(SubjectType.GROUP);
        assertThat(grp.targetGroupIds()).containsExactly("region:eu");
        for (RuleDefinition r : new RuleDefinition[] {abs, stuck, seq, grp}) {
            assertThat(validator.validate(r).errors()).as(r.id()).isEmpty();
        }
        assertThat(validator.validateReferences(seq, Map.of("a", abs, "s", stuck)).valid()).isTrue();
        assertThat(validator.validateReferences(seq, Map.of("a", grp)).valid()).isFalse();
    }

    @Test
    void acceptsDebeziumStyleTextColumns() {
        String row = """
            {"id":"x","name":"n","kind":"EVENT","subject_type":"STATION","target_group_ids":"{site:1,site:2}",
             "spec":"{\\"trigger\\":\\"Heartbeat\\",\\"condition\\":{\\"op\\":\\"exists\\",\\"field\\":\\"event.type\\"}}",
             "grace_window":"PT1M","severity":"INFO","channels":"{slack:#a,email:ops@x.io}","enabled":true,"version":3}
            """.replace("subject_type", "subjectType").replace("target_group_ids", "targetGroupIds")
               .replace("grace_window", "graceWindow");
        Result<RuleDefinition, String> r = RuleFixtures.PARSER.parse(row);
        assertThat(r.isOk()).as(r.toString()).isTrue();
        RuleDefinition d = ((Result.Ok<RuleDefinition, String>) r).value();
        assertThat(d.targetGroupIds()).isEqualTo(Set.of("site:1", "site:2"));
        assertThat(d.channels()).hasSize(2);
        assertThat(d.version()).isEqualTo(3);
    }

    @Test
    void reportsErrors() {
        assertThat(RuleFixtures.PARSER.parse("{\"id\":\"x\"}").isOk()).isFalse();
        RuleDefinition bad = RuleFixtures.parse("""
            {"id":"b","name":"","kind":"GROUP_AGGREGATE","subjectType":"STATION",
             "spec":{"source":{"type":"alerts"},"threshold":{}},"severity":"LOW"}
            """);
        assertThat(validator.validate(bad).errors()).hasSizeGreaterThanOrEqualTo(3);
    }
}
