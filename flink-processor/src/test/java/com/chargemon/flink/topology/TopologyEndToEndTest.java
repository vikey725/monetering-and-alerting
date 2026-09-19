package com.chargemon.flink.topology;

import static org.assertj.core.api.Assertions.assertThat;

import com.chargemon.alert.AlertEvent;
import com.chargemon.alert.AlertEventType;
import com.chargemon.flink.JobMain;
import com.chargemon.flink.config.JobConfig;
import com.chargemon.flink.model.RuleChange;
import com.chargemon.ocpp.codec.fixtures.Frames;
import com.chargemon.ocpp.model.OcppVersion;
import com.chargemon.rules.fixtures.RuleFixtures;
import java.time.Duration;
import java.time.Instant;
import com.chargemon.ocpp.model.station.GroupRecord;
import com.chargemon.ocpp.model.station.StationRecord;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TopologyEndToEndTest {

    static final MiniClusterWithClientResource CLUSTER = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(4).setNumberTaskManagers(1).build());

    @BeforeAll
    static void up() throws Exception {
        CLUSTER.before();
    }

    @AfterAll
    static void down() {
        CLUSTER.after();
    }

    private static JobConfig testConfig() {
        JobConfig d = JobConfig.defaults();
        return new JobConfig(d.kafkaBootstrap(), d.consumerGroup(), d.eventsTopic(), d.stationsTopic(), d.groupsTopic(),
                d.rulesTopic(), d.alertsTopic(), d.deadLetterTopic(), d.lateEventsTopic(),
                Duration.ofSeconds(2), d.maxOutOfOrderness(), d.sourceIdleness(), d.pendingCallTtl(),
                d.stationStateTtl(), d.sessionTrackTtl(), Duration.ofSeconds(5), 2,
                d.dbUrl(), d.dbUser(), d.dbPassword(), d.zeroEnergyWindows());
    }

    private static List<AlertEvent> run(List<String> envelopes, List<RuleChange> rules, int expected,
                                        Predicate<List<AlertEvent>> done, Duration timeout) throws Exception {
        return run(new ListSources(envelopes, rules, 300), expected, done, timeout);
    }

    private static CollectingSinks lastSinks;

    private static List<AlertEvent> run(ListSources sources, int expected, Predicate<List<AlertEvent>> done,
                                        Duration timeout) throws Exception {
        String id = UUID.randomUUID().toString();
        CollectingSinks sinks = new CollectingSinks(id);
        lastSinks = sinks;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(JobMain.baseConfig());
        JobConfig cfg = testConfig();
        env.setParallelism(2);
        TopologyBuilder.build(env, cfg, sources, sinks);
        JobClient client = env.executeAsync("e2e-" + id);
        try {
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                List<AlertEvent> got = sinks.alerts();
                if (got.size() >= expected && done.test(got)) {
                    return got;
                }
                Thread.sleep(100);
            }
            return sinks.alerts();
        } finally {
            try {
                client.cancel().get();
            } catch (Exception ignored) {
                // job may already be finished
            }
        }
    }

    private static String envelope(String station, OcppVersion v, Instant at, com.fasterxml.jackson.databind.JsonNode frame) {
        return Frames.envelopeJson(Frames.fromStation(station, v, at, frame));
    }

    @Test
    void faultedStatusOpensAlertAndAvailableResolvesIt() throws Exception {
        Instant t = Instant.now();
        List<String> envelopes = List.of(
                envelope("ST-1", OcppVersion.V16, t, Frames.call("1", "StatusNotification", Frames.status16(1, "Faulted", "GroundFailure", t))),
                envelope("ST-1", OcppVersion.V16, t, Frames.call("2", "StatusNotification", Frames.status16(1, "Available", "NoError", t))));
        List<RuleChange> rules = List.of(new RuleChange("faulted", RuleFixtures.faultedRule("faulted", "PT0S", "PT0S")));

        List<AlertEvent> alerts = run(envelopes, rules, 2, a -> true, Duration.ofSeconds(30));

        assertThat(alerts).extracting(AlertEvent::type).containsExactly(AlertEventType.OPENED, AlertEventType.RESOLVED);
        AlertEvent opened = alerts.get(0);
        assertThat(opened.ruleId()).isEqualTo("faulted");
        assertThat(opened.subjectId()).isEqualTo("ST-1");
        assertThat(opened.context()).containsEntry("event.errorCode", "GroundFailure");
        assertThat(opened.channels()).hasSize(1);
        assertThat(alerts.get(1).alertId()).isEqualTo(opened.alertId());
    }

    @Test
    void heartbeatAbsenceFiresAfterTimerAndGraceWindowIsHonoured() throws Exception {
        Instant t = Instant.now();
        List<String> envelopes = List.of(
                envelope("ST-2", OcppVersion.V201, t, Frames.call("1", "Heartbeat", Frames.heartbeat())));
        List<RuleChange> rules = List.of(new RuleChange("hb", """
            {"id":"hb","name":"No heartbeat","kind":"ABSENCE","spec":{"expectedAction":"Heartbeat","within":"PT1S"},
             "graceWindow":"PT1S","suppressionWindow":"PT0S","severity":"CRITICAL","channels":["pagerduty:svc"]}
            """));

        long start = System.currentTimeMillis();
        List<AlertEvent> alerts = run(envelopes, rules, 1, a -> true, Duration.ofSeconds(30));
        assertThat(alerts).hasSize(1);
        AlertEvent a = alerts.get(0);
        assertThat(a.type()).isEqualTo(AlertEventType.OPENED);
        assertThat(a.kind()).isEqualTo("ABSENCE");
        assertThat(a.context()).containsEntry("expectedAction", "Heartbeat");
        assertThat(Duration.between(a.triggeredAt(), a.openedAt())).isGreaterThanOrEqualTo(Duration.ofSeconds(1));
        assertThat(System.currentTimeMillis() - start).isGreaterThanOrEqualTo(300 + 1000 + 1000);
    }

    @Test
    void ruleScopedToGroupAppliesThroughHierarchyAndCarriesGroupIds() throws Exception {
        Instant t = Instant.now();
        var stations = List.of(
                new StationRecord("ST-EU", "n", "ACME", "X", "1", "2.0.1", Map.of(), Set.of("site:berlin"), false),
                new StationRecord("ST-US", "n", "ACME", "X", "1", "2.0.1", Map.of(), Set.of("site:austin"), false));
        var groups = List.of(
                new GroupRecord("country:de", null, "Germany", "country", Map.of(), false),
                new GroupRecord("region:eu", "country:de", "EU", "region", Map.of(), false),
                new GroupRecord("site:berlin", "region:eu", "Berlin", "site", Map.of(), false),
                new GroupRecord("site:austin", null, "Austin", "site", Map.of(), false));
        List<String> envelopes = List.of(
                envelope("ST-EU", OcppVersion.V201, t, Frames.call("1", "StatusNotification", Frames.status201(1, 1, "Faulted", t))),
                envelope("ST-US", OcppVersion.V201, t, Frames.call("2", "StatusNotification", Frames.status201(1, 1, "Faulted", t))));
        List<RuleChange> rules = List.of(new RuleChange("eu-fault", """
            {"id":"eu-fault","name":"EU faults","kind":"EVENT","targetGroupIds":["country:de"],
             "stationFilter":{"op":"eq","field":"station.vendor","value":"ACME"},
             "spec":{"trigger":{"actions":["StatusNotification"]},"condition":{"op":"eq","field":"event.status","value":"FAULTED"}},
             "severity":"MEDIUM"}
            """));
        List<AlertEvent> alerts = run(new ListSources(envelopes, rules, stations, groups, 1500), 1, a -> true,
                Duration.ofSeconds(20));
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).subjectId()).isEqualTo("ST-EU");
        assertThat(alerts.get(0).groupIds()).containsExactlyInAnyOrder("site:berlin", "region:eu", "country:de");
    }

    @Test
    void threeZeroEnergySessionsRaiseAggregateRule() throws Exception {
        Instant t = Instant.now();
        List<String> envelopes = new java.util.ArrayList<>();
        for (int i = 0; i < 3; i++) {
            String tx = "tx-" + i;
            envelopes.add(envelope("ST-Z", OcppVersion.V201, t, Frames.call("s" + i, "TransactionEvent",
                    Frames.txEvent201("Started", tx, 0, t.plusSeconds(i * 10), "Authorized", 1000L, 1))));
            envelopes.add(envelope("ST-Z", OcppVersion.V201, t, Frames.call("e" + i, "TransactionEvent",
                    Frames.txEvent201("Ended", tx, 1, t.plusSeconds(i * 10 + 5), "EVDisconnected", 1000L, 1))));
        }
        // one real session must not count
        envelopes.add(envelope("ST-Z", OcppVersion.V201, t, Frames.call("s9", "TransactionEvent",
                Frames.txEvent201("Started", "tx-9", 0, t, "Authorized", 1000L, 1))));
        envelopes.add(envelope("ST-Z", OcppVersion.V201, t, Frames.call("e9", "TransactionEvent",
                Frames.txEvent201("Ended", "tx-9", 1, t.plusSeconds(60), "EVDisconnected", 3500L, 1))));
        List<RuleChange> rules = List.of(new RuleChange("zero", RuleFixtures.zeroEnergyRule("zero", "daily", 3)));

        List<AlertEvent> alerts = run(envelopes, rules, 1, a -> true, Duration.ofSeconds(30));
        assertThat(alerts).as("snapshots seen: " + lastSinks.snapshots()).hasSize(1);
        assertThat(alerts.get(0).ruleId()).isEqualTo("zero");
        assertThat(alerts.get(0).context()).containsEntry("input", "zeroEnergy");
    }

    @Test
    void groupAggregateAndSequenceRulesFireOverStageOneAlerts() throws Exception {
        Instant t = Instant.now();
        var stations = List.of(
                new StationRecord("G1", "n", "ACME", "X", "1", "2.0.1", Map.of(), Set.of("site:a"), false),
                new StationRecord("G2", "n", "ACME", "X", "1", "2.0.1", Map.of(), Set.of("site:a"), false),
                new StationRecord("G3", "n", "ACME", "X", "1", "2.0.1", Map.of(), Set.of("site:a"), false));
        var groups = List.of(new GroupRecord("site:a", null, "A", "site", Map.of(), false));
        List<String> envelopes = List.of(
                envelope("G1", OcppVersion.V201, t, Frames.call("1", "StatusNotification", Frames.status201(1, 1, "Faulted", t))),
                envelope("G2", OcppVersion.V201, t, Frames.call("2", "StatusNotification", Frames.status201(1, 1, "Faulted", t))),
                envelope("G1", OcppVersion.V201, t, Frames.call("3", "SecurityEventNotification", Frames.securityEvent201("FirmwareMismatch", t))));
        List<RuleChange> rules = List.of(
                new RuleChange("faulted", RuleFixtures.faultedRule("faulted", "PT0S", "PT0S")),
                new RuleChange("sec", """
                    {"id":"sec","name":"Security event","kind":"EVENT",
                     "spec":{"trigger":{"actions":["SecurityEventNotification"]},"condition":{"op":"exists","field":"event.type"}},
                     "severity":"HIGH"}
                    """),
                new RuleChange("both", RuleFixtures.sequenceRule("both", "faulted", "sec", "PT15M")),
                new RuleChange("site-faults", """
                    {"id":"site-faults","name":"2 faults at site","kind":"GROUP_AGGREGATE","subjectType":"GROUP",
                     "targetGroupIds":["site:a"],
                     "spec":{"source":{"type":"alerts","ruleId":"faulted"},"window":"PT1H","threshold":{"count":2}},
                     "severity":"CRITICAL"}
                    """));
        List<AlertEvent> alerts = run(new ListSources(envelopes, rules, stations, groups, 1500), 5,
                a -> a.stream().anyMatch(x -> x.ruleId().equals("both")) && a.stream().anyMatch(x -> x.ruleId().equals("site-faults")),
                Duration.ofSeconds(30));
        assertThat(alerts).extracting(AlertEvent::ruleId).contains("faulted", "sec", "both", "site-faults");
        AlertEvent seq = alerts.stream().filter(a -> a.ruleId().equals("both")).findFirst().orElseThrow();
        assertThat(seq.subjectId()).isEqualTo("G1");
        assertThat(seq.kind()).isEqualTo("SEQUENCE");
        AlertEvent grp = alerts.stream().filter(a -> a.ruleId().equals("site-faults")).findFirst().orElseThrow();
        assertThat(grp.subjectType()).isEqualTo(com.chargemon.alert.SubjectType.GROUP);
        assertThat(grp.subjectId()).isEqualTo("site:a");
        assertThat(grp.context()).containsEntry("count", "2").containsEntry("members", "3");
    }

    @Test
    void stuckPreparingFiresAfterMaxDurationPerConnector() throws Exception {
        Instant t = Instant.now();
        List<String> envelopes = List.of(
                envelope("ST-P", OcppVersion.V16, t, Frames.call("1", "StatusNotification", Frames.status16(1, "Preparing", "NoError", t))),
                envelope("ST-P", OcppVersion.V16, t, Frames.call("2", "StatusNotification", Frames.status16(2, "Preparing", "NoError", t))),
                envelope("ST-P", OcppVersion.V16, t, Frames.call("3", "StatusNotification", Frames.status16(2, "Charging", "NoError", t))));
        List<RuleChange> rules = List.of(new RuleChange("stuck", RuleFixtures.stuckPreparingRule("stuck", "PT2S")));
        List<AlertEvent> alerts = run(envelopes, rules, 1, a -> true, Duration.ofSeconds(30));
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).ruleId()).isEqualTo("stuck");
        assertThat(alerts.get(0).context()).containsEntry("scope", "1");
    }

    @Test
    void bootRejectedViaCorrelationAndMalformedGoesToDeadLetter() throws Exception {
        Instant t = Instant.now();
        List<String> envelopes = List.of(
                "this is not json",
                envelope("ST-3", OcppVersion.V201, t, Frames.call("7", "BootNotification", Frames.boot201("ACME", "X", "1", "PowerUp"))),
                Frames.envelopeJson(Frames.fromCsms("ST-3", OcppVersion.V201, t, Frames.callResult("7", Frames.bootResponse("Rejected", 30, t)))));
        List<RuleChange> rules = List.of(new RuleChange("boot", """
            {"id":"boot","name":"Boot rejected","kind":"EVENT",
             "spec":{"trigger":{"actions":["BootCompleted"]},"condition":{"op":"eq","field":"event.status","value":"REJECTED"}},
             "severity":"HIGH"}
            """));
        List<AlertEvent> alerts = run(envelopes, rules, 1, a -> true, Duration.ofSeconds(30));
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).ruleId()).isEqualTo("boot");
        assertThat(alerts.get(0).context()).containsEntry("event.status", "REJECTED");
    }
}
