package com.chargemon.flink.topology;

import com.chargemon.alert.AlertEvent;
import com.chargemon.alert.ConditionSignal;
import com.chargemon.flink.aggregate.SubjectFanOut;
import com.chargemon.flink.aggregate.SubjectKey;
import com.chargemon.flink.aggregate.SubjectSession;
import com.chargemon.flink.aggregate.ZeroEnergyAggregator;
import com.chargemon.flink.config.JobConfig;
import com.chargemon.flink.energy.SessionEnergy;
import com.chargemon.flink.energy.ZeroEnergySessionDetector;
import com.chargemon.flink.model.AggregateSnapshot;
import java.time.ZoneOffset;
import com.chargemon.flink.control.RuleBroadcast;
import com.chargemon.flink.control.RuleLoader;
import com.chargemon.flink.correlate.CallCorrelationOperator;
import com.chargemon.flink.decode.FrameDecodeFunction;
import com.chargemon.flink.enrich.StationEnrichmentOperator;
import com.chargemon.flink.lifecycle.AlertLifecycleOperator;
import com.chargemon.flink.model.AlertKey;
import com.chargemon.flink.model.DecodedFrame;
import com.chargemon.flink.model.EnrichedEvent;
import com.chargemon.flink.model.GroupMemberDelta;
import com.chargemon.flink.model.KafkaRecord;
import com.chargemon.flink.model.RuleChange;
import com.chargemon.flink.model.RuleInput;
import com.chargemon.flink.model.StationStreamElement;
import com.chargemon.flink.rules.StationRuleEvaluatorOperator;
import com.chargemon.flink.serde.JsonTypes;
import com.chargemon.flink.sink.Sinks;
import com.chargemon.flink.stage2.GroupAggregateOperator;
import com.chargemon.flink.stage2.GroupInput;
import com.chargemon.flink.stage2.StationSequenceOperator;
import com.chargemon.alert.SubjectType;
import com.chargemon.flink.source.Sources;
import com.chargemon.ocpp.model.OcppEvent;
import com.chargemon.ocpp.model.station.GroupRecord;
import com.chargemon.ocpp.model.station.StationRecord;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Wires the operator DAG. Sources and sinks are injected so the same topology
 * runs against Kafka in production and against collections in tests.
 */
public final class TopologyBuilder {

    private TopologyBuilder() {
    }

    /** Streams other builders (zero-energy, stage 2) hang off. */
    public record Streams(DataStream<EnrichedEvent> enriched, DataStream<AlertEvent> stage1Alerts,
                          DataStream<GroupMemberDelta> memberDeltas, DataStream<AggregateSnapshot> groupSnapshots,
                          BroadcastStream<RuleChange> rules) {
    }

    public static Streams build(StreamExecutionEnvironment env, JobConfig cfg, Sources sources, Sinks sinks) {
        // ---- control plane ----
        DataStream<RuleChange> rules = sources.rules(env);
        BroadcastStream<RuleChange> rulesBroadcast = rules.broadcast(RuleBroadcast.DESCRIPTOR);
        RuleLoader loader = sources.ruleLoader();
        DataStream<StationRecord> stations = sources.stations(env);
        DataStream<GroupRecord> groups = sources.groups(env);
        sinks.stationMirror(stations);
        sinks.groupMirror(groups);

        // ---- decode + correlate ----
        // Decode inherits the source parallelism so it chains (forward partitioning): per-partition order
        // survives until the keyBy, which is what keeps CALL before CALLRESULT for a station.
        DataStream<KafkaRecord> raw = sources.events(env);
        SingleOutputStreamOperator<DecodedFrame> frames = raw
                .process(new FrameDecodeFunction())
                .setParallelism(raw.getParallelism())
                .uid("decode").name("decode");
        sinks.deadLetters(frames.getSideOutput(FrameDecodeFunction.DEAD_LETTER));

        DataStream<OcppEvent> events = frames
                .keyBy(f -> f.envelope().stationId())
                .process(new CallCorrelationOperator(cfg.correlationTimeout(), cfg.pendingCallTtl()))
                .returns(JsonTypes.of(OcppEvent.class))
                .uid("correlate").name("correlate");

        // ---- enrich with station + group hierarchy ----
        DataStream<StationStreamElement> stationInputs = events
                .map(StationStreamElement::of).returns(JsonTypes.of(StationStreamElement.class))
                .union(stations.map(StationStreamElement::of).returns(JsonTypes.of(StationStreamElement.class)));
        SingleOutputStreamOperator<EnrichedEvent> enriched = stationInputs
                .keyBy(StationStreamElement::stationId)
                .connect(groups.broadcast(StationEnrichmentOperator.GROUPS))
                .process(new StationEnrichmentOperator())
                .returns(JsonTypes.of(EnrichedEvent.class))
                .uid("enrich").name("enrich");
        DataStream<GroupMemberDelta> memberDeltas = enriched.getSideOutput(StationEnrichmentOperator.MEMBER_DELTAS);

        // ---- zero-energy sessions -> hourly-bucket aggregates per station and group ----
        DataStream<SessionEnergy> sessions = enriched
                .keyBy(EnrichedEvent::stationId)
                .process(new ZeroEnergySessionDetector(cfg.sessionTrackTtl()))
                .uid("sessions").name("sessions");
        SingleOutputStreamOperator<AggregateSnapshot> snapshots = sessions
                .flatMap(new SubjectFanOut())
                .keyBy(SubjectSession::subject, Types.POJO(SubjectKey.class))
                .process(new ZeroEnergyAggregator(cfg.zeroEnergyWindows(), ZoneOffset.UTC))
                .uid("zero-energy-agg").name("zero-energy aggregate");
        sinks.aggregates(snapshots);
        sinks.lateSessions(snapshots.getSideOutput(ZeroEnergyAggregator.LATE));
        DataStream<AggregateSnapshot> stationSnapshots = snapshots.filter(s -> s.subjectType() == com.chargemon.alert.SubjectType.STATION);
        DataStream<AggregateSnapshot> groupSnapshots = snapshots.filter(s -> s.subjectType() == com.chargemon.alert.SubjectType.GROUP);

        // ---- stage 1 rules + lifecycle ----
        DataStream<ConditionSignal> signals = enriched
                .map(RuleInput::of)
                .returns(JsonTypes.of(RuleInput.class))
                .union(stationSnapshots.map(RuleInput::of).returns(JsonTypes.of(RuleInput.class)))
                .keyBy(RuleInput::stationId)
                .connect(rulesBroadcast)
                .process(new StationRuleEvaluatorOperator(cfg.stationStateTtl(), loader))
                .returns(JsonTypes.of(ConditionSignal.class))
                .uid("rules-stage1").name("rules-stage1");

        DataStream<AlertEvent> alerts = signals
                .keyBy(s -> AlertKey.of(s.ruleId(), s.subject()), Types.POJO(AlertKey.class))
                .connect(rulesBroadcast)
                .process(new AlertLifecycleOperator(loader))
                .returns(JsonTypes.of(AlertEvent.class))
                .uid("lifecycle-stage1").name("lifecycle-stage1");

        // ---- stage 2: composite (per station) and group-aggregate rules over stage-1 alerts ----
        DataStream<ConditionSignal> sequenceSignals = alerts
                .filter(a -> a.subjectType() == SubjectType.STATION)
                .keyBy(AlertEvent::subjectId)
                .connect(rulesBroadcast)
                .process(new StationSequenceOperator(cfg.stationStateTtl(), loader))
                .returns(JsonTypes.of(ConditionSignal.class))
                .uid("rules-sequence").name("rules-sequence");

        DataStream<GroupInput> groupInputs = alerts
                .flatMap((AlertEvent a, org.apache.flink.util.Collector<GroupInput> out) -> {
                    if (a.subjectType() == SubjectType.STATION) {
                        a.groupIds().forEach(g -> out.collect(GroupInput.alert(g, a)));
                    }
                })
                .returns(JsonTypes.of(GroupInput.class))
                .union(memberDeltas.map(GroupInput::delta).returns(JsonTypes.of(GroupInput.class)))
                .union(groupSnapshots.map(GroupInput::snapshot).returns(JsonTypes.of(GroupInput.class)));
        DataStream<ConditionSignal> groupSignals = groupInputs
                .keyBy(GroupInput::groupId)
                .connect(rulesBroadcast)
                .process(new GroupAggregateOperator(cfg.stationStateTtl(), loader))
                .returns(JsonTypes.of(ConditionSignal.class))
                .uid("rules-group").name("rules-group");

        DataStream<AlertEvent> stage2Alerts = sequenceSignals.union(groupSignals)
                .keyBy(s -> AlertKey.of(s.ruleId(), s.subject()), Types.POJO(AlertKey.class))
                .connect(rulesBroadcast)
                .process(new AlertLifecycleOperator(loader))
                .returns(JsonTypes.of(AlertEvent.class))
                .uid("lifecycle-stage2").name("lifecycle-stage2");

        sinks.alerts(alerts.union(stage2Alerts));
        return new Streams(enriched, alerts, memberDeltas, groupSnapshots, rulesBroadcast);
    }
}
