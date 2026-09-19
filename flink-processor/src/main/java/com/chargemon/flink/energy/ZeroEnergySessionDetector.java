package com.chargemon.flink.energy;

import com.chargemon.flink.model.EnrichedEvent;
import com.chargemon.flink.serde.JsonTypes;
import java.time.Duration;
import java.util.Optional;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Keyed by station; emits one {@link SessionEnergy} per completed session (all energies, filter downstream). */
public final class ZeroEnergySessionDetector extends KeyedProcessFunction<String, EnrichedEvent, SessionEnergy> {

    private static final Logger LOG = LoggerFactory.getLogger(ZeroEnergySessionDetector.class);

    private final Duration trackTtl;
    private transient SessionTracker tracker;
    private transient MapState<String, SessionTrack> tracks;
    private transient Counter sessionsEnded;
    private transient Counter zeroEnergy;

    public ZeroEnergySessionDetector(Duration trackTtl) {
        this.trackTtl = trackTtl;
    }

    @Override
    public void open(OpenContext ctx) {
        tracker = new SessionTracker();
        MapStateDescriptor<String, SessionTrack> desc =
                new MapStateDescriptor<>("sessionTracks", Types.STRING, JsonTypes.of(SessionTrack.class));
        desc.enableTimeToLive(StateTtlConfig.newBuilder(trackTtl)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupInRocksdbCompactFilter(1000)
                .build());
        tracks = getRuntimeContext().getMapState(desc);
        sessionsEnded = getRuntimeContext().getMetricGroup().counter("sessionsEnded");
        zeroEnergy = getRuntimeContext().getMetricGroup().counter("zeroEnergySessions");
    }

    @Override
    public void processElement(EnrichedEvent e, Context ctx, Collector<SessionEnergy> out) throws Exception {
        Optional<String> id = tracker.sessionIdOf(e.event());
        if (id.isEmpty()) {
            return;
        }
        SessionTrack current = tracks.get(id.get());
        SessionTracker.Step step = tracker.apply(e.event(), current, e.station().allGroupIds());
        LOG.debug("session station={} action={} id={} hadTrack={} ended={}", e.stationId(), e.event().action(), id.get(),
                current != null, step.ended().map(SessionEnergy::energyWh).orElse(null));
        if (!step.touches()) {
            return;
        }
        if (step.track() == null) {
            tracks.remove(step.sessionId());
        } else {
            tracks.put(step.sessionId(), step.track());
        }
        if (step.ended().isPresent()) {
            sessionsEnded.inc();
            SessionEnergy s = step.ended().get();
            if (s.isZeroEnergy()) {
                zeroEnergy.inc();
            }
            out.collect(s);
        }
    }
}
